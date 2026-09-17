use std::sync::Arc;

use http::StatusCode;
use iceberg::TableIdent;

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::DatasetParameters,
        endpoints::EndpointFlat,
        iceberg::types::DropParams,
        management::v1::{DeleteKind, warehouse::TabularDeleteProfile},
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogDatasetOps, CatalogIdempotencyOps, CatalogStore, CatalogTabularOps, CatalogTaskOps,
        DatasetOwnership, NamedEntity, Result, SecretStore, State, TabularId, TabularListFlags,
        Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{
            APIEventContext,
            context::{ResolvedDataset, UserProvidedDataset},
        },
        idempotency::IdempotencyInfo,
        tasks::{
            CancelTasksFilter, ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
        },
    },
};

#[allow(clippy::too_many_lines)]
pub(super) async fn drop_dataset<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetParameters,
    DropParams {
        purge_requested,
        force,
    }: DropParams,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = &state.v1_state.authz;

    // ------------------- AUDIT CONTEXT -------------------
    // Built before the idempotency check so a served replay can be audited.
    let idempotency_key = request_metadata.idempotency_key().copied();
    let action = CatalogDatasetAction::Drop {
        force,
        purge: purge_requested,
    };
    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        action.clone(),
    );

    // ------------------- IDEMPOTENCY CHECK -------------------
    if let Some(ref key) = idempotency_key {
        let check = C::check_idempotency_key(
            warehouse_id,
            key,
            EndpointFlat::DatasetV1DropDataset,
            state.v1_state.catalog.clone(),
        )
        .await?;
        if check.is_replay() {
            event_ctx.emit_idempotent_replay(*key);
            return Ok(());
        }
    }

    // ------------------- AUTHZ -------------------

    let (event_ctx, (warehouse, _ns_hierarchy, info)) = event_ctx.emit_authz(
        authorizer
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;
    let dataset_id = info.tabular_id;

    // Ownership is dataset-specific and not carried by the tabular info authz
    // returns. By id: a concurrent rename plus create-with-the-same-name would
    // otherwise drop a different dataset than the one authorized.
    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let dataset_info =
        C::load_dataset_by_id(warehouse_id, info.tabular_id, t.transaction()).await?;
    t.commit().await?;
    let ownership = dataset_info.ownership;

    // An imported dataset borrows its prefix. Purging would delete objects
    // Lakekeeper does not own, so it is refused here: this is the only place that
    // still knows the caller asked for it.
    if purge_requested && ownership == DatasetOwnership::Imported {
        return Err(ErrorModel::bad_request(
            "Cannot purge an imported dataset: its files are not owned by Lakekeeper. \
             Drop without purge to remove the catalog metadata only.",
            "CannotPurgeImportedDataset",
            None,
        )
        .into());
    }

    let event_ctx = event_ctx.resolve(ResolvedDataset {
        warehouse: warehouse.clone(),
        dataset: Arc::new(dataset_info),
    });

    // ------------------- DROP -------------------
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;

    let delete_profile = if force {
        TabularDeleteProfile::Hard {}
    } else {
        warehouse.tabular_delete_profile
    };
    let project_id = &warehouse.project_id;

    match delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_tabular(
                warehouse_id,
                TabularId::Dataset(dataset_id),
                force,
                t.transaction(),
            )
            .await?;
            // As a namespace drop does: what is queued for the dataset has nothing
            // left to act on, and would hold up the warehouse's deletion. Listed once
            // the drop holds the dataset's rows, so a running task's follow-up, queued
            // as it finishes, is among them.
            let tasks = C::list_dataset_task_ids(warehouse_id, dataset_id, t.transaction())
                .await
                .map_err(ErrorModel::from)?;
            C::cancel_scheduled_tasks(
                None,
                &[],
                CancelTasksFilter::TaskIds(tasks),
                false,
                t.transaction(),
            )
            .await?;

            if purge_requested {
                TabularPurgeTask::schedule_task::<C>(
                    ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        scheduled_for: None,
                        entity: TaskEntity::EntityInWarehouse {
                            entity_name: dataset_ident.clone().into_name_parts(),
                            warehouse_id,
                            entity_id: WarehouseTaskEntityId::Dataset { dataset_id },
                        },
                    },
                    TabularPurgePayload {
                        tabular_location: location.to_string(),
                    },
                    t.transaction(),
                )
                .await?;

                tracing::debug!("Queued purge task for dropped dataset '{dataset_id}'.");
            }
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            let _ = TabularExpirationTask::schedule_task::<C>(
                ScheduleTaskMetadata {
                    project_id: project_id.clone(),
                    parent_task_id: None,
                    scheduled_for: Some(chrono::Utc::now() + expiration_seconds),
                    entity: TaskEntity::EntityInWarehouse {
                        entity_name: dataset_ident.clone().into_name_parts(),
                        entity_id: WarehouseTaskEntityId::Dataset { dataset_id },
                        warehouse_id,
                    },
                },
                TabularExpirationPayload {
                    deletion_kind: if purge_requested {
                        DeleteKind::Purge
                    } else {
                        DeleteKind::Default
                    },
                },
                t.transaction(),
            )
            .await?;

            C::mark_tabular_as_deleted(
                warehouse_id,
                TabularId::Dataset(dataset_id),
                force,
                t.transaction(),
            )
            .await?;

            tracing::debug!("Queued expiration task for dropped dataset '{dataset_id}'.");
        }
    }

    // Insert idempotency key in the same transaction.
    if let Some(ref key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::DatasetV1DropDataset)
                .http_status(StatusCode::NO_CONTENT)
                .build(),
            t.transaction(),
        )
        .await?
    {
        t.rollback()
            .await
            .inspect_err(|e| {
                tracing::warn!("Rollback failed after idempotency conflict: {e}");
            })
            .ok();
        return Err(ErrorModel::request_in_progress().into());
    }

    t.commit().await?;

    // Post-commit: best-effort authz cleanup for hard deletes
    if matches!(delete_profile, TabularDeleteProfile::Hard {}) {
        authorizer
            .delete_dataset(warehouse_id, dataset_id)
            .await
            .inspect_err(|e| {
                tracing::error!(?e, "Failed to delete dataset from authorizer: {}", e.error);
            })
            .ok();
    }

    event_ctx.emit_dataset_dropped_async(DropParams {
        purge_requested,
        force,
    });

    Ok(())
}
