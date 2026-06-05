use std::sync::Arc;

use http::StatusCode;

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::generic_tables::GenericTableParameters,
        endpoints::EndpointFlat,
        iceberg::types::DropParams,
        management::v1::{DeleteKind, warehouse::TabularDeleteProfile},
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogIdempotencyOps, CatalogStore, CatalogTabularOps, NamedEntity, Result, SecretStore,
        State, TabularId, Transaction,
        authz::{Authorizer, CatalogGenericTableAction},
        events::{APIEventContext, context::ResolvedGenericTable},
        idempotency::IdempotencyInfo,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
        },
    },
};

#[allow(clippy::too_many_lines)]
pub(super) async fn drop_generic_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: GenericTableParameters,
    DropParams {
        purge_requested,
        force,
    }: DropParams,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let GenericTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = &state.v1_state.authz;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
        if check.is_replay() {
            return Ok(());
        }
    }

    // ------------------- AUTHZ -------------------
    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());
    let event_ctx = APIEventContext::for_generic_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        table_ident.clone(),
        CatalogGenericTableAction::Drop,
    );

    let (event_ctx, (warehouse, _ns_hierarchy, info)) = event_ctx.emit_authz(
        super::load_and_authorize_generic_table_operation::<C, A>(
            authorizer,
            &request_metadata,
            warehouse_id,
            namespace.clone(),
            &table_name,
            CatalogGenericTableAction::Drop,
            state.v1_state.catalog.clone(),
        )
        .await,
    )?;
    let generic_table_id = info.generic_table_id;

    let event_ctx = event_ctx.resolve(ResolvedGenericTable {
        warehouse: warehouse.clone(),
        generic_table: Arc::new(info),
        storage_permissions: None,
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
                TabularId::GenericTable(generic_table_id),
                force,
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
                            entity_name: table_ident.clone().into_name_parts(),
                            warehouse_id,
                            entity_id: WarehouseTaskEntityId::GenericTable { generic_table_id },
                        },
                    },
                    TabularPurgePayload {
                        tabular_location: location.to_string(),
                    },
                    t.transaction(),
                )
                .await?;

                tracing::debug!(
                    "Queued purge task for dropped generic table '{generic_table_id}'."
                );
            }
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            let _ = TabularExpirationTask::schedule_task::<C>(
                ScheduleTaskMetadata {
                    project_id: project_id.clone(),
                    parent_task_id: None,
                    scheduled_for: Some(chrono::Utc::now() + expiration_seconds),
                    entity: TaskEntity::EntityInWarehouse {
                        entity_name: table_ident.clone().into_name_parts(),
                        entity_id: WarehouseTaskEntityId::GenericTable { generic_table_id },
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
                TabularId::GenericTable(generic_table_id),
                force,
                t.transaction(),
            )
            .await?;

            tracing::debug!(
                "Queued expiration task for dropped generic table '{generic_table_id}'."
            );
        }
    }

    // Insert idempotency key in the same transaction.
    if let Some(ref key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::GenericTableV1DropGenericTable)
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
            .delete_generic_table(warehouse_id, generic_table_id)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    ?e,
                    "Failed to delete generic table from authorizer: {}",
                    e.error
                );
            })
            .ok();
    }

    event_ctx.emit_generic_table_dropped_async(DropParams {
        purge_requested,
        force,
    });

    Ok(())
}
