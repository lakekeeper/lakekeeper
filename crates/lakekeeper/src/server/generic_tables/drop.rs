use std::sync::Arc;

use crate::{
    api::{
        ApiContext,
        data::v1::generic_tables::GenericTableParameters,
        endpoints::EndpointFlat,
        iceberg::types::DropParams,
        management::v1::{DeleteKind, warehouse::TabularDeleteProfile},
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tabular::claim_no_content_idempotency_key},
    service::{
        CatalogIdempotencyOps, CatalogStore, CatalogTabularOps, NamedEntity, Result, SecretStore,
        State, TabularId, Transaction,
        authz::{Authorizer, CatalogGenericTableAction, require_soft_deletion_skip_for_drop},
        events::{APIEventContext, context::ResolvedGenericTable},
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

    // ------------------- AUDIT CONTEXT -------------------
    // Built before the idempotency check so a served replay can be audited.
    let idempotency_key = request_metadata.idempotency_key().copied();
    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());
    let event_ctx = APIEventContext::for_generic_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        table_ident.clone(),
        CatalogGenericTableAction::Drop {
            force,
            purge: purge_requested,
        },
    );

    // ------------------- IDEMPOTENCY CHECK -------------------
    if let Some(ref key) = idempotency_key {
        let check = C::check_idempotency_key(
            warehouse_id,
            key,
            EndpointFlat::GenericTableV1DropGenericTable,
            state.v1_state.catalog.clone(),
        )
        .await?;
        if check.is_replay() {
            event_ctx.emit_idempotent_replay(*key);
            return Ok(());
        }
    }

    // ------------------- AUTHZ -------------------
    let authz_result = super::load_and_authorize_generic_table_operation::<C, A>(
        authorizer,
        &request_metadata,
        warehouse_id,
        namespace.clone(),
        &table_name,
        event_ctx.action().clone(),
        state.v1_state.catalog.clone(),
    )
    .await;
    let authz_result =
        require_soft_deletion_skip_for_drop(authorizer, &request_metadata, force, authz_result)
            .await;
    let (event_ctx, (warehouse, _ns_hierarchy, info)) = event_ctx.emit_authz(authz_result)?;
    let generic_table_id = info.generic_table_id;

    let event_ctx = event_ctx.resolve(ResolvedGenericTable {
        warehouse: warehouse.clone(),
        generic_table: Arc::new(info),
        storage_permissions: None,
    });

    // ------------------- DROP -------------------
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;

    let delete_profile = warehouse.tabular_delete_profile.for_drop(force);
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
                        resource_demand: None,
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
                    resource_demand: None,
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

    // Claim the idempotency key and commit, for both delete profiles.
    let t = claim_no_content_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::GenericTableV1DropGenericTable,
    )
    .await?;
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
