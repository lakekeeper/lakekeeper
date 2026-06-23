use std::sync::Arc;

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::{
        ApiContext,
        endpoints::EndpointFlat,
        iceberg::{types::DropParams, v1::ViewParameters},
        management::v1::{DeleteKind, warehouse::TabularDeleteProfile},
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        AuthZViewInfo as _, CatalogIdempotencyOps, CatalogStore, CatalogTabularOps, NamedEntity,
        Result, SecretStore, State, TabularId, TabularListFlags, Transaction,
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
        contract_verification::ContractVerification,
        events::{APIEventContext, context::ResolvedView},
        idempotency::IdempotencyInfo,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
        },
    },
};

#[allow(clippy::too_many_lines)]
pub async fn drop_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    DropParams {
        purge_requested,
        force,
    }: DropParams,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_table_or_view_ident(view)?;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
        if check.is_replay() {
            return Ok(());
        }
    }

    // ------------------- AUTHZ + BUSINESS LOGIC -------------------
    let authorizer = state.v1_state.authz;

    let event_ctx = APIEventContext::for_view(
        Arc::new(request_metadata),
        state.v1_state.events,
        warehouse_id,
        view.clone(),
        CatalogViewAction::Drop {
            force,
            purge: purge_requested,
        },
    );

    let authz_context = authorizer
        .load_and_authorize_view_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            TabularListFlags::active(),
            event_ctx.action().clone(),
            state.v1_state.catalog.clone(),
        )
        .await;

    let (event_ctx, (warehouse, _namespace, view_info)) = event_ctx.emit_authz(authz_context)?;

    let view_id = view_info.view_id();
    let event_ctx = event_ctx.resolve(ResolvedView {
        warehouse: warehouse.clone(),
        view: Arc::new(view_info),
    });

    // ------------------- BUSINESS LOGIC -------------------
    state
        .v1_state
        .contract_verifiers
        .check_drop(TabularId::View(view_id))
        .await?
        .into_result()?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;

    let delete_profile = if force {
        TabularDeleteProfile::Hard {}
    } else {
        warehouse.tabular_delete_profile
    };
    let project_id = &warehouse.project_id;

    match delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_tabular(warehouse_id, view_id, force, t.transaction()).await?;

            if purge_requested {
                TabularPurgeTask::schedule_task::<C>(
                    ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        scheduled_for: None,
                        entity: TaskEntity::EntityInWarehouse {
                            warehouse_id,
                            entity_id: WarehouseTaskEntityId::View { view_id },
                            entity_name: view.clone().into_name_parts(),
                        },
                    },
                    TabularPurgePayload {
                        tabular_location: location.to_string(),
                    },
                    t.transaction(),
                )
                .await?;
                tracing::debug!(
                    "Queued purge task for dropped view '{}' in warehouse {warehouse_id}.",
                    event_ctx.resolved().view.view_ident()
                );
            }
            // authorizer cleanup happens after commit (below)
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            let _ = TabularExpirationTask::schedule_task::<C>(
                ScheduleTaskMetadata {
                    project_id: project_id.clone(),
                    parent_task_id: None,
                    scheduled_for: Some(chrono::Utc::now() + expiration_seconds),
                    entity: TaskEntity::EntityInWarehouse {
                        warehouse_id,
                        entity_id: WarehouseTaskEntityId::View { view_id },
                        entity_name: view.clone().into_name_parts(),
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
                TabularId::View(view_id),
                force,
                t.transaction(),
            )
            .await?;

            tracing::debug!(
                "Queued expiration task for dropped view '{}' with id '{view_id}' in warehouse {warehouse_id}.",
                event_ctx.resolved().view.view_ident()
            );
        }
    }

    // Insert idempotency key and commit — shared across both delete profiles.
    if let Some(ref key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::CatalogV1DropView)
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
            .delete_view(warehouse_id, view_id)
            .await
            .inspect_err(|e| {
                tracing::error!(?e, "Failed to delete view from authorizer: {}", e.error);
            })
            .ok();
    }

    event_ctx.emit_view_dropped_async(DropParams {
        purge_requested,
        force,
    });

    Ok(())
}
