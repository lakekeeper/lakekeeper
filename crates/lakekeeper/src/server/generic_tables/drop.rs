use http::StatusCode;

use crate::{
    api::{
        ApiContext, ErrorModel, endpoints::EndpointFlat, v1::generic_tables::GenericTableParameters,
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogIdempotencyOps, CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, Result,
        SecretStore, State, Transaction, authz::Authorizer, idempotency::IdempotencyInfo,
    },
};

pub(super) async fn drop_generic_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: GenericTableParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let GenericTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
        if check.is_replay() {
            return Ok(());
        }
    }

    let _warehouse = C::get_active_warehouse_by_id(warehouse_id, state.v1_state.catalog.clone())
        .await?
        .ok_or_else(|| {
            ErrorModel::not_found("Warehouse not found".to_string(), "WarehouseNotFound", None)
        })?;

    let ns = C::get_namespace(
        warehouse_id,
        namespace.clone(),
        state.v1_state.catalog.clone(),
    )
    .await?
    .ok_or_else(|| {
        ErrorModel::not_found("Namespace not found".to_string(), "NamespaceNotFound", None)
    })?;

    let namespace_id = ns.namespace.namespace.namespace_id;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let generic_table_id =
        C::drop_generic_table_impl(warehouse_id, namespace_id, &table_name, t.transaction())
            .await?;

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

    state
        .v1_state
        .authz
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

    Ok(())
}
