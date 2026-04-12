use std::sync::Arc;

use http::StatusCode;

use crate::{
    api::{
        ApiContext, ErrorModel, endpoints::EndpointFlat, v1::generic_tables::GenericTableParameters,
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogGenericTableOps, CatalogIdempotencyOps, CatalogStore, Result, SecretStore, State,
        Transaction,
        authz::{Authorizer, CatalogGenericTableAction},
        events::{APIEventContext, context::ResolvedNamespace},
        idempotency::IdempotencyInfo,
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
    let event_ctx = APIEventContext::for_namespace(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        namespace.clone(),
        CatalogGenericTableAction::Drop,
    );

    let (event_ctx, (warehouse, ns_hierarchy, _info)) = event_ctx.emit_authz(
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

    let _event_ctx = event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: ns_hierarchy.namespace.clone(),
    });

    let namespace_id = ns_hierarchy.namespace.namespace_id();

    // ------------------- DROP -------------------
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let generic_table_id =
        C::drop_generic_table(warehouse_id, namespace_id, &table_name, t.transaction()).await?;

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

    // Post-commit: clean up authz state (best-effort)
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

    Ok(())
}
