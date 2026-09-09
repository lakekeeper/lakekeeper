use std::sync::Arc;

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::RenameTableRequest;

use crate::{
    WarehouseId,
    api::{ApiContext, endpoints::EndpointFlat, iceberg::types::Prefix},
    request_metadata::RequestMetadata,
    server::{
        require_warehouse_id,
        tables::validate_table_or_view_ident,
        tabular::{
            claim_rename_idempotency_key, commit_rename_with_reparent,
            ensure_authorized_destination,
        },
    },
    service::{
        AuthZViewInfo as _, CachePolicy, CatalogIdempotencyOps, CatalogNamespaceOps, CatalogStore,
        CatalogTabularOps, CatalogWarehouseOps, NamespaceHierarchy, ResolvedWarehouse, Result,
        SecretStore, State, TabularId, TabularListFlags, Transaction, ViewInfo,
        authz::{
            AuthZCannotSeeView, AuthZError, AuthZViewOps, Authorizer, AuthzNamespaceOps,
            AuthzWarehouseOps, CatalogNamespaceAction, CatalogViewAction, RequireViewActionError,
            refresh_warehouse_and_namespace_if_needed,
        },
        contract_verification::ContractVerification,
        events::{APIEventContext, context::ResolvedView},
    },
};

pub async fn rename_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: RenameTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let source = &request.source;
    let destination = &request.destination;
    validate_table_or_view_ident(source)?;
    validate_table_or_view_ident(destination)?;
    let source = source.clone();
    let destination = destination.clone();

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
        source.clone(),
        CatalogViewAction::Rename,
    );

    let authz_result = authorize_rename_view::<C, _>(
        event_ctx.request_metadata(),
        warehouse_id,
        &source,
        &destination,
        &authorizer,
        state.v1_state.catalog.clone(),
    )
    .await;
    let (
        event_ctx,
        AuthorizeRenameViewResult {
            warehouse,
            source_view_info,
            destination_namespace,
        },
    ) = event_ctx.emit_authz(authz_result)?;

    let source_id = source_view_info.view_id();
    let source_namespace_id = source_view_info.namespace_id();
    let destination_namespace_id = destination_namespace.namespace_id();
    let event_ctx = event_ctx.resolve(ResolvedView {
        warehouse: warehouse.clone(),
        view: Arc::new(source_view_info),
    });

    // ------------------- BUSINESS LOGIC -------------------
    if source == destination {
        return Ok(());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let renamed = C::rename_tabular(
        warehouse_id,
        source_id,
        source_namespace_id,
        destination_namespace_id,
        &source,
        &destination,
        t.transaction(),
    )
    .await?;
    // The statement pins the destination to the id passed above, so this holds by
    // construction. Kept as the invariant it asserts: nothing may land the tabular in a
    // namespace the request was not authorized against.
    ensure_authorized_destination(destination_namespace_id, renamed.namespace_id())?;
    state
        .v1_state
        .contract_verifiers
        .check_rename(TabularId::View(source_id), &destination)
        .await?
        .into_result()?;

    // Claims the key in the same transaction as the rename, so a committed key
    // always implies a committed rename.
    let t = claim_rename_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::CatalogV1RenameView,
    )
    .await?;
    // ------------------- AUTHZ HIERARCHY -------------------
    // Consumes the transaction: a rename across namespaces has to move the tabular's
    // parent edge, and the ordering around the commit is what keeps that fail-closed.
    commit_rename_with_reparent::<C, A>(
        t,
        &authorizer,
        event_ctx.request_metadata(),
        warehouse_id,
        TabularId::View(source_id),
        source_namespace_id,
        destination_namespace_id,
    )
    .await?;

    event_ctx.emit_view_renamed_async(destination_namespace.namespace, Arc::new(request));

    Ok(())
}

struct AuthorizeRenameViewResult {
    warehouse: Arc<ResolvedWarehouse>,
    source_view_info: ViewInfo,
    destination_namespace: NamespaceHierarchy,
}

async fn authorize_rename_view<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    source: &TableIdent,
    destination: &TableIdent,
    authorizer: &A,
    state: C::State,
) -> Result<AuthorizeRenameViewResult, AuthZError> {
    let (warehouse, destination_namespace, source_namespace, source_view_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, state.clone(),),
        // The destination is read uncached: it is the one resolution here with no version
        // anchor to detect staleness against, and a stale `ident -> id` entry is not
        // invalidated across replicas, so it would outlive the request, fail every retry,
        // and have authorization evaluated against a namespace that is not the destination.
        // `rename_tabular` pins the destination by id regardless.
        C::get_namespace_cache_aware(
            warehouse_id,
            &destination.namespace,
            CachePolicy::Skip,
            state.clone(),
        ),
        C::get_namespace(warehouse_id, &source.namespace, state.clone(),),
        C::get_view_info(
            warehouse_id,
            source.clone(),
            TabularListFlags::active(),
            state.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let source_namespace = authorizer.require_namespace_presence(
        warehouse_id,
        source.namespace.clone(),
        source_namespace,
    )?;
    let source_view_info =
        authorizer.require_view_presence(warehouse_id, source.clone(), source_view_info)?;

    let (warehouse, source_namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        source_namespace,
        &source_view_info,
        AuthZCannotSeeView::new_not_found(warehouse_id, source.clone()),
        authorizer,
        state.clone(),
    )
    .await?;

    let (destination_namespace, source_view_info) = tokio::join!(
        // Check 1)
        authorizer.require_namespace_action(
            request_metadata,
            &warehouse,
            &destination.namespace,
            destination_namespace,
            CatalogNamespaceAction::CreateView {
                name: Some(destination.name.clone()),
                properties: Arc::new(source_view_info.properties().clone().into_iter().collect()),
            },
        ),
        // Check 2)
        authorizer.require_view_action(
            request_metadata,
            &warehouse,
            &source_namespace,
            source.clone(),
            Ok::<_, RequireViewActionError>(Some(source_view_info)),
            CatalogViewAction::Rename,
        )
    );
    let source_view_info = source_view_info?;
    let destination_namespace = destination_namespace?;

    Ok(AuthorizeRenameViewResult {
        warehouse,
        source_view_info,
        destination_namespace,
    })
}
