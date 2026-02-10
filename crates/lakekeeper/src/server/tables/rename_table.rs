use std::sync::Arc;

use crate::{
    WarehouseId,
    api::iceberg::v1::{ApiContext, Prefix, RenameTableRequest, Result, TableIdent},
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        AuthZTableInfo as _, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        CatalogWarehouseOps, NamespaceHierarchy, ResolvedWarehouse, State, TableInfo,
        TabularListFlags, Transaction,
        authz::{
            AuthZCannotSeeTable, AuthZTableOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogNamespaceAction, CatalogTableAction, FetchWarehouseNamespaceTabularError,
            RequireTableActionError, refresh_warehouse_and_namespace_if_needed,
        },
        contract_verification::ContractVerification,
        events::{APIEventContext, context::ResolvedTable},
        secrets::SecretStore,
    },
};

/// Rename a table
pub(super) async fn rename_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: RenameTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let RenameTableRequest {
        source,
        destination,
    } = &request;
    validate_table_or_view_ident(source)?;
    validate_table_or_view_ident(destination)?;

    // ------------------- AUTHZ -------------------
    // Authorization is required for:
    // 1) creating a table in the destination namespace
    // 2) renaming the old table
    let authorizer = state.v1_state.authz;

    let event_ctx = APIEventContext::for_table(
        Arc::new(request_metadata),
        state.v1_state.events,
        warehouse_id,
        source.clone(),
        CatalogTableAction::Rename,
    );

    let authz_result = authorize_rename_table::<C, A>(
        event_ctx.request_metadata(),
        warehouse_id,
        source,
        destination,
        &authorizer,
        state.v1_state.catalog.clone(),
    )
    .await;

    let (event_ctx, (warehouse, destination_namespace, source_table_info)) =
        event_ctx.emit_authz(authz_result)?;

    let source_table_id = source_table_info.table_id();
    let event_ctx = event_ctx.resolve(ResolvedTable {
        warehouse: warehouse.clone(),
        table: Arc::new(source_table_info),
        storage_permissions: None,
    });

    // ------------------- BUSINESS LOGIC -------------------
    if source == destination {
        return Ok(());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::rename_tabular(
        warehouse_id,
        source_table_id,
        source,
        destination,
        t.transaction(),
    )
    .await?;

    state
        .v1_state
        .contract_verifiers
        .check_rename(source_table_id.into(), destination)
        .await?
        .into_result()?;

    t.commit().await?;

    event_ctx.emit_table_renamed_async(destination_namespace.namespace, Arc::new(request));

    Ok(())
}

async fn authorize_rename_table<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    source: &TableIdent,
    destination: &TableIdent,
    authorizer: &A,
    catalog_state: C::State,
) -> std::result::Result<
    (Arc<ResolvedWarehouse>, NamespaceHierarchy, TableInfo),
    FetchWarehouseNamespaceTabularError,
> {
    let (warehouse, destination_namespace, source_namespace, source_table_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_namespace(warehouse_id, &destination.namespace, catalog_state.clone(),),
        C::get_namespace(warehouse_id, &source.namespace, catalog_state.clone(),),
        C::get_table_info(
            warehouse_id,
            source.clone(),
            TabularListFlags::active(),
            catalog_state.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let source_namespace = authorizer.require_namespace_presence(
        warehouse_id,
        source.namespace.clone(),
        source_namespace,
    )?;
    let source_table_info =
        authorizer.require_table_presence(warehouse_id, source.clone(), source_table_info)?;

    let (warehouse, source_namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        source_namespace,
        &source_table_info,
        AuthZCannotSeeTable::new_not_found(warehouse_id, source.clone()),
        authorizer,
        catalog_state,
    )
    .await?;

    let user_provided_namespace = &destination.namespace;
    let (destination_namespace, source_table_info) = tokio::join!(
        // Check 1)
        authorizer.require_namespace_action(
            request_metadata,
            &warehouse,
            user_provided_namespace,
            destination_namespace,
            CatalogNamespaceAction::CreateTable {
                properties: Arc::new(source_table_info.properties().clone().into_iter().collect()),
            },
        ),
        // Check 2)
        authorizer.require_table_action(
            request_metadata,
            &warehouse,
            &source_namespace,
            source.clone(),
            Ok::<_, RequireTableActionError>(Some(source_table_info)),
            CatalogTableAction::Rename,
        )
    );

    let destination_namespace = destination_namespace?;
    let source_table_info = source_table_info?;

    Ok((warehouse, destination_namespace, source_table_info))
}
