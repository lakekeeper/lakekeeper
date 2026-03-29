use crate::{
    api::{
        ApiContext, ErrorModel,
        iceberg::v1::namespace::NamespaceParameters,
        v1::generic_tables::{
            GenericTableIdentifier, ListGenericTablesQuery, ListGenericTablesResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogGenericTableOps, CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, Result,
        SecretStore, State, Transaction, authz::Authorizer,
    },
};

pub(super) async fn list_generic_tables<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    query: ListGenericTablesQuery,
    state: ApiContext<State<A, C, S>>,
    _request_metadata: RequestMetadata,
) -> Result<ListGenericTablesResponse> {
    let NamespaceParameters { namespace, prefix } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

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

    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let (entries, next_page_token) = C::list_generic_tables(
        warehouse_id,
        namespace_id,
        namespace,
        query.page_size,
        query.page_token.as_deref(),
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    let identifiers = entries
        .into_iter()
        .map(|entry| GenericTableIdentifier {
            namespace: namespace.clone().inner(),
            name: entry.name,
            format: Some(entry.format),
            id: Some(entry.generic_table_id),
        })
        .collect();

    Ok(ListGenericTablesResponse {
        identifiers,
        next_page_token,
    })
}
