use crate::api::iceberg::v1::{ListTablesQuery, NamespaceParameters};
use crate::api::ApiContext;
use crate::api::Result;
use crate::catalog::namespace::validate_namespace_ident;
use crate::catalog::require_warehouse_id;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{
    Authorizer, CatalogNamespaceAction, CatalogViewAction, CatalogWarehouseAction,
};
use crate::service::{Catalog, SecretStore, State, Transaction};
use iceberg_ext::catalog::rest::ListTablesResponse;

pub(crate) async fn list_views<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    query: ListTablesQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListTablesResponse> {
    let return_uuids = query.return_uuids;
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters { namespace, prefix } = parameters;
    let warehouse_id = require_warehouse_id(prefix)?;
    validate_namespace_ident(&namespace)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            &CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t: <C as Catalog>::Transaction =
        C::Transaction::begin_read(state.v1_state.catalog).await?;
    let namespace_id = C::namespace_to_id(warehouse_id, &namespace, t.transaction()).await; // We can't fail before AuthZ.

    authorizer
        .require_namespace_action(
            &request_metadata,
            warehouse_id,
            namespace_id,
            &CatalogNamespaceAction::CanListViews,
        )
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let views = C::list_views(
        warehouse_id,
        &namespace,
        false,
        t.transaction(),
        query.into(),
    )
    .await?;

    // ToDo: Better pagination with non-empty pages
    let next_page_token = views.next_page_token;
    let (view_uuids, identifiers): (Vec<_>, Vec<_>) =
        futures::future::try_join_all(views.tabulars.iter().map(|t| {
            authorizer.is_allowed_view_action(
                &request_metadata,
                warehouse_id,
                *t.0,
                &CatalogViewAction::CanIncludeInList,
            )
        }))
        .await?
        .into_iter()
        .zip(views.tabulars.into_iter())
        .filter_map(|(allowed, table)| allowed.then_some((*table.0, table.1)))
        .collect();

    Ok(ListTablesResponse {
        next_page_token,
        identifiers,
        table_uuids: return_uuids.then_some(view_uuids),
    })
}
