use std::{collections::HashMap, sync::Arc};

use futures::FutureExt;
use itertools::Itertools;

use crate::{
    api::{
        ApiContext,
        data::v1::generic_tables::{
            GenericTableIdentifier, ListGenericTablesQuery, ListGenericTablesResponse,
        },
        iceberg::v1::{PageToken, PaginatedMapping, namespace::NamespaceParameters},
    },
    request_metadata::RequestMetadata,
    server::{UnfilteredPage, fetch_until_full_page, require_warehouse_id},
    service::{
        CachePolicy, CatalogGenericTableOps, CatalogStore, GenericTableId, GenericTableListEntry,
        NamespaceHierarchy, ResolvedWarehouse, Result, SecretStore, State,
        authz::{
            ActionOnGenericTable, AuthZGenericTableOps, Authorizer, AuthzNamespaceOps,
            CatalogGenericTableAction, CatalogNamespaceAction,
        },
        events::{
            APIEventContext,
            context::{ResolvedNamespace, UserProvidedNamespace, authz_to_error_no_audit},
        },
    },
};

pub(super) async fn list_generic_tables<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    query: ListGenericTablesQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListGenericTablesResponse> {
    let NamespaceParameters { namespace, prefix } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = &state.v1_state.authz;

    // ------------------- AUTHZ: namespace-level ListGenericTables -------------------
    let event_ctx = APIEventContext::for_namespace(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        namespace.clone(),
        CatalogNamespaceAction::ListGenericTables,
    );

    let (event_ctx, (warehouse, ns)) = event_ctx.emit_authz(
        authorizer
            .load_and_authorize_namespace_action::<C>(
                &request_metadata,
                UserProvidedNamespace::new(warehouse_id, namespace.clone()),
                CatalogNamespaceAction::ListGenericTables,
                CachePolicy::Use,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let _event_ctx = event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: ns.namespace.clone(),
    });

    let namespace_id = ns.namespace.namespace_id();

    // ------------------- AUTHZ: per-entry IncludeInList filtering -------------------
    let can_list_everything = authorizer
        .is_allowed_namespace_action(
            &request_metadata,
            None,
            &warehouse,
            &ns.parents,
            &ns.namespace,
            CatalogNamespaceAction::ListEverything,
        )
        .await
        .map_err(authz_to_error_no_audit)?
        .into_inner();

    // A missing or empty token starts at the first page, with the default page size.
    let page_token = query
        .page_token
        .map_or(PageToken::Empty, PageToken::new_present);

    let (entries, _, next_page_token) = fetch_until_full_page::<_, _, _, C>(
        query.page_size,
        page_token,
        state.v1_state.catalog,
        |page_size, page_token, t: &mut _| {
            let namespace = namespace.clone();
            async move {
                Ok(C::list_generic_tables(
                    warehouse_id,
                    namespace_id,
                    &namespace,
                    Some(page_size),
                    page_token.as_deref(),
                    t.transaction(),
                )
                .await?)
            }
            .boxed()
        },
        |page_size, page| {
            authorize_page(
                authorizer,
                &request_metadata,
                &warehouse,
                &ns,
                can_list_everything,
                page_size,
                page,
            )
            .boxed()
        },
    )
    .await?;

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

/// Marks the entries of one page that the caller may see in the listing.
async fn authorize_page<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    ns: &NamespaceHierarchy,
    can_list_everything: bool,
    page_size: i64,
    page: PaginatedMapping<GenericTableId, GenericTableListEntry>,
) -> Result<UnfilteredPage<GenericTableListEntry, GenericTableId>> {
    let (ids, entries, tokens): (Vec<_>, Vec<_>, Vec<_>) =
        page.into_iter_with_page_tokens().multiunzip();
    let masks = if can_list_everything {
        vec![true; entries.len()]
    } else {
        let actions: Vec<_> = entries
            .iter()
            .map(|entry| {
                (
                    &ns.namespace,
                    ActionOnGenericTable {
                        info: entry,
                        action: CatalogGenericTableAction::IncludeInList,
                        user: None,
                        is_delegated_execution: false,
                    },
                )
            })
            .collect();
        let parents: HashMap<_, _> = ns
            .parents
            .iter()
            .map(|n| (n.namespace_id(), n.clone()))
            .collect();
        authorizer
            .are_allowed_generic_table_actions_vec(request_metadata, warehouse, &parents, &actions)
            .await
            .map_err(authz_to_error_no_audit)?
            .into_allowed()
    };
    Ok(UnfilteredPage::new(
        entries,
        ids,
        tokens,
        masks,
        page_size
            .clamp(0, i64::MAX)
            .try_into()
            .expect("we clamped it"),
    ))
}
