use std::sync::Arc;

use crate::{
    api::{
        ApiContext,
        iceberg::v1::namespace::NamespaceParameters,
        v1::generic_tables::{
            GenericTableIdentifier, ListGenericTablesQuery, ListGenericTablesResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CachePolicy, CatalogGenericTableOps, CatalogStore, Result, SecretStore, State, Transaction,
        authz::{
            AuthZGenericTableOps, Authorizer, AuthzNamespaceOps, CatalogGenericTableAction,
            CatalogNamespaceAction,
        },
        events::{
            APIEventContext, AuthorizationFailureSource,
            context::{ResolvedNamespace, UserProvidedNamespace},
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
        .map_err(AuthorizationFailureSource::into_error_model)?
        .into_inner();

    let masks = if can_list_everything {
        vec![true; entries.len()]
    } else {
        let actions: Vec<_> = entries
            .iter()
            .map(|entry| {
                (
                    &ns.namespace,
                    entry,
                    CatalogGenericTableAction::IncludeInList,
                )
            })
            .collect();

        authorizer
            .are_allowed_generic_table_actions_vec(
                &request_metadata,
                None,
                &warehouse,
                &ns.parents
                    .iter()
                    .map(|n| (n.namespace_id(), n.clone()))
                    .collect(),
                &actions,
            )
            .await
            .map_err(AuthorizationFailureSource::into_error_model)?
            .into_inner()
    };

    let identifiers = entries
        .into_iter()
        .zip(masks)
        .filter(|(_, allowed)| *allowed)
        .map(|(entry, _)| GenericTableIdentifier {
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
