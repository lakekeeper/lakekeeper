use std::sync::Arc;

use futures::FutureExt;
use iceberg_ext::catalog::rest::ListTablesResponse;
use itertools::Itertools;

use crate::{
    api::{
        ApiContext, Result,
        iceberg::v1::{ListTablesQuery, NamespaceParameters},
    },
    request_metadata::RequestMetadata,
    server::{
        require_warehouse_id,
        tabular::{authorize_entities, fetch_entities},
    },
    service::{
        CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps,
        NamespaceHierarchy, ResolvedWarehouse, SecretStore, State, Transaction,
        authz::{
            AuthZError, AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogNamespaceAction, CatalogViewAction,
        },
        events::{
            APIEventContext,
            context::{ResolvedNamespace, UserProvidedNamespace, authz_to_error_no_audit},
        },
    },
};

pub(crate) async fn list_views<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    query: ListTablesQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListTablesResponse> {
    let return_uuids = query.return_uuids;
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters {
        namespace: provided_namespace,
        prefix,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;

    let event_ctx = APIEventContext::for_namespace(
        Arc::new(request_metadata),
        state.v1_state.events,
        warehouse_id,
        provided_namespace.clone(),
        CatalogNamespaceAction::ListViews,
    );

    let authz_result = authorize_list_views::<C, _>(
        authorizer.clone(),
        state.v1_state.catalog.clone(),
        event_ctx.user_provided_entity(),
        event_ctx.request_metadata(),
    )
    .await;

    let (event_ctx, (warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;

    let event_ctx = Arc::new(event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: namespace.namespace.clone(),
    }));

    // ------------------- BUSINESS LOGIC -------------------
    let can_list_everything = authorizer
        .is_allowed_namespace_action(
            event_ctx.request_metadata(),
            None,
            &warehouse,
            &namespace.parents,
            &namespace.namespace,
            CatalogNamespaceAction::ListEverything,
        )
        .await
        .map_err(authz_to_error_no_audit)?
        .into_inner();
    let (view_infos, view_uuids, next_page_token) =
        crate::server::fetch_until_full_page::<_, _, _, C>(
            query.page_size,
            query.page_token,
            state.v1_state.catalog,
            fetch_entities!(list_views, namespace, can_list_everything),
            authorize_entities!(View, warehouse, authorizer, event_ctx),
        )
        .await?;

    let mut identifiers = Vec::with_capacity(view_infos.len());
    let mut protection_status = Vec::with_capacity(view_infos.len());
    for view_info in view_infos {
        identifiers.push(view_info.tabular.tabular_ident);
        protection_status.push(view_info.tabular.protected);
    }

    Ok(ListTablesResponse {
        next_page_token,
        identifiers: Arc::new(identifiers),
        table_uuids: return_uuids.then_some(view_uuids.into_iter().map(|id| *id).collect()),
        protection_status: query.return_protection_status.then_some(protection_status),
    })
}

async fn authorize_list_views<C: CatalogStore, A: Authorizer>(
    authorizer: A,
    catalog_state: C::State,
    user_provided_ns: &UserProvidedNamespace,
    request_metadata: &RequestMetadata,
) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy), AuthZError> {
    let warehouse_id = user_provided_ns.warehouse_id;
    let provided_namespace = user_provided_ns.namespace.clone();
    let (warehouse, namespace) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_namespace(
            warehouse_id,
            provided_namespace.clone(),
            catalog_state.clone()
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    let namespace = authorizer
        .require_namespace_action(
            request_metadata,
            &warehouse,
            provided_namespace,
            namespace,
            CatalogNamespaceAction::ListViews,
        )
        .await?;

    Ok((warehouse, namespace))
}
