use std::sync::Arc;

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use strum::VariantArray;

use super::check::UserOrRole as APIUserOrRole;
use crate::{
    ProjectId, WarehouseId,
    api::{ApiContext, RequestMetadata},
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogRoleOps, CatalogStore, CatalogWarehouseOps,
        NamespaceId, Result, RoleId, SecretStore, State, TableId, TabularListFlags, UserId, ViewId,
        WarehouseStatus,
        authn::UserIdRef,
        authz::{
            AuthZCannotSeeNamespace, AuthZCannotSeeRole, AuthZCannotSeeTable, AuthZCannotSeeView,
            AuthZCannotUseWarehouseId, AuthZError, AuthZProjectActionForbidden, AuthZProjectOps,
            AuthZRoleOps, AuthZServerOps, AuthZTableOps, AuthZUserActionForbidden, AuthZUserOps,
            AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogProjectAction, CatalogRoleAction, CatalogServerAction, CatalogTableAction,
            CatalogUserAction, CatalogViewAction, CatalogWarehouseAction,
            RequireProjectActionError, RequireRoleActionError, RoleAssignee, UserOrRole,
            fetch_warehouse_namespace_table_by_id, fetch_warehouse_namespace_view_by_id,
            refresh_warehouse_and_namespace_if_needed,
        },
        events::{
            APIEventContext,
            context::{
                APIEventActions, IntrospectPermissions, ResolutionState, UserProvidedEntity,
            },
        },
    },
};

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct GetAccessQuery {
    /// The user to show actions for.
    /// If neither user nor role is specified, shows actions for the current user.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type=String))]
    pub principal_user: Option<UserId>,
    /// The role to show actions for.
    /// If neither user nor role is specified, shows actions for the current user.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type=Uuid))]
    pub principal_role: Option<RoleId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedAccessQuery {
    pub principal: Option<APIUserOrRole>,
}

impl GetAccessQuery {
    pub fn try_parse(self) -> Result<ParsedAccessQuery, ErrorModel> {
        ParsedAccessQuery::try_from(self)
    }
}

impl TryFrom<GetAccessQuery> for ParsedAccessQuery {
    type Error = ErrorModel;

    fn try_from(query: GetAccessQuery) -> Result<Self, ErrorModel> {
        let principal = match (query.principal_user, query.principal_role) {
            (Some(user), None) => Some(APIUserOrRole::User(user)),
            (None, Some(role)) => Some(APIUserOrRole::Role(role.into_api_assignee())),
            (Some(_), Some(_)) => {
                return Err(ErrorModel::bad_request(
                    "Cannot specify both user and role in GetAccessQuery".to_string(),
                    "InvalidGetAccessQuery",
                    None,
                ));
            }
            (None, None) => None,
        };
        Ok(Self { principal })
    }
}

/// Macro to generate action response structs
macro_rules! action_response {
    ($name:ident, $action_type:ty) => {
        #[derive(Debug, Clone, Serialize, PartialEq)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            pub allowed_actions: Vec<$action_type>,
        }
    };
}

fn push_for_user_context<P: UserProvidedEntity, R: ResolutionState, A: APIEventActions>(
    event_ctx: &mut APIEventContext<P, R, A>,
    for_user: Option<&APIUserOrRole>,
) {
    if let Some(for_user) = for_user {
        let s = match for_user {
            APIUserOrRole::User(id) => format!("User({id})"),
            APIUserOrRole::Role(assignee) => format!("Role({})", assignee.role_id()),
        };
        event_ctx.push_extra_context("for-user", s);
    }
}

// Generate response structs for all action types
action_response!(GetLakekeeperRoleActionsResponse, CatalogRoleAction);
action_response!(GetLakekeeperServerActionsResponse, CatalogServerAction);
action_response!(GetLakekeeperProjectActionsResponse, CatalogProjectAction);
action_response!(
    GetLakekeeperWarehouseActionsResponse,
    CatalogWarehouseAction
);
action_response!(
    GetLakekeeperNamespaceActionsResponse,
    CatalogNamespaceAction
);
action_response!(GetLakekeeperTableActionsResponse, CatalogTableAction);
action_response!(GetLakekeeperViewActionsResponse, CatalogViewAction);
action_response!(GetLakekeeperUserActionsResponse, CatalogUserAction);

/// Resolve an API-level principal (which may contain only a `RoleId`) into the authz `UserOrRole`
/// by fetching the full role from the catalog when needed.
async fn resolve_principal<C: CatalogStore>(
    principal: Option<APIUserOrRole>,
    catalog_state: C::State,
) -> Result<Option<UserOrRole>, AuthZError> {
    match principal {
        None => Ok(None),
        Some(APIUserOrRole::User(id)) => Ok(Some(UserOrRole::User(id))),
        Some(APIUserOrRole::Role(assignee)) => {
            let role = C::get_role_by_id_across_projects(assignee.role_id(), catalog_state).await?;
            Ok(Some(UserOrRole::Role(RoleAssignee::from_role(role))))
        }
    }
}

pub(super) async fn get_allowed_server_actions<C: CatalogStore, A: Authorizer, S: SecretStore>(
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
) -> Result<Vec<CatalogServerAction>, ErrorModel> {
    let for_user_api = query.try_parse()?.principal;
    let actions = CatalogServerAction::VARIANTS;

    let mut event_ctx = APIEventContext::for_server(
        Arc::new(request_metadata),
        state.v1_state.events,
        IntrospectPermissions {},
        state.v1_state.authz.server_id(),
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let authz_result: Result<_, AuthZError> = async {
        let for_user = resolve_principal::<C>(for_user_api, state.v1_state.catalog.clone()).await?;
        Ok(state
            .v1_state
            .authz
            .are_allowed_server_actions_vec(
                event_ctx.request_metadata(),
                for_user.as_ref(),
                actions,
            )
            .await?
            .into_inner())
    }
    .await;
    let (_event_ctx, results) = event_ctx.emit_authz(authz_result)?;

    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(
            |(allowed, action)| {
                if *allowed { Some(*action) } else { None }
            },
        )
        .collect();

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_user_actions<C: CatalogStore, A: Authorizer, S: SecretStore>(
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
    object: UserIdRef,
) -> Result<Vec<CatalogUserAction>> {
    let for_user_api = query.try_parse()?.principal;

    let mut event_ctx = APIEventContext::for_user(
        Arc::new(request_metadata),
        state.v1_state.events,
        object,
        IntrospectPermissions {},
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let allowed_actions = authorize_get_user_actions::<C>(
        event_ctx.request_metadata(),
        state.v1_state.authz,
        for_user_api,
        event_ctx.user_provided_entity(),
        state.v1_state.catalog,
    )
    .await;

    let (_event_ctx, allowed_actions) = event_ctx.emit_authz(allowed_actions)?;

    Ok(allowed_actions)
}

async fn authorize_get_user_actions<C: CatalogStore>(
    request_metadata: &RequestMetadata,
    authorizer: impl Authorizer,
    for_user_api: Option<APIUserOrRole>,
    object: &UserId,
    catalog_state: C::State,
) -> Result<Vec<CatalogUserAction>, AuthZError> {
    let for_user = resolve_principal::<C>(for_user_api, catalog_state).await?;
    let actions = CatalogUserAction::VARIANTS;
    let can_see_permission = CatalogUserAction::Read;

    let results = authorizer
        .are_allowed_user_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &actions
                .iter()
                .map(|action| (object, *action))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_inner();

    let mut can_see = false;
    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(|(allowed, action)| {
            if *allowed {
                if action == &can_see_permission {
                    can_see = true;
                }
                Some(*action)
            } else {
                None
            }
        })
        .collect();

    if !can_see {
        return Err(AuthZUserActionForbidden::new(can_see_permission).into());
    }

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_role_actions<A: Authorizer, C: CatalogStore, S: SecretStore>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
    role_id: RoleId,
) -> Result<Vec<CatalogRoleAction>> {
    let authorizer = context.v1_state.authz;
    let for_user_api = query.try_parse()?.principal;
    let project_id = request_metadata.require_project_id(None)?;

    let mut event_ctx = APIEventContext::for_role(
        Arc::new(request_metadata),
        context.v1_state.events,
        role_id,
        IntrospectPermissions {},
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let authz_result = authorize_get_role_actions::<C>(
        event_ctx.request_metadata(),
        authorizer,
        for_user_api,
        project_id,
        role_id,
        context.v1_state.catalog,
    )
    .await;
    let (_event_ctx, allowed_actions) = event_ctx.emit_authz(authz_result)?;

    Ok(allowed_actions)
}

async fn authorize_get_role_actions<C: CatalogStore>(
    request_metadata: &RequestMetadata,
    authorizer: impl Authorizer,
    for_user_api: Option<APIUserOrRole>,
    project_id: ProjectId,
    role_id: RoleId,
    catalog_state: C::State,
) -> Result<Vec<CatalogRoleAction>, AuthZError> {
    let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
    let actions = CatalogRoleAction::VARIANTS;
    let can_see_permission = CatalogRoleAction::Read;
    let role = C::get_role_by_id(&project_id, role_id, catalog_state).await;
    let role = authorizer.require_role_presence(role)?;

    let results = authorizer
        .are_allowed_role_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &actions
                .iter()
                .map(|action| (&*role, *action))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_inner();

    let mut can_see = false;
    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(|(allowed, action)| {
            if *allowed {
                if action == &can_see_permission {
                    can_see = true;
                }
                Some(*action)
            } else {
                None
            }
        })
        .collect();

    if !can_see {
        let err: RequireRoleActionError =
            AuthZCannotSeeRole::new(project_id, role_id, false, vec![]).into();
        return Err(err.into());
    }

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_project_actions<C: CatalogStore, A: Authorizer, S: SecretStore>(
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
    object: &ProjectId,
) -> Result<Vec<CatalogProjectAction>> {
    let for_user_api = query.try_parse()?.principal;

    let mut event_ctx = APIEventContext::for_project(
        Arc::new(request_metadata),
        state.v1_state.events,
        object.clone(),
        IntrospectPermissions {},
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let authz_result = authorize_get_project_actions::<C>(
        event_ctx.request_metadata(),
        state.v1_state.authz,
        for_user_api,
        object,
        state.v1_state.catalog,
    )
    .await;
    let (_event_ctx, allowed_actions) = event_ctx.emit_authz(authz_result)?;

    Ok(allowed_actions)
}

async fn authorize_get_project_actions<C: CatalogStore>(
    request_metadata: &RequestMetadata,
    authorizer: impl Authorizer,
    for_user_api: Option<APIUserOrRole>,
    object: &ProjectId,
    catalog_state: C::State,
) -> Result<Vec<CatalogProjectAction>, AuthZError> {
    let for_user = resolve_principal::<C>(for_user_api, catalog_state).await?;
    let actions = CatalogProjectAction::VARIANTS;
    let can_see_permission = CatalogProjectAction::GetMetadata;

    let results = authorizer
        .are_allowed_project_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &actions
                .iter()
                .map(|action| (object, *action))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_inner();

    let mut can_see = false;
    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(|(allowed, action)| {
            if *allowed {
                if action == &can_see_permission {
                    can_see = true;
                }
                Some(*action)
            } else {
                None
            }
        })
        .collect();

    if !can_see {
        let err: RequireProjectActionError =
            AuthZProjectActionForbidden::new(object.clone(), can_see_permission).into();
        return Err(err.into());
    }

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_warehouse_actions<
    A: Authorizer,
    C: CatalogStore,
    S: SecretStore,
>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
    object: WarehouseId,
) -> Result<Vec<CatalogWarehouseAction>> {
    let for_user_api = query.try_parse()?.principal;

    let mut event_ctx = APIEventContext::for_warehouse(
        Arc::new(request_metadata),
        context.v1_state.events,
        object,
        IntrospectPermissions {},
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let authz_result = authorize_get_warehouse_actions::<C>(
        event_ctx.request_metadata(),
        context.v1_state.authz,
        for_user_api,
        object,
        context.v1_state.catalog,
    )
    .await;
    let (_event_ctx, allowed_actions) = event_ctx.emit_authz(authz_result)?;

    Ok(allowed_actions)
}

async fn authorize_get_warehouse_actions<C: CatalogStore>(
    request_metadata: &RequestMetadata,
    authorizer: impl Authorizer,
    for_user_api: Option<APIUserOrRole>,
    object: WarehouseId,
    catalog_state: C::State,
) -> Result<Vec<CatalogWarehouseAction>, AuthZError> {
    let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
    let actions = CatalogWarehouseAction::variants();
    let can_see_permission = CatalogWarehouseAction::IncludeInList;

    let warehouse = C::get_warehouse_by_id_cache_aware(
        object,
        WarehouseStatus::active_and_inactive(),
        CachePolicy::Skip,
        catalog_state,
    )
    .await;
    let warehouse = authorizer.require_warehouse_presence(object, warehouse)?;

    let results = authorizer
        .are_allowed_warehouse_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &actions
                .iter()
                .map(|action| (&*warehouse, action.clone()))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_inner();

    let mut can_see = false;
    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(|(allowed, action)| {
            if *allowed {
                if action == &can_see_permission {
                    can_see = true;
                }
                Some(action.clone())
            } else {
                None
            }
        })
        .collect();

    if !can_see {
        return Err(AuthZCannotUseWarehouseId::new_access_denied(object).into());
    }

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_namespace_actions<
    A: Authorizer,
    C: CatalogStore,
    S: SecretStore,
>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
    warehouse_id: WarehouseId,
    provided_namespace_id: NamespaceId,
) -> Result<Vec<CatalogNamespaceAction>> {
    let for_user_api = query.try_parse()?.principal;

    let mut event_ctx = APIEventContext::for_namespace(
        Arc::new(request_metadata),
        context.v1_state.events,
        warehouse_id,
        provided_namespace_id,
        IntrospectPermissions {},
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let authz_result = authorize_get_namespace_actions::<C>(
        event_ctx.request_metadata(),
        context.v1_state.authz,
        for_user_api,
        warehouse_id,
        provided_namespace_id,
        context.v1_state.catalog,
    )
    .await;
    let (_event_ctx, allowed_actions) = event_ctx.emit_authz(authz_result)?;

    Ok(allowed_actions)
}

async fn authorize_get_namespace_actions<C: CatalogStore>(
    request_metadata: &RequestMetadata,
    authorizer: impl Authorizer,
    for_user_api: Option<APIUserOrRole>,
    warehouse_id: WarehouseId,
    provided_namespace_id: NamespaceId,
    catalog_state: C::State,
) -> Result<Vec<CatalogNamespaceAction>, AuthZError> {
    let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
    let actions = CatalogNamespaceAction::variants();
    let can_see_permission = CatalogNamespaceAction::IncludeInList;

    let (warehouse, namespace) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_namespace_cache_aware(
            warehouse_id,
            provided_namespace_id,
            CachePolicy::Skip,
            catalog_state
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, provided_namespace_id, namespace)?;

    let results = authorizer
        .are_allowed_namespace_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &warehouse,
            &namespace
                .parents
                .into_iter()
                .map(|ns| (ns.namespace_id(), ns))
                .collect(),
            &actions
                .iter()
                .map(|action| (&namespace.namespace, action.clone()))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_inner();

    let mut can_see = false;
    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(|(allowed, action)| {
            if *allowed {
                if action == &can_see_permission {
                    can_see = true;
                }
                Some(action.clone())
            } else {
                None
            }
        })
        .collect();

    if !can_see {
        return Err(
            AuthZCannotSeeNamespace::new_forbidden(warehouse_id, provided_namespace_id).into(),
        );
    }

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_table_actions<A: Authorizer, C: CatalogStore, S: SecretStore>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<Vec<CatalogTableAction>> {
    let for_user_api = query.try_parse()?.principal;

    let mut event_ctx = APIEventContext::for_table(
        Arc::new(request_metadata),
        context.v1_state.events,
        warehouse_id,
        table_id,
        IntrospectPermissions {},
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let authz_result = authorize_get_table_actions::<C>(
        event_ctx.request_metadata(),
        context.v1_state.authz,
        for_user_api,
        warehouse_id,
        table_id,
        context.v1_state.catalog,
    )
    .await;
    let (_event_ctx, allowed_actions) = event_ctx.emit_authz(authz_result)?;

    Ok(allowed_actions)
}

async fn authorize_get_table_actions<C: CatalogStore>(
    request_metadata: &RequestMetadata,
    authorizer: impl Authorizer,
    for_user_api: Option<APIUserOrRole>,
    warehouse_id: WarehouseId,
    table_id: TableId,
    catalog_state: C::State,
) -> Result<Vec<CatalogTableAction>, AuthZError> {
    let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
    let actions = CatalogTableAction::variants();
    let can_see_permission = CatalogTableAction::IncludeInList;

    let (warehouse, namespace, table_info) = fetch_warehouse_namespace_table_by_id::<C, _>(
        &authorizer,
        warehouse_id,
        table_id,
        TabularListFlags::all(),
        catalog_state.clone(),
    )
    .await?;

    // Validate warehouse and namespace ID and version consistency (with TOCTOU protection)
    let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        namespace,
        &table_info,
        AuthZCannotSeeTable::new_forbidden(warehouse_id, table_id),
        &authorizer,
        catalog_state,
    )
    .await?;

    let parents_map = namespace
        .parents
        .into_iter()
        .map(|ns| (ns.namespace_id(), ns))
        .collect();

    let results = authorizer
        .are_allowed_table_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &warehouse,
            &parents_map,
            &actions
                .iter()
                .map(|action| (&namespace.namespace, &table_info, action.clone()))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_inner();

    let mut can_see = false;
    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(|(allowed, action)| {
            if *allowed {
                if action == &can_see_permission {
                    can_see = true;
                }
                Some(action.clone())
            } else {
                None
            }
        })
        .collect();

    if !can_see {
        return Err(AuthZCannotSeeTable::new_forbidden(warehouse_id, table_id).into());
    }

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_view_actions<A: Authorizer, C: CatalogStore, S: SecretStore>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    query: GetAccessQuery,
    warehouse_id: WarehouseId,
    view_id: ViewId,
) -> Result<Vec<CatalogViewAction>> {
    let for_user_api = query.try_parse()?.principal;

    let mut event_ctx = APIEventContext::for_view(
        Arc::new(request_metadata),
        context.v1_state.events,
        warehouse_id,
        view_id,
        IntrospectPermissions {},
    );
    push_for_user_context(&mut event_ctx, for_user_api.as_ref());

    let authz_result = authorize_get_view_actions::<C>(
        event_ctx.request_metadata(),
        context.v1_state.authz,
        for_user_api,
        warehouse_id,
        view_id,
        context.v1_state.catalog,
    )
    .await;
    let (_event_ctx, allowed_actions) = event_ctx.emit_authz(authz_result)?;

    Ok(allowed_actions)
}

async fn authorize_get_view_actions<C: CatalogStore>(
    request_metadata: &RequestMetadata,
    authorizer: impl Authorizer,
    for_user_api: Option<APIUserOrRole>,
    warehouse_id: WarehouseId,
    view_id: ViewId,
    catalog_state: C::State,
) -> Result<Vec<CatalogViewAction>, AuthZError> {
    let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
    let actions = CatalogViewAction::variants();
    let can_see_permission = CatalogViewAction::IncludeInList;

    let (warehouse, namespace, view_info) = fetch_warehouse_namespace_view_by_id::<C, _>(
        &authorizer,
        warehouse_id,
        view_id,
        TabularListFlags::all(),
        catalog_state.clone(),
    )
    .await?;

    // Validate warehouse and namespace ID and version consistency (with TOCTOU protection)
    let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        namespace,
        &view_info,
        AuthZCannotSeeView::new_forbidden(warehouse_id, view_id),
        &authorizer,
        catalog_state,
    )
    .await?;

    let parents_map = namespace
        .parents
        .into_iter()
        .map(|ns| (ns.namespace_id(), ns))
        .collect();

    let results = authorizer
        .are_allowed_view_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &warehouse,
            &parents_map,
            &actions
                .iter()
                .map(|action| (&namespace.namespace, &view_info, action.clone()))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_inner();

    let mut can_see = false;
    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(|(allowed, action)| {
            if *allowed {
                if action == &can_see_permission {
                    can_see = true;
                }
                Some(action.clone())
            } else {
                None
            }
        })
        .collect();

    if !can_see {
        return Err(AuthZCannotSeeView::new_forbidden(warehouse_id, view_id).into());
    }

    Ok(allowed_actions)
}
