use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use strum::VariantArray;

use crate::{
    api::{ApiContext, RequestMetadata},
    service::{
        authz::{
            fetch_warehouse_namespace_table_by_id, fetch_warehouse_namespace_view_by_id,
            refresh_warehouse_and_namespace_if_needed, AuthZCannotSeeTable, AuthZCannotSeeView,
            AuthZProjectOps, AuthZRoleOps, AuthZServerOps, AuthZTableOps, AuthZUserOps,
            AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogProjectAction, CatalogRoleAction, CatalogServerAction, CatalogTableAction,
            CatalogUserAction, CatalogViewAction, CatalogWarehouseAction, UserOrRole,
        },
        CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, NamespaceId, Result,
        RoleId, SecretStore, State, TableId, TabularListFlags, UserId, ViewId, WarehouseStatus,
    },
    ProjectId, WarehouseId,
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
    pub principal: Option<UserOrRole>,
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
            (Some(user), None) => Some(UserOrRole::User(user)),
            (None, Some(role)) => Some(UserOrRole::Role(role.into_assignees())),
            (Some(_), Some(_)) => {
                return Err(ErrorModel::bad_request(
                    "Cannot specify both user and role in GetAccessQuery".to_string(),
                    "InvalidGetAccessQuery",
                    None,
                ))
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

pub(super) async fn get_allowed_server_actions(
    authorizer: impl Authorizer,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
) -> Result<Vec<CatalogServerAction>> {
    let for_user = query.try_parse()?.principal;
    let actions = CatalogServerAction::VARIANTS;

    let results = authorizer
        .are_allowed_server_actions_vec(request_metadata, for_user.as_ref(), actions)
        .await?
        .into_inner();

    let allowed_actions = results
        .iter()
        .zip(actions)
        .filter_map(
            |(allowed, action)| {
                if *allowed {
                    Some(*action)
                } else {
                    None
                }
            },
        )
        .collect();

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_user_actions(
    authorizer: impl Authorizer,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
    object: UserId,
) -> Result<Vec<CatalogUserAction>> {
    let for_user = query.try_parse()?.principal;
    let actions = CatalogUserAction::VARIANTS;
    let can_see_permission = CatalogUserAction::Read;

    let results = authorizer
        .are_allowed_user_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &actions
                .iter()
                .map(|action| (&object, *action))
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

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_role_actions(
    authorizer: impl Authorizer,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
    object: RoleId,
) -> Result<Vec<CatalogRoleAction>> {
    let for_user = query.try_parse()?.principal;
    let actions = CatalogRoleAction::VARIANTS;
    let can_see_permission = CatalogRoleAction::Read;

    let results = authorizer
        .are_allowed_role_actions_vec(
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

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_project_actions(
    authorizer: impl Authorizer,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
    object: &ProjectId,
) -> Result<Vec<CatalogProjectAction>> {
    let for_user = query.try_parse()?.principal;
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

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_warehouse_actions<
    A: Authorizer,
    C: CatalogStore,
    S: SecretStore,
>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
    object: WarehouseId,
) -> Result<Vec<CatalogWarehouseAction>> {
    let for_user = query.try_parse()?.principal;
    let authorizer = context.v1_state.authz;
    let actions = CatalogWarehouseAction::VARIANTS;
    let can_see_permission = CatalogWarehouseAction::IncludeInList;

    let warehouse = C::get_warehouse_by_id_cache_aware(
        object,
        WarehouseStatus::active_and_inactive(),
        CachePolicy::Skip,
        context.v1_state.catalog,
    )
    .await;
    let warehouse = authorizer.require_warehouse_presence(object, warehouse)?;

    let results = authorizer
        .are_allowed_warehouse_actions_vec(
            request_metadata,
            for_user.as_ref(),
            &actions
                .iter()
                .map(|action| (&*warehouse, *action))
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

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_namespace_actions<
    A: Authorizer,
    C: CatalogStore,
    S: SecretStore,
>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
    warehouse_id: WarehouseId,
    provided_namespace_id: NamespaceId,
) -> Result<Vec<CatalogNamespaceAction>> {
    let for_user = query.try_parse()?.principal;
    let authorizer = context.v1_state.authz;
    let actions = CatalogNamespaceAction::VARIANTS;
    let can_see_permission = CatalogNamespaceAction::IncludeInList;

    let (warehouse, namespace) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, context.v1_state.catalog.clone()),
        C::get_namespace_cache_aware(
            warehouse_id,
            provided_namespace_id,
            CachePolicy::Skip,
            context.v1_state.catalog
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
            &actions
                .iter()
                .map(|action| (&namespace, *action))
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

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_table_actions<A: Authorizer, C: CatalogStore, S: SecretStore>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<Vec<CatalogTableAction>> {
    let for_user = query.try_parse()?.principal;
    let authorizer = context.v1_state.authz;
    let catalog_state = context.v1_state.catalog;
    let actions = CatalogTableAction::VARIANTS;
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
    let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _, _>(
        &authorizer,
        &warehouse,
        &table_info,
        namespace,
        catalog_state,
        AuthZCannotSeeTable::new(warehouse_id, table_id),
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
                .map(|action| (&namespace.namespace, &table_info, *action))
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

    Ok(allowed_actions)
}

pub(super) async fn get_allowed_view_actions<A: Authorizer, C: CatalogStore, S: SecretStore>(
    context: ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
    query: GetAccessQuery,
    warehouse_id: WarehouseId,
    view_id: ViewId,
) -> Result<Vec<CatalogViewAction>> {
    let for_user = query.try_parse()?.principal;
    let authorizer = context.v1_state.authz;
    let catalog_state = context.v1_state.catalog;
    let actions = CatalogViewAction::VARIANTS;
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
    let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _, _>(
        &authorizer,
        &warehouse,
        &view_info,
        namespace,
        catalog_state,
        AuthZCannotSeeView::new(warehouse_id, view_id),
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
                .map(|action| (&namespace.namespace, &view_info, *action))
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

    Ok(allowed_actions)
}
