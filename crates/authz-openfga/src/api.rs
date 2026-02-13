#![allow(clippy::needless_for_each)]
#![allow(deprecated)]

use std::{collections::HashSet, sync::Arc};

use http::StatusCode;
#[cfg(feature = "open-api")]
use lakekeeper::api::management::v1::PROJECT_ID_HEADER_DESCRIPTION;
use lakekeeper::{
    ProjectId, WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        management::v1::lakekeeper_actions::{GetAccessQuery, ParsedAccessQuery},
    },
    axum::{
        Extension, Json, Router,
        extract::{Path, Query, State as AxumState},
        routing::{get, post},
    },
    service::{
        Actor, CatalogStore, NamespaceId, Result, RoleId, SecretStore, State, TableId, ViewId,
        authz::UserOrRole,
        events::{
            APIEventContext,
            context::{APIEventAction, IntrospectPermissions, authz_to_error_no_audit},
        },
    },
};
use openfga_client::client::{
    CheckRequestTupleKey, ReadRequestTupleKey, TupleKey, TupleKeyWithoutCondition,
};
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
#[cfg(feature = "open-api")]
use utoipa::OpenApi;

use super::{
    check::check,
    relations::{
        APINamespaceAction as NamespaceAction, APINamespaceRelation as NamespaceRelation,
        APIProjectAction as ProjectAction, APIProjectRelation as ProjectRelation,
        APIRoleAction as RoleAction, APIRoleRelation as RoleRelation,
        APIServerAction as ServerAction, APIServerRelation as ServerRelation,
        APITableAction as TableAction, APITableRelation as TableRelation,
        APIViewAction as ViewAction, APIViewRelation as ViewRelation,
        APIWarehouseAction as WarehouseAction, APIWarehouseRelation as WarehouseRelation,
        Assignment, GrantableRelation, NamespaceAssignment,
        NamespaceRelation as AllNamespaceRelations, ProjectAssignment,
        ProjectRelation as AllProjectRelations, ReducedRelation, RoleAssignment,
        RoleRelation as AllRoleRelations, ServerAssignment, ServerRelation as AllServerAction,
        TableAssignment, TableRelation as AllTableRelations, ViewAssignment,
        ViewRelation as AllViewRelations, WarehouseAssignment,
        WarehouseRelation as AllWarehouseRelation,
    },
};
#[cfg(feature = "open-api")]
use crate::check::__path_check;
use crate::{
    OpenFGAAuthorizer, OpenFGAError, OpenFGAResult,
    entities::OpenFgaEntity,
    relations::{
        OpenFGANamespaceAction, OpenFGAProjectAction, OpenFGARoleAction, OpenFGAServerAction,
        OpenFGATableAction, OpenFGAViewAction, OpenFGAWarehouseAction,
    },
};

const _MAX_ASSIGNMENTS_PER_RELATION: i32 = 200;

macro_rules! access_response {
    ($name:ident, $action_type:ty) => {
        #[derive(Debug, Clone, Serialize, PartialEq)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        #[serde(rename_all = "kebab-case")]
        struct $name {
            allowed_actions: Vec<$action_type>,
        }
    };
}

access_response!(GetOpenFGARoleActionsResponse, OpenFGARoleAction);
access_response!(GetOpenFGAServerActionsResponse, OpenFGAServerAction);
access_response!(GetOpenFGAProjectActionsResponse, OpenFGAProjectAction);
access_response!(GetOpenFGAWarehouseActionsResponse, OpenFGAWarehouseAction);
access_response!(GetOpenFGANamespaceActionsResponse, OpenFGANamespaceAction);
access_response!(GetOpenFGATableActionsResponse, OpenFGATableAction);
access_response!(GetOpenFGAViewActionsResponse, OpenFGAViewAction);

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetRoleAccessResponse {
    allowed_actions: Vec<RoleAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetServerAccessResponse {
    allowed_actions: Vec<ServerAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetProjectAccessResponse {
    allowed_actions: Vec<ProjectAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetWarehouseAccessResponse {
    allowed_actions: Vec<WarehouseAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetNamespaceAccessResponse {
    allowed_actions: Vec<NamespaceAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetTableAccessResponse {
    allowed_actions: Vec<TableAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetViewAccessResponse {
    allowed_actions: Vec<ViewAction>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
struct GetRoleAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    relations: Option<Vec<RoleRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetRoleAssignmentsResponse {
    assignments: Vec<RoleAssignment>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
struct GetServerAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    relations: Option<Vec<ServerRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetServerAssignmentsResponse {
    assignments: Vec<ServerAssignment>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub(super) struct GetProjectAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    relations: Option<Vec<ProjectRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetProjectAssignmentsResponse {
    assignments: Vec<ProjectAssignment>,
    #[cfg_attr(feature = "open-api", schema(value_type = Uuid))]
    project_id: ProjectId,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub(super) struct GetWarehouseAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    relations: Option<Vec<WarehouseRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetWarehouseAssignmentsResponse {
    assignments: Vec<WarehouseAssignment>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub(super) struct GetNamespaceAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    relations: Option<Vec<NamespaceRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetNamespaceAssignmentsResponse {
    assignments: Vec<NamespaceAssignment>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub(super) struct GetTableAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    relations: Option<Vec<TableRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetTableAssignmentsResponse {
    assignments: Vec<TableAssignment>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub(super) struct GetViewAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    relations: Option<Vec<ViewRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetViewAssignmentsResponse {
    assignments: Vec<ViewAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct UpdateServerAssignmentsRequest {
    #[serde(default)]
    writes: Vec<ServerAssignment>,
    #[serde(default)]
    deletes: Vec<ServerAssignment>,
}
impl APIEventAction for UpdateServerAssignmentsRequest {
    fn event_action_str(&self) -> String {
        "update_server_assignments".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct UpdateProjectAssignmentsRequest {
    #[serde(default)]
    writes: Vec<ProjectAssignment>,
    #[serde(default)]
    deletes: Vec<ProjectAssignment>,
}
impl APIEventAction for UpdateProjectAssignmentsRequest {
    fn event_action_str(&self) -> String {
        "update_project_assignments".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct UpdateWarehouseAssignmentsRequest {
    #[serde(default)]
    writes: Vec<WarehouseAssignment>,
    #[serde(default)]
    deletes: Vec<WarehouseAssignment>,
}
impl APIEventAction for UpdateWarehouseAssignmentsRequest {
    fn event_action_str(&self) -> String {
        "update_warehouse_assignments".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct UpdateNamespaceAssignmentsRequest {
    #[serde(default)]
    writes: Vec<NamespaceAssignment>,
    #[serde(default)]
    deletes: Vec<NamespaceAssignment>,
}
impl APIEventAction for UpdateNamespaceAssignmentsRequest {
    fn event_action_str(&self) -> String {
        "update_namespace_assignments".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct UpdateTableAssignmentsRequest {
    #[serde(default)]
    writes: Vec<TableAssignment>,
    #[serde(default)]
    deletes: Vec<TableAssignment>,
}
impl APIEventAction for UpdateTableAssignmentsRequest {
    fn event_action_str(&self) -> String {
        "update_table_assignments".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct UpdateViewAssignmentsRequest {
    #[serde(default)]
    writes: Vec<ViewAssignment>,
    #[serde(default)]
    deletes: Vec<ViewAssignment>,
}
impl APIEventAction for UpdateViewAssignmentsRequest {
    fn event_action_str(&self) -> String {
        "update_view_assignments".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct UpdateRoleAssignmentsRequest {
    #[serde(default)]
    writes: Vec<RoleAssignment>,
    #[serde(default)]
    deletes: Vec<RoleAssignment>,
}
impl APIEventAction for UpdateRoleAssignmentsRequest {
    fn event_action_str(&self) -> String {
        "update_role_assignments".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetWarehouseAuthPropertiesResponse {
    managed_access: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct GetNamespaceAuthPropertiesResponse {
    managed_access: bool,
    managed_access_inherited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
struct SetManagedAccessRequest {
    managed_access: bool,
}

/// Get my access to a role
///
/// **Deprecated:** Use `/management/v1/permissions/role/{role_id}/authorizer-actions` for Authorizer permissions
/// or `/management/v1/role/{role_id}/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/role/{role_id}/access",
    params(
        ("role_id" = Uuid, Path, description = "Role ID"),
    ),
    responses(
            (status = 200, body = GetRoleAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/permissions/role/{role_id}/authorizer-actions and /management/v1/role/{role_id}/actions instead"
)]
async fn get_role_access_by_id<C: CatalogStore, S: SecretStore>(
    Path(role_id): Path<RoleId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetRoleAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_role(
        Arc::new(metadata),
        api_context.v1_state.events,
        role_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &role_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetRoleAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get allowed Authorizer actions on a role
///
/// Returns Authorizer permissions (OpenFGA relations) for the specified role.
/// For Catalog permissions, use `/management/v1/role/{role_id}/actions` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/role/{role_id}/authorizer-actions",
    params(
        GetAccessQuery,
        ("role_id" = Uuid, Path, description = "Role ID"),
    ),
    responses(
            (status = 200, body = GetOpenFGARoleActionsResponse),
    )
))]
async fn get_authorizer_role_actions<C: CatalogStore, S: SecretStore>(
    Path(role_id): Path<RoleId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetOpenFGARoleActionsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_role(
        Arc::new(metadata),
        api_context.v1_state.events,
        role_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &role_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetOpenFGARoleActionsResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to the server
///
/// **Deprecated:** Use `/management/v1/permissions/server/authorizer-actions` for Authorizer permissions
/// or `/management/v1/server/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/server/access",
    params(GetAccessQuery),
    responses(
        (status = 200, description = "Server Access", body = GetServerAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/server/actions and /management/v1/permissions/server/authorizer-actions instead"
)]
async fn get_server_access<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetServerAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;
    let openfga_server = authorizer.openfga_server().clone();

    let event_ctx = APIEventContext::for_server(
        Arc::new(metadata),
        api_context.v1_state.events,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &openfga_server,
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetServerAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get allowed Authorizer actions on the server
///
/// Returns Authorizer permissions (OpenFGA relations) for the server.
/// For Catalog permissions, use `/management/v1/server/actions` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/server/authorizer-actions",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Access", body = GetOpenFGAServerActionsResponse),
    )
))]
async fn get_authorizer_server_actions<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetOpenFGAServerActionsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;
    let openfga_server = authorizer.openfga_server().clone();

    let event_ctx = APIEventContext::for_server(
        Arc::new(metadata),
        api_context.v1_state.events,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &openfga_server,
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetOpenFGAServerActionsResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to the default project
///
/// **Deprecated:** Use `/management/v1/permissions/project/authorizer-actions` for Authorizer permissions
/// or `/management/v1/project/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/project/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Relations", body = GetProjectAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/project/actions and /management/v1/permissions/project/authorizer-actions instead"
)]
async fn get_project_access<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetProjectAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;
    let project_id = metadata
        .preferred_project_id()
        .ok_or(OpenFGAError::NoProjectId)
        .map_err(authz_to_error_no_audit)?;

    let event_ctx = APIEventContext::for_project(
        Arc::new(metadata),
        api_context.v1_state.events,
        project_id.clone(),
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &project_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetProjectAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get allowed Authorizer actions on the default project
///
/// Returns Authorizer permissions (OpenFGA relations) for the default project.
/// For Catalog permissions, use `/management/v1/project/actions` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/project/authorizer-actions",
    params(GetAccessQuery, ("x-project-id" = Option<String>, Header, description = "Optional project ID")),
    responses(
        (status = 200, description = "Project Authorizer Actions", body = GetOpenFGAProjectActionsResponse),
    )
))]
async fn get_authorizer_project_actions<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetOpenFGAProjectActionsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;
    let project_id = metadata
        .preferred_project_id()
        .ok_or(OpenFGAError::NoProjectId)
        .map_err(authz_to_error_no_audit)?;

    let event_ctx = APIEventContext::for_project(
        Arc::new(metadata),
        api_context.v1_state.events,
        project_id.clone(),
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &project_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetOpenFGAProjectActionsResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a project
///
/// **Deprecated:** Use `/management/v1/permissions/project/authorizer-actions` for Authorizer permissions
/// or `/management/v1/project/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/project/{project_id}/access",
    params(
        GetAccessQuery,
        ("project_id" = Option<String>, Path, description = PROJECT_ID_HEADER_DESCRIPTION),
    ),
    responses(
            (status = 200, description = "Server Relations", body = GetProjectAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/project/actions and /management/v1/permissions/project/authorizer-actions instead"
)]
async fn get_project_access_by_id<C: CatalogStore, S: SecretStore>(
    Path(project_id): Path<ProjectId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetProjectAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_project(
        Arc::new(metadata),
        api_context.v1_state.events,
        project_id.clone(),
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &project_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetProjectAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a warehouse
///
/// **Deprecated:** Use `/management/v1/permissions/warehouse/{warehouse_id}/authorizer-actions` for Authorizer permissions
/// or `/management/v1/warehouse/{warehouse_id}/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/access",
    params(
        GetAccessQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
    ),
    responses(
            (status = 200, body = GetWarehouseAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/warehouse/{warehouse_id}/actions and /management/v1/permissions/warehouse/{warehouse_id}/authorizer-actions instead"
)]
async fn get_warehouse_access_by_id<C: CatalogStore, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetWarehouseAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_warehouse(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &warehouse_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetWarehouseAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get allowed Authorizer actions on a warehouse
///
/// Returns Authorizer permissions (OpenFGA relations) for the specified warehouse.
/// For Catalog permissions, use `/management/v1/warehouse/{warehouse_id}/actions` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/authorizer-actions",
    params(
        GetAccessQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
    ),
    responses(
            (status = 200, description = "Warehouse Authorizer Actions", body = GetOpenFGAWarehouseActionsResponse),
    )
))]
async fn get_authorizer_warehouse_actions<C: CatalogStore, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetOpenFGAWarehouseActionsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_warehouse(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &warehouse_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;
    Ok((
        StatusCode::OK,
        Json(GetOpenFGAWarehouseActionsResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get Authorization properties of a warehouse
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}",
    params(
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
    ),
    responses(
            (status = 200, body = GetWarehouseAuthPropertiesResponse),
    )
))]
async fn get_warehouse_by_id<C: CatalogStore, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<(StatusCode, Json<GetWarehouseAuthPropertiesResponse>)> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_warehouse(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        AllWarehouseRelation::CanGetMetadata,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &warehouse_id.to_openfga(),
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let managed_access = get_managed_access(&authorizer, &warehouse_id)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetWarehouseAuthPropertiesResponse { managed_access }),
    ))
}

/// Set managed access property of a warehouse
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/managed-access",
    params(
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
    ),
    responses(
            (status = 200),
    )
))]
async fn set_warehouse_managed_access<C: CatalogStore, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<SetManagedAccessRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_warehouse(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        AllWarehouseRelation::CanSetManagedAccess,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &warehouse_id.to_openfga(),
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    set_managed_access(authorizer, &warehouse_id, request.managed_access)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok(StatusCode::OK)
}

/// Set managed access property of a namespace
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/namespace/{namespace_id}/managed-access",
    params(
        ("namespace_id" = Uuid, Path, description = "Namespace ID"),
    ),
    request_body = SetManagedAccessRequest,
    responses(
            (status = 200),
    )
))]
async fn set_namespace_managed_access<C: CatalogStore, S: SecretStore>(
    Path(namespace_id): Path<NamespaceId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<SetManagedAccessRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_namespace_only_id(
        Arc::new(metadata),
        api_context.v1_state.events,
        namespace_id,
        AllNamespaceRelations::CanSetManagedAccess,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &namespace_id.to_openfga(),
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    set_managed_access(authorizer, &namespace_id, request.managed_access)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok(StatusCode::OK)
}

/// Get Authorization properties of a namespace
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/namespace/{namespace_id}",
    params(
        ("namespace_id" = Uuid, Path, description = "Namespace ID"),
    ),
    responses(
            (status = 200, body = GetNamespaceAuthPropertiesResponse),
    )
))]
async fn get_namespace_by_id<C: CatalogStore, S: SecretStore>(
    Path(namespace_id): Path<NamespaceId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<(StatusCode, Json<GetNamespaceAuthPropertiesResponse>)> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_namespace_only_id(
        Arc::new(metadata),
        api_context.v1_state.events,
        namespace_id,
        AllNamespaceRelations::CanGetMetadata,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &namespace_id.to_openfga(),
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let managed_access = get_managed_access(&authorizer, &namespace_id)
        .await
        .map_err(authz_to_error_no_audit)?;
    let managed_access_inherited = authorizer
        .check(CheckRequestTupleKey {
            user: "user:*".to_string(),
            relation: AllNamespaceRelations::ManagedAccessInheritance.to_string(),
            object: namespace_id.to_openfga(),
        })
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetNamespaceAuthPropertiesResponse {
            managed_access,
            managed_access_inherited,
        }),
    ))
}

/// Get my access to a namespace
///
/// **Deprecated:** Use `/management/v1/permissions/namespace/{namespace_id}/authorizer-actions` for Authorizer permissions
/// or `/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/namespace/{namespace_id}/access",
    params(
        GetAccessQuery,
        ("namespace_id" = Uuid, Path, description = "Namespace ID")
    ),
    responses(
            (status = 200, description = "Server Relations", body = GetNamespaceAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/actions and /management/v1/permissions/namespace/{namespace_id}/authorizer-actions instead"
)]
async fn get_namespace_access_by_id<C: CatalogStore, S: SecretStore>(
    Path(namespace_id): Path<NamespaceId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetNamespaceAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_namespace_only_id(
        Arc::new(metadata),
        api_context.v1_state.events,
        namespace_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &namespace_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;

    Ok((
        StatusCode::OK,
        Json(GetNamespaceAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get allowed Authorizer actions on a namespace
///
/// Returns Authorizer permissions (OpenFGA relations) for the specified namespace.
/// For Catalog permissions, use `/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/actions` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/namespace/{namespace_id}/authorizer-actions",
    params(
        GetAccessQuery,
        ("namespace_id" = Uuid, Path, description = "Namespace ID")
    ),
    responses(
            (status = 200, description = "Namespace Authorizer Actions", body = GetOpenFGANamespaceActionsResponse),
    )
))]
async fn get_authorizer_namespace_actions<C: CatalogStore, S: SecretStore>(
    Path(namespace_id): Path<NamespaceId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetOpenFGANamespaceActionsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_namespace_only_id(
        Arc::new(metadata),
        api_context.v1_state.events,
        namespace_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &namespace_id.to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;

    Ok((
        StatusCode::OK,
        Json(GetOpenFGANamespaceActionsResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a table
///
/// **Deprecated:** Use `/management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/authorizer-actions` for Authorizer permissions
/// or `/management/v1/warehouse/{warehouse_id}/table/{table_id}/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/access",
    params(
        GetAccessQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("table_id" = Uuid, Path, description = "Table ID")
    ),
    responses(
            (status = 200, description = "Server Relations", body = GetTableAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/warehouse/{warehouse_id}/table/{table_id}/actions and /management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/authorizer-actions instead"
)]
async fn get_table_access_by_id<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, table_id)): Path<(WarehouseId, TableId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetTableAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_table(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        table_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &(warehouse_id, table_id).to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;

    Ok((
        StatusCode::OK,
        Json(GetTableAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get allowed Authorizer actions on a table
///
/// Returns Authorizer permissions (OpenFGA relations) for the specified table.
/// For Catalog permissions, use `/management/v1/warehouse/{warehouse_id}/table/{table_id}/actions` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/authorizer-actions",
    params(
        GetAccessQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("table_id" = Uuid, Path, description = "Table ID")
    ),
    responses(
            (status = 200, description = "Table Authorizer Actions", body = GetOpenFGATableActionsResponse),
    )
))]
async fn get_authorizer_table_actions<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, table_id)): Path<(WarehouseId, TableId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetOpenFGATableActionsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_table(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        table_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &(warehouse_id, table_id).to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;

    Ok((
        StatusCode::OK,
        Json(GetOpenFGATableActionsResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a view
///
/// **Deprecated:** Use `/management/v1/permissions/warehouse/{warehouse_id}/view/{view_id}/authorizer-actions` for Authorizer permissions
/// or `/management/v1/warehouse/{warehouse_id}/view/{view_id}/actions` for Catalog permissions instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/view/{view_id}/access",
    params(
        GetAccessQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("view_id" = Uuid, Path, description = "View ID"),
    ),
    responses(
            (status = 200, body = GetViewAccessResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/warehouse/{warehouse_id}/view/{view_id}/actions and /management/v1/permissions/warehouse/{warehouse_id}/view/{view_id}/authorizer-actions instead"
)]
async fn get_view_access_by_id<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, view_id)): Path<(WarehouseId, ViewId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetViewAccessResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_view(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        view_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &(warehouse_id, view_id).to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;

    Ok((
        StatusCode::OK,
        Json(GetViewAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get allowed Authorizer actions on a view
///
/// Returns Authorizer permissions (OpenFGA relations) for the specified view.
/// For Catalog permissions, use `/management/v1/warehouse/{warehouse_id}/view/{view_id}/actions` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/view/{view_id}/authorizer-actions",
    params(
        GetAccessQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("view_id" = Uuid, Path, description = "View ID"),
    ),
    responses(
            (status = 200, description = "View Authorizer Actions", body = GetOpenFGAViewActionsResponse),
    )
))]
async fn get_authorizer_view_actions<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, view_id)): Path<(WarehouseId, ViewId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetOpenFGAViewActionsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let query = ParsedAccessQuery::try_from(query)?;

    let event_ctx = APIEventContext::for_view(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        view_id,
        IntrospectPermissions {},
    );

    let relations = get_allowed_actions(
        authorizer,
        event_ctx.request_metadata().actor(),
        &(warehouse_id, view_id).to_openfga(),
        query.principal.as_ref(),
    )
    .await;

    let (_, relations) = event_ctx.emit_authz(relations)?;

    Ok((
        StatusCode::OK,
        Json(GetOpenFGAViewActionsResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get user and role assignments of a role
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/role/{role_id}/assignments",
    params(
        GetRoleAssignmentsQuery,
        ("role_id" = Uuid, Path, description = "Role ID"),
    ),
    responses(
            (status = 200, body = GetRoleAssignmentsResponse),
    )
))]
async fn get_role_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(role_id): Path<RoleId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetRoleAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetRoleAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_role(
        Arc::new(metadata),
        api_context.v1_state.events,
        role_id,
        AllRoleRelations::CanReadAssignments,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &role_id.to_openfga(),
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &role_id.to_openfga())
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetRoleAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments of the server
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/server/assignments",
    params(GetServerAssignmentsQuery),
    responses(
            (status = 200, body = GetServerAssignmentsResponse),
    )
))]
async fn get_server_assignments<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetServerAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetServerAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let server_id = authorizer.openfga_server().clone();

    let event_ctx = APIEventContext::for_server(
        Arc::new(metadata),
        api_context.v1_state.events,
        AllServerAction::CanReadAssignments,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &server_id,
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &server_id)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetServerAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments of a project
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/project/assignments",
    params(GetProjectAssignmentsQuery),
    responses(
            (status = 200, body = GetProjectAssignmentsResponse),
    )
))]
async fn get_project_assignments<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetProjectAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetProjectAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let project_id = metadata
        .preferred_project_id()
        .ok_or(OpenFGAError::NoProjectId)
        .map_err(authz_to_error_no_audit)?;

    let event_ctx = APIEventContext::for_project(
        Arc::new(metadata),
        api_context.v1_state.events,
        project_id,
        AllProjectRelations::CanReadAssignments,
    );
    let project_id_openfga = event_ctx.user_provided_entity().to_openfga();

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &project_id_openfga,
        )
        .await;

    let (event_ctx, _) = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &project_id_openfga)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAssignmentsResponse {
            assignments,
            project_id: event_ctx.user_provided_entity().clone(),
        }),
    ))
}

/// Get user and role assignments to a project
///
/// **Deprecated:** Use `/management/v1/permissions/project/assignments` instead.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/project/{project_id}/assignments",
    params(
        GetProjectAssignmentsQuery,
        ("project_id" = Option<String>, Path, description = PROJECT_ID_HEADER_DESCRIPTION),
    ),
    responses(
            (status = 200, body = GetProjectAssignmentsResponse),
    )
))]
#[deprecated(
    since = "0.11.0",
    note = "Use /management/v1/permissions/project/assignments instead"
)]
async fn get_project_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(project_id): Path<ProjectId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetProjectAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetProjectAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_project(
        Arc::new(metadata),
        api_context.v1_state.events,
        project_id,
        AllProjectRelations::CanReadAssignments,
    );
    let project_id_openfga = event_ctx.user_provided_entity().to_openfga();

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &project_id_openfga,
        )
        .await;

    let (event_ctx, _) = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &project_id_openfga)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAssignmentsResponse {
            assignments,
            project_id: event_ctx.user_provided_entity().clone(),
        }),
    ))
}

/// Get user and role assignments for a warehouse
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/assignments",
    params(
        GetWarehouseAssignmentsQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
    ),
    responses(
            (status = 200, body = GetWarehouseAssignmentsResponse),
    )
))]
async fn get_warehouse_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetWarehouseAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetWarehouseAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let object = warehouse_id.to_openfga();

    let event_ctx = APIEventContext::for_warehouse(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        AllWarehouseRelation::CanReadAssignments,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &object,
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &object)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetWarehouseAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments for a namespace
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/namespace/{namespace_id}/assignments",
    params(
        GetNamespaceAssignmentsQuery,
        ("namespace_id" = Uuid, Path, description = "Namespace ID"),
    ),
    responses(
            (status = 200, body = GetNamespaceAssignmentsResponse),
    )
))]
async fn get_namespace_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(namespace_id): Path<NamespaceId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetNamespaceAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetNamespaceAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let object = namespace_id.to_openfga();

    let event_ctx = APIEventContext::for_namespace_only_id(
        Arc::new(metadata),
        api_context.v1_state.events,
        namespace_id,
        AllNamespaceRelations::CanReadAssignments,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &object,
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &object)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetNamespaceAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments for a table
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/assignments",
    params(
        GetTableAssignmentsQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("table_id" = Uuid, Path, description = "Table ID"),
    ),
    responses(
            (status = 200, body = GetTableAssignmentsResponse),
    )
))]
async fn get_table_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, table_id)): Path<(WarehouseId, TableId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetTableAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetTableAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let object = (warehouse_id, table_id).to_openfga();

    let event_ctx = APIEventContext::for_table(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        table_id,
        AllTableRelations::CanReadAssignments,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &object,
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &object)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetTableAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments for a view
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/view/{view_id}/assignments",
    params(
        GetViewAssignmentsQuery,
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("view_id" = Uuid, Path, description = "View ID"),
    ),
    responses(
            (status = 200, body = GetViewAssignmentsResponse),
    )
))]
async fn get_view_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, view_id)): Path<(WarehouseId, ViewId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetViewAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetViewAssignmentsResponse>)> {
    let authorizer = api_context.v1_state.authz;
    let object = (warehouse_id, view_id).to_openfga();

    let event_ctx = APIEventContext::for_view(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        view_id,
        AllViewRelations::CanReadAssignments,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            event_ctx.action().clone(),
            &object,
        )
        .await;

    let _ = event_ctx.emit_authz(authz_result)?;

    let assignments = get_relations(authorizer, query.relations, &object)
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok((
        StatusCode::OK,
        Json(GetViewAssignmentsResponse { assignments }),
    ))
}

/// Update permissions for this server
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/server/assignments",
    request_body = UpdateServerAssignmentsRequest,
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_server_assignments<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateServerAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;
    let server_id = authorizer.openfga_server().clone();

    let event_ctx = APIEventContext::for_server(
        Arc::new(metadata),
        api_context.v1_state.events,
        Arc::new(request.clone()),
    );
    let authz_result = checked_write(
        authorizer,
        event_ctx.request_metadata().actor(),
        request.writes,
        request.deletes,
        &server_id,
    )
    .await;
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Update permissions for the default project
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/project/assignments",
    request_body = UpdateProjectAssignmentsRequest,
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_project_assignments<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateProjectAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;
    let project_id = metadata
        .preferred_project_id()
        .ok_or(OpenFGAError::NoProjectId)
        .map_err(authz_to_error_no_audit)?;

    let event_ctx = APIEventContext::for_project(
        Arc::new(metadata),
        api_context.v1_state.events,
        project_id,
        Arc::new(request.clone()),
    );
    let authz_result = checked_write(
        authorizer,
        event_ctx.request_metadata().actor(),
        request.writes,
        request.deletes,
        &event_ctx.user_provided_entity().to_openfga(),
    )
    .await;
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Update permissions for a project
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/project/{project_id}/assignments",
    request_body = UpdateProjectAssignmentsRequest,
    params(
        ("project_id" = Option<String>, Path, description = PROJECT_ID_HEADER_DESCRIPTION),
    ),
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_project_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(project_id): Path<ProjectId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateProjectAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_project(
        Arc::new(metadata),
        api_context.v1_state.events,
        project_id,
        Arc::new(request.clone()),
    );
    let authz_result = checked_write(
        authorizer,
        event_ctx.request_metadata().actor(),
        request.writes,
        request.deletes,
        &event_ctx.user_provided_entity().to_openfga(),
    )
    .await;
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Update permissions for a warehouse
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/assignments",
    request_body = UpdateWarehouseAssignmentsRequest,
    params(
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
    ),
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_warehouse_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateWarehouseAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_warehouse(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        Arc::new(request.clone()),
    );
    let authz_result = checked_write(
        authorizer,
        event_ctx.request_metadata().actor(),
        request.writes,
        request.deletes,
        &event_ctx.user_provided_entity().to_openfga(),
    )
    .await;
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Update permissions for a namespace
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/namespace/{namespace_id}/assignments",
    request_body = UpdateNamespaceAssignmentsRequest,
    params(
        ("namespace_id" = Uuid, Path, description = "Namespace ID"),
    ),
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_namespace_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(namespace_id): Path<NamespaceId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateNamespaceAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_namespace_only_id(
        Arc::new(metadata),
        api_context.v1_state.events,
        namespace_id,
        Arc::new(request.clone()),
    );
    let authz_result = checked_write(
        authorizer,
        event_ctx.request_metadata().actor(),
        request.writes,
        request.deletes,
        &namespace_id.to_openfga(),
    )
    .await;
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Update permissions for a table
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/assignments",
    request_body = UpdateTableAssignmentsRequest,
    params(
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("table_id" = Uuid, Path, description = "Table ID"),
    ),
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_table_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, table_id)): Path<(WarehouseId, TableId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateTableAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_table(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        table_id,
        Arc::new(request.clone()),
    );
    let authz_result = checked_write(
        authorizer,
        event_ctx.request_metadata().actor(),
        request.writes,
        request.deletes,
        &(warehouse_id, table_id).to_openfga(),
    )
    .await;
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Update permissions for a view
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/warehouse/{warehouse_id}/view/{view_id}/assignments",
    request_body = UpdateViewAssignmentsRequest,
    params(
        ("warehouse_id" = Uuid, Path, description = "Warehouse ID"),
        ("view_id" = Uuid, Path, description = "View ID"),
    ),
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_view_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path((warehouse_id, view_id)): Path<(WarehouseId, ViewId)>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateViewAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_view(
        Arc::new(metadata),
        api_context.v1_state.events,
        warehouse_id,
        view_id,
        Arc::new(request.clone()),
    );
    let authz_result = checked_write(
        authorizer,
        event_ctx.request_metadata().actor(),
        request.writes,
        request.deletes,
        &(warehouse_id, view_id).to_openfga(),
    )
    .await;
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

// Update permissions for a role
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/role/{role_id}/assignments",
    request_body = UpdateRoleAssignmentsRequest,
    params(
        ("role_id" = Uuid, Path, description = "Role ID"),
    ),
    responses(
            (status = 204, description = "Permissions updated successfully"),
    )
))]
async fn update_role_assignments_by_id<C: CatalogStore, S: SecretStore>(
    Path(role_id): Path<RoleId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateRoleAssignmentsRequest>,
) -> Result<StatusCode> {
    let authorizer = api_context.v1_state.authz;

    let event_ctx = APIEventContext::for_role(
        Arc::new(metadata),
        api_context.v1_state.events,
        role_id,
        Arc::new(request.clone()),
    );

    // Improve error message of role being assigned to itself
    let authz_result = 'authz: {
        for assignment in &request.writes {
            let assignee = match assignment {
                RoleAssignment::Ownership(r) | RoleAssignment::Assignee(r) => r,
            };
            if assignee == &UserOrRole::Role(role_id.into_assignees()) {
                break 'authz Err(OpenFGAError::SelfAssignment(role_id.to_string()));
            }
        }
        checked_write(
            authorizer,
            event_ctx.request_metadata().actor(),
            request.writes,
            request.deletes,
            &role_id.to_openfga(),
        )
        .await
    };
    let _ = event_ctx.emit_authz(authz_result)?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg_attr(feature = "open-api", derive(OpenApi))]
#[cfg_attr(feature = "open-api", openapi(
    tags(
        (name = "permissions-openfga", description = "Authorization and permissions management using OpenFGA"),
    ),
    paths(
        check,
        get_authorizer_namespace_actions,
        get_authorizer_project_actions,
        get_authorizer_role_actions,
        get_authorizer_server_actions,
        get_authorizer_table_actions,
        get_authorizer_view_actions,
        get_authorizer_warehouse_actions,
        get_namespace_access_by_id,
        get_namespace_assignments_by_id,
        get_namespace_by_id,
        get_project_access_by_id,
        get_project_access,
        get_project_assignments_by_id,
        get_project_assignments,
        get_role_access_by_id,
        get_role_assignments_by_id,
        get_server_access,
        get_server_assignments,
        get_table_access_by_id,
        get_table_assignments_by_id,
        get_view_access_by_id,
        get_view_assignments_by_id,
        get_warehouse_access_by_id,
        get_warehouse_assignments_by_id,
        get_warehouse_by_id,
        set_namespace_managed_access,
        set_warehouse_managed_access,
        update_namespace_assignments_by_id,
        update_project_assignments_by_id,
        update_project_assignments,
        update_role_assignments_by_id,
        update_server_assignments,
        update_table_assignments_by_id,
        update_view_assignments_by_id,
        update_warehouse_assignments_by_id,
    ),
    // auto-discovery seems to be broken for these
    components(schemas(NamespaceRelation,
                       ProjectRelation,
                       RoleRelation,
                       ServerRelation,
                       TableRelation,
                       ViewRelation,
                       WarehouseRelation))
))]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct ApiDoc;

#[allow(clippy::too_many_lines)]
pub(super) fn new_v1_router<C: CatalogStore, S: SecretStore>()
-> Router<ApiContext<State<OpenFGAAuthorizer, C, S>>> {
    Router::new()
        .route(
            "/permissions/role/{role_id}/access",
            get(get_role_access_by_id),
        )
        .route(
            "/permissions/role/{role_id}/authorizer-actions",
            get(get_authorizer_role_actions),
        )
        .route("/permissions/server/access", get(get_server_access))
        .route(
            "/permissions/server/authorizer-actions",
            get(get_authorizer_server_actions),
        )
        .route("/permissions/project/access", get(get_project_access))
        .route(
            "/permissions/project/authorizer-actions",
            get(get_authorizer_project_actions),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/access",
            get(get_warehouse_access_by_id),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/authorizer-actions",
            get(get_authorizer_warehouse_actions),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}",
            get(get_warehouse_by_id),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/managed-access",
            post(set_warehouse_managed_access),
        )
        .route(
            "/permissions/project/assignments",
            get(get_project_assignments).post(update_project_assignments),
        )
        .route(
            "/permissions/project/{project_id}/access",
            get(get_project_access_by_id),
        )
        .route(
            "/permissions/namespace/{namespace_id}/access",
            get(get_namespace_access_by_id),
        )
        .route(
            "/permissions/namespace/{namespace_id}/authorizer-actions",
            get(get_authorizer_namespace_actions),
        )
        .route(
            "/permissions/namespace/{namespace_id}",
            get(get_namespace_by_id),
        )
        .route(
            "/permissions/namespace/{namespace_id}/managed-access",
            post(set_namespace_managed_access),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/table/{table_id}/access",
            get(get_table_access_by_id),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/table/{table_id}/authorizer-actions",
            get(get_authorizer_table_actions),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/view/{view_id}/access",
            get(get_view_access_by_id),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/view/{view_id}/authorizer-actions",
            get(get_authorizer_view_actions),
        )
        .route(
            "/permissions/role/{role_id}/assignments",
            get(get_role_assignments_by_id).post(update_role_assignments_by_id),
        )
        .route(
            "/permissions/server/assignments",
            get(get_server_assignments).post(update_server_assignments),
        )
        .route(
            "/permissions/project/{project_id}/assignments",
            get(get_project_assignments_by_id).post(update_project_assignments_by_id),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/assignments",
            get(get_warehouse_assignments_by_id).post(update_warehouse_assignments_by_id),
        )
        .route(
            "/permissions/namespace/{namespace_id}/assignments",
            get(get_namespace_assignments_by_id).post(update_namespace_assignments_by_id),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/table/{table_id}/assignments",
            get(get_table_assignments_by_id).post(update_table_assignments_by_id),
        )
        .route(
            "/permissions/warehouse/{warehouse_id}/view/{view_id}/assignments",
            get(get_view_assignments_by_id).post(update_view_assignments_by_id),
        )
        .route("/permissions/check", post(check))
}

async fn get_relations<RA: Assignment>(
    authorizer: OpenFGAAuthorizer,
    query_relations: Option<Vec<RA::Relation>>,
    object: &str,
) -> OpenFGAResult<Vec<RA>> {
    let relations = query_relations.unwrap_or_else(|| RA::Relation::iter().collect());

    let relations = relations.iter().map(|relation| async {
        authorizer
            .clone()
            .read_all(Some(ReadRequestTupleKey {
                user: String::new(),
                relation: relation.to_openfga().to_string(),
                object: object.to_string(),
            }))
            .await?
            .into_iter()
            .filter_map(|t| t.key)
            .map(|t| RA::try_from_user(&t.user, relation))
            .collect::<OpenFGAResult<Vec<RA>>>()
    });

    let relations = futures::future::try_join_all(relations)
        .await?
        .into_iter()
        .flatten()
        .collect();

    Ok(relations)
}

async fn get_allowed_actions<A: ReducedRelation + IntoEnumIterator>(
    authorizer: OpenFGAAuthorizer,
    actor: &Actor,
    object: &str,
    for_principal: Option<&UserOrRole>,
) -> OpenFGAResult<Vec<A>> {
    let openfga_actor = actor.to_openfga();
    let openfga_object = object.to_string();

    if for_principal.is_some() || actor == &Actor::Anonymous {
        // AuthZ
        let key = CheckRequestTupleKey {
            user: openfga_actor.clone(),
            // This is identical for all entities and checked in unittests. Hence we use `RoleAction`
            relation: RoleAction::ReadAssignments.to_openfga().to_string(),
            object: openfga_object.clone(),
        };

        let allowed = authorizer.clone().check(key).await?;
        if !allowed {
            return Err(OpenFGAError::Unauthorized {
                relation: RoleAction::ReadAssignments.to_openfga().to_string(),
                object: object.to_string(),
            });
        }
    }

    let actions = A::iter().collect::<Vec<_>>();
    let for_principal = for_principal
        .map(super::entities::OpenFgaEntity::to_openfga)
        .unwrap_or(openfga_actor.clone());

    let actions = actions.iter().map(|action| async {
        let key = CheckRequestTupleKey {
            user: for_principal.clone(),
            relation: action.to_openfga().to_string(),
            object: openfga_object.clone(),
        };

        let allowed = authorizer.clone().check(key).await?;

        OpenFGAResult::Ok(Some(action.clone()).filter(|_| allowed))
    });
    let actions = futures::future::try_join_all(actions)
        .await?
        .into_iter()
        .flatten()
        .collect();

    Ok(actions)
}

async fn checked_write<RA: Assignment>(
    authorizer: OpenFGAAuthorizer,
    actor: &Actor,
    writes: Vec<RA>,
    deletes: Vec<RA>,
    object: &str,
) -> OpenFGAResult<()> {
    // Fail fast
    if actor == &Actor::Anonymous {
        return Err(OpenFGAError::AuthenticationRequired);
    }
    let all_modifications = writes.iter().chain(deletes.iter()).collect::<Vec<_>>();
    // ---------------------------- AUTHZ CHECKS ----------------------------
    let openfga_actor = actor.to_openfga();

    let grant_relations = all_modifications
        .iter()
        .map(|action| action.relation().grant_relation())
        .collect::<HashSet<_>>();

    if matches!(
        actor,
        Actor::Role {
            principal: _,
            assumed_role: _
        }
    ) && (object.starts_with("namespace:")
        || object.starts_with("lakekeeper_table")
        || object.starts_with("lakekeeper_view"))
    {
        // Currently not supported as we are missing public usersets for managed access
        return Err(OpenFGAError::GrantRoleWithAssumedRole);
    }

    futures::future::try_join_all(grant_relations.iter().map(|relation| async {
        let key = CheckRequestTupleKey {
            user: openfga_actor.clone(),
            relation: relation.to_string(),
            object: object.to_string(),
        };

        let allowed = authorizer.clone().check(key).await?;
        if allowed {
            Ok(())
        } else {
            Err(OpenFGAError::Unauthorized {
                relation: relation.to_string(),
                object: object.to_string(),
            })
        }
    }))
    .await?;

    // ---------------------- APPLY WRITE OPERATIONS -----------------------
    let writes = writes
        .into_iter()
        .map(|ra| TupleKey {
            user: ra.openfga_user(),
            relation: ra.relation().to_openfga().to_string(),
            object: object.to_string(),
            condition: None,
        })
        .collect();
    let deletes = deletes
        .into_iter()
        .map(|ra| TupleKeyWithoutCondition {
            user: ra.openfga_user(),
            relation: ra.relation().to_openfga().to_string(),
            object: object.to_string(),
        })
        .collect();
    authorizer.write(Some(writes), Some(deletes)).await
}

async fn get_managed_access<T: OpenFgaEntity>(
    authorizer: &OpenFGAAuthorizer,
    entity: &T,
) -> OpenFGAResult<bool> {
    let tuples = authorizer
        .read(
            2,
            ReadRequestTupleKey {
                user: String::new(),
                relation: AllNamespaceRelations::ManagedAccess.to_string(),
                object: entity.to_openfga(),
            },
            None,
        )
        .await?;

    Ok(!tuples.tuples.is_empty())
}

async fn set_managed_access<T: OpenFgaEntity>(
    authorizer: OpenFGAAuthorizer,
    entity: &T,
    managed: bool,
) -> OpenFGAResult<()> {
    let has_managed_access = get_managed_access(&authorizer, entity).await?;
    if managed == has_managed_access {
        return Ok(());
    }

    let tuples = vec![
        TupleKey {
            user: "user:*".to_string(),
            relation: AllNamespaceRelations::ManagedAccess.to_string(),
            object: entity.to_openfga(),
            condition: None,
        },
        TupleKey {
            user: "role:*".to_string(),
            relation: AllNamespaceRelations::ManagedAccess.to_string(),
            object: entity.to_openfga(),
            condition: None,
        },
    ];

    if managed {
        authorizer.write(Some(tuples), None).await?;
    } else {
        let tuples_without_condition = tuples
            .into_iter()
            .map(|t| TupleKeyWithoutCondition {
                user: t.user,
                relation: t.relation,
                object: t.object,
            })
            .collect();
        authorizer
            .write(None, Some(tuples_without_condition))
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use lakekeeper::service::{NamespaceHierarchy, UserId};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_namespace_manage_access_is_equal_to_warehouse_manage_access() {
        // Required for set_managed_access / get_managed_access
        assert_eq!(
            AllNamespaceRelations::ManagedAccess.to_string(),
            AllWarehouseRelation::_ManagedAccess.to_string()
        );
    }

    fn random_namespace(namespace_id: NamespaceId) -> NamespaceHierarchy {
        NamespaceHierarchy::new_with_id(Uuid::nil().into(), namespace_id)
    }

    #[test]
    fn test_get_role_assignments_response_serde() {
        let response = GetRoleAssignmentsResponse {
            assignments: vec![
                RoleAssignment::Ownership(UserOrRole::User(UserId::new_unchecked("oidc", "user1"))),
                RoleAssignment::Assignee(UserOrRole::Role(
                    RoleId::new(Uuid::from_str("b0ef03ea-f314-42df-ae26-dc5eeea8259f").unwrap())
                        .into_assignees(),
                )),
            ],
        };
        let serialized = serde_json::to_value(&response).unwrap();
        println!(
            "Serialized: {}",
            serde_json::to_string_pretty(&response).unwrap()
        );
        let expected = serde_json::json!({
          "assignments": [
            {
              "type": "ownership",
              "user": "oidc~user1"
            },
            {
              "type": "assignee",
              "role": "b0ef03ea-f314-42df-ae26-dc5eeea8259f"
            }
          ]
        });
        assert_eq!(serialized, expected);
    }

    mod openfga_integration_tests {
        use std::collections::HashMap;

        use lakekeeper::{
            service::{
                ResolvedWarehouse,
                authn::UserId,
                authz::{Authorizer, NamespaceParent},
            },
            tokio,
        };
        use openfga_client::client::TupleKey;
        use uuid::Uuid;

        use super::{super::*, *};
        use crate::migration::tests::authorizer_for_empty_store;

        #[tokio::test]
        async fn test_cannot_assign_role_to_itself() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let role_id = RoleId::new(Uuid::nil());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id.to_openfga(),
                        relation: RoleRelation::Ownership.to_openfga().to_string(),
                        object: role_id.to_openfga(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let result = checked_write(
                authorizer.clone(),
                &Actor::Principal(user_id.clone()),
                vec![RoleAssignment::Assignee(role_id.into_assignees().into())],
                vec![],
                &role_id.to_openfga(),
            )
            .await;
            result.unwrap_err();
        }

        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_get_relations() {
            let (_, authorizer) = authorizer_for_empty_store().await;
            let openfga_server = authorizer.openfga_server();
            let relations: Vec<ServerAssignment> =
                get_relations(authorizer.clone(), None, &openfga_server)
                    .await
                    .unwrap();
            assert!(
                relations.is_empty(),
                "Expected no relations, found: {relations:?}",
            );

            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id.to_openfga(),
                        relation: ServerRelation::Admin.to_openfga().to_string(),
                        object: openfga_server.clone(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let relations: Vec<ServerAssignment> =
                get_relations(authorizer.clone(), None, &openfga_server)
                    .await
                    .unwrap();
            assert_eq!(relations.len(), 1);
            assert_eq!(relations, vec![ServerAssignment::Admin(user_id.into())]);
        }

        /// Verifies that [`Authorizer::batch_check`] correctly dis- and reassembles the input.
        ///
        /// Generates a user and a large number of namespaces. For each namespace, it is chosen
        /// randomly whether the user is granted `modify` permissions. Permissions for all of these
        /// namespaces are then queried via `batch_check`. As there are many namespaces with random
        /// `modify` assignments, getting the correct response provides sufficiently high
        /// probability that `batch_check` is implemented correctly.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_batch_check() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let user_id_assignee = UserId::new_unchecked("kubernetes", &Uuid::now_v7().to_string());

            // Generate namespaces. For each randomly decide if assignee is granted modify.
            let write_chunk_size = 100; // see [`Authorizer::write`]
            let namespace_ids: Vec<_> = (0..1000).map(|_| NamespaceId::new_random()).collect();
            let mut permissions = Vec::with_capacity(namespace_ids.len());
            let mut to_grant = vec![];
            let mut rng = fastrand::Rng::with_seed(42);
            for ns in &namespace_ids {
                let may_modify: bool = rng.bool();
                permissions.push(may_modify);
                if may_modify {
                    to_grant.push(TupleKey {
                        user: user_id_assignee.to_openfga(),
                        relation: NamespaceRelation::Modify.to_openfga().to_string(),
                        object: ns.to_openfga(),
                        condition: None,
                    });
                }
            }

            // Initially assignee can not delete any of the namespaces.
            let namespaces = namespace_ids
                .iter()
                .copied()
                .map(random_namespace)
                .collect::<Vec<_>>();
            let res = authorizer
                .are_allowed_namespace_actions_impl(
                    &RequestMetadata::test_user(user_id_assignee.clone()),
                    None,
                    &ResolvedWarehouse::new_random(),
                    &HashMap::new(),
                    &namespaces
                        .iter()
                        .map(|ns| (&ns.namespace, AllNamespaceRelations::CanDelete))
                        .collect::<Vec<_>>(),
                )
                .await
                .unwrap();
            assert_eq!(res, vec![false; namespace_ids.len()]);

            for grant_chunk in to_grant.chunks(write_chunk_size) {
                authorizer
                    .write(Some(grant_chunk.to_vec()), None)
                    .await
                    .unwrap();
            }

            // The response matches the randomly granted permissions.
            // Note: `are_allowed_namespace_actions` calls `batch_check` internally.
            let res = authorizer
                .are_allowed_namespace_actions_impl(
                    &RequestMetadata::test_user(user_id_assignee.clone()),
                    None,
                    &ResolvedWarehouse::new_random(),
                    &HashMap::new(),
                    &namespaces
                        .iter()
                        .map(|ns| (&ns.namespace, AllNamespaceRelations::CanDelete))
                        .collect::<Vec<_>>(),
                )
                .await
                .unwrap();
            assert_eq!(res, permissions);
        }

        #[test]
        #[tracing_test::traced_test]
        fn test_can_read_assignments_identical() {
            let role_assignment = RoleAction::ReadAssignments.to_openfga().to_string();
            assert_eq!(
                role_assignment,
                ServerAction::ReadAssignments.to_openfga().to_string()
            );
            assert_eq!(
                role_assignment,
                ProjectAction::ReadAssignments.to_openfga().to_string()
            );
            assert_eq!(
                role_assignment,
                WarehouseAction::ReadAssignments.to_openfga().to_string()
            );
            assert_eq!(
                role_assignment,
                NamespaceAction::ReadAssignments.to_openfga().to_string()
            );
            assert_eq!(
                role_assignment,
                TableAction::ReadAssignments.to_openfga().to_string()
            );
            assert_eq!(
                role_assignment,
                ViewAction::ReadAssignments.to_openfga().to_string()
            );
        }

        #[tokio::test]
        async fn test_get_allowed_actions_as_user() {
            let (_, authorizer) = authorizer_for_empty_store().await;
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let actor = Actor::Principal(user_id.clone());
            let openfga_server = authorizer.openfga_server();
            let access: Vec<ServerAction> =
                get_allowed_actions(authorizer.clone(), &actor, &openfga_server, None)
                    .await
                    .unwrap();
            assert!(access.is_empty());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id.to_openfga(),
                        relation: ServerRelation::Admin.to_openfga().to_string(),
                        object: openfga_server.clone(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let access: Vec<ServerAction> =
                get_allowed_actions(authorizer.clone(), &actor, &openfga_server, None)
                    .await
                    .unwrap();
            for action in ServerAction::iter() {
                assert!(access.contains(&action));
            }
        }

        #[tokio::test]
        async fn test_get_allowed_actions_as_role() {
            let (_, authorizer) = authorizer_for_empty_store().await;
            let openfga_server = authorizer.openfga_server();
            let role_id = RoleId::new(Uuid::now_v7());
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let actor = Actor::Role {
                principal: user_id.clone(),
                assumed_role: role_id,
            };
            let access: Vec<ServerAction> =
                get_allowed_actions(authorizer.clone(), &actor, &openfga_server, None)
                    .await
                    .unwrap();
            assert!(access.is_empty());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: role_id.into_assignees().to_openfga(),
                        relation: ServerRelation::Admin.to_openfga().to_string(),
                        object: openfga_server.clone(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let access: Vec<ServerAction> =
                get_allowed_actions(authorizer.clone(), &actor, &openfga_server, None)
                    .await
                    .unwrap();
            for action in ServerAction::iter() {
                assert!(access.contains(&action));
            }
        }

        #[tokio::test]
        async fn test_get_allowed_actions_for_other_principal() {
            let (_, authorizer) = authorizer_for_empty_store().await;
            let openfga_server = authorizer.openfga_server();
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let role_id = RoleId::new(Uuid::now_v7());
            let actor = Actor::Principal(user_id.clone());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id.to_openfga(),
                        relation: ServerRelation::Admin.to_openfga().to_string(),
                        object: openfga_server.clone(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let access: Vec<ServerAction> = get_allowed_actions(
                authorizer.clone(),
                &actor,
                &openfga_server,
                Some(&role_id.into_assignees().into()),
            )
            .await
            .unwrap();
            assert!(access.is_empty());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: role_id.into_assignees().to_openfga(),
                        relation: ServerRelation::Admin.to_openfga().to_string(),
                        object: openfga_server.clone(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let access: Vec<ServerAction> = get_allowed_actions(
                authorizer.clone(),
                &actor,
                &openfga_server,
                Some(&role_id.into_assignees().into()),
            )
            .await
            .unwrap();
            for action in ServerAction::iter() {
                assert!(access.contains(&action));
            }
        }

        #[tokio::test]
        async fn test_checked_write() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let user1_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let user2_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            let openfga_server = authorizer.openfga_server();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user1_id.to_openfga(),
                        relation: ServerRelation::Admin.to_openfga().to_string(),
                        object: openfga_server.clone(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            checked_write(
                authorizer.clone(),
                &Actor::Principal(user1_id.clone()),
                vec![ServerAssignment::Admin(user2_id.into())],
                vec![],
                &openfga_server,
            )
            .await
            .unwrap();

            let relations: Vec<ServerAssignment> =
                get_relations(authorizer.clone(), None, &openfga_server)
                    .await
                    .unwrap();
            assert_eq!(relations.len(), 2);
        }

        #[tokio::test]
        async fn test_assign_to_role() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let user_id_owner = UserId::new_unchecked("kubernetes", &Uuid::now_v7().to_string());
            let role_id_1 = RoleId::new(Uuid::nil());
            let role_id_2 = RoleId::new(Uuid::now_v7());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id_owner.to_openfga(),
                        relation: RoleRelation::Ownership.to_openfga().to_string(),
                        object: role_id_1.to_openfga(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            checked_write(
                authorizer.clone(),
                &Actor::Principal(user_id_owner.clone()),
                vec![
                    RoleAssignment::Assignee(user_id_owner.into()),
                    RoleAssignment::Assignee(role_id_2.into_assignees().into()),
                ],
                vec![],
                &role_id_1.to_openfga(),
            )
            .await
            .unwrap();

            let relations: Vec<RoleAssignment> =
                get_relations(authorizer.clone(), None, &role_id_1.to_openfga())
                    .await
                    .unwrap();
            assert_eq!(relations.len(), 3);
        }

        #[tokio::test]
        async fn test_assign_to_project() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let user_id_owner = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let user_id_assignee = UserId::new_unchecked("kubernetes", &Uuid::nil().to_string());
            let role_id = RoleId::new(Uuid::now_v7());
            let project_id = ProjectId::from(Uuid::nil());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id_owner.to_openfga(),
                        relation: ProjectRelation::ProjectAdmin.to_openfga().to_string(),
                        object: project_id.to_openfga(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            checked_write(
                authorizer.clone(),
                &Actor::Principal(user_id_owner.clone()),
                vec![
                    ProjectAssignment::Describe(UserOrRole::Role(role_id.into_assignees())),
                    ProjectAssignment::DataAdmin(UserOrRole::Role(role_id.into_assignees())),
                    ProjectAssignment::DataAdmin(UserOrRole::User(user_id_assignee.clone())),
                ],
                vec![],
                &project_id.to_openfga(),
            )
            .await
            .unwrap();

            let relations: Vec<ProjectAssignment> =
                get_relations(authorizer.clone(), None, &project_id.to_openfga())
                    .await
                    .unwrap();
            assert_eq!(relations.len(), 4);
        }

        #[tokio::test]
        async fn test_set_namespace_managed_access() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let namespace_id = NamespaceId::from(Uuid::now_v7());
            let managed = get_managed_access(&authorizer, &namespace_id)
                .await
                .unwrap();
            assert!(!managed);

            set_managed_access(authorizer.clone(), &namespace_id, false)
                .await
                .unwrap();

            let managed = get_managed_access(&authorizer, &namespace_id)
                .await
                .unwrap();
            assert!(!managed);

            set_managed_access(authorizer.clone(), &namespace_id, true)
                .await
                .unwrap();

            let managed = get_managed_access(&authorizer, &namespace_id)
                .await
                .unwrap();
            assert!(managed);

            set_managed_access(authorizer.clone(), &namespace_id, true)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_warehouse_managed_access() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let warehouse_id = WarehouseId::from(Uuid::now_v7());
            let managed = get_managed_access(&authorizer, &warehouse_id)
                .await
                .unwrap();
            assert!(!managed);

            set_managed_access(authorizer.clone(), &warehouse_id, false)
                .await
                .unwrap();

            let managed = get_managed_access(&authorizer, &warehouse_id)
                .await
                .unwrap();
            assert!(!managed);

            set_managed_access(authorizer.clone(), &warehouse_id, true)
                .await
                .unwrap();

            let managed = get_managed_access(&authorizer, &warehouse_id)
                .await
                .unwrap();
            assert!(managed);

            set_managed_access(authorizer.clone(), &warehouse_id, true)
                .await
                .unwrap();
        }

        /// Test batch warehouse authorization with mixed permissions
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_batch_warehouse_authorization_mixed() {
            use lakekeeper::service::authz::Authorizer;

            let (_, authorizer) = authorizer_for_empty_store().await;
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            // Create 5 warehouses
            let warehouse_ids: Vec<WarehouseId> =
                (0..5).map(|_| WarehouseId::from(Uuid::now_v7())).collect();

            // Grant Describe permission to warehouses 1 and 3
            for &warehouse_id in &[warehouse_ids[1], warehouse_ids[3]] {
                authorizer
                    .write(
                        Some(vec![TupleKey {
                            user: user_id.to_openfga(),
                            relation: WarehouseRelation::Describe.to_openfga().to_string(),
                            object: warehouse_id.to_openfga(),
                            condition: None,
                        }]),
                        None,
                    )
                    .await
                    .unwrap();
            }

            let metadata = RequestMetadata::test_user(user_id);
            let warehouses: Vec<ResolvedWarehouse> = warehouse_ids
                .iter()
                .map(|&id| ResolvedWarehouse::new_with_id(id))
                .collect();

            let actions: Vec<_> = warehouses
                .iter()
                .map(|w| (w, AllWarehouseRelation::CanGetMetadata))
                .collect();

            let results = authorizer
                .are_allowed_warehouse_actions_impl(&metadata, None, &actions)
                .await
                .unwrap();

            // Should be: false, true, false, true, false
            assert_eq!(results, vec![false, true, false, true, false]);
        }

        /// Test batch namespace authorization with large batch to test batch chunking
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_batch_namespace_authorization_large_batch() {
            use lakekeeper::service::authz::Authorizer;

            let (_, authorizer) = authorizer_for_empty_store().await;
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            // Create a large number of namespaces (more than typical batch size of 50)
            let num_namespaces = 120;
            let namespace_ids: Vec<NamespaceId> = (0..num_namespaces)
                .map(|_| NamespaceId::new_random())
                .collect();

            // Grant Modify permission to every other namespace
            for (i, &namespace_id) in namespace_ids.iter().enumerate() {
                if i % 2 == 0 {
                    authorizer
                        .write(
                            Some(vec![TupleKey {
                                user: user_id.to_openfga(),
                                relation: NamespaceRelation::Modify.to_openfga().to_string(),
                                object: namespace_id.to_openfga(),
                                condition: None,
                            }]),
                            None,
                        )
                        .await
                        .unwrap();
                }
            }

            let metadata = RequestMetadata::test_user(user_id);
            let warehouse = ResolvedWarehouse::new_random();

            let namespaces: Vec<_> = namespace_ids
                .iter()
                .map(|&id| random_namespace(id))
                .collect();

            let actions: Vec<_> = namespaces
                .iter()
                .map(|ns| (&ns.namespace, AllNamespaceRelations::CanDelete))
                .collect();

            let results = authorizer
                .are_allowed_namespace_actions_impl(
                    &metadata,
                    None,
                    &warehouse,
                    &HashMap::new(),
                    &actions,
                )
                .await
                .unwrap();

            // Verify results match expected pattern (every other one allowed)
            assert_eq!(results.len(), num_namespaces);
            for (i, &allowed) in results.iter().enumerate() {
                assert_eq!(allowed, i % 2 == 0, "Namespace {i} permission mismatch");
            }
        }

        /// Test batch project authorization with mixed permissions
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_batch_project_authorization_mixed() {
            use lakekeeper::service::authz::Authorizer;

            let (_, authorizer) = authorizer_for_empty_store().await;
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            // Create 4 projects
            let project_ids: Vec<ProjectId> =
                (0..4).map(|_| ProjectId::from(Uuid::now_v7())).collect();

            // Grant Describe to projects 0 and 2
            for idx in [0, 2] {
                authorizer
                    .write(
                        Some(vec![TupleKey {
                            user: user_id.to_openfga(),
                            relation: ProjectRelation::Describe.to_openfga().to_string(),
                            object: project_ids[idx].to_openfga(),
                            condition: None,
                        }]),
                        None,
                    )
                    .await
                    .unwrap();
            }

            let metadata = RequestMetadata::test_user(user_id);

            let actions = project_ids
                .iter()
                .map(|p| (p, AllProjectRelations::CanGetMetadata))
                .collect::<Vec<_>>();

            let results = authorizer
                .are_allowed_project_actions_impl(&metadata, None, &actions)
                .await
                .unwrap();

            // Should be: true, false, true, false
            assert_eq!(results, vec![true, false, true, false]);
        }

        /// Test that batch operations handle all denied permissions correctly
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_batch_authorization_all_denied() {
            use lakekeeper::service::authz::Authorizer;

            let (_, authorizer) = authorizer_for_empty_store().await;
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            // Create namespaces but don't grant any permissions
            let namespace_ids: Vec<NamespaceId> =
                (0..10).map(|_| NamespaceId::new_random()).collect();

            let metadata = RequestMetadata::test_user(user_id);
            let warehouse = ResolvedWarehouse::new_random();

            let namespaces: Vec<_> = namespace_ids
                .iter()
                .map(|&id| random_namespace(id))
                .collect();

            let actions: Vec<_> = namespaces
                .iter()
                .map(|ns| (&ns.namespace, AllNamespaceRelations::CanDelete))
                .collect();

            let results = authorizer
                .are_allowed_namespace_actions_impl(
                    &metadata,
                    None,
                    &warehouse,
                    &HashMap::new(),
                    &actions,
                )
                .await
                .unwrap();

            // All should be denied
            assert_eq!(results, vec![false; 10]);
        }

        /// Test that batch operations handle all allowed permissions correctly
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_batch_authorization_all_allowed() {
            use lakekeeper::service::authz::Authorizer;

            let (_, authorizer) = authorizer_for_empty_store().await;
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            // Create namespaces and grant all permissions
            let namespace_ids: Vec<NamespaceId> =
                (0..8).map(|_| NamespaceId::new_random()).collect();

            // Grant Describe permission to all namespaces
            for &namespace_id in &namespace_ids {
                authorizer
                    .write(
                        Some(vec![TupleKey {
                            user: user_id.to_openfga(),
                            relation: NamespaceRelation::Describe.to_openfga().to_string(),
                            object: namespace_id.to_openfga(),
                            condition: None,
                        }]),
                        None,
                    )
                    .await
                    .unwrap();
            }

            let metadata = RequestMetadata::test_user(user_id);
            let warehouse = ResolvedWarehouse::new_random();

            let namespaces: Vec<_> = namespace_ids
                .iter()
                .map(|&id| random_namespace(id))
                .collect();

            let actions: Vec<_> = namespaces
                .iter()
                .map(|ns| (&ns.namespace, AllNamespaceRelations::CanGetMetadata))
                .collect();

            let results = authorizer
                .are_allowed_namespace_actions_impl(
                    &metadata,
                    None,
                    &warehouse,
                    &HashMap::new(),
                    &actions,
                )
                .await
                .unwrap();

            // All should be allowed
            assert_eq!(results, vec![true; 8]);
        }

        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_managed_access_warehouse_inheritance_user() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Create two users: user A will be namespace owner, user B will receive grants
            let user_a = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let user_b = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let user_c = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            // Create warehouse and namespace
            let warehouse_id = WarehouseId::from(Uuid::now_v7());
            let namespace_id = NamespaceId::new_random();

            // Setup hierarchy: warehouse -> namespace
            authorizer
                .create_namespace(
                    &RequestMetadata::test_user(user_a.clone()),
                    namespace_id,
                    NamespaceParent::Warehouse(warehouse_id),
                )
                .await
                .unwrap();

            // User A grants select on namespace to user B - should succeed (no managed access yet)
            let result = checked_write(
                authorizer.clone(),
                &Actor::Principal(user_a.clone()),
                vec![NamespaceAssignment::Select(user_b.clone().into())],
                vec![],
                &namespace_id.to_openfga(),
            )
            .await;

            assert!(
                result.is_ok(),
                "User A should be able to grant select before managed access is enabled"
            );

            // Set warehouse to managed access
            set_managed_access(authorizer.clone(), &warehouse_id, true)
                .await
                .unwrap();

            // Verify managed access is enabled
            let managed = get_managed_access(&authorizer, &warehouse_id)
                .await
                .unwrap();
            assert!(managed, "Warehouse should have managed access enabled");

            // User A tries to grant select on namespace to user B again - should FAIL
            let result = checked_write(
                authorizer.clone(),
                &Actor::Principal(user_a.clone()),
                vec![NamespaceAssignment::Select(user_c.clone().into())],
                vec![],
                &namespace_id.to_openfga(),
            )
            .await;

            assert!(
                result.is_err(),
                "User A should NOT be able to grant select when warehouse has managed access enabled"
            );
        }

        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test_managed_access_warehouse_inheritance_role() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Create two users: user A will be namespace owner, user B will receive grants
            let user_a = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let role_a = RoleId::new(Uuid::now_v7());

            let actor = Actor::Role {
                principal: user_a.clone(),
                assumed_role: role_a,
            };

            // Create warehouse and namespace
            let warehouse_id = WarehouseId::from(Uuid::now_v7());
            let namespace_id = NamespaceId::new_random();

            // Setup hierarchy: warehouse -> namespace
            authorizer
                .create_namespace(
                    &RequestMetadata::test_user_assumed_role(user_a.clone(), role_a),
                    namespace_id,
                    NamespaceParent::Warehouse(warehouse_id),
                )
                .await
                .unwrap();

            // // User A grants select on namespace- should succeed (no managed access yet)
            // Currently unsupported as the userset check for ownership does not support the public tuple
            // role:*
            // This worked with OpenFGA < 1.8.13
            // checked_write(
            //     authorizer.clone(),
            //     &actor,
            //     vec![NamespaceAssignment::Select(
            //         UserId::new_unchecked("oidc", &Uuid::now_v7().to_string()).into(),
            //     )],
            //     vec![],
            //     &namespace_id.to_openfga(),
            // )
            // .await
            // .unwrap();

            // Set warehouse to managed access
            set_managed_access(authorizer.clone(), &warehouse_id, true)
                .await
                .unwrap();

            // Verify managed access is enabled
            let managed = get_managed_access(&authorizer, &warehouse_id)
                .await
                .unwrap();
            assert!(managed, "Warehouse should have managed access enabled");

            // User A tries to grant select on namespace to user B again - should FAIL
            let result = checked_write(
                authorizer.clone(),
                &Actor::Principal(user_a.clone()),
                vec![NamespaceAssignment::Select(
                    UserId::new_unchecked("oidc", &Uuid::now_v7().to_string()).into(),
                )],
                vec![],
                &namespace_id.to_openfga(),
            )
            .await;
            assert!(
                result.is_err(),
                "User A should NOT be able to grant select when warehouse has managed access enabled"
            );
            let result = checked_write(
                authorizer.clone(),
                &actor,
                vec![NamespaceAssignment::Select(
                    UserId::new_unchecked("oidc", &Uuid::now_v7().to_string()).into(),
                )],
                vec![],
                &namespace_id.to_openfga(),
            )
            .await;
            assert!(
                result.is_err(),
                "User A with assumed role should NOT be able to grant select when warehouse has managed access enabled"
            );
        }
    }
}
