use std::sync::Arc;

use axum::{Json, response::IntoResponse};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId,
    api::{
        ApiContext,
        iceberg::{types::PageToken, v1::PaginationQuery},
        management::v1::{ApiServer, impl_arc_into_response},
    },
    request_metadata::RequestMetadata,
    service::{
        CatalogBackendError, CatalogCreateRoleRequest, CatalogListRolesFilter, CatalogRoleOps,
        CatalogStore, CreateRoleError, DeleteRoleError, Result, RoleId, SecretStore, State,
        Transaction, UpdateRoleError,
        authz::{
            AuthZError, AuthZProjectOps, AuthZRoleOps, Authorizer, CatalogProjectAction,
            CatalogRoleAction,
        },
        events::{APIEventContext, context::Unresolved},
    },
};

#[derive(Debug, Deserialize, typed_builder::TypedBuilder)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateRoleRequest {
    /// Name of the role to create
    pub name: String,
    /// Description of the role
    #[serde(default)]
    #[builder(default)]
    pub description: Option<String>,
    /// Project ID in which the role is created.
    /// Deprecated: Please use the `x-project-id` header instead.
    #[serde(default)]
    #[builder(default)]
    #[cfg_attr(feature = "open-api", schema(value_type=Option::<String>))]
    pub project_id: Option<ProjectId>,
    /// Identifier of the role in an external system (source of truth).
    /// `source-id` must be unique within a project.
    #[serde(default)]
    #[builder(default)]
    pub source_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct Role {
    /// Globally unique id of this role
    #[cfg_attr(feature = "open-api", schema(value_type=uuid::Uuid))]
    pub id: RoleId,
    /// Name of the role
    pub name: String,
    /// Description of the role
    pub description: Option<String>,
    /// Project ID in which the role is created.
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub project_id: ProjectId,
    /// Identifier of the role in an external system (source of truth).
    /// `source-id` is guaranteed to be unique within a project.
    pub source_id: Option<String>,
    /// Timestamp when the role was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the role was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
/// Metadata of a role with reduced information.
/// Returned for cross-project role references.
pub struct RoleMetadata {
    /// Globally unique id of this role
    #[cfg_attr(feature = "open-api", schema(value_type=uuid::Uuid))]
    pub id: RoleId,
    /// Name of the role
    pub name: String,
    /// Project ID in which the role is created.
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub project_id: ProjectId,
}

impl_arc_into_response!(RoleMetadata);

#[cfg(feature = "test-utils")]
impl Role {
    #[must_use]
    pub fn new_random() -> Self {
        let role_id = RoleId::new_random();
        Self {
            id: role_id,
            name: format!("role-{role_id}"),
            description: Some("A randomly generated role".to_string()),
            source_id: None,
            project_id: ProjectId::new_random(),
            created_at: chrono::Utc::now(),
            updated_at: None,
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct SearchRoleResponse {
    /// List of roles matching the search criteria
    pub roles: Vec<Arc<Role>>,
}

impl_arc_into_response!(SearchRoleResponse);

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UpdateRoleRequest {
    /// Name of the role to create
    pub name: String,
    /// Description of the role. If not set, the description will be removed.
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UpdateRoleSourceSystemRequest {
    /// New Source ID / External ID of the role.
    pub source_id: String,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListRolesResponse {
    pub roles: Vec<Arc<Role>>,
    #[serde(alias = "next_page_token")]
    pub next_page_token: Option<String>,
}

impl_arc_into_response!(ListRolesResponse);

impl IntoResponse for ListRolesResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SearchRoleRequest {
    /// Search string for fuzzy search.
    /// Length is truncated to 64 characters.
    pub search: String,
    /// Deprecated: Please use the `x-project-id` header instead.
    /// Project ID in which the role is created.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", schema(value_type=Option::<String>))]
    pub project_id: Option<ProjectId>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListRolesQuery {
    /// Next page token
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default)]
    pub page_size: Option<i64>,
    /// Project ID from which roles should be listed
    /// Deprecated: Please use the `x-project-id` header instead.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(value_type=Option<String>))]
    pub project_id: Option<ProjectId>,
    /// Filter by role IDs
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(value_type=Option<Vec<uuid::Uuid>>))]
    pub role_ids: Option<Vec<RoleId>>,
    /// Filter by source IDs
    #[serde(default)]
    pub source_ids: Option<Vec<String>>,
}

impl ListRolesQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: self.page_size,
        }
    }
}

impl IntoResponse for SearchRoleResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> Service<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    async fn create_role(
        request: CreateRoleRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<Arc<Role>> {
        // -------------------- VALIDATIONS --------------------
        if request.name.is_empty() {
            return Err(ErrorModel::bad_request(
                "Role name cannot be empty".to_string(),
                "EmptyRoleName",
                None,
            )
            .into());
        }

        let authorizer = context.v1_state.authz;
        let project_id = request_metadata.require_project_id(request.project_id.clone())?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            CatalogProjectAction::CreateRole,
        );
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_create_role::<A, C>(authorizer, catalog_state, &event_ctx, request).await;
        let (event_ctx, role) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = Arc::new(event_ctx.resolve(role));
        Ok(event_ctx.resolved().clone())
    }

    async fn list_roles(
        context: ApiContext<State<A, C, S>>,
        query: ListRolesQuery,
        request_metadata: RequestMetadata,
    ) -> Result<Arc<ListRolesResponse>> {
        // -------------------- VALIDATIONS --------------------
        let project_id = request_metadata.require_project_id(query.project_id.clone())?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            CatalogProjectAction::ListRoles,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_list_roles::<A, C>(authorizer, catalog_state, &event_ctx, query).await;
        let (event_ctx, roles) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(roles);
        Ok(event_ctx.resolved().clone())
    }

    async fn get_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<Arc<Role>> {
        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::Read,
        );
        let authorizer = context.v1_state.authz;

        let role = C::get_role_by_id(
            &event_ctx.request_metadata().require_project_id(None)?,
            role_id,
            context.v1_state.catalog,
        )
        .await;

        let authz_result = authorizer
            .require_role_action(event_ctx.request_metadata(), role, CatalogRoleAction::Read)
            .await;

        let (event_ctx, role) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(role);

        Ok(event_ctx.resolved().clone())
    }

    async fn get_role_metadata(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<Arc<RoleMetadata>> {
        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::ReadMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_get_role_metadata::<A, C>(authorizer, catalog_state, &event_ctx).await;
        let (event_ctx, role_metadata) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(role_metadata);
        Ok(event_ctx.resolved().clone())
    }

    async fn search_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: SearchRoleRequest,
    ) -> Result<Arc<SearchRoleResponse>> {
        let project_id = request_metadata.require_project_id(request.project_id.clone())?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            CatalogProjectAction::SearchRoles,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_search_role::<A, C>(authorizer, catalog_state, &event_ctx, request).await;
        let (event_ctx, response) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(response);
        Ok(event_ctx.resolved().clone())
    }

    async fn delete_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::Delete,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_delete_role::<A, C>(authorizer, catalog_state, &event_ctx, project_id).await;
        event_ctx.emit_authz(authz_result)?;
        Ok(())
    }

    async fn update_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        request: UpdateRoleRequest,
    ) -> Result<Arc<Role>> {
        // -------------------- VALIDATIONS --------------------
        if request.name.is_empty() {
            return Err(ErrorModel::bad_request(
                "Role name cannot be empty".to_string(),
                "EmptyRoleName",
                None,
            )
            .into());
        }

        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::Update,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_update_role::<A, C>(
            authorizer,
            catalog_state,
            &event_ctx,
            project_id,
            request,
        )
        .await;
        let (event_ctx, role) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(role);
        Ok(event_ctx.resolved().clone())
    }

    async fn update_role_source_system(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        request: UpdateRoleSourceSystemRequest,
    ) -> Result<Arc<Role>> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::Update,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_update_role_source_system::<A, C>(
            authorizer,
            catalog_state,
            &event_ctx,
            project_id,
            request,
        )
        .await;
        let (event_ctx, role) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(role);
        Ok(event_ctx.resolved().clone())
    }
}

async fn authorize_create_role<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<ProjectId, Unresolved, CatalogProjectAction>,
    request: CreateRoleRequest,
) -> Result<Arc<Role>, AuthZError> {
    let project_id = event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();
    authorizer
        .require_project_action(request_metadata, project_id, *action)
        .await?;

    // -------------------- Business Logic --------------------
    let description = request.description.filter(|d| !d.is_empty());
    let role_id = RoleId::new_random();
    let mut t: <C as CatalogStore>::Transaction =
        C::Transaction::begin_write(catalog_state.clone())
            .await
            .map_err(|e| CatalogBackendError::new_unexpected(e.error))
            .map_err(CreateRoleError::from)?;
    let catalog_create_role_request = CatalogCreateRoleRequest {
        role_id,
        role_name: &request.name,
        description: description.as_deref(),
        source_id: request.source_id.as_deref(),
    };
    let role = C::create_role(project_id, catalog_create_role_request, t.transaction()).await?;
    authorizer
        .create_role(request_metadata, role_id, project_id.clone())
        .await
        .map_err::<CreateRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    t.commit()
        .await
        .map_err::<CreateRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    Ok(role)
}

async fn authorize_list_roles<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<ProjectId, Unresolved, CatalogProjectAction>,
    query: ListRolesQuery,
) -> Result<Arc<ListRolesResponse>, AuthZError> {
    let project_id = event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();
    authorizer
        .require_project_action(request_metadata, project_id, *action)
        .await?;

    // -------------------- Business Logic --------------------
    let pagination_query = query.pagination_query();
    let roles = C::list_roles(
        project_id,
        CatalogListRolesFilter::builder()
            .role_ids(query.role_ids.as_deref())
            .source_ids(
                query
                    .source_ids
                    .as_ref()
                    .map(|ids| ids.iter().map(String::as_str).collect::<Vec<_>>())
                    .as_deref(),
            )
            .build(),
        pagination_query,
        catalog_state,
    )
    .await?;
    Ok(roles.into())
}

async fn authorize_get_role_metadata<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<RoleId, Unresolved, CatalogRoleAction>,
) -> Result<Arc<RoleMetadata>, AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let role = C::get_role_by_id_across_projects(role_id, catalog_state).await?;

    let role = authorizer
        .require_role_action(request_metadata, Ok(role), *action)
        .await?;

    let role_metadata = RoleMetadata {
        id: role.id,
        name: role.name.clone(),
        project_id: role.project_id.clone(),
    };

    Ok(role_metadata.into())
}

async fn authorize_search_role<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<ProjectId, Unresolved, CatalogProjectAction>,
    request: SearchRoleRequest,
) -> Result<Arc<SearchRoleResponse>, AuthZError> {
    let project_id = event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();
    authorizer
        .require_project_action(request_metadata, project_id, *action)
        .await?;

    // -------------------- Business Logic --------------------
    let mut search = request.search;
    if search.chars().count() > 64 {
        search = search.chars().take(64).collect();
    }
    let result = C::search_role(project_id, &search, catalog_state).await?;
    Ok(result.into())
}

async fn authorize_delete_role<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<RoleId, Unresolved, CatalogRoleAction>,
    project_id: ProjectId,
) -> Result<(), AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();

    let role = C::get_role_by_id(&project_id, role_id, catalog_state.clone()).await;
    let action = event_ctx.action();

    authorizer
        .require_role_action(request_metadata, role, *action)
        .await?;

    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err::<DeleteRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    C::delete_role(&project_id, role_id, t.transaction()).await?;
    authorizer
        .delete_role(request_metadata, role_id)
        .await
        .map_err::<DeleteRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    t.commit()
        .await
        .map_err::<DeleteRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    Ok(())
}

async fn authorize_update_role<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<RoleId, Unresolved, CatalogRoleAction>,
    project_id: ProjectId,
    request: UpdateRoleRequest,
) -> Result<Arc<Role>, AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let role = C::get_role_by_id(&project_id, role_id, catalog_state.clone()).await;

    authorizer
        .require_role_action(request_metadata, role, *action)
        .await?;

    // -------------------- Business Logic --------------------
    let description = request.description.filter(|d| !d.is_empty());

    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err::<UpdateRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    let role = C::update_role(
        &project_id,
        role_id,
        &request.name,
        description.as_deref(),
        t.transaction(),
    )
    .await?;
    t.commit()
        .await
        .map_err::<UpdateRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    Ok(role)
}

async fn authorize_update_role_source_system<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<RoleId, Unresolved, CatalogRoleAction>,
    project_id: ProjectId,
    request: UpdateRoleSourceSystemRequest,
) -> Result<Arc<Role>, AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let role = C::get_role_by_id(&project_id, role_id, catalog_state.clone()).await;

    authorizer
        .require_role_action(request_metadata, role, *action)
        .await?;

    // -------------------- Business Logic --------------------
    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err::<UpdateRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    let role = C::set_role_source_system(&project_id, role_id, &request, t.transaction()).await?;
    t.commit()
        .await
        .map_err::<UpdateRoleError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    Ok(role)
}
