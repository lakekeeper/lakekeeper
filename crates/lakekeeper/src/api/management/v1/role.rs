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
        ArcProjectId, ArcRole, ArcRoleIdent, CachePolicy, CatalogBackendError,
        CatalogCreateRoleRequest, CatalogListRolesByIdFilter, CatalogRoleOps, CatalogStore,
        CreateRoleError, DeleteRoleError, Result, RoleId, RoleProviderId, RoleSourceId,
        SecretStore, State, Transaction, UpdateRoleError,
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
    /// Provider that owns this role (e.g. `"lakekeeper"`, `"oidc"`).
    /// Must be provided together with `source-id`. Omit both to let the server
    /// assign `provider-id = "lakekeeper"` and a fresh UUIDv7 `source-id`.
    #[serde(default)]
    #[builder(default)]
    #[cfg_attr(feature = "open-api", schema(value_type=Option::<String>))]
    pub provider_id: Option<RoleProviderId>,
    /// Identifier of the role in the provider.
    /// Must be provided together with `provider-id`.
    #[builder(default)]
    #[cfg_attr(feature = "open-api", schema(value_type=Option::<String>))]
    pub source_id: Option<RoleSourceId>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct Role {
    /// Globally unique UUID identifier
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub id: RoleId,
    /// Composite project-scoped identifier (`provider~source_id`).
    /// Unique within a project.
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub ident: ArcRoleIdent,
    /// Provider that owns this role (e.g. `"lakekeeper"`, `"oidc"`).
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub provider_id: RoleProviderId,
    /// Identifier of the role in the provider.
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub source_id: RoleSourceId,
    /// Name of the role
    pub name: String,
    /// Description of the role
    pub description: Option<String>,
    /// Project ID in which the role is created.
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub project_id: ArcProjectId,
    /// Timestamp when the role was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the role was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<crate::service::Role> for Role {
    fn from(value: crate::service::Role) -> Self {
        Self {
            id: value.id,
            provider_id: value.ident.provider_id().clone(),
            source_id: value.ident.source_id().clone(),
            ident: value.ident,
            name: value.name,
            description: value.description,
            project_id: value.project_id,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
/// Metadata of a role with reduced information.
/// Returned for cross-project role references.
pub struct RoleMetadata {
    /// Globally unique UUID identifier
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub id: RoleId,
    /// Composite project-scoped identifier (`provider~source_id`).
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub ident: ArcRoleIdent,
    /// Provider that owns this role (e.g. `"lakekeeper"`, `"oidc"`).
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub provider_id: RoleProviderId,
    /// Identifier of the role in the provider.
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub source_id: RoleSourceId,
    /// Name of the role
    pub name: String,
    /// Project ID in which the role is created.
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub project_id: ArcProjectId,
}

impl_arc_into_response!(RoleMetadata);

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct SearchRoleResponse {
    /// List of roles matching the search criteria
    pub roles: Vec<Role>,
}

impl From<crate::service::SearchRoleResponse> for SearchRoleResponse {
    fn from(value: crate::service::SearchRoleResponse) -> Self {
        Self {
            roles: value
                .roles
                .into_iter()
                .map(|r| (*r).clone().into())
                .collect(),
        }
    }
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
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub source_id: RoleSourceId,
    /// New Provider ID of the role.
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub provider_id: RoleProviderId,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListRolesResponse {
    pub roles: Vec<Role>,
    #[serde(alias = "next_page_token")]
    pub next_page_token: Option<String>,
}

impl From<crate::service::ListRolesResponse> for ListRolesResponse {
    fn from(value: crate::service::ListRolesResponse) -> Self {
        Self {
            roles: value
                .roles
                .into_iter()
                .map(|r| (*r).clone().into())
                .collect(),
            next_page_token: value.next_page_token,
        }
    }
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
    #[cfg_attr(feature = "open-api", param(value_type=Option<Vec<String>>))]
    pub source_ids: Option<Vec<RoleSourceId>>,
    /// Filter by provider IDs
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(value_type=Option<Vec<String>>))]
    pub provider_ids: Option<Vec<RoleProviderId>>,
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
    ) -> Result<Role> {
        // -------------------- VALIDATIONS --------------------
        if request.name.is_empty() {
            return Err(ErrorModel::bad_request(
                "Role name cannot be empty".to_string(),
                "EmptyRoleName",
                None,
            )
            .into());
        }
        match (&request.provider_id, &request.source_id) {
            (None, None) | (Some(_), Some(_)) => {}
            _ => {
                return Err(ErrorModel::bad_request(
                    "provider-id and source-id must be provided together, or both omitted",
                    "InvalidRoleIdentifier",
                    None,
                )
                .into());
            }
        }

        let authorizer = context.v1_state.authz;
        let project_id = request_metadata.require_project_id(request.project_id.clone())?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project_arc(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            Arc::new(CatalogProjectAction::CreateRole),
        );
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_create_role::<A, C>(authorizer, catalog_state, &event_ctx, request).await;
        let (event_ctx, role) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(role);
        let result = (**event_ctx.resolved()).clone().into();
        event_ctx.emit_role_created();
        Ok(result)
    }

    async fn list_roles(
        context: ApiContext<State<A, C, S>>,
        query: ListRolesQuery,
        request_metadata: RequestMetadata,
    ) -> Result<ListRolesResponse> {
        // -------------------- VALIDATIONS --------------------
        let project_id = request_metadata.require_project_id(query.project_id.clone())?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project_arc(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            Arc::new(CatalogProjectAction::ListRoles),
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_list_roles::<A, C>(authorizer, catalog_state, &event_ctx, query).await;
        let (_event_ctx, roles) = event_ctx.emit_authz(authz_result)?;
        Ok(roles.into())
    }

    async fn get_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<Role> {
        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::Read,
        );
        let authorizer = context.v1_state.authz;

        let role = C::get_role_by_id_cache_aware(
            &event_ctx.request_metadata().require_project_id(None)?,
            role_id,
            CachePolicy::Skip,
            context.v1_state.catalog,
        )
        .await;

        let authz_result = authorizer
            .require_role_action(event_ctx.request_metadata(), role, CatalogRoleAction::Read)
            .await;

        let (event_ctx, role) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(role);

        Ok((**event_ctx.resolved()).clone().into())
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
    ) -> Result<SearchRoleResponse> {
        let project_id = request_metadata.require_project_id(request.project_id.clone())?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project_arc(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            Arc::new(CatalogProjectAction::SearchRoles),
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_search_role::<A, C>(authorizer, catalog_state, &event_ctx, request).await;
        let (_event_ctx, response) = event_ctx.emit_authz(authz_result)?;
        Ok(response.into())
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
            authorized_delete_role::<A, C>(authorizer, catalog_state, &event_ctx, project_id).await;
        let (event_ctx, role) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(role);
        event_ctx.emit_role_deleted();
        Ok(())
    }

    async fn update_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        request: UpdateRoleRequest,
    ) -> Result<Role> {
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
        let result = (**event_ctx.resolved()).clone().into();
        event_ctx.emit_role_updated();
        Ok(result)
    }

    async fn update_role_source_system(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        request: UpdateRoleSourceSystemRequest,
    ) -> Result<Role> {
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
        let result = (**event_ctx.resolved()).clone().into();
        event_ctx.emit_role_updated();
        Ok(result)
    }
}

async fn authorize_create_role<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<ProjectId, Unresolved, CatalogProjectAction>,
    request: CreateRoleRequest,
) -> Result<ArcRole, AuthZError> {
    let project_id = event_ctx.user_provided_entity_arc_ref();
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

    let source_id = request
        .source_id
        .unwrap_or_else(|| RoleSourceId::new_from_role_id(role_id));
    let provider_id = request.provider_id.unwrap_or_default();
    let catalog_create_role_request = CatalogCreateRoleRequest {
        role_id,
        role_name: &request.name,
        description: description.as_deref(),
        source_id: &source_id,
        provider_id: &provider_id,
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
) -> Result<crate::service::ListRolesResponse, AuthZError> {
    let project_id = event_ctx.user_provided_entity_arc();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();
    authorizer
        .require_project_action(request_metadata, &project_id, *action)
        .await?;

    // -------------------- Business Logic --------------------
    let pagination_query = query.pagination_query();
    let provider_ids = query
        .provider_ids
        .as_ref()
        .map(|v| v.iter().collect::<Vec<_>>());
    let source_ids = query
        .source_ids
        .as_ref()
        .map(|v| v.iter().collect::<Vec<_>>());
    let roles = C::list_roles(
        project_id,
        CatalogListRolesByIdFilter::builder()
            .role_ids(query.role_ids.as_deref())
            .source_ids(source_ids.as_deref())
            .provider_ids(provider_ids.as_deref())
            .build(),
        pagination_query,
        catalog_state,
    )
    .await?;
    Ok(roles)
}

async fn authorize_get_role_metadata<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<RoleId, Unresolved, CatalogRoleAction>,
) -> Result<Arc<RoleMetadata>, AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let role =
        C::get_role_by_id_across_projects_cache_aware(role_id, CachePolicy::Use, catalog_state)
            .await?;

    let role = authorizer
        .require_role_action(request_metadata, Ok(role), *action)
        .await?;

    let role_metadata = RoleMetadata {
        id: role.id,
        source_id: role.source_id().clone(),
        provider_id: role.provider_id().clone(),
        ident: role.ident.clone(),
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
) -> Result<crate::service::SearchRoleResponse, AuthZError> {
    let project_id = event_ctx.user_provided_entity_arc_ref();
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
    Ok(result)
}

async fn authorized_delete_role<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<RoleId, Unresolved, CatalogRoleAction>,
    project_id: ArcProjectId,
) -> Result<ArcRole, AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();

    let role = C::get_role_by_id_cache_aware(
        &project_id,
        role_id,
        CachePolicy::Skip,
        catalog_state.clone(),
    )
    .await;
    let action = event_ctx.action();

    let role = authorizer
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
    Ok(role)
}

async fn authorize_update_role<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<RoleId, Unresolved, CatalogRoleAction>,
    project_id: ArcProjectId,
    request: UpdateRoleRequest,
) -> Result<ArcRole, AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let role = C::get_role_by_id_cache_aware(
        &project_id,
        role_id,
        CachePolicy::Skip,
        catalog_state.clone(),
    )
    .await;

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
    project_id: ArcProjectId,
    request: UpdateRoleSourceSystemRequest,
) -> Result<ArcRole, AuthZError> {
    let role_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let role = C::get_role_by_id_cache_aware(
        &project_id,
        role_id,
        CachePolicy::Skip,
        catalog_state.clone(),
    )
    .await;

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
