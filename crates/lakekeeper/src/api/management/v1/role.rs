use std::sync::Arc;

use axum::{Json, response::IntoResponse};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId,
    api::{
        ApiContext,
        iceberg::{types::PageToken, v1::PaginationQuery},
        management::v1::ApiServer,
    },
    logging::audit::{
        AuditContext,
        events::{AuthorizationDeniedEvent, CreateRoleEvent, DeleteRoleEvent, UpdateRoleEvent},
    },
    request_metadata::RequestMetadata,
    service::{
        CatalogCreateRoleRequest, CatalogListRolesFilter, CatalogRoleOps, CatalogStore, Result,
        RoleId, SecretStore, State, Transaction,
        authz::{
            AuthZProjectOps, AuthZRoleOps, Authorizer, CatalogProjectAction, CatalogRoleAction,
        },
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

        let project_id = request_metadata.require_project_id(request.project_id)?;

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::CreateRole,
            )
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "create_role".to_string(),
                    error: e.to_string(),
                });
            })?;

        // -------------------- Business Logic --------------------
        let description = request.description.filter(|d| !d.is_empty());
        let role_id = RoleId::new_random();

        request_metadata.log_audit(CreateRoleEvent {
            project_id: project_id.clone(),
            role_id,
        });

        let mut t: <C as CatalogStore>::Transaction =
            C::Transaction::begin_write(context.v1_state.catalog).await?;
        let catalog_create_role_request = CatalogCreateRoleRequest {
            role_id,
            role_name: &request.name,
            description: description.as_deref(),
            source_id: request.source_id.as_deref(),
        };
        let user =
            C::create_role(&project_id, catalog_create_role_request, t.transaction()).await?;
        authorizer
            .create_role(&request_metadata, role_id, project_id)
            .await?;
        t.commit().await?;
        Ok(user)
    }

    async fn list_roles(
        context: ApiContext<State<A, C, S>>,
        query: ListRolesQuery,
        request_metadata: RequestMetadata,
    ) -> Result<ListRolesResponse> {
        // -------------------- VALIDATIONS --------------------
        let pagination_query = query.pagination_query();
        let ListRolesQuery {
            role_ids,
            source_ids,
            page_token: _,
            page_size: _,
            project_id,
        } = query;

        let project_id = request_metadata.require_project_id(project_id)?;

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::ListRoles,
            )
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "list_roles".to_string(),
                    error: e.to_string(),
                });
            })?;

        // -------------------- Business Logic --------------------
        C::list_roles(
            &project_id,
            CatalogListRolesFilter::builder()
                .role_ids(role_ids.as_deref())
                .source_ids(
                    source_ids
                        .as_ref()
                        .map(|ids| ids.iter().map(String::as_str).collect::<Vec<_>>())
                        .as_deref(),
                )
                .build(),
            pagination_query,
            context.v1_state.catalog,
        )
        .await
        .map_err(Into::into)
    }

    async fn get_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<Arc<Role>> {
        let authorizer = context.v1_state.authz;

        let role = C::get_role_by_id(
            &request_metadata.require_project_id(None)?,
            role_id,
            context.v1_state.catalog,
        )
        .await;

        let role = authorizer
            .require_role_action(&request_metadata, role, CatalogRoleAction::Read)
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "get_role".to_string(),
                    error: e.to_string(),
                });
            })?;

        Ok(role)
    }

    async fn get_role_metadata(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<RoleMetadata> {
        let authorizer = context.v1_state.authz;

        let role = C::get_role_by_id_across_projects(role_id, context.v1_state.catalog).await?;

        let role = authorizer
            .require_role_action(&request_metadata, Ok(role), CatalogRoleAction::ReadMetadata)
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "get_role_metadata".to_string(),
                    error: e.to_string(),
                });
            })?;

        let role_metadata = RoleMetadata {
            id: role.id,
            name: role.name.clone(),
            project_id: role.project_id.clone(),
        };

        Ok(role_metadata)
    }

    async fn search_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: SearchRoleRequest,
    ) -> Result<SearchRoleResponse> {
        let SearchRoleRequest {
            mut search,
            project_id,
        } = request;
        let project_id = request_metadata.require_project_id(project_id)?;

        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::SearchRoles,
            )
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "search_role".to_string(),
                    error: e.to_string(),
                });
            })?;

        // ------------------- Business Logic -------------------
        if search.chars().count() > 64 {
            search = search.chars().take(64).collect();
        }
        C::search_role(&project_id, &search, context.v1_state.catalog)
            .await
            .map_err(Into::into)
    }

    async fn delete_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<()> {
        let authorizer = context.v1_state.authz;
        let project_id = request_metadata.require_project_id(None)?;

        let role = C::get_role_by_id(&project_id, role_id, context.v1_state.catalog.clone()).await;

        authorizer
            .require_role_action(&request_metadata, role, CatalogRoleAction::Delete)
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "delete_role".to_string(),
                    error: e.to_string(),
                });
            })?;

        request_metadata.log_audit(DeleteRoleEvent { role_id });

        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        C::delete_role(&project_id, role_id, t.transaction()).await?;
        authorizer.delete_role(&request_metadata, role_id).await?;
        t.commit().await
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

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        let project_id = request_metadata.require_project_id(None)?;

        let role = C::get_role_by_id(&project_id, role_id, context.v1_state.catalog.clone()).await;

        authorizer
            .require_role_action(&request_metadata, role, CatalogRoleAction::Update)
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "update_role".to_string(),
                    error: e.to_string(),
                });
            })?;

        request_metadata.log_audit(UpdateRoleEvent { role_id });

        // -------------------- Business Logic --------------------
        let description = request.description.filter(|d| !d.is_empty());

        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let role = C::update_role(
            &project_id,
            role_id,
            &request.name,
            description.as_deref(),
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        Ok(role)
    }

    async fn update_role_source_system(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        request: UpdateRoleSourceSystemRequest,
    ) -> Result<Arc<Role>> {
        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        let project_id = request_metadata.require_project_id(None)?;

        let role = C::get_role_by_id(&project_id, role_id, context.v1_state.catalog.clone()).await;

        authorizer
            .require_role_action(&request_metadata, role, CatalogRoleAction::Update)
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "update_role_source_system".to_string(),
                    error: e.to_string(),
                });
            })?;

        // -------------------- Business Logic --------------------
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let role =
            C::set_role_source_system(&project_id, role_id, &request, t.transaction()).await?;
        t.commit().await?;
        Ok(role)
    }
}
