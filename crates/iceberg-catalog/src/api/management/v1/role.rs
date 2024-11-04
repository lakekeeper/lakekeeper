use super::default_page_size;
use crate::api::iceberg::types::PageToken;
use crate::api::iceberg::v1::PaginationQuery;
use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, CatalogProjectAction, CatalogRoleAction};
use crate::service::{Catalog, Result, RoleId, SecretStore, State, Transaction};
use crate::{ProjectIdent, DEFAULT_PROJECT_ID};
use axum::response::IntoResponse;
use axum::Json;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateRoleRequest {
    /// Name of the role to create
    pub name: String,
    /// Description of the role
    #[serde(default)]
    pub description: Option<String>,
    /// Project ID in which the role is created.
    /// Only required if the project ID cannot be inferred and no default project is set.
    #[serde(default)]
    #[schema(value_type=uuid::Uuid)]
    pub project_id: Option<ProjectIdent>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct Role {
    /// Globally unique id of this role
    #[schema(value_type=uuid::Uuid)]
    pub id: RoleId,
    /// Name of the role
    pub name: String,
    /// Description of the role
    pub description: Option<String>,
    /// Project ID in which the role is created.
    #[schema(value_type=uuid::Uuid)]
    pub project_id: ProjectIdent,
    /// Timestamp when the role was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the role was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SearchRoleResponse {
    /// List of users matching the search criteria
    pub roles: Vec<Role>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateRoleRequest {
    /// Name of the role to create
    pub name: String,
    /// Description of the role. If not set, the description will be removed.
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListRolesResponse {
    pub roles: Vec<Role>,
    pub next_page_token: Option<String>,
}

impl IntoResponse for ListRolesResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct SearchRoleRequest {
    /// Search string for fuzzy search.
    /// Length is truncated to 64 characters.
    pub search: String,
    /// Project ID in which the role is created.
    /// Only required if the project ID cannot be inferred from the
    /// users token and no default project is set.
    #[serde(default)]
    pub project_id: Option<ProjectIdent>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListRolesQuery {
    /// Search for a specific role name
    #[serde(default)]
    pub name: Option<String>,
    /// Next page token
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default = "default_page_size")]
    pub page_size: i64,
    /// Project ID from which roles should be listed
    /// Only required if the project ID cannot be inferred from the
    /// users token and no default project is set.
    #[serde(default)]
    #[param(value_type=uuid::Uuid)]
    pub project_id: Option<ProjectIdent>,
}

impl ListRolesQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: Some(self.page_size),
        }
    }
}

impl IntoResponse for SearchRoleResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(super) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
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

        let project_id = require_project_id(request.project_id, &request_metadata)?;

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                project_id,
                &CatalogProjectAction::CanCreateRole,
            )
            .await?;

        // -------------------- Business Logic --------------------
        let description = request.description.filter(|d| !d.is_empty());
        let role_id = RoleId::default();
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let user = C::create_role(
            role_id,
            project_id,
            &request.name,
            description.as_deref(),
            t.transaction(),
        )
        .await?;
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
        let project_id = require_project_id(query.project_id, &request_metadata)?;

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                project_id,
                &CatalogProjectAction::CanListRoles,
            )
            .await?;

        // -------------------- Business Logic --------------------
        let filter_role_id = None;
        let pagination_query = query.pagination_query();
        C::list_roles(
            Some(project_id),
            filter_role_id,
            query.name,
            pagination_query,
            context.v1_state.catalog,
        )
        .await
    }

    async fn get_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<Role> {
        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_role_action(&request_metadata, role_id, &CatalogRoleAction::CanRead)
            .await?;

        // -------------------- Business Logic --------------------
        let roles = C::list_roles(
            None,
            Some(vec![role_id]),
            None,
            PaginationQuery {
                page_size: Some(1),
                page_token: PageToken::NotSpecified,
            },
            context.v1_state.catalog,
        )
        .await?;

        let role = roles.roles.into_iter().next().ok_or(ErrorModel::not_found(
            format!("Role with id {role_id} not found."),
            "RoleNotFound",
            None,
        ))?;

        Ok(role)
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
        let project_id = require_project_id(project_id, &request_metadata)?;

        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                project_id,
                &CatalogProjectAction::CanSearchRoles,
            )
            .await?;

        // ------------------- Business Logic -------------------
        search.truncate(64);
        C::search_role(&search, context.v1_state.catalog).await
    }

    async fn delete_role(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
    ) -> Result<()> {
        let authorizer = context.v1_state.authz;
        authorizer
            .require_role_action(&request_metadata, role_id, &CatalogRoleAction::CanDelete)
            .await?;

        // ------------------- Business Logic -------------------
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let deleted = C::delete_role(role_id, t.transaction()).await?;
        if deleted.is_none() {
            return Err(ErrorModel::not_found(
                format!("Role with id {role_id} not found."),
                "RoleNotFound",
                None,
            )
            .into());
        }
        authorizer.delete_role(&request_metadata, role_id).await?;
        t.commit().await
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

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_role_action(&request_metadata, role_id, &CatalogRoleAction::CanUpdate)
            .await?;

        // -------------------- Business Logic --------------------
        let description = request.description.filter(|d| !d.is_empty());

        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let role = C::update_role(
            role_id,
            &request.name,
            description.as_deref(),
            t.transaction(),
        )
        .await?;
        if let Some(role) = role {
            t.commit().await?;
            Ok(role)
        } else {
            t.rollback().await?;
            Err(ErrorModel::not_found(
                format!("Role with id {role_id} not found."),
                "RoleNotFound",
                None,
            )
            .into())
        }
    }
}

pub(super) fn require_project_id(
    specified_project_id: Option<ProjectIdent>,
    request_metadata: &RequestMetadata,
) -> Result<ProjectIdent> {
    specified_project_id
        .or(request_metadata.auth_details.project_id())
        .or(*DEFAULT_PROJECT_ID)
        .ok_or_else(|| {
            ErrorModel::bad_request(
                "Project ID is required to create a role".to_string(),
                "MissingProjectId",
                None,
            )
            .into()
        })
}
