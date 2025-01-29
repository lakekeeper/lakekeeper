use super::default_page_size;
use crate::api::iceberg::v1::{PageToken, PaginationQuery};
use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authn::UserId;
use crate::service::authz::{Authorizer, CatalogServerAction, CatalogUserAction};
use crate::service::{
    AuthDetails, Catalog, CreateOrUpdateUserResponse, Result, SecretStore, State, Transaction,
};
use axum::response::IntoResponse;
use axum::Json;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

/// How the user was last updated
#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum UserLastUpdatedWith {
    /// The user was updated or created by the `/management/v1/user/update-from-token` - typically via the UI
    CreateEndpoint,
    /// The user was created by the `/catalog/v1/config` endpoint
    ConfigCallCreation,
    /// The user was updated by one of the dedicated update endpoints
    UpdateEndpoint,
}

/// Type of a User
#[derive(Copy, Debug, PartialEq, Deserialize, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum UserType {
    /// Human User
    Human,
    /// Application / Technical User
    Application,
}

/// User of the catalog
#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct User {
    /// Name of the user
    pub name: String,
    /// Email of the user
    #[serde(default)]
    pub email: Option<String>,
    /// The user's ID
    #[schema(value_type=String)]
    pub id: UserId,
    /// Type of the user
    pub user_type: UserType,
    /// The endpoint that last updated the user
    pub last_updated_with: UserLastUpdatedWith,
    /// Timestamp when the user was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the user was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SearchUser {
    /// Name of the user
    pub name: String,
    /// ID of the user
    #[schema(value_type=String)]
    pub id: UserId,
    /// Type of the user
    pub user_type: UserType,
    /// Email of the user. If id is not specified, the email is extracted
    /// from the provided token.
    #[serde(default)]
    pub email: Option<String>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct CreateUserRequest {
    /// Update the user if it already exists
    /// Default: false
    #[serde(default)]
    pub update_if_exists: bool,
    /// Name of the user. If id is not specified, the name is extracted
    /// from the provided token.
    #[serde(default)]
    pub name: Option<String>,
    /// Email of the user. If id is not specified, the email is extracted
    /// from the provided token.
    #[serde(default)]
    pub email: Option<String>,
    /// Type of the user. Useful to override wrongly classified users
    #[serde(default)]
    pub user_type: Option<UserType>,
    /// Subject id of the user - allows user provisioning.
    /// The id must be identical to the subject in JWT tokens.
    /// To create users in self-service manner, do not set the id.
    /// The id is then extracted from the passed JWT token.
    #[serde(default)]
    #[schema(value_type=Option<String>)]
    pub id: Option<UserId>,
}

/// Search result for users
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SearchUserResponse {
    /// List of users matching the search criteria
    pub users: Vec<SearchUser>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListUsersQuery {
    /// Search for a specific username
    #[serde(default)]
    pub name: Option<String>,
    /// Next page token
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default = "default_page_size")]
    pub page_size: i64,
}

impl ListUsersQuery {
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

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListUsersResponse {
    pub users: Vec<User>,
    pub next_page_token: Option<String>,
}

impl IntoResponse for ListUsersResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

impl IntoResponse for SearchUserResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SearchUserRequest {
    /// Search string for fuzzy search.
    /// Length is truncated to 64 characters.
    pub search: String,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct UpdateUserRequest {
    pub name: String,
    #[serde(default)]
    pub email: Option<String>,
    pub user_type: UserType,
}

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(crate) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn create_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: CreateUserRequest,
    ) -> Result<CreateOrUpdateUserResponse> {
        let CreateUserRequest {
            update_if_exists,
            name,
            email,
            id,
            user_type,
        } = request;
        let email = email.filter(|e| !e.is_empty());
        let name = name.filter(|n| !n.is_empty());
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let principal = match request_metadata.auth_details.clone() {
            AuthDetails::Unauthenticated => None,
            AuthDetails::Principal(principal) => Some(principal),
        };
        let acting_user_id = principal.as_ref().map(|p| p.user_id().clone());

        // Everything else is self-registration
        let self_provision = if acting_user_id.is_none() || (id.is_some() && (id != acting_user_id))
        {
            authorizer
                .require_server_action(&request_metadata, &CatalogServerAction::CanProvisionUsers)
                .await?;
            false
        } else {
            true
        };

        // ------------------- Business Logic -------------------
        let id = id
            .or_else(|| acting_user_id.clone())
            .ok_or(ErrorModel::bad_request(
                "User ID could not be extracted from the token and must be provided.",
                "MissingUserId",
                None,
            ))?;

        let (name, user_type) = if let (Some(name), Some(user_type)) = (name.clone(), user_type) {
            (name, user_type)
        } else {
            if !self_provision {
                return Err(ErrorModel::bad_request(
                    "Name and user_type must be provided for user provisioning",
                    "MissingName",
                    None,
                )
                .into());
            }

            let (p_name, p_type) = principal
                .as_ref()
                .map(|p| p.get_name_and_type())
                .transpose()?
                .ok_or(ErrorModel::bad_request(
                    "User name could not be extracted from the token and must be provided",
                    "MissingUserName",
                    None,
                ))?;

            (
                name.unwrap_or(p_name.to_string()),
                user_type.unwrap_or(p_type),
            )
        };

        if name.is_empty() {
            return Err(ErrorModel::bad_request("Name cannot be empty", "EmptyName", None).into());
        }

        let email = email.or_else(|| {
            if self_provision {
                principal
                    .as_ref()
                    .and_then(|p| p.email().map(ToString::to_string))
            } else {
                None
            }
        });

        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let user = C::create_or_update_user(
            &id,
            &name,
            email.as_deref(),
            UserLastUpdatedWith::CreateEndpoint,
            user_type,
            t.transaction(),
        )
        .await?;

        if !matches!(user, CreateOrUpdateUserResponse::Created(_)) && !update_if_exists {
            t.rollback().await?;
            return Err(ErrorModel::conflict(
                format!("User with id {id} already exists."),
                "UserAlreadyExists",
                None,
            )
            .into());
        }

        t.commit().await?;

        Ok(user)
    }

    async fn search_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: SearchUserRequest,
    ) -> Result<SearchUserResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer.require_search_users(&request_metadata).await?;

        // ------------------- Business Logic -------------------
        let SearchUserRequest { mut search } = request;
        search.truncate(64);
        C::search_user(&search, context.v1_state.catalog).await
    }

    async fn get_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
    ) -> Result<User> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_user_action(&request_metadata, &user_id, &CatalogUserAction::CanRead)
            .await?;

        // ------------------- Business Logic -------------------
        let filter_user_id = Some(vec![user_id.clone()]);
        let filter_name = None;
        let users = C::list_user(
            filter_user_id,
            filter_name,
            PaginationQuery {
                page_size: Some(1),
                page_token: PageToken::NotSpecified,
            },
            context.v1_state.catalog,
        )
        .await?;

        let user = users.users.into_iter().next().ok_or(ErrorModel::not_found(
            format!("User with id {user_id} not found."),
            "UserNotFound",
            None,
        ))?;

        Ok(user)
    }

    async fn list_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListUsersQuery,
    ) -> Result<ListUsersResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_server_action(&request_metadata, &CatalogServerAction::CanListUsers)
            .await?;

        // ------------------- Business Logic -------------------
        let filter_user_id = None;
        let pagination_query = query.pagination_query();
        let users = C::list_user(
            filter_user_id,
            query.name,
            pagination_query,
            context.v1_state.catalog,
        )
        .await?;

        Ok(users)
    }

    async fn update_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
        request: UpdateUserRequest,
    ) -> Result<()> {
        if request.name.is_empty() {
            return Err(ErrorModel::bad_request("Name cannot be empty", "EmptyName", None).into());
        }
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_user_action(&request_metadata, &user_id, &CatalogUserAction::CanUpdate)
            .await?;

        // ------------------- Business Logic -------------------
        let email = request.email.as_deref().filter(|e| !e.is_empty());
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let user = C::create_or_update_user(
            &user_id,
            &request.name,
            email,
            UserLastUpdatedWith::UpdateEndpoint,
            request.user_type,
            t.transaction(),
        )
        .await?;

        if matches!(user, CreateOrUpdateUserResponse::Created(_)) {
            t.rollback().await?;
            Err(ErrorModel::not_found("User does not exist", "UserNotFound", None).into())
        } else {
            t.commit().await
        }
    }

    async fn delete_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_user_action(&request_metadata, &user_id, &CatalogUserAction::CanDelete)
            .await?;

        // ------------------- Business Logic -------------------
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let deleted = C::delete_user(user_id.clone(), t.transaction()).await?;
        if deleted.is_none() {
            return Err(ErrorModel::not_found(
                format!("User with id {} not found.", user_id.clone()),
                "UserNotFound",
                None,
            )
            .into());
        }
        authorizer.delete_user(&request_metadata, user_id).await?;
        t.commit().await
    }
}
