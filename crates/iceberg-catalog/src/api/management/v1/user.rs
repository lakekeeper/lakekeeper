use axum::{response::IntoResponse, Json};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use super::default_page_size;
use crate::{
    api::{
        iceberg::v1::{PageToken, PaginationQuery},
        management::v1::ApiServer,
        ApiContext,
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogServerAction, CatalogUserAction},
        Catalog, CreateOrUpdateUserResponse, Result, SecretStore, State, Transaction, UserId,
    },
};

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

impl From<limes::PrincipalType> for UserType {
    fn from(principal_type: limes::PrincipalType) -> Self {
        match principal_type {
            limes::PrincipalType::Human => UserType::Human,
            limes::PrincipalType::Application => UserType::Application,
        }
    }
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

/// Parse a create user request and extend with information
/// from the request metadata if this is a self-provisioning request.
pub(crate) fn parse_create_user_request(
    request_metadata: &RequestMetadata,
    request: Option<CreateUserRequest>,
) -> Result<(UserId, String, UserType, Option<String>)> {
    let (request_name, request_email, request_id, request_user_type) = match request {
        Some(CreateUserRequest {
            update_if_exists: _,
            name: request_name,
            email: request_email,
            id: request_id,
            user_type: request_user_type,
        }) => (request_name, request_email, request_id, request_user_type),
        None => (None, None, None, None),
    };

    let request_email = request_email.filter(|e| !e.is_empty());
    let request_name = request_name.filter(|n| !n.is_empty());
    let self_provision = is_self_provisioning(request_metadata.user_id(), request_id.as_ref());

    let creation_user_id = request_id
        .or_else(|| request_metadata.user_id().cloned())
        .ok_or(ErrorModel::bad_request(
        "User ID could not be extracted from the token and must be provided for creating a user.",
        "MissingUserId",
        None,
    ))?;

    let (name, user_type, email) = if self_provision {
        // If this is self provisioning, provided values take precedence, but we may
        // use auth info from the token.
        let auth_details = request_metadata.authentication();
        let name =
            request_name.or(auth_details.and_then(|a| a.full_name().map(ToString::to_string)));
        let email = request_email.or(auth_details.and_then(|a| a.email().map(ToString::to_string)));
        // Human users can typically be identified by the token, which is why we default to Application
        let user_type = request_user_type
            .or_else(|| auth_details.and_then(|a| a.principal_type().map(Into::into)))
            .unwrap_or(UserType::Application);
        let name = name.ok_or(ErrorModel::bad_request(
            "User name could not be extracted from the token and must be provided",
            "MissingUserName",
            None,
        ))?;

        (name, user_type, email)
    } else {
        // If this is not self-provisioning, token data may not be used
        let name = request_name.ok_or(ErrorModel::bad_request(
            "Name must be provided for user provisioning",
            "MissingUserName",
            None,
        ))?;
        let user_type = request_user_type.ok_or(ErrorModel::bad_request(
            "Name and user_type must be provided for user provisioning",
            "MissingUserType",
            None,
        ))?;
        (name, user_type, request_email)
    };

    if name.is_empty() {
        return Err(ErrorModel::bad_request("Name cannot be empty", "EmptyName", None).into());
    }
    let email = email.filter(|e| !e.is_empty());

    Ok((creation_user_id, name, user_type, email))
}

#[async_trait::async_trait]
pub(crate) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn create_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: CreateUserRequest,
    ) -> Result<CreateOrUpdateUserResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let acting_user_id = request_metadata.user_id();

        let self_provision = is_self_provisioning(acting_user_id, request.id.as_ref());
        if !self_provision {
            authorizer
                .require_server_action(&request_metadata, CatalogServerAction::CanProvisionUsers)
                .await?;
        };

        // ------------------- Business Logic -------------------
        let update_if_exists = request.update_if_exists;
        let (creation_user_id, name, user_type, email) =
            parse_create_user_request(&request_metadata, Some(request))?;

        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let user = C::create_or_update_user(
            &creation_user_id,
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
                format!("User with id {creation_user_id} already exists."),
                "UserAlreadyExists",
                None,
            )
            .into());
        }
        tracing::debug!("User created: {:?}", user);

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
            .require_user_action(&request_metadata, &user_id, CatalogUserAction::CanRead)
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
            .require_server_action(&request_metadata, CatalogServerAction::CanListUsers)
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
            .require_user_action(&request_metadata, &user_id, CatalogUserAction::CanUpdate)
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
            .require_user_action(&request_metadata, &user_id, CatalogUserAction::CanDelete)
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

fn is_self_provisioning(acting_user_id: Option<&UserId>, request_id: Option<&UserId>) -> bool {
    if let Some(acting_user_id) = acting_user_id {
        if let Some(request_id) = request_id {
            request_id == acting_user_id
        } else {
            true
        }
    } else {
        false
    }
}
