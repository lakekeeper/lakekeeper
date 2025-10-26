#![allow(deprecated)]

pub mod v1 {
    pub mod namespace;
    pub mod project;
    pub mod role;
    pub mod server;
    pub mod table;
    pub mod tabular;
    pub mod tasks;
    pub mod user;
    pub mod view;
    pub mod warehouse;

    #[cfg(feature = "open-api")]
    pub mod openapi;

    use std::marker::PhantomData;

    use axum::{
        extract::{Path, Query, State as AxumState},
        response::{IntoResponse, Response},
        routing::{get, post},
        Extension, Json, Router,
    };
    use http::StatusCode;
    use iceberg_ext::catalog::rest::ErrorModel;
    #[cfg(feature = "open-api")]
    use iceberg_ext::catalog::rest::IcebergErrorResponse;
    use namespace::NamespaceManagementService as _;
    #[cfg(feature = "open-api")]
    pub use openapi::api_doc;
    use project::{
        CreateProjectRequest, CreateProjectResponse, GetProjectResponse, ListProjectsResponse,
        RenameProjectRequest, Service as _,
    };
    use role::{
        CreateRoleRequest, ListRolesQuery, ListRolesResponse, Role, SearchRoleRequest,
        SearchRoleResponse, Service as _, UpdateRoleRequest,
    };
    use serde::{Deserialize, Serialize};
    use server::{BootstrapRequest, ServerInfo, Service as _};
    use table::TableManagementService as _;
    use tabular::TabularManagementService as _;
    use typed_builder::TypedBuilder;
    use user::{
        CreateUserRequest, SearchUserRequest, SearchUserResponse, Service as _, UpdateUserRequest,
        User,
    };
    use view::ViewManagementService as _;
    use warehouse::{
        CreateWarehouseRequest, CreateWarehouseResponse, GetWarehouseResponse,
        ListDeletedTabularsQuery, ListWarehousesRequest, ListWarehousesResponse,
        RenameWarehouseRequest, Service as _, UpdateWarehouseCredentialRequest,
        UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest,
        WarehouseStatisticsResponse,
    };

    use crate::{
        api::{
            endpoints::ManagementV1Endpoint,
            iceberg::{types::PageToken, v1::PaginationQuery},
            management::v1::{
                project::{EndpointStatisticsResponse, GetEndpointStatisticsRequest},
                tabular::{SearchTabularRequest, SearchTabularResponse},
                tasks::{
                    ControlTasksRequest, GetTaskDetailsQuery, GetTaskDetailsResponse,
                    ListTasksRequest, ListTasksResponse, Service,
                },
                user::{ListUsersQuery, ListUsersResponse},
                warehouse::{
                    GetTaskQueueConfigResponse, SetTaskQueueConfigRequest, UndropTabularsRequest,
                },
            },
            ApiContext, Result,
        },
        request_metadata::RequestMetadata,
        service::{
            authn::UserId, authz::Authorizer, tasks::TaskId, Actor, CatalogStore,
            CreateOrUpdateUserResponse, NamespaceId, RoleId, SecretStore, State, TableId,
            TabularId, ViewId,
        },
        ProjectId, WarehouseId,
    };

    #[derive(Clone, Debug)]
    pub struct ApiServer<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> {
        auth_handler: PhantomData<A>,
        config_server: PhantomData<C>,
        secret_store: PhantomData<S>,
    }

    /// Get Server Info
    ///
    /// Returns basic information about the server configuration and status.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "server",
        path = ManagementV1Endpoint::ServerInfo.path(),
        responses(
            (status = 200, description = "Server info", body = ServerInfo),
            (status = "4XX", body = IcebergErrorResponse),
            (status = 500, description = "Unauthorized", body = IcebergErrorResponse)
        )
    ))]
    async fn get_server_info<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, Json<ServerInfo>)> {
        ApiServer::<C, A, S>::server_info(api_context, metadata)
            .await
            .map(|user| (StatusCode::OK, Json(user)))
    }

    /// Bootstrap
    ///
    /// Initializes the Lakekeeper server and sets the initial administrator account.
    /// This operation can only be performed once.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "server",
        path = ManagementV1Endpoint::Bootstrap.path(),
        request_body = BootstrapRequest,
        responses(
            (status = 204, description = "Server bootstrapped successfully"),
            (status = "4XX", body = IcebergErrorResponse),
            (status = 500, description = "InternalError", body = IcebergErrorResponse)
        )
    ))]
    async fn bootstrap<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<BootstrapRequest>,
    ) -> Result<StatusCode> {
        ApiServer::<C, A, S>::bootstrap(api_context, metadata, request).await?;
        Ok(StatusCode::NO_CONTENT)
    }

    /// Provision User
    ///
    /// Creates a new user or updates an existing user's metadata from the provided token.
    /// The token should include "profile" and "email" scopes for complete user information.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "user",
        path = ManagementV1Endpoint::CreateUser.path(),
        request_body = CreateUserRequest,
        responses(
            (status = 200, description = "User updated", body = User),
            (status = 201, description = "User created", body = User),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn create_user<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateUserRequest>,
    ) -> Result<(StatusCode, Json<User>)> {
        ApiServer::<C, A, S>::create_user(api_context, metadata, request)
            .await
            .map(|u| match u {
                CreateOrUpdateUserResponse::Created(user) => (StatusCode::CREATED, Json(user)),
                CreateOrUpdateUserResponse::Updated(user) => (StatusCode::OK, Json(user)),
            })
    }

    /// Search User
    ///
    /// Performs a fuzzy search for users based on the provided criteria.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "user",
        path = ManagementV1Endpoint::SearchUser.path(),
        request_body = SearchUserRequest,
        responses(
            (status = 200, description = "List of users", body = SearchUserResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn search_user<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<SearchUserRequest>,
    ) -> Result<SearchUserResponse> {
        ApiServer::<C, A, S>::search_user(api_context, metadata, request).await
    }

    /// Get User by ID
    ///
    /// Retrieves detailed information about a specific user.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "user",
        path = ManagementV1Endpoint::GetUser.path(),
        params(("user_id" = String,)),
        responses(
            (status = 200, description = "User details", body = User),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_user<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(user_id): Path<UserId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, Json<User>)> {
        ApiServer::<C, A, S>::get_user(api_context, metadata, user_id)
            .await
            .map(|user| (StatusCode::OK, Json(user)))
    }

    /// Whoami
    ///
    /// Returns information about the user associated with the current authentication token.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "user",
        path = ManagementV1Endpoint::Whoami.path(),
        responses(
            (status = 200, description = "User details", body = User),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn whoami<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, Json<User>)> {
        let id = match metadata.actor() {
            Actor::Role { principal, .. } | Actor::Principal(principal) => principal.clone(),
            Actor::Anonymous => {
                return Err(ErrorModel::unauthorized(
                    "No token provided",
                    "GetMyUserWithoutToken",
                    None,
                )
                .into())
            }
        };

        ApiServer::<C, A, S>::get_user(api_context, metadata, id)
            .await
            .map(|user| (StatusCode::OK, Json(user)))
    }

    /// Replace User
    ///
    /// Replaces the current user details with the new details provided in the request.
    /// If a field is not provided, it will be set to `None`.
    #[cfg_attr(feature = "open-api", utoipa::path(
        put,
        tag = "user",
        path = ManagementV1Endpoint::UpdateUser.path(),
        params(("user_id" = String,)),
        request_body = UpdateUserRequest,
        responses(
            (status = 200, description = "User details updated successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn update_user<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(user_id): Path<UserId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateUserRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::update_user(api_context, metadata, user_id, request).await
    }

    /// List Users
    ///
    /// Returns a paginated list of users based on the provided query parameters.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "user",
        path = ManagementV1Endpoint::ListUser.path(),
        params(ListUsersQuery),
        responses(
            (status = 200, description = "List of users", body = ListUsersResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn list_user<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Query(query): Query<ListUsersQuery>,
    ) -> Result<ListUsersResponse> {
        ApiServer::<C, A, S>::list_user(api_context, metadata, query).await
    }

    /// Delete User
    ///
    /// Permanently removes a user and all their associated permissions.
    /// If the user is re-registered later, their permissions will need to be re-added.
    #[cfg_attr(feature = "open-api", utoipa::path(
        delete,
        tag = "user",
        path =  ManagementV1Endpoint::DeleteUser.path(),
        params(("user_id" = String,)),
        responses(
            (status = 204, description = "User deleted successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn delete_user<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(user_id): Path<UserId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, ())> {
        ApiServer::<C, A, S>::delete_user(api_context, metadata, user_id)
            .await
            .map(|()| (StatusCode::NO_CONTENT, ()))
    }

    /// Create Role
    ///
    /// Creates a role with the specified name, description, and permissions.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "role",
        path = ManagementV1Endpoint::CreateRole.path(),
        request_body = CreateRoleRequest,
        responses(
            (status = 201, description = "Role successfully created", body = Role),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn create_role<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateRoleRequest>,
    ) -> Response {
        match ApiServer::<C, A, S>::create_role(request, api_context, metadata).await {
            Ok(role) => (StatusCode::CREATED, Json(role)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    /// Search Role
    ///
    /// Performs a fuzzy search for roles based on the provided criteria.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "role",
        path = ManagementV1Endpoint::SearchRole.path(),
        request_body = SearchRoleRequest,
        responses(
            (status = 200, description = "List of users", body = SearchRoleResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn search_role<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<SearchRoleRequest>,
    ) -> Result<SearchRoleResponse> {
        ApiServer::<C, A, S>::search_role(api_context, metadata, request).await
    }

    /// List Roles
    ///
    /// Returns all roles in the project that the current user has access to view.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "role",
        path = ManagementV1Endpoint::ListRole.path(),
        params(ListRolesQuery),
        responses(
            (status = 200, description = "List of roles", body = ListRolesResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn list_roles<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Query(query): Query<ListRolesQuery>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListRolesResponse> {
        ApiServer::<C, A, S>::list_roles(api_context, query, metadata).await
    }

    /// Delete Role
    ///
    /// Permanently removes a role and all its associated permissions.
    #[cfg_attr(feature = "open-api", utoipa::path(
        delete,
        tag = "role",
        path = ManagementV1Endpoint::DeleteRole.path(),
        params(("role_id" = Uuid,)),
        responses(
            (status = 204, description = "Role deleted successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn delete_role<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(role_id): Path<RoleId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, ())> {
        ApiServer::<C, A, S>::delete_role(api_context, metadata, role_id)
            .await
            .map(|()| (StatusCode::NO_CONTENT, ()))
    }

    /// Get Role
    ///
    /// Retrieves detailed information about a specific role.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "role",
        path = ManagementV1Endpoint::GetRole.path(),
        params(("role_id" = Uuid,)),
        responses(
            (status = 200, description = "Role details", body = Role),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_role<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(role_id): Path<RoleId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, Json<Role>)> {
        ApiServer::<C, A, S>::get_role(api_context, metadata, role_id)
            .await
            .map(|role| (StatusCode::OK, Json(role)))
    }

    /// Update Role
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "role",
        path = ManagementV1Endpoint::UpdateRole.path(),
        params(("role_id" = Uuid,)),
        request_body = UpdateRoleRequest,
        responses(
            (status = 200, description = "Role updated successfully", body = Role),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn update_role<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(role_id): Path<RoleId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateRoleRequest>,
    ) -> Result<(StatusCode, Json<Role>)> {
        ApiServer::<C, A, S>::update_role(api_context, metadata, role_id, request)
            .await
            .map(|role| (StatusCode::OK, Json(role)))
    }

    /// Create Warehouse
    ///
    /// Creates a new warehouse in the specified project with the provided configuration.
    /// The project of a warehouse cannot be changed after creation.
    /// This operation validates the storage configuration.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::CreateWarehouse.path(),
        request_body = CreateWarehouseRequest,
        responses(
            (status = 201, description = "Warehouse created successfully", body = CreateWarehouseResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn create_warehouse<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateWarehouseRequest>,
    ) -> Result<CreateWarehouseResponse> {
        ApiServer::<C, A, S>::create_warehouse(request, api_context, metadata).await
    }

    /// List Projects
    ///
    /// Lists all projects that the requesting user has access to.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "project",
        path = ManagementV1Endpoint::ListProjects.path(),
        responses(
            (status = 200, description = "List of projects", body = ListProjectsResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn list_projects<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListProjectsResponse> {
        ApiServer::<C, A, S>::list_projects(api_context, metadata).await
    }

    /// Create Project
    ///
    /// Creates a new project with the specified configuration.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "project",
        path = ManagementV1Endpoint::CreateProject.path(),
        responses(
            (status = 201, description = "Project created successfully", body = CreateProjectResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn create_project<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateProjectRequest>,
    ) -> Result<CreateProjectResponse> {
        ApiServer::<C, A, S>::create_project(request, api_context, metadata).await
    }

    /// Get Project
    ///
    /// Retrieves information about the user's default project.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "project",
        path = ManagementV1Endpoint::GetDefaultProject.path(),
        params(("x-project-id" = String, Header, description = "Optional project ID"),),
        responses(
            (status = 200, description = "Project details", body = GetProjectResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_default_project<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<GetProjectResponse> {
        ApiServer::<C, A, S>::get_project(None, api_context, metadata).await
    }

    /// Get Default Project
    ///
    /// Retrieves information about the user's default project.
    /// This endpoint is deprecated and will be removed in a future version.
    #[cfg_attr(feature = "open-api", utoipa::path(
            get,
            tag = "project",
            path = ManagementV1Endpoint::GetDefaultProjectDeprecated.path(),
            responses(
                (status = 200, description = "Project details", body = GetProjectResponse),
                (status = "4XX", body = IcebergErrorResponse),
            )
        ))]
    #[deprecated(
        since = "0.8.0",
        note = "This endpoint is deprecated and will be removed in a future version. Use `/v1/projects/default` instead."
    )]
    async fn get_default_project_deprecated<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<GetProjectResponse> {
        ApiServer::<C, A, S>::get_project(None, api_context, metadata).await
    }

    /// Get Project
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "project",
        path = ManagementV1Endpoint::GetDefaultProjectById.path(),
        params(("project_id" = String,)),
        responses(
            (status = 200, description = "Project details", body = GetProjectResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_project_by_id<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(project_id): Path<ProjectId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<GetProjectResponse> {
        ApiServer::<C, A, S>::get_project(Some(project_id), api_context, metadata).await
    }

    /// Delete Project
    #[cfg_attr(feature = "open-api", utoipa::path(
        delete,
        tag = "project",
        path = ManagementV1Endpoint::DeleteDefaultProject.path(),
        params(("x-project-id" = String, Header, description = "Optional project ID"),),
        responses(
            (status = 204, description = "Project deleted successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn delete_default_project<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, ())> {
        ApiServer::<C, A, S>::delete_project(None, api_context, metadata)
            .await
            .map(|()| (StatusCode::NO_CONTENT, ()))
    }

    /// Delete default Project
    ///
    /// Removes the user's default project and all its resources.
    /// This endpoint is deprecated and will be removed in a future version.
    #[cfg_attr(feature = "open-api", utoipa::path(
            delete,
            tag = "project",
            path = ManagementV1Endpoint::DeleteDefaultProjectDeprecated .path(),
            responses(
                (status = 204, description = "Project deleted successfully"),
                (status = "4XX", body = IcebergErrorResponse),
            )
        ))]
    #[deprecated(
        since = "0.8.0",
        note = "This endpoint is deprecated and will be removed in a future version. Use `/v1/projects/default` instead."
    )]
    async fn delete_default_project_deprecated<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, ())> {
        ApiServer::<C, A, S>::delete_project(None, api_context, metadata)
            .await
            .map(|()| (StatusCode::NO_CONTENT, ()))
    }

    /// Delete Project by ID
    ///
    /// Permanently removes a specific project and all its associated resources.
    #[cfg_attr(feature = "open-api", utoipa::path(
        delete,
        tag = "project",
        path = ManagementV1Endpoint::DeleteProjectById.path(),
        params(("project_id" = String,)),
        responses(
            (status = 204, description = "Project deleted successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn delete_project_by_id<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(project_id): Path<ProjectId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, ())> {
        ApiServer::<C, A, S>::delete_project(Some(project_id), api_context, metadata)
            .await
            .map(|()| (StatusCode::NO_CONTENT, ()))
    }

    /// Rename Project
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "project",
        path = ManagementV1Endpoint::RenameDefaultProject.path(),
        params(("x-project-id" = String, Header, description = "Optional project ID"),),
        responses(
            (status = 200, description = "Project renamed successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn rename_default_project<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<RenameProjectRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::rename_project(None, request, api_context, metadata).await
    }

    /// Rename the default project.
    ///
    /// Updates the name of the user's default project.
    /// This endpoint is deprecated and will be removed in a future version.
    #[cfg_attr(feature = "open-api", utoipa::path(
            post,
            tag = "project",
            path = ManagementV1Endpoint::RenameDefaultProjectDeprecated.path(),
            responses(
                (status = 200, description = "Project renamed successfully"),
                (status = "4XX", body = IcebergErrorResponse),
            )
        ))]
    #[deprecated(
        since = "0.8.0",
        note = "This endpoint is deprecated and will be removed in a future version. Use `/v1/projects/default` instead."
    )]
    async fn rename_default_project_deprecated<C: CatalogStore, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<RenameProjectRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::rename_project(None, request, api_context, metadata).await
    }

    /// Rename Project by ID
    ///
    /// Updates the name of a specific project.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "project",
        path = ManagementV1Endpoint::RenameProjectById.path(),
        params(("project_id" = String,)),
        responses(
            (status = 200, description = "Project renamed successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn rename_project_by_id<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(project_id): Path<ProjectId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<RenameProjectRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::rename_project(Some(project_id), request, api_context, metadata).await
    }

    /// List Warehouses
    ///
    /// Returns all warehouses in the project that the current user has access to.
    /// By default, deactivated warehouses are not included in the results.
    /// Set the `include_deactivated` query parameter to `true` to include them.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "warehouse",
        path = ManagementV1Endpoint::ListWarehouses.path(),
        params(ListWarehousesRequest),
        responses(
            (status = 200, description = "List of warehouses", body = ListWarehousesResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn list_warehouses<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Query(request): Query<ListWarehousesRequest>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListWarehousesResponse> {
        ApiServer::<C, A, S>::list_warehouses(request, api_context, metadata).await
    }

    /// Get Warehouse
    ///
    /// Retrieves detailed information about a specific warehouse.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "warehouse",
        path = ManagementV1Endpoint::GetWarehouse.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 200, description = "Warehouse details", body = GetWarehouseResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_warehouse<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<GetWarehouseResponse> {
        ApiServer::<C, A, S>::get_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    #[derive(Debug, Deserialize, TypedBuilder)]
    #[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
    pub struct DeleteWarehouseQuery {
        #[serde(
            deserialize_with = "crate::api::iceberg::types::deserialize_bool",
            default
        )]
        #[builder(setter(strip_bool))]
        pub force: bool,
    }

    /// Delete Warehouse
    ///
    /// Permanently removes a warehouse and all its associated resources.
    /// Use the `force` parameter to delete protected warehouses.
    #[cfg_attr(feature = "open-api", utoipa::path(
        delete,
        tag = "warehouse",
        path = ManagementV1Endpoint::DeleteWarehouse.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 204, description = "Warehouse deleted successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn delete_warehouse<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        Query(query): Query<DeleteWarehouseQuery>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, ())> {
        ApiServer::<C, A, S>::delete_warehouse(warehouse_id.into(), query, api_context, metadata)
            .await
            .map(|()| (StatusCode::NO_CONTENT, ()))
    }

    /// Rename Warehouse
    ///
    /// Updates the name of a specific warehouse.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::RenameWarehouse.path(),
        params(("warehouse_id" = Uuid,)),
        request_body = RenameWarehouseRequest,
        responses(
            (status = 200, body=GetWarehouseResponse, description = "Warehouse renamed successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn rename_warehouse<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<RenameWarehouseRequest>,
    ) -> Result<GetWarehouseResponse> {
        ApiServer::<C, A, S>::rename_warehouse(warehouse_id.into(), request, api_context, metadata)
            .await
    }

    /// Update Deletion Profile
    ///
    /// Configures the soft-delete behavior for a warehouse.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::UpdateWarehouseDeleteProfile.path(),
        params(("warehouse_id" = Uuid,)),
        request_body = UpdateWarehouseDeleteProfileRequest,
        responses(
            (status = 200, description = "Deletion Profile updated successfully"),
        (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn update_warehouse_delete_profile<
        C: CatalogStore,
        A: Authorizer + Clone,
        S: SecretStore,
    >(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateWarehouseDeleteProfileRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::update_warehouse_delete_profile(
            warehouse_id.into(),
            request,
            api_context,
            metadata,
        )
        .await
    }

    /// Deactivate Warehouse
    ///
    /// Temporarily disables access to a warehouse without deleting its data.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::DeactivateWarehouse.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 200, description = "Warehouse deactivated successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn deactivate_warehouse<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::deactivate_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Activate Warehouse
    ///
    /// Re-enables access to a previously deactivated warehouse.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::ActivateWarehouse.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 200, description = "Warehouse activated successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn activate_warehouse<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::activate_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Update Storage Profile
    ///
    /// Updates both the storage profile and credentials of a warehouse.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::UpdateStorageProfile.path(),
        params(("warehouse_id" = Uuid,)),
        request_body = UpdateWarehouseStorageRequest,
        responses(
            (status = 200, body=GetWarehouseResponse, description = "Storage profile updated successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn update_storage_profile<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateWarehouseStorageRequest>,
    ) -> Result<GetWarehouseResponse> {
        ApiServer::<C, A, S>::update_storage(warehouse_id.into(), request, api_context, metadata)
            .await
    }

    /// Update Storage Credential
    ///
    /// Updates only the storage credential of a warehouse without modifying the storage profile.
    /// Useful for refreshing expiring credentials.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::UpdateStorageCredential.path(),
        params(("warehouse_id" = Uuid,)),
        request_body = UpdateWarehouseCredentialRequest,
        responses(
            (status = 200, body=GetWarehouseResponse, description = "Storage credential updated successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn update_storage_credential<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateWarehouseCredentialRequest>,
    ) -> Result<GetWarehouseResponse> {
        ApiServer::<C, A, S>::update_storage_credential(
            warehouse_id.into(),
            request,
            api_context,
            metadata,
        )
        .await
    }

    #[derive(Deserialize, Debug)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    pub struct SetProtectionRequest {
        /// Setting this to `true` will prevent the entity from being deleted unless `force` is used.
        pub protected: bool,
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
    pub struct GetWarehouseStatisticsQuery {
        /// Next page token
        #[serde(skip_serializing_if = "PageToken::skip_serialize")]
        #[cfg_attr(feature = "open-api", param(value_type=String))]
        pub page_token: PageToken,
        /// Signals an upper bound of the number of results that a client will receive.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        pub page_size: Option<i64>,
    }

    impl GetWarehouseStatisticsQuery {
        fn to_pagination_query(&self) -> PaginationQuery {
            PaginationQuery {
                page_token: self.page_token.clone(),
                page_size: self.page_size,
            }
        }
    }

    /// Get Warehouse Statistics
    ///
    /// Retrieves statistical data about a warehouse's usage and resources over time.
    /// Statistics are aggregated hourly when changes occur.
    ///
    /// We lazily create a new statistics entry every hour, in between hours, the existing entry is
    /// being updated. If there's a change at `created_at + 1 hour`, a new entry is created.
    /// If there's been no change, no new entry is created, meaning there may be gaps.
    ///
    /// Example:
    /// - 00:16:32: warehouse created:
    ///     - `timestamp: 01:00:00, created_at: 00:16:32, updated_at: null, 0 tables, 0 views`
    /// - 00:30:00: table created:
    ///     - `timestamp: 01:00:00, created_at: 00:16:32, updated_at: 00:30:00, 1 table, 0 views`
    /// - 00:45:00: view created:
    ///     - `timestamp: 01:00:00, created_at: 00:16:32, updated_at: 00:45:00, 1 table, 1 view`
    /// - 01:00:36: table deleted:
    ///     - `timestamp: 02:00:00, created_at: 01:00:36, updated_at: null, 0 tables, 1 view`
    ///     - `timestamp: 01:00:00, created_at: 00:16:32, updated_at: 00:45:00, 1 table, 1 view`
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "warehouse",
        path = ManagementV1Endpoint::GetWarehouseStatistics.path(),
        params(("warehouse_id" = Uuid,), GetWarehouseStatisticsQuery),
        responses(
            (status = 200, description = "Warehouse statistics", body = WarehouseStatisticsResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_warehouse_statistics<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        Query(query): Query<GetWarehouseStatisticsQuery>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<Json<WarehouseStatisticsResponse>> {
        ApiServer::<C, A, S>::get_warehouse_statistics(
            warehouse_id.into(),
            query,
            api_context,
            metadata,
        )
        .await
        .map(Json)
    }

    /// Get API Statistics
    ///
    /// Retrieves detailed endpoint call statistics for your project, allowing you to monitor API usage patterns,
    /// track frequency of operations, and analyze response codes.
    ///
    /// ## Data Collection
    ///
    /// The statistics include:
    /// - Endpoint paths and HTTP methods
    /// - Response status codes
    /// - Call counts per endpoint
    /// - Warehouse context (when applicable)
    /// - Timestamps of activity
    ///
    /// ## Time Aggregation
    ///
    /// Statistics are aggregated hourly. Within each hour window:
    /// - An initial entry is created on the first API call
    /// - Subsequent calls update the existing hourly entry
    /// - Each hour boundary creates a new aggregation bucket
    /// - Hours with no API activity have no entries (gaps in data)
    ///
    /// ## Response Format
    ///
    /// The response includes timestamp buckets (in UTC) and corresponding endpoint metrics,
    /// allowing for time-series analysis of API usage patterns.
    ///
    /// Example:
    /// - 00:00:00-00:16:32: no activity
    ///     - `timestamps: []`
    /// - 00:16:32: warehouse created:
    ///     - `{timestamps: ["01:00:00"], called_endpoints: [[{"count": 1, "http_route": "POST /management/v1/warehouse", "status_code": 201, "warehouse_id": null, "warehouse_name": null, "created_at": "00:16:32", "updated_at": null}]]}`
    /// - 00:30:00: table created:
    ///     - `timestamps: ["01:00:00"], called_endpoints: [[{"count": 1, "http_route": "POST /management/v1/warehouse", "status_code": 201, "warehouse_id": null, "warehouse_name": null, "created_at": "00:16:32", "updated_at": null}, {"count": 1, "http_route": "POST /catalog/v1/{prefix}/namespaces/{namespace}/tables", "status_code": 201, "warehouse_id": "ff17f1d0-90ad-4e7d-bf02-be718b78c2ee", "warehouse_name": "staging", "created_at": "00:30:00", "updated_at": null}]]`
    /// - 00:45:00: table created:
    ///     - `timestamps: ["01:00:00"], called_endpoints: [[{"count": 1, "http_route": "POST /management/v1/warehouse", "status_code": 201, "warehouse_id": null, "warehouse_name": null, "created_at": "00:16:32", "updated_at": null}, {"count": 1, "http_route": "POST /catalog/v1/{prefix}/namespaces/{namespace}/tables", "status_code": 201, "warehouse_id": "ff17f1d0-90ad-4e7d-bf02-be718b78c2ee", "warehouse_name": "staging", "created_at": "00:30:00", "updated_at": "00:45:00"}]]`
    /// - 01:00:36: table deleted:
    ///     - `timestamps: ["01:00:00","02:00:00"], called_endpoints: [[{"count": 1, "http_route": "POST /management/v1/warehouse", "status_code": 201, "warehouse_id": null, "warehouse_name": null, "created_at": "00:16:32", "updated_at": null},{"count": 1, "http_route": "POST /catalog/v1/{prefix}/namespaces/{namespace}/tables", "status_code": 201, "warehouse_id": "ff17f1d0-90ad-4e7d-bf02-be718b78c2ee", "warehouse_name": "staging", "created_at": "00:30:00", "updated_at": "00:45:00"}],[{"count": 1, "http_route": "DELETE /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}", "status_code": 200, "warehouse_id": "ff17f1d0-90ad-4e7d-bf02-be718b78c2ee", "warehouse_name": "staging", "created_at": "01:00:36", "updated_at": "null"}]]`
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "project",
        path = ManagementV1Endpoint::LoadEndpointStatistics.path(),
        request_body = GetEndpointStatisticsRequest,
        responses(
            (status = 200, description = "Endpoint statistics", body = EndpointStatisticsResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_endpoint_statistics<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(query): Json<GetEndpointStatisticsRequest>,
    ) -> Result<Json<EndpointStatisticsResponse>> {
        ApiServer::<C, A, S>::get_endpoint_statistics(api_context, query, metadata)
            .await
            .map(Json)
    }

    /// Search Tabulars
    ///
    /// Performs a fuzzy search for tabulars based on the provided criteria. If the search string
    /// can be parsed as uuid:
    /// - if there is tabular with that uuid, the tabular is in the response
    /// - if there is a namespace with that uuid, tables in that namespace are in the response
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::SearchTabular.path(),
        params(("warehouse_id" = Uuid,)),
        request_body = SearchTabularRequest,
        responses(
            (status = 200, description = "List of tabulars", body = SearchTabularResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn search_tabular<C: CatalogStore, A: Authorizer, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<SearchTabularRequest>,
    ) -> Result<Json<SearchTabularResponse>> {
        ApiServer::<C, A, S>::search_tabular(warehouse_id.into(), api_context, metadata, request)
            .await
            .map(Json)
    }

    /// List Soft-Deleted Tabulars
    ///
    /// Returns all soft-deleted tables and views in the warehouse that are visible to the current user.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "warehouse",
        path = ManagementV1Endpoint::ListDeletedTabulars.path(),
        params(("warehouse_id" = Uuid,), ListDeletedTabularsQuery),
        responses(
            (status = 200, description = "List of soft-deleted tabulars", body = ListDeletedTabularsResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn list_deleted_tabulars<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        Query(query): Query<ListDeletedTabularsQuery>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<Json<ListDeletedTabularsResponse>> {
        ApiServer::<C, A, S>::list_soft_deleted_tabulars(
            warehouse_id.into(),
            query,
            api_context,
            metadata,
        )
        .await
        .map(Json)
    }

    /// Undrop Tabular
    ///
    /// Restores previously deleted tables or views to make them accessible again.
    /// This endpoint is deprecated and will be removed soon.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::UndropTabularsDeprecated.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 204, description = "Tabular undropped successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    #[deprecated(
        since = "0.7.0",
        note = "This endpoint is deprecated and will be removed soon, please use /management/v1/warehouse/{warehouse_id}/deleted-tabulars/undrop instead."
    )]
    async fn undrop_tabulars_deprecated<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UndropTabularsRequest>,
    ) -> Result<StatusCode> {
        ApiServer::<C, A, S>::undrop_tabulars(
            WarehouseId::from(warehouse_id),
            metadata,
            request,
            api_context,
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    }

    /// Undrop Tabular
    ///
    /// Restores previously deleted tables or views to make them accessible again.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::UndropTabulars.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 204, description = "Tabular undropped successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn undrop_tabulars<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UndropTabularsRequest>,
    ) -> Result<StatusCode> {
        ApiServer::<C, A, S>::undrop_tabulars(
            WarehouseId::from(warehouse_id),
            metadata,
            request,
            api_context,
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    }

    #[derive(Serialize, Deserialize, Debug)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    pub struct ProtectionResponse {
        /// Indicates whether the entity is protected
        pub protected: bool,
        /// Updated at
        pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    }

    impl IntoResponse for ProtectionResponse {
        fn into_response(self) -> Response {
            (StatusCode::OK, Json(self)).into_response()
        }
    }

    /// Get Table Protection
    ///
    /// Retrieves whether a table is protected from deletion.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "warehouse",
        path = ManagementV1Endpoint::GetTableProtection.path(),
        params(("warehouse_id" = Uuid,),("table_id" = Uuid,)),
        responses(
            (status = 200, body =  ProtectionResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_table_protection<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, table_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
    ) -> Result<ProtectionResponse> {
        ApiServer::<C, A, S>::get_table_protection(
            TableId::from(table_id),
            warehouse_id.into(),
            api_context,
            metadata,
        )
        .await
    }

    /// Set Table Protection
    ///
    /// Configures whether a table should be protected from deletion.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::SetTableProtection.path(),
        params(("warehouse_id" = Uuid,),("table_id" = Uuid,)),
        responses(
            (status = 200, body =  ProtectionResponse, description = "Table protection set successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn set_table_protection<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, table_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Json(SetProtectionRequest { protected }): Json<SetProtectionRequest>,
    ) -> Result<ProtectionResponse> {
        ApiServer::<C, A, S>::set_table_protection(
            TableId::from(table_id),
            warehouse_id.into(),
            protected,
            api_context,
            metadata,
        )
        .await
    }

    /// Get View Protection
    ///
    /// Retrieves whether a view is protected from deletion.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "warehouse",
        path = ManagementV1Endpoint::GetViewProtection.path(),
        params(("warehouse_id" = Uuid,),("view_id" = Uuid,)),
        responses(
            (status = 200, body = ProtectionResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_view_protection<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, view_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
    ) -> Result<ProtectionResponse> {
        ApiServer::<C, A, S>::get_view_protection(
            ViewId::from(view_id),
            warehouse_id.into(),
            api_context,
            metadata,
        )
        .await
    }

    /// Set View Protection
    ///
    /// Configures whether a view should be protected from deletion.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::SetViewProtection.path(),
        params(("warehouse_id" = Uuid,),("view_id" = Uuid,)),
        responses(
            (status = 200, body = ProtectionResponse, description = "View protection set successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn set_view_protection<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, view_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Json(SetProtectionRequest { protected }): Json<SetProtectionRequest>,
    ) -> Result<ProtectionResponse> {
        ApiServer::<C, A, S>::set_view_protection(
            ViewId::from(view_id),
            warehouse_id.into(),
            protected,
            api_context,
            metadata,
        )
        .await
    }

    /// Get Namespace Protection
    ///
    /// Retrieves whether a namespace is protected from deletion.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "warehouse",
        path = ManagementV1Endpoint::GetNamespaceProtection.path(),
        params(("warehouse_id" = Uuid,),("namespace_id" = Uuid,)),
        responses(
            (status = 200, body = ProtectionResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_namespace_protection<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, namespace_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
    ) -> Result<ProtectionResponse> {
        ApiServer::<C, A, S>::get_namespace_protection(
            NamespaceId::from(namespace_id),
            warehouse_id.into(),
            api_context,
            metadata,
        )
        .await
    }

    /// Set Namespace Protection
    ///
    /// Configures whether a namespace should be protected from deletion.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::SetNamespaceProtection.path(),
        params(("warehouse_id" = Uuid,),("namespace_id" = Uuid,)),
        responses(
            (status = 200, body = ProtectionResponse, description = "Namespace protection set successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn set_namespace_protection<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, namespace_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Json(SetProtectionRequest { protected }): Json<SetProtectionRequest>,
    ) -> Result<ProtectionResponse> {
        ApiServer::<C, A, S>::set_namespace_protection(
            NamespaceId::from(namespace_id),
            warehouse_id.into(),
            protected,
            api_context,
            metadata,
        )
        .await
    }

    /// Set Warehouse Protection
    ///
    /// Configures whether a warehouse should be protected from deletion.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "warehouse",
        path = ManagementV1Endpoint::SetWarehouseProtection.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 200, body = ProtectionResponse, description = "Warehouse protection set successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn set_warehouse_protection<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Json(SetProtectionRequest { protected }): Json<SetProtectionRequest>,
    ) -> Result<ProtectionResponse> {
        ApiServer::<C, A, S>::set_warehouse_protection(
            warehouse_id.into(),
            protected,
            api_context,
            metadata,
        )
        .await
    }

    /// Set the configuration for a Task Queue.
    ///
    /// These configurations are global per warehouse and shared across all instances of this kind of task.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "tasks",
        path = ManagementV1Endpoint::SetTaskQueueConfig.path(),
        params(("warehouse_id" = Uuid,)),
        responses(
            (status = 204, description = "Task queue config set successfully"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn set_task_queue_config<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, queue_name)): Path<(uuid::Uuid, String)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Json(request): Json<SetTaskQueueConfigRequest>,
    ) -> Result<StatusCode> {
        let queue_name = queue_name.into();
        ApiServer::<C, A, S>::set_task_queue_config(
            warehouse_id.into(),
            &queue_name,
            request,
            api_context,
            metadata,
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    }

    /// Get the configuration for a Task Queue.
    ///
    /// These configurations are global per warehouse and shared across all instances of this kind of task.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "tasks",
        path = ManagementV1Endpoint::GetTaskQueueConfig.path(),
        params(("warehouse_id" = Uuid,),("queue_name" = String,)),
        responses(
            (status = 200, body = GetTaskQueueConfigResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_task_queue_config<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, queue_name)): Path<(uuid::Uuid, String)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
    ) -> Result<GetTaskQueueConfigResponse> {
        let queue_name = queue_name.into();
        ApiServer::<C, A, S>::get_task_queue_config(
            warehouse_id.into(),
            &queue_name,
            api_context,
            metadata,
        )
        .await
    }

    /// List active and historic tasks.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "tasks",
        path = ManagementV1Endpoint::ListTasks.path(),
        params(("warehouse_id" = Uuid,)),
        request_body = ListTasksRequest,
        responses(
            (status = 200, body = ListTasksResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn list_tasks<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Json(request): Json<ListTasksRequest>,
    ) -> Result<ListTasksResponse> {
        ApiServer::<C, A, S>::list_tasks(warehouse_id.into(), request, api_context, metadata).await
    }

    /// Get Details about a specific task by its ID.
    #[cfg_attr(feature = "open-api", utoipa::path(
        get,
        tag = "tasks",
        path = ManagementV1Endpoint::GetTaskDetails.path(),
        params(("warehouse_id" = Uuid,),("task_id" = Uuid,),GetTaskDetailsQuery),
        responses(
            (status = 200, body = GetTaskDetailsResponse),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn get_task_details<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path((warehouse_id, task_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Query(query): Query<GetTaskDetailsQuery>,
    ) -> Result<GetTaskDetailsResponse> {
        let warehouse_id = WarehouseId::from(warehouse_id);
        let task_id = TaskId::from(task_id);
        ApiServer::<C, A, S>::get_task_details(warehouse_id, task_id, query, api_context, metadata)
            .await
    }

    /// Control a set of tasks by their IDs (e.g., cancel, request stop, run now)
    ///
    /// Accepts at most 100 task IDs in one request.
    #[cfg_attr(feature = "open-api", utoipa::path(
        post,
        tag = "tasks",
        path = ManagementV1Endpoint::ControlTasks.path(),
        params(("warehouse_id" = Uuid,)),
        request_body = ControlTasksRequest,
        responses(
            (status = 204, description = "All requested actions were successful"),
            (status = "4XX", body = IcebergErrorResponse),
        )
    ))]
    async fn control_tasks<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        Extension(metadata): Extension<RequestMetadata>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Json(request): Json<ControlTasksRequest>,
    ) -> Result<StatusCode> {
        ApiServer::<C, A, S>::control_tasks(warehouse_id.into(), request, api_context, metadata)
            .await?;
        Ok(StatusCode::NO_CONTENT)
    }

    #[derive(Debug, Serialize)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    #[serde(rename_all = "kebab-case")]
    pub struct ListDeletedTabularsResponse {
        /// List of tabulars
        pub tabulars: Vec<DeletedTabularResponse>,
        /// Token to fetch the next page
        pub next_page_token: Option<String>,
    }

    #[derive(Debug, Serialize)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    #[serde(rename_all = "kebab-case")]
    pub struct DeletedTabularResponse {
        /// Unique identifier of the tabular
        pub id: uuid::Uuid,
        /// Name of the tabular
        pub name: String,
        /// List of namespace parts the tabular belongs to
        pub namespace: Vec<String>,
        /// Type of the tabular
        pub typ: TabularType,
        /// Warehouse ID where the tabular is stored
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        pub warehouse_id: WarehouseId,
        /// Date when the tabular was created
        pub created_at: chrono::DateTime<chrono::Utc>,
        /// Date when the tabular was deleted
        pub deleted_at: chrono::DateTime<chrono::Utc>,
        /// Date when the tabular will not be recoverable anymore
        pub expiration_date: chrono::DateTime<chrono::Utc>,
    }

    impl From<TabularId> for TabularType {
        fn from(ident: TabularId) -> Self {
            match ident {
                TabularId::Table(_) => TabularType::Table,
                TabularId::View(_) => TabularType::View,
            }
        }
    }

    /// Type of tabular
    #[derive(Debug, Deserialize, Serialize, Clone, Copy, strum::Display, PartialEq, Eq)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    #[serde(rename_all = "kebab-case")]
    pub enum TabularType {
        Table,
        View,
    }

    #[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, strum_macros::Display)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    #[serde(rename_all = "kebab-case")]
    pub enum DeleteKind {
        Default,
        Purge,
    }

    impl<C: CatalogStore, A: Authorizer, S: SecretStore> ApiServer<C, A, S> {
        #[allow(clippy::too_many_lines)]
        pub fn new_v1_router(authorizer: &A) -> Router<ApiContext<State<A, C, S>>> {
            Router::new()
                // Server
                .route("/info", get(get_server_info))
                .route("/bootstrap", post(bootstrap))
                .route("/endpoint-statistics", post(get_endpoint_statistics))
                // Role management
                .route("/role", get(list_roles).post(create_role))
                .route(
                    "/role/{role_id}",
                    get(get_role).post(update_role).delete(delete_role),
                )
                .route("/search/role", post(search_role))
                // User management
                .route("/whoami", get(whoami))
                .route("/search/user", post(search_user))
                .route(
                    "/user/{user_id}",
                    get(get_user).put(update_user).delete(delete_user),
                )
                .route("/user", get(list_user).post(create_user))
                // Default project
                .route(
                    "/default-project",
                    get(get_default_project_deprecated).delete(delete_default_project_deprecated),
                )
                .route(
                    "/default-project/rename",
                    post(rename_default_project_deprecated),
                )
                .route("/project/rename", post(rename_default_project))
                // Create a new project
                .route(
                    "/project",
                    post(create_project)
                        .get(get_default_project)
                        .delete(delete_default_project),
                )
                .route(
                    "/project/{project_id}",
                    get(get_project_by_id).delete(delete_project_by_id),
                )
                .route("/project/{project_id}/rename", post(rename_project_by_id))
                // Create a new warehouse
                .route("/warehouse", post(create_warehouse).get(list_warehouses))
                // List all projects
                .route("/project-list", get(list_projects))
                .route(
                    "/warehouse/{warehouse_id}",
                    get(get_warehouse).delete(delete_warehouse),
                )
                // Rename warehouse
                .route("/warehouse/{warehouse_id}/rename", post(rename_warehouse))
                // Deactivate warehouse
                .route(
                    "/warehouse/{warehouse_id}/deactivate",
                    post(deactivate_warehouse),
                )
                .route(
                    "/warehouse/{warehouse_id}/activate",
                    post(activate_warehouse),
                )
                // Update storage profile and credential.
                // The old credential is not re-used. If credentials are not provided,
                // we assume that this endpoint does not require a secret.
                .route(
                    "/warehouse/{warehouse_id}/storage",
                    post(update_storage_profile),
                )
                // Update only the storage credential - keep the storage profile as is
                .route(
                    "/warehouse/{warehouse_id}/storage-credential",
                    post(update_storage_credential),
                )
                // Get warehouse statistics
                .route(
                    "/warehouse/{warehouse_id}/statistics",
                    get(get_warehouse_statistics),
                )
                .route(
                    ManagementV1Endpoint::SearchTabular.path_in_management_v1(),
                    post(search_tabular),
                )
                .route(
                    "/warehouse/{warehouse_id}/deleted-tabulars",
                    get(list_deleted_tabulars),
                )
                .route(
                    "/warehouse/{warehouse_id}/deleted_tabulars/undrop",
                    #[allow(deprecated)]
                    post(undrop_tabulars_deprecated),
                )
                .route(
                    "/warehouse/{warehouse_id}/deleted-tabulars/undrop",
                    post(undrop_tabulars),
                )
                .route(
                    "/warehouse/{warehouse_id}/delete-profile",
                    post(update_warehouse_delete_profile),
                )
                .route(
                    "/warehouse/{warehouse_id}/table/{table_id}/protection",
                    get(get_table_protection).post(set_table_protection),
                )
                .route(
                    "/warehouse/{warehouse_id}/view/{view_id}/protection",
                    get(get_view_protection).post(set_view_protection),
                )
                .route(
                    "/warehouse/{warehouse_id}/namespace/{namespace_id}/protection",
                    get(get_namespace_protection).post(set_namespace_protection),
                )
                .route(
                    "/warehouse/{warehouse_id}/protection",
                    post(set_warehouse_protection),
                )
                .route(
                    ManagementV1Endpoint::SetTaskQueueConfig.path_in_management_v1(),
                    post(set_task_queue_config).get(get_task_queue_config),
                )
                .route(
                    ManagementV1Endpoint::ListTasks.path_in_management_v1(),
                    post(list_tasks),
                )
                .route(
                    ManagementV1Endpoint::GetTaskDetails.path_in_management_v1(),
                    get(get_task_details),
                )
                .route(
                    ManagementV1Endpoint::ControlTasks.path_in_management_v1(),
                    post(control_tasks),
                )
                .merge(authorizer.new_router())
        }
    }
}
