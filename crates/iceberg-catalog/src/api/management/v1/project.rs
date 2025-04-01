use chrono::Utc;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub use crate::service::{
    storage::{
        AdlsProfile, AzCredential, GcsCredential, GcsProfile, GcsServiceKey, S3Credential,
        S3Profile, StorageCredential, StorageProfile,
    },
    WarehouseStatus,
};
use crate::{
    api::{management::v1::ApiServer, ApiContext, Result},
    request_metadata::RequestMetadata,
    service::{
        authz::{
            Authorizer, CatalogProjectAction, CatalogServerAction, CatalogWarehouseAction,
            ListProjectsResponse as AuthZListProjectsResponse,
        },
        secrets::SecretStore,
        Catalog, State, Transaction,
    },
    ProjectId,
};

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct GetProjectResponse {
    /// ID of the project.
    #[schema(value_type = String)]
    pub project_id: ProjectId,
    /// Name of the project
    pub project_name: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RenameProjectRequest {
    /// New name for the project.
    pub new_name: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ListProjectsResponse {
    /// List of projects
    pub projects: Vec<GetProjectResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateProjectRequest {
    /// Name of the project to create.
    pub project_name: String,
    /// Request a specific project ID - optional.
    /// If not provided, a new project ID will be generated (recommended).
    #[schema(value_type = Option::<String>)]
    pub project_id: Option<ProjectId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateProjectResponse {
    /// ID of the created project.
    #[schema(value_type = String)]
    pub project_id: ProjectId,
}

impl axum::response::IntoResponse for CreateProjectResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::CREATED, axum::Json(self)).into_response()
    }
}

impl axum::response::IntoResponse for GetProjectResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl<C: Catalog, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn create_project(
        request: CreateProjectRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateProjectResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_server_action(&request_metadata, CatalogServerAction::CanCreateProject)
            .await?;

        // ------------------- Business Logic -------------------
        let CreateProjectRequest {
            project_name,
            project_id,
        } = request;
        validate_project_name(&project_name)?;
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let project_id = project_id.unwrap_or(ProjectId::from(uuid::Uuid::now_v7()));
        C::create_project(&project_id, project_name, t.transaction()).await?;
        authorizer
            .create_project(&request_metadata, &project_id)
            .await?;
        t.commit().await?;

        Ok(CreateProjectResponse { project_id })
    }

    async fn rename_project(
        project_id: Option<ProjectId>,
        request: RenameProjectRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(project_id)?;
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::CanRename,
            )
            .await?;

        // ------------------- Business Logic -------------------
        validate_project_name(&request.new_name)?;
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        C::rename_project(&project_id, &request.new_name, transaction.transaction()).await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn get_project(
        project_id: Option<ProjectId>,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetProjectResponse> {
        let project_id = request_metadata.require_project_id(project_id)?;
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::CanGetMetadata,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut t = C::Transaction::begin_read(context.v1_state.catalog).await?;
        let project =
            C::get_project(&project_id, t.transaction())
                .await?
                .ok_or(ErrorModel::not_found(
                    format!("Project with id {project_id} not found."),
                    "ProjectNotFound",
                    None,
                ))?;
        t.commit().await?;

        Ok(GetProjectResponse {
            project_id,
            project_name: project.name,
        })
    }

    async fn delete_project(
        project_id: Option<ProjectId>,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(project_id)?;
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::CanDelete,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::delete_project(&project_id, transaction.transaction()).await?;
        authorizer
            .delete_project(&request_metadata, project_id)
            .await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn list_projects(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListProjectsResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        let projects = authorizer.list_projects(&request_metadata).await?;

        // ------------------- Business Logic -------------------
        let project_id_filter = match projects {
            AuthZListProjectsResponse::All => None,
            AuthZListProjectsResponse::Projects(projects) => Some(projects),
        };
        let mut trx = C::Transaction::begin_read(context.v1_state.catalog).await?;

        let projects = C::list_projects(project_id_filter, trx.transaction()).await?;
        trx.commit().await?;

        Ok(ListProjectsResponse {
            projects: projects
                .into_iter()
                .map(|project| GetProjectResponse {
                    project_id: project.project_id,
                    project_name: project.name,
                })
                .collect(),
        })
    }

    async fn get_endpoint_statistics(
        context: ApiContext<State<A, C, S>>,
        request: GetEndpointStatisticsRequest,
        request_metadata: RequestMetadata,
    ) -> Result<EndpointStatisticsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        match request.warehouse {
            WarehouseFilter::WarehouseId { id } => {
                authorizer
                    .require_warehouse_action(
                        &request_metadata,
                        id.into(),
                        CatalogWarehouseAction::CanGetMetadata,
                    )
                    .await?;
            }
            WarehouseFilter::All | WarehouseFilter::Unmapped => {
                authorizer
                    .require_project_action(
                        &request_metadata,
                        &project_id,
                        CatalogProjectAction::CanGetMetadata,
                    )
                    .await?;
            }
        }

        C::get_endpoint_statistics(
            project_id,
            request.warehouse,
            request
                .range_specifier
                .unwrap_or(TimeWindowSelector::Window {
                    end: Utc::now(),
                    interval: chrono::Duration::days(1),
                }),
            request.status_codes.as_deref(),
            context.v1_state.catalog,
        )
        .await
    }
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct EndpointStatistic {
    /// Number of requests to this endpoint for the current time-slice.
    pub count: i64,
    /// The route of the endpoint.
    ///
    /// Format: `METHOD /path/to/endpoint`
    pub http_route: String,
    /// The status code of the response.
    pub status_code: u16,
    /// The ID of the warehouse that handled the request.
    ///
    /// Only present for requests that could be associated with a warehouse. Some management
    /// endpoints cannot be associated with a warehouse, e.g. warehouse creation or user management
    /// will not have a `warehouse_id`.
    pub warehouse_id: Option<Uuid>,
    /// The name of the warehouse that handled the request.
    ///
    /// Only present for requests that could be associated with a warehouse. Some management
    /// endpoints cannot be associated with a warehouse, e.g. warehouse creation or user management
    /// will not have a `warehouse_id`
    pub warehouse_name: Option<String>,
    /// Timestamp at which the datapoint was created in the database.
    ///
    /// This is the exact time at which the current endpoint-status-warehouse combination was called
    /// for the first time in the current time-slice.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp at which the datapoint was last updated.
    ///
    /// This is the exact time at which the current datapoint was last updated.
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct EndpointStatisticsResponse {
    /// Array of timestamps indicating the time at which each entry in the `called_endpoints` array
    /// is valid.
    ///
    /// We lazily create a new statistics entry every hour, in between hours, the existing entry
    /// is being updated. If any endpoint is called in the following hour, there'll be an entry in
    /// `timestamps` for the following hour. If not, then there'll be no entry.
    pub timestamps: Vec<chrono::DateTime<chrono::Utc>>,
    /// Array of arrays of statistics detailing each called endpoint for each `timestamp`.
    ///
    /// See docs of `timestamps` for more details.
    pub called_endpoints: Vec<Vec<EndpointStatistic>>,
    /// Token to get the previous page of results.
    ///
    /// Endpoint statistics are not paginated through page-limits, we paginate them by stepping
    /// through time. By default, the list-statistics endpoint will return all statistics for
    /// `now()` - 1 day to `now()`. In the request, you can specify a `range_specifier` to set the end
    /// date and step interval. The `previous_page_token` will then move to the neighboring window.
    /// E.g. in the default case of `now()` and 1 day, it'd be `now()` - 2 days to `now()` - 1 day.
    pub previous_page_token: String,
    /// Token to get the next page of results.
    ///
    /// Inverse of `previous_page_token`, see its documentation above.
    pub next_page_token: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum TimeWindowSelector {
    Window {
        /// End timestamp of the time window
        end: chrono::DateTime<chrono::Utc>,
        /// Duration/span of the time window
        ///
        /// The effective time range = `window_end` - `window_duration` to `end`
        interval: chrono::Duration,
    },
    PageToken {
        /// Opaque Token from previous response for paginating through time windows
        ///
        /// Use the `next_page_token` or `previous_page_token` from a previous response
        token: String,
    },
}

#[derive(Deserialize, ToSchema, Debug)]
pub struct GetEndpointStatisticsRequest {
    /// Warehouse filter
    ///
    /// Can return statistics for a specific warehouse, all warehouses or requests that could not be
    /// associated to any warehouse.
    pub warehouse: WarehouseFilter,
    /// Status code filter
    ///
    /// Optional filter to only return statistics for requests with specific status codes.
    pub status_codes: Option<Vec<u16>>,
    /// Range specifier
    ///
    /// Either for a explicit range or a page token to paginate through the results. See the docs of
    /// `RangeSpecifier` for more details.
    pub range_specifier: Option<TimeWindowSelector>,
}

#[derive(Deserialize, Serialize, ToSchema, Debug)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum WarehouseFilter {
    /// Filter for a specific warehouse
    WarehouseId { id: Uuid },
    /// Filter for requests that could not be associated with any warehouse, e.g. some management
    /// endpoints
    Unmapped,
    /// Do not filter by warehouse and return all statistics for all warehouses and unmapped requests
    /// for the current project.
    All,
}

impl axum::response::IntoResponse for ListProjectsResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

fn validate_project_name(project_name: &str) -> Result<()> {
    if project_name.is_empty() {
        return Err(ErrorModel::bad_request(
            "Project name cannot be empty",
            "EmptyProjectName",
            None,
        )
        .into());
    }

    if project_name.len() > 128 {
        return Err(ErrorModel::bad_request(
            "Project name must be shorter than 128 chars",
            "ProjectNameTooLong",
            None,
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use crate::api::management::v1::project::WarehouseFilter;

    #[test]
    fn test_deserialize_warehouse_filter() {
        let js = serde_json::json!({"type": "warehouse-id","id": Uuid::new_v4().to_string()});
        let _ = serde_json::from_value::<WarehouseFilter>(js).unwrap();

        let js = serde_json::json!({"type": "unmapped"});
        let _ = serde_json::from_value::<WarehouseFilter>(js).unwrap();

        let js = serde_json::json!({"type": "all"});
        let _ = serde_json::from_value::<WarehouseFilter>(js).unwrap();
    }
}
