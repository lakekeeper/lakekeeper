use super::default_page_size;
use crate::api::iceberg::v1::{PageToken, PaginationQuery};
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, CatalogProjectAction};
use crate::service::task_queue::{Task, TaskId, TaskInstance};
use crate::service::{Catalog, Result, SecretStore, State};
use crate::ProjectIdent;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Deserialize, Serialize, IntoParams)]
pub struct ListTasksQuery {
    pub(crate) page_token: Option<String>,
    #[serde(default = "default_page_size")]
    pub(crate) page_size: i64,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ListTasksRequest {
    pub project_ident: Option<ProjectIdent>,
}

impl ListTasksQuery {
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

#[derive(Debug, Deserialize, Serialize)]
pub struct ListTasksResponse {
    pub(crate) tasks: Vec<Task>,
    pub(crate) continuation_token: Option<String>,
}

impl IntoResponse for ListTasksResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema, IntoParams)]
pub struct ListTaskInstancesQuery {
    pub task_id: Option<TaskId>,
    pub(crate) page_token: Option<String>,
    #[serde(default = "default_page_size")]
    pub(crate) page_size: i64,
}

impl ListTaskInstancesQuery {
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

#[derive(Debug, Deserialize, Serialize)]
pub struct ListTaskInstancesResponse {
    pub(crate) tasks: Vec<TaskInstance>,
    pub(crate) continuation_token: Option<String>,
}

impl IntoResponse for ListTaskInstancesResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> TaskService<C, A, S>
    for ApiContext<State<A, C, S>>
{
    async fn list_tasks(
        &self,
        request_metadata: RequestMetadata,
        body: ListTasksRequest,
        list_tasks_query: ListTasksQuery,
    ) -> Result<ListTasksResponse> {
        let mut tasks = C::list_tasks(
            list_tasks_query.pagination_query(),
            body,
            self.v1_state.catalog.clone(),
        )
        .await?;
        let mut project_idents = vec![];
        for t in &tasks.tasks {
            project_idents.push(self.v1_state.authz.is_allowed_project_action(
                &request_metadata,
                t.project_id,
                CatalogProjectAction::CanListTasks,
            ));
        }
        let outcome = futures::future::try_join_all(project_idents).await?;
        tasks.tasks = tasks
            .tasks
            .into_iter()
            .zip(outcome)
            .filter(|(_, o)| *o)
            .map(|(t, _)| t)
            .collect();
        // TODO: deal with empty pages
        Ok(tasks)
    }

    async fn list_task_instances(
        &self,
        request_metadata: RequestMetadata,
        list_task_instances_query: ListTaskInstancesQuery,
    ) -> Result<ListTaskInstancesResponse> {
        let mut t = C::list_task_instances(
            list_task_instances_query.task_id,
            list_task_instances_query.pagination_query(),
            self.v1_state.catalog.clone(),
        )
        .await?;
        let mut tasks = vec![];
        for t in &mut t.tasks {
            tasks.push(self.v1_state.authz.is_allowed_project_action(
                &request_metadata,
                t.project_ident,
                CatalogProjectAction::CanListTasks,
            ));
        }
        let outcome = futures::future::try_join_all(tasks).await?;
        t.tasks = t
            .tasks
            .into_iter()
            .zip(outcome)
            .filter(|(_, o)| *o)
            .map(|(t, _)| t)
            .collect();
        // TODO: deal with empty pages
        Ok(t)
    }
}

#[async_trait::async_trait]
pub(crate) trait TaskService<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn list_tasks(
        &self,
        request_metadata: RequestMetadata,
        body: ListTasksRequest,
        list_tasks_query: ListTasksQuery,
    ) -> Result<ListTasksResponse>;

    async fn list_task_instances(
        &self,
        request_metadata: RequestMetadata,
        list_task_instances_query: ListTaskInstancesQuery,
    ) -> Result<ListTaskInstancesResponse>;
}
