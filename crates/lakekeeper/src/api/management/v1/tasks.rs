use std::{collections::HashSet, sync::Arc};

use axum::{Json, response::IntoResponse};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId, WarehouseId,
    api::{ApiContext, management::v1::ApiServer},
    request_metadata::RequestMetadata,
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogTaskOps,
        CatalogWarehouseOps, ResolvedTask, ResolvedWarehouse, Result, SecretStore, State,
        TabularId, TabularListFlags, TaskDetails, TaskList, Transaction, ViewOrTableInfo,
        authz::{
            AuthZCannotSeeTable, AuthZCannotSeeView, AuthZCannotUseWarehouseId, AuthZProjectOps,
            AuthZTableOps as _, AuthZViewOps as _, Authorizer, AuthzNamespaceOps,
            AuthzWarehouseOps, CatalogProjectAction, CatalogTableAction, CatalogViewAction,
            CatalogWarehouseAction, RequireTableActionError, RequireViewActionError,
        },
        require_namespace_for_tabular,
        tasks::{
            EntityId, TaskDetailsScope, TaskEntity, TaskEntityNamed, TaskFilter, TaskId, TaskInfo,
            TaskOutcome as TQTaskOutcome, TaskQueueName, TaskResolveScope,
            TaskStatus as TQTaskStatus,
            tabular_expiration_queue::QUEUE_NAME as TABULAR_EXPIRATION_QUEUE_NAME,
        },
    },
};

const GET_TASK_PERMISSION_TABLE: CatalogTableAction = CatalogTableAction::GetTasks;
const GET_TASK_PERMISSION_VIEW: CatalogViewAction = CatalogViewAction::GetTasks;
const CONTROL_TASK_PERMISSION_TABLE: CatalogTableAction = CatalogTableAction::ControlTasks;
const CONTROL_TASK_PERMISSION_VIEW: CatalogViewAction = CatalogViewAction::ControlTasks;
const CONTROL_TASK_WAREHOUSE_PERMISSION: CatalogWarehouseAction =
    CatalogWarehouseAction::ControlAllTasks;
const CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION: CatalogWarehouseAction =
    CatalogWarehouseAction::GetAllTasks;
const DEFAULT_ATTEMPTS: u16 = 5;

// -------------------- REQUEST/RESPONSE TYPES --------------------
#[derive(Debug, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TaskInfoAPI {
    /// Unique identifier for the task
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub task_id: TaskId,
    /// Project ID associated with the task
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub project_id: ProjectId,
    /// Warehouse ID associated with the task
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub warehouse_id: WarehouseId,
    /// Name of the queue processing this task
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub queue_name: TaskQueueName,
    /// Type of entity this task operates on
    pub entity: TaskEntity,
    /// Name of the entity this task operates on
    pub entity_name: Vec<String>,
    /// Current status of the task
    pub status: TaskStatus,
    /// When the latest attempt of the task is scheduled for
    pub scheduled_for: chrono::DateTime<chrono::Utc>,
    /// When the latest attempt of the task was picked up for processing by a worker.
    pub picked_up_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Current attempt number
    pub attempt: i32,
    /// Last heartbeat timestamp for running tasks
    pub last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Progress of the task (0.0 to 1.0)
    pub progress: f32,
    /// Parent task ID if this is a sub-task
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub parent_task_id: Option<TaskId>,
    /// When this task attempt was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When the task was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl TryFrom<TaskInfo> for TaskInfoAPI {
    type Error = ErrorModel;

    fn try_from(value: TaskInfo) -> Result<Self, Self::Error> {
        let status = value
            .status()
            .copied()
            .map(TaskStatus::from)
            .or(value.outcome().copied().map(TaskStatus::from))
            .ok_or(ErrorModel::internal(
                "Task must have either status or outcome",
                "StatusOrOutcomeMissing",
                None,
            ))?;
        Ok(TaskInfoAPI {
            task_id: value.task_id(),
            project_id: value.project_id().clone(),
            warehouse_id: value.warehouse_id().ok_or(ErrorModel::internal(
                "WarehouseListTask needs WarehouseId",
                "WarehouseIdMissing",
                None,
            ))?,
            queue_name: value.queue_name().clone(),
            entity: match value.entity_id() {
                EntityId::Table(table_id) => TaskEntity::Table {
                    table_id: *table_id,
                },
                EntityId::View(view_id) => TaskEntity::View { view_id: *view_id },
                _ => {
                    return Err(ErrorModel::internal(
                        "Only Table and View entities are supported in WarehouseListTask",
                        "UnsupportedEntityType",
                        None,
                    ));
                }
            },
            entity_name: value.entity_name().cloned().ok_or(ErrorModel::internal(
                "WarehouseListTask needs entity_name",
                "EntityNameMissing",
                None,
            ))?,
            status,
            scheduled_for: value.scheduled_for().ok_or(ErrorModel::internal(
                "WarehouseListTask needs to be scheduled",
                "ScheduledForMissing",
                None,
            ))?,
            picked_up_at: value.picked_up_at(),
            attempt: value.attempt(),
            last_heartbeat_at: value.last_heartbeat_at(),
            progress: value.progress(),
            parent_task_id: value.parent_task_id(),
            created_at: value.created_at(),
            updated_at: value.updated_at(),
        })
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ProjectListTask {
    /// Unique identifier for the task
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub task_id: TaskId,
    /// Project ID associated with the task
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub project_id: ProjectId,
    /// Name of the queue processing this task
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub queue_name: TaskQueueName,
    /// Current status of the task
    pub status: TaskStatus,
    /// When the latest attempt of the task is scheduled for
    pub scheduled_for: chrono::DateTime<chrono::Utc>,
    /// When the latest attempt of the task was picked up for processing by a worker.
    pub picked_up_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Current attempt number
    pub attempt: i32,
    /// Last heartbeat timestamp for running tasks
    pub last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Progress of the task (0.0 to 1.0)
    pub progress: f32,
    /// Parent task ID if this is a sub-task
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub parent_task_id: Option<TaskId>,
    /// When this task attempt was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When the task was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl TryFrom<TaskInfo> for ProjectListTask {
    type Error = ErrorModel;

    fn try_from(value: TaskInfo) -> Result<Self, Self::Error> {
        let status = value
            .status()
            .copied()
            .map(TaskStatus::from)
            .or(value.outcome().copied().map(TaskStatus::from))
            .ok_or(ErrorModel::internal(
                "Task must have either status or outcome",
                "StatusOrOutcomeMissing",
                None,
            ))?;
        Ok(ProjectListTask {
            task_id: value.task_id(),
            project_id: value.project_id().clone(),
            queue_name: value.queue_name().clone(),
            status,
            scheduled_for: value.scheduled_for().ok_or(ErrorModel::internal(
                "ProjectListTask needs to be scheduled",
                "ScheduledForMissing",
                None,
            ))?,
            picked_up_at: value.picked_up_at(),
            attempt: value.attempt(),
            last_heartbeat_at: value.last_heartbeat_at(),
            progress: value.progress(),
            parent_task_id: value.parent_task_id(),
            created_at: value.created_at(),
            updated_at: value.updated_at(),
        })
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTaskDetailsResponse {
    /// Most recent task information
    #[serde(flatten)]
    pub task: TaskInfoAPI,
    /// Task-specific data
    #[cfg_attr(feature = "open-api", schema(value_type = Object))]
    pub task_data: serde_json::Value,
    /// Execution details for the current attempt
    #[cfg_attr(feature = "open-api", schema(value_type = Option<Object>))]
    pub execution_details: Option<serde_json::Value>,
    /// History of past attempts
    pub attempts: Vec<TaskAttempt>,
}

impl TryFrom<TaskDetails> for GetTaskDetailsResponse {
    type Error = ErrorModel;

    fn try_from(value: TaskDetails) -> Result<Self, Self::Error> {
        Ok(Self {
            task: TaskInfoAPI::try_from(value.task.clone())?,
            task_data: value.data,
            execution_details: value.execution_details,
            attempts: value.attempts,
        })
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetProjectTaskDetailsResponse {
    /// Most recent task information
    #[serde(flatten)]
    pub task: ProjectListTask,
    /// Task-specific data
    #[cfg_attr(feature = "open-api", schema(value_type = Object))]
    pub task_data: serde_json::Value,
    /// Execution details for the current attempt
    #[cfg_attr(feature = "open-api", schema(value_type = Option<Object>))]
    pub execution_details: Option<serde_json::Value>,
    /// History of past attempts
    pub attempts: Vec<TaskAttempt>,
}

impl IntoResponse for GetProjectTaskDetailsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

impl TryFrom<TaskDetails> for GetProjectTaskDetailsResponse {
    type Error = ErrorModel;

    fn try_from(value: TaskDetails) -> Result<Self, Self::Error> {
        Ok(Self {
            task: ProjectListTask::try_from(value.task)?,
            task_data: value.data,
            execution_details: value.execution_details,
            attempts: value.attempts,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TaskAttempt {
    /// Attempt number
    pub attempt: i32,
    /// Status of this attempt
    pub status: TaskStatus,
    /// When this attempt was scheduled for
    pub scheduled_for: chrono::DateTime<chrono::Utc>,
    /// When this attempt started
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    /// How long this attempt took
    #[cfg_attr(feature = "open-api", schema(example = "PT1H30M45.5S"))]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    pub duration: Option<chrono::Duration>,
    /// Message associated with this attempt
    pub message: Option<String>,
    /// When this attempt was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Progress achieved in this attempt
    pub progress: f32,
    /// Execution details for this attempt
    #[cfg_attr(feature = "open-api", schema(value_type = Option<Object>))]
    pub execution_details: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    /// Task is currently being processed
    Running,
    /// Task is scheduled and waiting to be picked up after the `scheduled_for` time
    Scheduled,
    /// Stop signal has been sent to the task, but termination is not yet reported
    Stopping,
    /// Task has been cancelled. This is a final state. The task won't be retried.
    Cancelled,
    /// Task completed successfully. This is a final state.
    Success,
    /// Task failed. This is a final state.
    Failed,
}

impl From<TQTaskStatus> for TaskStatus {
    fn from(value: TQTaskStatus) -> Self {
        match value {
            TQTaskStatus::Running => TaskStatus::Running,
            TQTaskStatus::Scheduled => TaskStatus::Scheduled,
            TQTaskStatus::ShouldStop => TaskStatus::Stopping,
        }
    }
}

impl From<TQTaskOutcome> for TaskStatus {
    fn from(value: TQTaskOutcome) -> Self {
        match value {
            TQTaskOutcome::Cancelled => TaskStatus::Cancelled,
            TQTaskOutcome::Success => TaskStatus::Success,
            TQTaskOutcome::Failed => TaskStatus::Failed,
        }
    }
}

impl TaskStatus {
    #[must_use]
    pub fn split(&self) -> (Option<TQTaskStatus>, Option<TQTaskOutcome>) {
        match self {
            TaskStatus::Running => (Some(TQTaskStatus::Running), None),
            TaskStatus::Scheduled => (Some(TQTaskStatus::Scheduled), None),
            TaskStatus::Stopping => (Some(TQTaskStatus::ShouldStop), None),
            TaskStatus::Cancelled => (None, Some(TQTaskOutcome::Cancelled)),
            TaskStatus::Success => (None, Some(TQTaskOutcome::Success)),
            TaskStatus::Failed => (None, Some(TQTaskOutcome::Failed)),
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListTasksResponse {
    /// List of tasks
    pub tasks: Vec<TaskInfoAPI>,
    /// Token for the next page of results
    pub next_page_token: Option<String>,
}

impl TryFrom<TaskList> for ListTasksResponse {
    type Error = ErrorModel;

    fn try_from(value: TaskList) -> Result<Self, Self::Error> {
        Ok(Self {
            tasks: value
                .tasks
                .into_iter()
                .map(TaskInfoAPI::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            next_page_token: value.next_page_token,
        })
    }
}

impl IntoResponse for ListTasksResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListProjectTasksResponse {
    /// List of tasks
    pub tasks: Vec<ProjectListTask>,
    /// Token for the next page of results
    pub next_page_token: Option<String>,
}

impl IntoResponse for ListProjectTasksResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

impl TryFrom<TaskList> for ListProjectTasksResponse {
    type Error = ErrorModel;

    fn try_from(value: TaskList) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            tasks: value
                .tasks
                .into_iter()
                .map(ProjectListTask::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            next_page_token: value.next_page_token,
        })
    }
}

impl IntoResponse for GetTaskDetailsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

// -------------------- QUERY PARAMETERS --------------------
#[derive(Debug, Deserialize, Default, typed_builder::TypedBuilder)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListTasksRequest {
    /// Filter by task status
    #[serde(default)]
    #[builder(default)]
    pub status: Option<Vec<TaskStatus>>,
    /// Filter by one or more queue names
    #[serde(default)]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<Vec<String>>))]
    #[builder(default)]
    pub queue_name: Option<Vec<TaskQueueName>>,
    /// Filter by specific entity
    #[serde(default)]
    #[builder(default)]
    pub entities: Option<Vec<TaskEntity>>,
    /// Filter tasks created after this timestamp
    #[serde(default)]
    #[builder(default)]
    #[cfg_attr(feature = "open-api", schema(example = "2025-12-31T23:59:59Z"))]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Filter tasks created before this timestamp
    #[serde(default)]
    #[builder(default)]
    #[cfg_attr(feature = "open-api", schema(example = "2025-12-31T23:59:59Z"))]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Next page token, re-use the same request as for the original request,
    /// but set this to the `next_page_token` from the previous response.
    /// Stop iterating when no more items are returned in a page.
    #[serde(default)]
    #[builder(default)]
    pub page_token: Option<String>,
    /// Number of results per page
    #[serde(default)]
    #[builder(default)]
    pub page_size: Option<i64>,
}

impl From<ListProjectTasksRequest> for ListTasksRequest {
    fn from(value: ListProjectTasksRequest) -> Self {
        Self {
            status: value.status,
            queue_name: value.queue_name,
            entities: None,
            created_after: value.created_after,
            created_before: value.created_before,
            page_token: value.page_token,
            page_size: value.page_size,
        }
    }
}

#[derive(Debug, Deserialize, Default, typed_builder::TypedBuilder)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListProjectTasksRequest {
    /// Filter by task status
    #[serde(default)]
    #[builder(default)]
    pub status: Option<Vec<TaskStatus>>,
    /// Filter by one or more queue names
    #[serde(default)]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<Vec<String>>))]
    #[builder(default)]
    pub queue_name: Option<Vec<TaskQueueName>>,
    /// Filter tasks created after this timestamp
    #[serde(default)]
    #[builder(default)]
    #[cfg_attr(feature = "open-api", schema(example = "2025-12-31T23:59:59Z"))]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Filter tasks created before this timestamp
    #[serde(default)]
    #[builder(default)]
    #[cfg_attr(feature = "open-api", schema(example = "2025-12-31T23:59:59Z"))]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Next page token, re-use the same request as for the original request,
    /// but set this to the `next_page_token` from the previous response.
    /// Stop iterating when no more items are returned in a page.
    #[serde(default)]
    #[builder(default)]
    pub page_token: Option<String>,
    /// Number of results per page
    #[serde(default)]
    #[builder(default)]
    pub page_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct GetTaskDetailsQuery {
    /// Number of attempts to retrieve (default: 5)
    #[cfg_attr(feature = "open-api", param(default = 5))]
    pub num_attempts: Option<u16>,
}

// -------------------- CONTROL REQUESTS --------------------

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ControlTasksRequest {
    /// The action to perform on the task
    pub action: ControlTaskAction,
    /// Tasks to apply the action to
    #[cfg_attr(feature = "open-api", schema(value_type = Vec<uuid::Uuid>))]
    pub task_ids: Vec<TaskId>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "action-type")]
pub enum ControlTaskAction {
    /// Stop the task gracefully. The task will be retried.
    Stop,
    /// Cancel the task permanently. The task is not retried.
    Cancel,
    /// Run the task immediately, moving the `scheduled_for` time to now.
    /// Affects only tasks in `Scheduled` or `Stopping` state.
    RunNow,
    /// Run the task at the specified time, moving the `scheduled_for` time to the provided timestamp.
    /// Affects only tasks in `Scheduled` or `Stopping` state.
    /// Timestamps must be in RFC 3339 format.
    #[serde(rename_all = "kebab-case")]
    RunAt {
        /// The time to run the task at
        #[cfg_attr(feature = "open-api", schema(example = "2025-12-31T23:59:59Z"))]
        #[serde(alias = "scheduled_for")]
        scheduled_for: chrono::DateTime<chrono::Utc>,
    },
}

// -------------------- SERVICE TRAIT --------------------

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> Service<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub(crate) trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    /// List tasks with optional filtering
    async fn list_tasks(
        warehouse_id: WarehouseId,
        query: ListTasksRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTasksResponse> {
        if let Some(entities) = &query.entities {
            if entities.len() > 100 {
                return Err(ErrorModel::bad_request(
                    "Cannot filter by more than 100 entities at once.",
                    "TooManyEntities",
                    None,
                )
                .into());
            }
            if entities.is_empty() {
                return Ok(ListTasksResponse {
                    tasks: vec![],
                    next_page_token: None,
                });
            }
        }

        if let Some(queue_names) = &query.queue_name {
            if queue_names.len() > 100 {
                return Err(ErrorModel::bad_request(
                    "Cannot filter by more than 100 queue names at once.",
                    "TooManyQueueNames",
                    None,
                )
                .into());
            }
            if queue_names.is_empty() {
                return Ok(ListTasksResponse {
                    tasks: vec![],
                    next_page_token: None,
                });
            }
        }

        let authorizer = context.v1_state.authz;
        // -------------------- AUTHZ --------------------
        let warehouse =
            C::get_active_warehouse_by_id(warehouse_id, context.v1_state.catalog.clone()).await;
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

        authorize_list_tasks::<A, C>(
            &authorizer,
            context.v1_state.catalog.clone(),
            &request_metadata,
            &warehouse,
            query.entities.as_ref(),
        )
        .await?;

        // -------------------- Business Logic --------------------
        let project_id = &warehouse.project_id;
        let filter = TaskFilter::WarehouseId {
            warehouse_id,
            project_id: project_id.clone(),
        };
        let mut t = C::Transaction::begin_read(context.v1_state.catalog).await?;
        let tasks = C::list_tasks(&filter, query, t.transaction()).await?;
        t.commit().await?;
        Ok(ListTasksResponse::try_from(tasks)?)
    }

    /// Get detailed information about a specific task including attempt history
    async fn get_task_details(
        warehouse_id: WarehouseId,
        task_id: TaskId,
        query: GetTaskDetailsQuery,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetTaskDetailsResponse> {
        let authorizer = context.v1_state.authz;

        // -------------------- AUTHZ --------------------
        let warehouse =
            C::get_active_warehouse_by_id(warehouse_id, context.v1_state.catalog.clone()).await;
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

        let [authz_can_use, authz_get_all_warehouse] = authorizer
            .are_allowed_warehouse_actions_arr(
                &request_metadata,
                None,
                &[
                    (&warehouse, CatalogWarehouseAction::Use),
                    (&warehouse, CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION),
                ],
            )
            .await?
            .into_inner();

        if !authz_can_use {
            return Err(AuthZCannotUseWarehouseId::new(warehouse_id).into());
        }

        // -------------------- Business Logic --------------------
        let num_attempts = query.num_attempts.unwrap_or(DEFAULT_ATTEMPTS);
        let r = C::get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id: warehouse.project_id.clone(),
                warehouse_id,
            },
            num_attempts,
            context.v1_state.catalog.clone(),
        )
        .await?;

        let task_details = r.ok_or_else(|| {
            ErrorModel::not_found(
                format!("Task with id {task_id} not found"),
                "TaskNotFound",
                None,
            )
        })?;

        let task_details = GetTaskDetailsResponse::try_from(task_details)?;

        if !authz_get_all_warehouse {
            authorize_get_task_details::<A, C>(
                context.v1_state.catalog,
                &authorizer,
                &request_metadata,
                &warehouse,
                &task_details,
            )
            .await?;
        }

        Ok(task_details)
    }

    /// Control a task (stop or cancel)
    #[allow(clippy::too_many_lines)]
    async fn control_tasks(
        warehouse_id: WarehouseId,
        query: ControlTasksRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        if query.task_ids.len() > 100 {
            return Err(ErrorModel::bad_request(
                "Cannot control more than 100 tasks at once.",
                "TooManyTasks",
                None,
            )
            .into());
        }

        // Each task id may only appear once
        let unique_task_ids: HashSet<TaskId> = query.task_ids.iter().copied().collect();
        if unique_task_ids.len() != query.task_ids.len() {
            return Err(ErrorModel::bad_request(
                "Duplicate task IDs are not allowed in the request.",
                "DuplicateTaskIds",
                None,
            )
            .into());
        }

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        let warehouse =
            C::get_active_warehouse_by_id(warehouse_id, context.v1_state.catalog.clone()).await;
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

        let [authz_can_use, authz_control_all] = authorizer
            .are_allowed_warehouse_actions_arr(
                &request_metadata,
                None,
                &[
                    (&warehouse, CatalogWarehouseAction::Use),
                    (&warehouse, CONTROL_TASK_WAREHOUSE_PERMISSION),
                ],
            )
            .await?
            .into_inner();

        if !authz_can_use {
            return Err(AuthZCannotUseWarehouseId::new(warehouse_id).into());
        }
        if query.task_ids.is_empty() {
            return Ok(());
        }

        let project_id = warehouse.project_id.clone();

        // If some tasks are not part of this warehouse, this will return an error.
        let entities = C::resolve_required_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &query.task_ids,
            context.v1_state.catalog.clone(),
        )
        .await?;

        let tabular_expiration_entities = entities
            .iter()
            .filter_map(|(_, resolved_task)| {
                if resolved_task.queue_name == *TABULAR_EXPIRATION_QUEUE_NAME {
                    let named_entity = &resolved_task.entity;
                    match named_entity {
                        TaskEntityNamed::Table(t) => Some(TabularId::Table(t.table_id)),
                        TaskEntityNamed::View(v) => Some(TabularId::View(v.view_id)),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect_vec();
        if !authz_control_all {
            authorize_control_tasks::<_, C>(
                &authorizer,
                &request_metadata,
                &warehouse,
                &entities.values().collect::<Vec<_>>(),
                context.v1_state.catalog.clone(),
            )
            .await?;
        }

        // -------------------- Business Logic --------------------
        let task_ids: Vec<TaskId> = query.task_ids;
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        match query.action {
            ControlTaskAction::Stop => C::stop_tasks(&task_ids, t.transaction()).await?,
            ControlTaskAction::Cancel => {
                if !tabular_expiration_entities.is_empty() {
                    C::clear_tabular_deleted_at(
                        &tabular_expiration_entities,
                        warehouse_id,
                        t.transaction(),
                    )
                    .await.map_err(|e| e.append_detail("Some of the specified tasks are tabular expiration / soft-deletion tasks that require Table undrop."))?;
                }
                C::cancel_scheduled_tasks(
                    None,
                    TaskFilter::TaskIds(task_ids),
                    true,
                    t.transaction(),
                )
                .await?;
            }
            ControlTaskAction::RunNow => {
                C::run_tasks_at(&task_ids, None, t.transaction()).await?;
            }
            ControlTaskAction::RunAt { scheduled_for } => {
                C::run_tasks_at(&task_ids, Some(scheduled_for), t.transaction()).await?;
            }
        }
        t.commit().await?;

        Ok(())
    }

    async fn list_project_tasks(
        query: ListProjectTasksRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListProjectTasksResponse> {
        let authorizer = context.v1_state.authz;
        // -------------------- AUTHZ --------------------
        let project_id = request_metadata.require_project_id(None)?;

        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::GetAllTasks,
            )
            .await?;

        // -------------------- Business Logic --------------------
        let filter = TaskFilter::ProjectId {
            project_id,
            include_sub_tasks: false, // Not yet implemented, so fixed here
        };
        let mut t = C::Transaction::begin_read(context.v1_state.catalog).await?;
        let tasks = C::list_tasks(&filter, query.into(), t.transaction()).await?;
        t.commit().await?;
        Ok(ListProjectTasksResponse::try_from(tasks)?)
    }

    async fn get_project_task_details(
        task_id: TaskId,
        query: GetTaskDetailsQuery,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetProjectTaskDetailsResponse> {
        let authorizer = context.v1_state.authz;

        // -------------------- AUTHZ --------------------
        let project_id = request_metadata.require_project_id(None)?;

        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::GetAllTasks,
            )
            .await?;

        // -------------------- Business Logic --------------------
        let num_attempts = query.num_attempts.unwrap_or(DEFAULT_ATTEMPTS);
        let r = C::get_task_details(
            task_id,
            TaskDetailsScope::Project { project_id },
            num_attempts,
            context.v1_state.catalog.clone(),
        )
        .await?;

        let task_details = r.ok_or_else(|| {
            ErrorModel::not_found(
                format!("Task with id {task_id} not found"),
                "TaskNotFound",
                None,
            )
        })?;

        Ok(GetProjectTaskDetailsResponse::try_from(task_details)?)
    }

    #[allow(clippy::too_many_lines)]
    async fn control_project_tasks(
        query: ControlTasksRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        if query.task_ids.len() > 100 {
            return Err(ErrorModel::bad_request(
                "Cannot control more than 100 tasks at once.",
                "TooManyTasks",
                None,
            )
            .into());
        }
        if query.task_ids.is_empty() {
            return Ok(());
        }

        // Each task id may only appear once
        let unique_task_ids: HashSet<TaskId> = query.task_ids.iter().copied().collect();
        if unique_task_ids.len() != query.task_ids.len() {
            return Err(ErrorModel::bad_request(
                "Duplicate task IDs are not allowed in the request.",
                "DuplicateTaskIds",
                None,
            )
            .into());
        }

        // -------------------- AUTHZ --------------------
        let project_id = request_metadata.require_project_id(None)?;

        let authorizer = context.v1_state.authz;

        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::ControlAllTasks,
            )
            .await?;

        // If some tasks are not part of this project, this will return an error.
        C::resolve_required_tasks(
            TaskResolveScope::Project { project_id },
            &query.task_ids,
            context.v1_state.catalog.clone(),
        )
        .await?;

        // -------------------- Business Logic --------------------
        let task_ids: Vec<TaskId> = query.task_ids;
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        match query.action {
            ControlTaskAction::Stop => C::stop_tasks(&task_ids, t.transaction()).await?,
            ControlTaskAction::Cancel => {
                C::cancel_scheduled_tasks(
                    None,
                    TaskFilter::TaskIds(task_ids),
                    true,
                    t.transaction(),
                )
                .await?;
            }
            ControlTaskAction::RunNow => {
                C::run_tasks_at(&task_ids, None, t.transaction()).await?;
            }
            ControlTaskAction::RunAt { scheduled_for } => {
                C::run_tasks_at(&task_ids, Some(scheduled_for), t.transaction()).await?;
            }
        }
        t.commit().await?;

        Ok(())
    }
}

async fn authorize_list_tasks<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    entities: Option<&Vec<TaskEntity>>,
) -> Result<()> {
    let warehouse_id = warehouse.warehouse_id;

    let [can_use, can_list_everything] = authorizer
        .are_allowed_warehouse_actions_arr(
            request_metadata,
            None,
            &[
                (warehouse, CatalogWarehouseAction::Use),
                (warehouse, CatalogWarehouseAction::ListEverything),
            ],
        )
        .await?
        .into_inner();

    if !can_use {
        return Err(AuthZCannotUseWarehouseId::new(warehouse_id).into());
    }

    if can_list_everything {
        return Ok(());
    }

    let Some(entities) = entities else {
        return Err(ErrorModel::forbidden(
            "Not authorized to see all tasks in the Warehouse. Add the `entity` filter to query tasks for specific entities.",
            "NotAuthorized",
            None,
        ).into());
    };

    let tabular_ids = entities
        .iter()
        .map(|entity| match entity {
            TaskEntity::Table { table_id } => TabularId::from(*table_id),
            TaskEntity::View { view_id } => TabularId::from(*view_id),
        })
        .collect::<Vec<_>>();

    let tabulars = C::get_tabular_infos_by_id(
        warehouse_id,
        &tabular_ids,
        TabularListFlags::all(),
        catalog_state.clone(),
    )
    .await
    // Use AuthZ Error for potential anonymization
    .map_err(RequireTableActionError::from)?;
    let found_ids = tabulars
        .iter()
        .map(ViewOrTableInfo::tabular_id)
        .collect::<HashSet<_>>();

    let missing_tabular_ids = tabular_ids
        .iter()
        .filter(|id| !found_ids.contains(id))
        .copied()
        .collect::<Vec<_>>();
    if let Some(missing) = missing_tabular_ids.first() {
        match missing {
            TabularId::Table(t) => {
                return Err(AuthZCannotSeeTable::new(warehouse_id, *t).into());
            }
            TabularId::View(v) => {
                return Err(AuthZCannotSeeView::new(warehouse_id, *v).into());
            }
        }
    }

    let namespaces = C::get_namespaces_by_id(
        warehouse_id,
        &tabulars
            .iter()
            .map(ViewOrTableInfo::namespace_id)
            .collect::<Vec<_>>(),
        catalog_state,
    )
    .await?;

    let actions = tabulars
        .iter()
        .map(|t| {
            Ok::<_, ErrorModel>((
                require_namespace_for_tabular(&namespaces, t)?,
                t.as_action_request(GET_TASK_PERMISSION_VIEW, GET_TASK_PERMISSION_TABLE),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    authorizer
        .require_tabular_actions(request_metadata, warehouse, &namespaces, &actions)
        .await?;

    Ok(())
}

async fn authorize_get_task_details<A: Authorizer, C: CatalogStore>(
    catalog_state: C::State,
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    details: &GetTaskDetailsResponse,
) -> Result<()> {
    let warehouse_id = warehouse.warehouse_id;

    match &details.task.entity {
        TaskEntity::Table { table_id } => {
            let tabular_info = C::get_table_info(
                warehouse_id,
                *table_id,
                TabularListFlags::all(),
                catalog_state.clone(),
            )
            .await
            .map_err(RequireTableActionError::from)?
            .ok_or_else(|| AuthZCannotSeeTable::new(warehouse_id, *table_id))?;

            let namespace_id = tabular_info.namespace_id;
            let namespace = C::get_namespace_cache_aware(
                warehouse_id,
                namespace_id,
                CachePolicy::RequireMinimumVersion(*tabular_info.namespace_version),
                catalog_state,
            )
            .await;
            let namespace =
                authorizer.require_namespace_presence(warehouse_id, namespace_id, namespace)?;

            authorizer
                .require_table_action(
                    request_metadata,
                    warehouse,
                    &namespace,
                    *table_id,
                    Ok::<_, RequireTableActionError>(Some(tabular_info)),
                    GET_TASK_PERMISSION_TABLE,
                )
                .await?;
        }
        TaskEntity::View { view_id } => {
            let view_info = C::get_view_info(
                warehouse_id,
                *view_id,
                TabularListFlags::all(),
                catalog_state.clone(),
            )
            .await
            .map_err(RequireViewActionError::from)?
            .ok_or_else(|| AuthZCannotSeeView::new(warehouse_id, *view_id))?;

            let namespace_id = view_info.namespace_id;
            let namespace = C::get_namespace_cache_aware(
                warehouse_id,
                view_info.namespace_id,
                CachePolicy::RequireMinimumVersion(*view_info.namespace_version),
                catalog_state,
            )
            .await;
            let namespace =
                authorizer.require_namespace_presence(warehouse_id, namespace_id, namespace)?;

            authorizer
                .require_view_action(
                    request_metadata,
                    warehouse,
                    &namespace,
                    *view_id,
                    Ok::<_, RequireViewActionError>(Some(view_info)),
                    GET_TASK_PERMISSION_VIEW,
                )
                .await?;
        }
    }
    Ok(())
}

async fn authorize_control_tasks<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    tasks: &[&Arc<ResolvedTask>],
    catalog_state: C::State,
) -> Result<()> {
    let required_tabular_ids = tasks
        .iter()
        .filter_map(|t| match &t.entity {
            TaskEntityNamed::Table(tabular) => Some(TabularId::Table(tabular.table_id)),
            TaskEntityNamed::View(tabular) => Some(TabularId::View(tabular.view_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let required_namespace_idents = tasks
        .iter()
        .filter_map(|t| match &t.entity {
            TaskEntityNamed::Table(tabular) => Some(&tabular.table_ident.namespace),
            TaskEntityNamed::View(tabular) => Some(&tabular.view_ident.namespace),
            _ => None,
        })
        .collect::<Vec<_>>();

    let (table_infos, namespaces) = tokio::join!(
        C::get_tabular_infos_by_id(
            warehouse.warehouse_id,
            &required_tabular_ids,
            TabularListFlags::all(),
            catalog_state.clone(),
        ),
        C::get_namespaces_by_ident(
            warehouse.warehouse_id,
            &required_namespace_idents,
            catalog_state,
        ),
    );
    let table_infos = table_infos.map_err(RequireTableActionError::from)?;
    let namespaces = namespaces?;

    let found_table_ids = table_infos
        .iter()
        .map(ViewOrTableInfo::tabular_id)
        .collect::<HashSet<_>>();

    for required_tabular_id in required_tabular_ids {
        if !found_table_ids.contains(&required_tabular_id) {
            match required_tabular_id {
                TabularId::Table(t) => {
                    return Err(AuthZCannotSeeTable::new(warehouse.warehouse_id, t).into());
                }
                TabularId::View(v) => {
                    return Err(AuthZCannotSeeView::new(warehouse.warehouse_id, v).into());
                }
            }
        }
    }

    let tabular_actions = table_infos
        .iter()
        .map(|t| {
            Ok::<_, ErrorModel>((
                require_namespace_for_tabular(&namespaces, t)?,
                t.as_action_request(CONTROL_TASK_PERMISSION_VIEW, CONTROL_TASK_PERMISSION_TABLE),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    authorizer
        .require_tabular_actions(request_metadata, warehouse, &namespaces, &tabular_actions)
        .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_control_task_request_serde() {
        let request = ControlTasksRequest {
            action: ControlTaskAction::RunAt {
                scheduled_for: "2025-12-31T23:59:59Z"
                    .parse()
                    .expect("Failed to parse datetime"),
            },
            task_ids: vec![TaskId::from(
                uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            )],
        };
        let request_json = serde_json::json!({
            "action": {
                "action-type": "run-at",
                "scheduled-for": "2025-12-31T23:59:59Z"
            },
            "task-ids": ["550e8400-e29b-41d4-a716-446655440000"]
        });

        assert_eq!(
            serde_json::to_value(&request).expect("Failed to serialize"),
            request_json
        );

        let deserialized: ControlTasksRequest =
            serde_json::from_value(request_json).expect("Failed to deserialize");
        assert_eq!(deserialized, request);
    }
}
