use std::{collections::HashSet, sync::Arc};

use axum::{Json, response::IntoResponse};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId, WarehouseId,
    api::{
        ApiContext,
        management::v1::{ApiServer, impl_arc_into_response},
    },
    request_metadata::{ProjectIdMissing, RequestMetadata},
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogTaskOps,
        CatalogWarehouseOps, NoWarehouseTaskError, ResolvedTask, ResolvedWarehouse, Result,
        SecretStore, State, TableId, TabularId, TabularListFlags, TaskDetails, TaskList,
        TaskNotFoundError, Transaction, ViewId, ViewOrTableInfo,
        authz::{
            AuthZCannotListAllTasks, AuthZCannotSeeTable, AuthZCannotSeeView,
            AuthZCannotUseWarehouseId, AuthZError, AuthZProjectOps, AuthZTableOps as _,
            AuthZViewOps as _, AuthZWarehouseActionForbidden, Authorizer, AuthzNamespaceOps,
            AuthzWarehouseOps, CatalogProjectAction, CatalogTableAction, CatalogViewAction,
            CatalogWarehouseAction, RequireNamespaceActionError, RequireTableActionError,
            RequireViewActionError, RequireWarehouseActionError,
        },
        events::{
            APIEventContext,
            context::{GetTaskDetailsAction, Unresolved, UserProvidedTask},
        },
        require_namespace_for_tabular,
        tasks::{
            CancelTasksFilter, ResolvedTaskEntity, TaskDetailsScope, TaskEntity, TaskFilter,
            TaskId, TaskInfo, TaskIntermediateStatus, TaskMetadata, TaskOutcome, TaskQueueName,
            TaskResolveScope, WarehouseTaskEntityId,
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
pub struct WarehouseTaskInfo {
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
    /// Type of the sub-entity this task operates on. None if this is a warehouse-level task.
    pub entity: Option<WarehouseTaskEntityId>,
    /// Name of the entity this task operates on. None if this is a warehouse-level task.
    pub entity_name: Option<Vec<String>>,
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

impl TryFrom<TaskInfo> for WarehouseTaskInfo {
    type Error = ErrorModel;

    fn try_from(value: TaskInfo) -> Result<Self, Self::Error> {
        let TaskInfo {
            task_metadata,
            queue_name,
            id,
            status,
            picked_up_at,
            last_heartbeat_at,
            progress,
            created_at,
            updated_at,
        } = value;

        let task_id = id.task_id;
        let attempt = id.attempt;

        let TaskMetadata {
            project_id,
            parent_task_id,
            scheduled_for,
            entity: scope,
        } = task_metadata;

        let (warehouse_id, sub_entity) = match scope {
            TaskEntity::Project => {
                return Err(ErrorModel::internal(
                    "Expected Warehouse task but received project task",
                    "EntityMetadataMissing",
                    None,
                ));
            }
            TaskEntity::Warehouse { warehouse_id } => (warehouse_id, None),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name,
            } => (warehouse_id, Some((entity_id, entity_name))),
        };

        let (entity_id, entity_name) = match sub_entity {
            Some((entity_id, entity_name)) => (Some(entity_id), Some(entity_name)),
            None => (None, None),
        };

        Ok(WarehouseTaskInfo {
            task_id,
            warehouse_id,
            entity: entity_id,
            status,
            scheduled_for,
            picked_up_at,
            attempt,
            last_heartbeat_at,
            progress,
            parent_task_id,
            created_at,
            updated_at,
            entity_name,
            queue_name,
            project_id,
        })
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ProjectTaskInfo {
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

impl TryFrom<TaskInfo> for ProjectTaskInfo {
    type Error = ErrorModel;

    fn try_from(value: TaskInfo) -> Result<Self, Self::Error> {
        // Validate that this is actually a project-scoped task
        if !matches!(value.task_metadata.entity, TaskEntity::Project) {
            return Err(ErrorModel::internal(
                "Expected Project task but received warehouse or entity task",
                "ProjectTaskExpected",
                None,
            ));
        }

        Ok(ProjectTaskInfo {
            task_id: value.task_id(),
            status: value.status(),
            scheduled_for: value.scheduled_for(),
            picked_up_at: value.picked_up_at(),
            attempt: value.attempt(),
            last_heartbeat_at: value.last_heartbeat_at(),
            progress: value.progress(),
            parent_task_id: value.parent_task_id(),
            created_at: value.created_at(),
            updated_at: value.updated_at(),
            project_id: value.task_metadata.project_id().clone(),
            queue_name: value.queue_name,
        })
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTaskDetailsResponse {
    /// Most recent task information
    #[serde(flatten)]
    pub task: WarehouseTaskInfo,
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
        let TaskDetails {
            task,
            data,
            execution_details,
            attempts,
        } = value;

        Ok(Self {
            task: WarehouseTaskInfo::try_from(task)?,
            task_data: data,
            execution_details,
            attempts,
        })
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetProjectTaskDetailsResponse {
    /// Most recent task information
    #[serde(flatten)]
    pub task: ProjectTaskInfo,
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
        let TaskDetails {
            task,
            data,
            execution_details,
            attempts,
        } = value;

        Ok(Self {
            task: ProjectTaskInfo::try_from(task)?,
            task_data: data,
            execution_details,
            attempts,
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

impl From<TaskIntermediateStatus> for TaskStatus {
    fn from(value: TaskIntermediateStatus) -> Self {
        match value {
            TaskIntermediateStatus::Running => TaskStatus::Running,
            TaskIntermediateStatus::Scheduled => TaskStatus::Scheduled,
            TaskIntermediateStatus::ShouldStop => TaskStatus::Stopping,
        }
    }
}

impl From<TaskOutcome> for TaskStatus {
    fn from(value: TaskOutcome) -> Self {
        match value {
            TaskOutcome::Cancelled => TaskStatus::Cancelled,
            TaskOutcome::Success => TaskStatus::Success,
            TaskOutcome::Failed => TaskStatus::Failed,
        }
    }
}

impl TaskStatus {
    #[must_use]
    pub fn split(&self) -> (Option<TaskIntermediateStatus>, Option<TaskOutcome>) {
        match self {
            TaskStatus::Running => (Some(TaskIntermediateStatus::Running), None),
            TaskStatus::Scheduled => (Some(TaskIntermediateStatus::Scheduled), None),
            TaskStatus::Stopping => (Some(TaskIntermediateStatus::ShouldStop), None),
            TaskStatus::Cancelled => (None, Some(TaskOutcome::Cancelled)),
            TaskStatus::Success => (None, Some(TaskOutcome::Success)),
            TaskStatus::Failed => (None, Some(TaskOutcome::Failed)),
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListTasksResponse {
    /// List of tasks
    pub tasks: Vec<WarehouseTaskInfo>,
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
                .map(WarehouseTaskInfo::try_from)
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
    pub tasks: Vec<ProjectTaskInfo>,
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
                .map(ProjectTaskInfo::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            next_page_token: value.next_page_token,
        })
    }
}

impl_arc_into_response!(GetTaskDetailsResponse);

// -------------------- QUERY PARAMETERS --------------------
#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum WarehouseTaskEntityFilter {
    /// Get tasks for a specific table
    #[serde(rename_all = "kebab-case")]
    Table {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        table_id: TableId,
    },
    /// Get tasks for a specific view
    #[serde(rename_all = "kebab-case")]
    View {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        view_id: ViewId,
    },
    /// Get Warehouse-level tasks which are not associated with a specific entity
    /// inside the warehouse
    Warehouse,
}

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
    pub entities: Option<Vec<WarehouseTaskEntityFilter>>,
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
        let events = context.v1_state.events;
        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            events,
            warehouse_id,
            CatalogWarehouseAction::GetAllTasks,
        );
        let authz_result = authorize_list_tasks::<A, C>(
            &authorizer,
            context.v1_state.catalog.clone(),
            event_ctx.request_metadata(),
            warehouse_id,
            query.entities.as_ref(),
        )
        .await;

        let (event_ctx, warehouse) = event_ctx.emit_authz(authz_result)?;

        let event_ctx = Arc::new(event_ctx.resolve(warehouse));

        // -------------------- Business Logic --------------------
        let project_id = event_ctx.resolved().project_id.clone();
        let filter = TaskFilter::WarehouseId {
            warehouse_id,
            project_id: project_id,
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
    ) -> Result<Arc<GetTaskDetailsResponse>> {
        let authorizer = context.v1_state.authz;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_task(
            request_metadata.clone().into(),
            context.v1_state.events,
            warehouse_id,
            task_id,
            GetTaskDetailsAction {},
        );

        let authz_result = check_get_task_details_authorization::<A, C>(
            &authorizer,
            &query,
            context.v1_state.catalog,
            &event_ctx,
            warehouse_id,
        )
        .await;

        let (event_ctx, task_details) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = Arc::new(event_ctx.resolve(task_details));

        Ok(event_ctx.resolved().clone())
    }

    /// Control a task (stop or cancel)
    #[allow(clippy::too_many_lines)]
    async fn control_tasks(
        warehouse_id: WarehouseId,
        query: ControlTasksRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        if query.task_ids.is_empty() {
            return Ok(());
        }

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
        let catalog_state = context.v1_state.catalog;

        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.clone().into(),
            context.v1_state.events,
            warehouse_id,
            CatalogWarehouseAction::ControlAllTasks,
        );

        let authz_result = check_control_tasks_authorization::<A, C>(
            &authorizer,
            catalog_state.clone(),
            event_ctx.request_metadata(),
            &query,
            event_ctx.user_provided_entity().clone(),
        )
        .await;

        let (event_ctx, tabular_expiration_entities) = event_ctx.emit_authz(authz_result)?;

        let event_ctx = Arc::new(event_ctx.resolve(tabular_expiration_entities.clone()));

        // -------------------- Business Logic --------------------
        let task_ids: Vec<TaskId> = query.task_ids;
        let mut t = C::Transaction::begin_write(catalog_state).await?;
        match query.action {
            ControlTaskAction::Stop => C::stop_tasks(&task_ids, t.transaction()).await?,
            ControlTaskAction::Cancel => {
                if !event_ctx.resolved().is_empty() {
                    C::clear_tabular_deleted_at(
                        &event_ctx.resolved(),
                        warehouse_id,
                        t.transaction(),
                    )
                    .await.map_err(|e| e.append_detail("Some of the specified tasks are tabular expiration / soft-deletion tasks that require Table undrop."))?;
                }
                C::cancel_scheduled_tasks(
                    None,
                    CancelTasksFilter::TaskIds(task_ids),
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
        let event_ctx = APIEventContext::for_project(
            request_metadata.clone().into(),
            context.v1_state.events,
            project_id.clone(),
            CatalogProjectAction::GetProjectTasks,
        );

        let authz_result = authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::GetProjectTasks,
            )
            .await;

        let (event_ctx, _) = event_ctx.emit_authz(authz_result)?;

        // -------------------- Business Logic --------------------
        let project_id = event_ctx.user_provided_entity().clone();
        let filter = TaskFilter::ProjectId {
            project_id,
            include_sub_tasks: false, // Not yet implemented, so hardcoded here
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

        let event_ctx = APIEventContext::for_project(
            request_metadata.clone().into(),
            context.v1_state.events,
            project_id.clone(),
            CatalogProjectAction::GetProjectTasks,
        );

        let authz_result = authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::GetProjectTasks,
            )
            .await;
        let (event_ctx, _) = event_ctx.emit_authz(authz_result)?;

        // -------------------- Business Logic --------------------
        let project_id = event_ctx.user_provided_entity().clone();
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

        let event_ctx = APIEventContext::for_project(
            request_metadata.clone().into(),
            context.v1_state.events,
            project_id.clone(),
            CatalogProjectAction::GetProjectTasks,
        );

        let authorizer = context.v1_state.authz;

        let authz_result = authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::ControlProjectTasks,
            )
            .await;

        let (event_ctx, _) = event_ctx.emit_authz(authz_result)?;

        let project_id = event_ctx.user_provided_entity().clone();

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
                    CancelTasksFilter::TaskIds(task_ids),
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
    warehouse_id: WarehouseId,
    entities: Option<&Vec<WarehouseTaskEntityFilter>>,
) -> Result<Arc<ResolvedWarehouse>, AuthZError> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    let [can_use, can_list_everything] = authorizer
        .are_allowed_warehouse_actions_arr(
            request_metadata,
            None,
            &[
                (&warehouse, CatalogWarehouseAction::Use),
                (&warehouse, CatalogWarehouseAction::ListEverything),
            ],
        )
        .await?
        .into_inner();

    if !can_use {
        return Err(AuthZCannotUseWarehouseId::new_access_denied(warehouse_id).into());
    }

    if can_list_everything {
        return Ok(warehouse);
    }

    let Some(entities) = entities else {
        return Err(
            RequireWarehouseActionError::from(AuthZCannotListAllTasks::new(warehouse_id)).into(),
        );
    };

    let tabular_ids = entities
        .iter()
        .map(|entity| match entity {
            WarehouseTaskEntityFilter::Table { table_id } => Ok(TabularId::from(*table_id)),
            WarehouseTaskEntityFilter::View { view_id } => Ok(TabularId::from(*view_id)),
            WarehouseTaskEntityFilter::Warehouse => Err(RequireWarehouseActionError::from(
                AuthZCannotListAllTasks::new(warehouse_id),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

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
                return Err(AuthZCannotSeeTable::new_not_found(warehouse_id, *t).into());
            }
            TabularId::View(v) => {
                return Err(AuthZCannotSeeView::new_not_found(warehouse_id, *v).into());
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
    .await
    .map_err(RequireNamespaceActionError::from)?;

    let actions = tabulars
        .iter()
        .map(|t| {
            Ok::<_, AuthZError>((
                require_namespace_for_tabular(&namespaces, t)?,
                t.as_action_request(GET_TASK_PERMISSION_VIEW, GET_TASK_PERMISSION_TABLE),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    authorizer
        .require_tabular_actions(request_metadata, &warehouse, &namespaces, &actions)
        .await?;

    Ok(warehouse)
}

async fn check_get_task_details_authorization<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    query: &GetTaskDetailsQuery,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTask, Unresolved, GetTaskDetailsAction>,
    warehouse_id: WarehouseId,
) -> Result<Arc<GetTaskDetailsResponse>, AuthZError> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    let [authz_can_use, authz_get_all_warehouse] = authorizer
        .are_allowed_warehouse_actions_arr(
            event_ctx.request_metadata(),
            None,
            &[
                (&warehouse, CatalogWarehouseAction::Use),
                (&warehouse, CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION),
            ],
        )
        .await?
        .into_inner();

    if !authz_can_use {
        return Err(AuthZCannotUseWarehouseId::new_access_denied(warehouse_id).into());
    }
    // -------------------- Business Logic --------------------
    let task_id = event_ctx.user_provided_entity().task_id;
    let num_attempts = query.num_attempts.unwrap_or(DEFAULT_ATTEMPTS);
    let r = C::get_task_details(
        task_id,
        TaskDetailsScope::Warehouse {
            project_id: warehouse.project_id.clone(),
            warehouse_id,
        },
        num_attempts,
        catalog_state.clone(),
    )
    .await?;
    let task_details = r.ok_or_else(|| TaskNotFoundError {
        task_id,
        stack: Vec::new(),
    })?;

    let task_details = GetTaskDetailsResponse::try_from(task_details)
        .map_err(|_| NoWarehouseTaskError { stack: Vec::new() })?;

    if !authz_get_all_warehouse {
        authorize_get_task_details::<A, C>(
            catalog_state,
            &authorizer,
            event_ctx.request_metadata(),
            &warehouse,
            &task_details,
        )
        .await?;
    }

    Ok(task_details.into())
}

async fn authorize_get_task_details<A: Authorizer, C: CatalogStore>(
    catalog_state: C::State,
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    details: &GetTaskDetailsResponse,
) -> Result<(), AuthZError> {
    let warehouse_id = warehouse.warehouse_id;

    if let Some(sub_entity) = &details.task.entity {
        match sub_entity {
            WarehouseTaskEntityId::Table { table_id } => {
                let tabular_info = C::get_table_info(
                    warehouse_id,
                    *table_id,
                    TabularListFlags::all(),
                    catalog_state.clone(),
                )
                .await
                .map_err(RequireTableActionError::from)?
                .ok_or_else(|| AuthZCannotSeeTable::new_not_found(warehouse_id, *table_id))?;

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
            WarehouseTaskEntityId::View { view_id } => {
                let view_info = C::get_view_info(
                    warehouse_id,
                    *view_id,
                    TabularListFlags::all(),
                    catalog_state.clone(),
                )
                .await
                .map_err(RequireViewActionError::from)?
                .ok_or_else(|| AuthZCannotSeeView::new_not_found(warehouse_id, *view_id))?;

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
    } else {
        // Warehouse permission already checked before calling this function
        return Err(AuthZWarehouseActionForbidden::new(
            warehouse.warehouse_id,
            &CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION,
        )
        .into());
    }
    Ok(())
}

async fn authorize_control_tasks<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    tasks: &[&Arc<ResolvedTask>],
    catalog_state: C::State,
) -> Result<(), AuthZError> {
    let (required_tabular_ids, required_namespace_idents) = tasks
        .iter()
        .map(|t| match &t.entity {
            ResolvedTaskEntity::Table(tabular) => Ok((
                TabularId::Table(tabular.table_id),
                &tabular.table_ident.namespace,
            )),
            ResolvedTaskEntity::View(tabular) => Ok((
                TabularId::View(tabular.view_id),
                &tabular.view_ident.namespace,
            )),
            ResolvedTaskEntity::Warehouse(warehouse_id) => {
                Err(AuthZCannotUseWarehouseId::new_access_denied(warehouse_id.clone()).into())
            }
            ResolvedTaskEntity::Project => Err(AuthZError::ProjectIdMissing(ProjectIdMissing)),
        })
        .collect::<Result<(Vec<_>, Vec<_>), AuthZError>>()?;

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
    let namespaces = namespaces.map_err(RequireNamespaceActionError::from)?;

    let found_table_ids = table_infos
        .iter()
        .map(ViewOrTableInfo::tabular_id)
        .collect::<HashSet<_>>();

    for required_tabular_id in required_tabular_ids {
        if !found_table_ids.contains(&required_tabular_id) {
            match required_tabular_id {
                TabularId::Table(t) => {
                    return Err(
                        AuthZCannotSeeTable::new_not_found(warehouse.warehouse_id, t).into(),
                    );
                }
                TabularId::View(v) => {
                    return Err(AuthZCannotSeeView::new_not_found(warehouse.warehouse_id, v).into());
                }
            }
        }
    }

    let tabular_actions = table_infos
        .iter()
        .map(|t| {
            Ok::<_, AuthZError>((
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

async fn check_control_tasks_authorization<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
    query: &ControlTasksRequest,
    warehouse_id: WarehouseId,
) -> Result<Vec<TabularId>, AuthZError> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
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
        return Err(AuthZCannotUseWarehouseId::new_access_denied(warehouse_id).into());
    }

    let project_id = warehouse.project_id.clone();

    // If some tasks are not part of this warehouse, this will return an error.
    let resolved_tasks = C::resolve_required_tasks(
        TaskResolveScope::Warehouse {
            project_id,
            warehouse_id: Some(warehouse_id),
        },
        &query.task_ids,
        catalog_state.clone(),
    )
    .await?;

    let tabular_expiration_entities = resolved_tasks
        .iter()
        .filter_map(|(_, resolved_task)| {
            if resolved_task.queue_name == *TABULAR_EXPIRATION_QUEUE_NAME {
                let resolved_task = &resolved_task.entity;
                match resolved_task {
                    ResolvedTaskEntity::Table(t) => Some(TabularId::Table(t.table_id)),
                    ResolvedTaskEntity::View(v) => Some(TabularId::View(v.view_id)),
                    ResolvedTaskEntity::Warehouse { .. } | ResolvedTaskEntity::Project => None, // Project not returned due to scope
                }
            } else {
                None
            }
        })
        .collect_vec();
    if !authz_control_all {
        authorize_control_tasks::<A, C>(
            authorizer,
            request_metadata,
            &warehouse,
            &resolved_tasks.values().collect::<Vec<_>>(),
            catalog_state.clone(),
        )
        .await?;
    }
    Ok(tabular_expiration_entities)
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
