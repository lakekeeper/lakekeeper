use axum::{response::IntoResponse, Json};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use crate::{
    api::{management::v1::ApiServer, ApiContext},
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogTableAction, CatalogWarehouseAction},
        task_queue::{
            tabular_expiration_queue::QUEUE_NAME as TABULAR_EXPIRATION_QUEUE_NAME, TaskEntity,
            TaskFilter, TaskId, TaskOutcome as TQTaskOutcome, TaskQueueName,
            TaskStatus as TQTaskStatus,
        },
        Catalog, Result, SecretStore, State, TableId, Transaction,
    },
    WarehouseId,
};

const GET_TASK_PERMISSION: CatalogTableAction = CatalogTableAction::CanCommit;
const CONTROL_TASK_PERMISSION: CatalogTableAction = CatalogTableAction::CanDrop;
const CONTROL_TASK_WAREHOUSE_PERMISSION: CatalogWarehouseAction = CatalogWarehouseAction::CanDelete;
const CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION: CatalogWarehouseAction =
    CatalogWarehouseAction::CanListEverything;
const DEFAULT_ATTEMPTS: u16 = 5;

// -------------------- REQUEST/RESPONSE TYPES --------------------
#[derive(Debug, Serialize, utoipa::ToSchema, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct Task {
    /// Unique identifier for the task
    #[schema(value_type = uuid::Uuid)]
    pub task_id: TaskId,
    /// Warehouse ID associated with the task
    #[schema(value_type = uuid::Uuid)]
    pub warehouse_id: WarehouseId,
    /// Name of the queue processing this task
    #[schema(value_type = String)]
    pub queue_name: TaskQueueName,
    /// Type of entity this task operates on
    pub entity: TaskEntity,
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
    #[schema(value_type = Option<uuid::Uuid>)]
    pub parent_task_id: Option<TaskId>,
    /// When this task attempt was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When the task was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct GetTaskDetailsResponse {
    /// Most recent task information
    #[serde(flatten)]
    pub task: Task,
    /// Task-specific data
    #[schema(value_type = Object)]
    pub task_data: serde_json::Value,
    /// Execution details for the current attempt
    #[schema(value_type = Option<Object>)]
    pub execution_details: Option<serde_json::Value>,
    /// History of past attempts
    pub attempts: Vec<TaskAttempt>,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
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
    #[schema(example = "PT1H30M45.5S")]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    pub duration: Option<chrono::Duration>,
    /// Message associated with this attempt
    pub message: Option<String>,
    /// When this attempt was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Progress achieved in this attempt
    pub progress: f32,
    /// Execution details for this attempt
    #[schema(value_type = Option<Object>)]
    pub execution_details: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ListTasksResponse {
    /// List of tasks
    pub tasks: Vec<Task>,
    /// Token for the next page of results
    pub next_page_token: Option<String>,
}

impl IntoResponse for ListTasksResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

impl IntoResponse for GetTaskDetailsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

// -------------------- QUERY PARAMETERS --------------------
#[derive(Debug, Deserialize, utoipa::ToSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub struct ListTasksRequest {
    /// Filter by task status
    #[serde(default)]
    pub status: Option<Vec<TaskStatus>>,
    /// Filter by one or more queue names
    #[serde(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub queue_name: Option<Vec<TaskQueueName>>,
    /// Filter by specific entity
    #[serde(default)]
    pub entities: Option<Vec<TaskEntity>>,
    /// Filter tasks created after this timestamp
    #[serde(default)]
    #[schema(example = "2025-12-31T23:59:59Z")]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Filter tasks created before this timestamp
    #[serde(default)]
    #[schema(example = "2025-12-31T23:59:59Z")]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Next page token, re-use the same request as for the original request,
    /// but set this to the `next_page_token` from the previous response.
    /// Stop iterating when no more items are returned in a page.
    #[serde(default)]
    pub page_token: Option<String>,
    /// Number of results per page
    #[serde(default)]
    pub page_size: Option<i64>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct GetTaskDetailsQuery {
    /// Number of attempts to retrieve (default: 5)
    #[param(default = 5)]
    pub num_attempts: Option<u16>,
}

// -------------------- CONTROL REQUESTS --------------------

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ControlTasksRequest {
    /// The action to perform on the task
    pub action: ControlTaskAction,
    /// Tasks to apply the action to
    #[schema(value_type = Vec<uuid::Uuid>)]
    pub task_ids: Vec<TaskId>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", tag = "action")]
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
    RunAt {
        /// The time to run the task at
        #[schema(example = "2025-12-31T23:59:59Z")]
        scheduled_for: chrono::DateTime<chrono::Utc>,
    },
}

// -------------------- SERVICE TRAIT --------------------

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(crate) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
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

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        authorize_list_tasks(
            &authorizer,
            &request_metadata,
            warehouse_id,
            query.entities.as_ref(),
        )
        .await?;

        // -------------------- Business Logic --------------------
        let mut t = C::Transaction::begin_read(context.v1_state.catalog).await?;
        let tasks = C::list_tasks(warehouse_id, query, t.transaction()).await?;
        t.commit().await?;
        Ok(tasks)
    }

    /// Get detailed information about a specific task including attempt history
    async fn get_task_details(
        warehouse_id: WarehouseId,
        task_id: TaskId,
        query: GetTaskDetailsQuery,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetTaskDetailsResponse> {
        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        let (authz_can_use, authz_warehouse) = tokio::join!(
            authorizer.require_warehouse_action(
                &request_metadata,
                warehouse_id,
                CatalogWarehouseAction::CanUse,
            ),
            authorizer.is_allowed_warehouse_action(
                &request_metadata,
                warehouse_id,
                CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION
            )
        );
        authz_can_use
            .map_err(|e| e.append_detail("Not authorized to get tasks in the Warehouse."))?;
        let authz_warehouse = authz_warehouse?.into_inner();

        // -------------------- Business Logic --------------------
        let num_attempts = query.num_attempts.unwrap_or(DEFAULT_ATTEMPTS);
        let r = C::get_task_details(
            warehouse_id,
            task_id,
            num_attempts,
            context.v1_state.catalog,
        )
        .await?;

        let task_details = r.ok_or_else(|| {
            ErrorModel::not_found(
                format!("Task with id {task_id} not found"),
                "TaskNotFound",
                None,
            )
        })?;

        let entity = &task_details.task.entity;
        let TaskEntity::Table {
            table_id,
            warehouse_id: entity_warehouse_id,
        } = entity;

        if *entity_warehouse_id != warehouse_id {
            return Err(ErrorModel::internal(
                "The specified task does not belong to the specified warehouse.",
                "TaskWarehouseMismatch",
                None,
            )
            .into());
        }

        if !authz_warehouse {
            authorize_get_task_details_for_table_id(&authorizer, &request_metadata, *table_id)
                .await?;
        }

        Ok(task_details)
    }

    /// Control a task (stop or cancel)
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

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        let (authz_can_use, authz_warehouse) = tokio::join!(
            authorizer.require_warehouse_action(
                &request_metadata,
                warehouse_id,
                CatalogWarehouseAction::CanUse,
            ),
            authorizer.is_allowed_warehouse_action(
                &request_metadata,
                warehouse_id,
                CONTROL_TASK_WAREHOUSE_PERMISSION
            )
        );
        authz_can_use?;
        let authz_warehouse = authz_warehouse?.into_inner();

        if query.task_ids.is_empty() {
            return Ok(());
        }

        // If some tasks are not part of this warehouse, this will return an error.
        let entities = C::resolve_required_tasks(
            Some(warehouse_id),
            &query.task_ids,
            context.v1_state.catalog.clone(),
        )
        .await?;

        let expire_snapshots_table_ids = entities
            .iter()
            .filter_map(|(_, (entity, queue_name))| {
                if queue_name == &*TABULAR_EXPIRATION_QUEUE_NAME {
                    match entity {
                        TaskEntity::Table { table_id, .. } => Some(*table_id),
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<TableId>>();
        if !authz_warehouse {
            let task_to_table_map = entities
                .into_iter()
                .map(|(task_id, (entity, queue_name))| match entity {
                    TaskEntity::Table {
                        table_id,
                        warehouse_id: _,
                    } => (
                        task_id,
                        table_id,
                        if queue_name == *TABULAR_EXPIRATION_QUEUE_NAME {
                            CatalogTableAction::CanUndrop
                        } else {
                            CONTROL_TASK_PERMISSION
                        },
                    ),
                })
                .collect::<Vec<_>>();
            authorize_control_tasks_for_table_ids(
                &authorizer,
                &request_metadata,
                warehouse_id,
                &task_to_table_map,
            )
            .await?;
        }

        // -------------------- Business Logic --------------------
        let task_ids: Vec<TaskId> = query.task_ids;
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        match query.action {
            ControlTaskAction::Stop => C::stop_tasks(&task_ids, t.transaction()).await?,
            ControlTaskAction::Cancel => {
                if !expire_snapshots_table_ids.is_empty() {
                    C::clear_tabular_deleted_at(
                        &expire_snapshots_table_ids,
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
            ControlTaskAction::RunNow => C::run_tasks_at(&task_ids, None, t.transaction()).await?,
            ControlTaskAction::RunAt { scheduled_for } => {
                C::run_tasks_at(&task_ids, Some(scheduled_for), t.transaction()).await?;
            }
        }
        t.commit().await?;

        Ok(())
    }
}

async fn authorize_list_tasks<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    entities: Option<&Vec<TaskEntity>>,
) -> Result<()> {
    let can_list_everything = authorizer
        .require_warehouse_action(
            request_metadata,
            warehouse_id,
            CatalogWarehouseAction::CanListEverything,
        )
        .await
        .map_err(|e| e.append_detail("Not authorized to see all objects in the Warehouse. Add the `entity` filter to query tasks for specific entities."));

    // If warehouse_id is specified, check permission for that warehouse
    if let Some(entities) = entities {
        if can_list_everything.is_err() {
            let table_ids: Vec<(WarehouseId, TableId)> = entities
                .iter()
                .map(|entity| match entity {
                    TaskEntity::Table {
                        table_id,
                        warehouse_id,
                    } => (*warehouse_id, *table_id),
                })
                .collect();
            let allowed = authorizer
                .are_allowed_table_actions(
                    request_metadata,
                    table_ids
                        .iter()
                        .map(|(_warehouse_id, table_id)| (*table_id, GET_TASK_PERMISSION))
                        .collect(),
                )
                .await?
                .into_inner();
            let all_allowed = allowed.iter().all(|t| *t);
            if !all_allowed {
                return Err(ErrorModel::forbidden(
                    "Not allowed to get tasks for at least one of the specified entities.",
                    "NotAuthorized",
                    None,
                )
                .into());
            }
        } else {
            can_list_everything?;
        }
    } else {
        can_list_everything?;
    }
    Ok(())
}

async fn authorize_get_task_details_for_table_id<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    table_id: TableId,
) -> Result<()> {
    if !authorizer
        .is_allowed_table_action(request_metadata, table_id, GET_TASK_PERMISSION)
        .await?
        .into_inner()
    {
        return Err(ErrorModel::forbidden(
            "Not authorized to get tasks for the entity associated with the task.",
            "NotAuthorized",
            None,
        )
        .into());
    }

    Ok(())
}

async fn authorize_control_tasks_for_table_ids<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    _warehouse_id: WarehouseId,
    tasks: &[(TaskId, TableId, CatalogTableAction)],
) -> Result<()> {
    let allowed = authorizer
        .are_allowed_table_actions(request_metadata, tasks.iter().map(|t| (t.1, t.2)).collect())
        .await?
        .into_inner();
    let all_allowed = allowed.iter().all(|t| *t);
    let forbidden_tasks = tasks
        .iter()
        .zip(allowed.iter())
        .filter_map(|((task_id, table_id, action), is_allowed)| {
            if *is_allowed {
                None
            } else {
                Some(format!(
                    "`{action}` on task `{task_id}` with table `{table_id}`"
                ))
            }
        })
        .take(5)
        .join(", ");
    if !all_allowed {
        return Err(ErrorModel::forbidden(
            "Not authorized to perform actions on some entities.".to_string(),
            "NotAuthorized",
            None,
        )
        .append_detail(format!("Forbidden actions: {forbidden_tasks}"))
        .into());
    }
    Ok(())
}
