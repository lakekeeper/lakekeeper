use std::{collections::HashSet, sync::Arc};

use axum::{response::IntoResponse, Json};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use crate::{
    api::{management::v1::ApiServer, ApiContext},
    request_metadata::RequestMetadata,
    service::{
        authz::{
            ActionOnTableOrView, AuthZCannotSeeTable, AuthZCannotSeeView,
            AuthZCannotUseWarehouseId, AuthZTableOps as _, AuthZViewOps as _, Authorizer,
            AuthzWarehouseOps, CatalogTableAction, CatalogViewAction, CatalogWarehouseAction,
            RequireTableActionError, RequireViewActionError,
        },
        build_tabular_ident_from_vec,
        tasks::{
            tabular_expiration_queue::QUEUE_NAME as TABULAR_EXPIRATION_QUEUE_NAME, TaskEntity,
            TaskEntityNamed, TaskFilter, TaskId, TaskOutcome as TQTaskOutcome, TaskQueueName,
            TaskStatus as TQTaskStatus,
        },
        CatalogStore, CatalogTabularOps, CatalogTaskOps, InvalidTabularIdentifier, ResolvedTask,
        Result, SecretStore, State, TableNamed, TabularId, TabularListFlags, Transaction,
        ViewNamed, ViewOrTableInfo,
    },
    WarehouseId,
};

const GET_TASK_PERMISSION_TABLE: CatalogTableAction = CatalogTableAction::CanGetTasks;
const GET_TASK_PERMISSION_VIEW: CatalogViewAction = CatalogViewAction::CanGetTasks;
const CONTROL_TASK_PERMISSION_TABLE: CatalogTableAction = CatalogTableAction::CanControlTasks;
const CONTROL_TASK_PERMISSION_VIEW: CatalogViewAction = CatalogViewAction::CanControlTasks;
const CONTROL_TASK_WAREHOUSE_PERMISSION: CatalogWarehouseAction =
    CatalogWarehouseAction::CanControlAllTasks;
const CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION: CatalogWarehouseAction =
    CatalogWarehouseAction::CanGetAllTasks;
const DEFAULT_ATTEMPTS: u16 = 5;

// -------------------- REQUEST/RESPONSE TYPES --------------------
#[derive(Debug, Serialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct Task {
    /// Unique identifier for the task
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub task_id: TaskId,
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

impl Task {
    pub fn task_entity(&self) -> std::result::Result<TaskEntityNamed, InvalidTabularIdentifier> {
        match self.entity {
            TaskEntity::Table { table_id } => Ok(TaskEntityNamed::Table(TableNamed {
                warehouse_id: self.warehouse_id,
                table_ident: build_tabular_ident_from_vec(&self.entity_name)?,
                table_id,
            })),
            TaskEntity::View { view_id } => Ok(TaskEntityNamed::View(ViewNamed {
                warehouse_id: self.warehouse_id,
                view_ident: build_tabular_ident_from_vec(&self.entity_name)?,
                view_id,
            })),
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTaskDetailsResponse {
    /// Most recent task information
    #[serde(flatten)]
    pub task: Task,
    /// Task-specific data
    #[cfg_attr(feature = "open-api", schema(value_type = Object))]
    pub task_data: serde_json::Value,
    /// Execution details for the current attempt
    #[cfg_attr(feature = "open-api", schema(value_type = Option<Object>))]
    pub execution_details: Option<serde_json::Value>,
    /// History of past attempts
    pub attempts: Vec<TaskAttempt>,
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

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        authorize_list_tasks::<A, C>(
            &authorizer,
            context.v1_state.catalog.clone(),
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

        let [authz_can_use, authz_get_all_warehouse] = authorizer
            .are_allowed_warehouse_actions_arr(
                &request_metadata,
                &[
                    (warehouse_id, CatalogWarehouseAction::CanUse),
                    (warehouse_id, CAN_GET_ALL_TASKS_DETAILS_WAREHOUSE_PERMISSION),
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

        if !authz_get_all_warehouse {
            authorize_get_task_details(&authorizer, &request_metadata, &task_details).await?;
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

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        let [authz_can_use, authz_control_all] = authorizer
            .are_allowed_warehouse_actions_arr(
                &request_metadata,
                &[
                    (warehouse_id, CatalogWarehouseAction::CanUse),
                    (warehouse_id, CONTROL_TASK_WAREHOUSE_PERMISSION),
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

        // If some tasks are not part of this warehouse, this will return an error.
        let entities = C::resolve_required_tasks(
            warehouse_id,
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
                    }
                } else {
                    None
                }
            })
            .collect_vec();
        if !authz_control_all {
            authorize_control_tasks(&authorizer, &request_metadata, entities.values()).await?;
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
            ControlTaskAction::RunNow => C::run_tasks_at(&task_ids, None, t.transaction()).await?,
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
    entities: Option<&Vec<TaskEntity>>,
) -> Result<()> {
    let [can_use, can_list_everything] = authorizer
        .are_allowed_warehouse_actions_arr(
            request_metadata,
            &[
                (warehouse_id, CatalogWarehouseAction::CanUse),
                (warehouse_id, CatalogWarehouseAction::CanListEverything),
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

    let tabulars = C::get_tabular_infos_by_id_impl(
        warehouse_id,
        &tabular_ids,
        TabularListFlags::all(),
        catalog_state,
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

    let actions = tabulars
        .iter()
        .map(|t| t.as_action_request(GET_TASK_PERMISSION_VIEW, GET_TASK_PERMISSION_TABLE))
        .collect_vec();

    authorizer
        .require_tabular_actions(request_metadata, &actions)
        .await?;

    Ok(())
}

async fn authorize_get_task_details<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    details: &GetTaskDetailsResponse,
) -> Result<()> {
    let task_entity = details.task.task_entity()?;

    match task_entity {
        TaskEntityNamed::Table(t) => {
            authorizer
                .require_table_action(
                    request_metadata,
                    t.warehouse_id,
                    t.table_ident.clone(),
                    Ok::<_, RequireTableActionError>(Some(t)),
                    GET_TASK_PERMISSION_TABLE,
                )
                .await?;
        }
        TaskEntityNamed::View(v) => {
            authorizer
                .require_view_action(
                    request_metadata,
                    v.warehouse_id,
                    v.view_ident.clone(),
                    Ok::<_, RequireViewActionError>(Some(v)),
                    GET_TASK_PERMISSION_VIEW,
                )
                .await?;
        }
    }

    Ok(())
}

async fn authorize_control_tasks<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    tasks: impl Iterator<Item = &Arc<ResolvedTask>>,
) -> Result<()> {
    let tabular_actions = tasks
        .map(|t| match &t.entity {
            TaskEntityNamed::Table(t) => {
                ActionOnTableOrView::Table((t, CONTROL_TASK_PERMISSION_TABLE))
            }
            TaskEntityNamed::View(v) => {
                ActionOnTableOrView::View((v, CONTROL_TASK_PERMISSION_VIEW))
            }
        })
        .collect_vec();

    authorizer
        .require_tabular_actions(request_metadata, &tabular_actions)
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
