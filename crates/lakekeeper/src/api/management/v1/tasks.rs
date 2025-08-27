use axum::{response::IntoResponse, Json};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::default_page_size;
use crate::{
    api::{
        iceberg::{types::PageToken, v1::PaginationQuery},
        management::v1::ApiServer,
        ApiContext,
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogTableAction, CatalogWarehouseAction},
        Catalog, Result, SecretStore, State, TableId, Transaction,
    },
    WarehouseId,
};

// -------------------- REQUEST/RESPONSE TYPES --------------------

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct Task {
    /// Unique identifier for the task
    pub task_id: Uuid,
    /// Warehouse ID associated with the task
    #[schema(value_type = String)]
    pub warehouse_id: WarehouseId,
    /// Name of the queue processing this task
    pub queue_name: String,
    /// Current status of the task
    pub status: TaskStatus,
    /// When the task was picked up for processing.
    pub picked_up_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When the task is scheduled to run
    pub scheduled_for: chrono::DateTime<chrono::Utc>,
    /// Current attempt number
    pub attempt: i32,
    /// Parent task ID if this is a sub-task
    pub parent_task_id: Option<Uuid>,
    /// When the task was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When the task was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Type of entity this task operates on
    pub entity: TaskEntity,
    /// Last heartbeat timestamp for running tasks
    pub last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Progress of the task (0.0 to 1.0)
    pub progress: f32,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct TaskDetails {
    /// Current task information
    #[serde(flatten)]
    pub task: Task,
    /// Task-specific data
    pub task_data: serde_json::Value,
    /// Execution details for the current attempt
    pub execution_details: Option<serde_json::Value>,
    /// History of past attempts
    pub attempts: Vec<TaskAttempt>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct TaskAttempt {
    /// Attempt number
    pub attempt: i32,
    /// Status of this attempt
    pub status: TaskStatus,
    /// When this attempt started
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    /// How long this attempt took
    pub duration: Option<chrono::Duration>,
    /// Message associated with this attempt
    pub message: Option<String>,
    /// When this attempt was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Progress achieved in this attempt
    pub progress: f32,
    /// Execution details for this attempt
    pub execution_details: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
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

impl IntoResponse for TaskDetails {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

// -------------------- ENTITY FILTER --------------------

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", tag = "type", content = "id")]
/// Identifies catalog objects that tasks are associated with.
pub enum TaskEntity {
    /// Filter by table
    #[schema(value_type = uuid::Uuid)]
    Table(TableId),
}

// -------------------- QUERY PARAMETERS --------------------
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksQuery {
    /// Filter by task status
    #[serde(default)]
    pub status: Option<TaskStatus>,
    /// Filter by queue name
    #[serde(default)]
    pub queue_name: Option<String>,
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
    /// Next page token, other filters are ignored if this is set.
    #[serde(default)]
    pub page_token: Option<String>,
    /// Number of results per page (default: 100)
    #[serde(default = "default_page_size")]
    pub page_size: i64,
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

// -------------------- CONTROL REQUESTS --------------------

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct TaskControlRequest {
    /// The action to perform on the task
    pub action: TaskControlAction,
    /// Optional reason for the action
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskControlAction {
    /// Stop the task (can be retried if attempts not exhausted)
    Stop,
    /// Cancel the task permanently (no retries)
    Cancel,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct TaskControlResponse {
    /// Whether the action was successful
    pub success: bool,
    /// Message describing the result
    pub message: String,
    /// Updated task information
    pub task: Task,
}

impl IntoResponse for TaskControlResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

// -------------------- SERVICE TRAIT --------------------

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(crate) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    /// List tasks with optional filtering
    async fn list_tasks(
        warehouse_id: WarehouseId,
        query: ListTasksQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTasksResponse> {
        // -------------------- VALIDATIONS --------------------

        // -------------------- AUTHZ --------------------
        let authorizer = state.v1_state.authz;

        let can_list_everything = authorizer
                .require_warehouse_action(
                    &request_metadata,
                    warehouse_id,
                    CatalogWarehouseAction::CanListEverything,
                )
                .await
                .map_err(|e| e.append_detail("Not authorized to see all objects in the Warehouse. Add the `entity` filter to query tasks for specific entities."));

        // If warehouse_id is specified, check permission for that warehouse
        if let Some(entities) = &query.entities {
            let table_ids: Vec<TableId> = entities
                .iter()
                .filter_map(|entity| match entity {
                    TaskEntity::Table(table_id) => Some(*table_id),
                })
                .collect();
            if !entities.is_empty() && can_list_everything.is_err() {
                let allowed = authorizer
                    .are_allowed_table_actions(
                        &request_metadata,
                        table_ids
                            .iter()
                            .map(|id| (*id, CatalogTableAction::CanCommit))
                            .collect(),
                    )
                    .await;
            } else {
                can_list_everything?;
            }
        } else {
            can_list_everything?;
        };

        // -------------------- Business Logic --------------------
        // TODO: Implement task listing logic
        todo!("Implement list_tasks business logic")
    }

    /// Get detailed information about a specific task including attempt history
    async fn get_task_details(
        warehouse_id: WarehouseId,
        task_id: Uuid,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<TaskDetails> {
        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        // TODO: First get the task to determine warehouse_id, then check permissions
        // For now, we'll implement a placeholder authorization

        // -------------------- Business Logic --------------------
        // TODO: Implement get task details logic
        // 1. Fetch task from database
        // 2. Fetch all attempts from task_log
        // 3. Check authorization based on warehouse_id
        // 4. Return combined data
        todo!("Implement get_task_details business logic")
    }

    /// Control a task (stop or cancel)
    async fn control_task(
        warehouse_id: WarehouseId,
        task_id: Uuid,
        request: TaskControlRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<TaskControlResponse> {
        // -------------------- VALIDATIONS --------------------
        // TODO: Validate that task exists and is in a controllable state

        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        // TODO: First get the task to determine warehouse_id, then check permissions
        // For now, we'll implement a placeholder authorization

        // -------------------- Business Logic --------------------
        match request.action {
            TaskControlAction::Stop => {
                // TODO: Implement stop logic
                // - Mark task as stopped
                // - Allow retry if attempts not exhausted
                todo!("Implement stop task logic")
            }
            TaskControlAction::Cancel => {
                // TODO: Implement cancel logic
                // - Mark task as permanently cancelled
                // - No retries allowed
                todo!("Implement cancel task logic")
            }
        }
    }
}
