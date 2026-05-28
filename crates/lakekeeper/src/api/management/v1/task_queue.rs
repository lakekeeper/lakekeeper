use std::collections::HashMap;

use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    WarehouseId,
    api::{ApiContext, Result},
    service::{
        ArcProjectId, CatalogStore, CatalogTaskOps, SecretStore, State, Transaction,
        authz::Authorizer,
        task_configs::TaskQueueConfigFilter,
        tasks::{TaskFilter, TaskId, TaskQueueName, WarehouseTaskEntityId},
    },
};

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SetTaskQueueConfigRequest {
    pub queue_config: QueueConfig,
    pub max_seconds_since_last_heartbeat: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(transparent)]
pub struct QueueConfig(pub(crate) serde_json::Value);

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTaskQueueConfigResponse {
    pub queue_config: QueueConfigResponse,
    pub max_seconds_since_last_heartbeat: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct QueueConfigResponse {
    #[serde(flatten)]
    pub(crate) config: serde_json::Value,
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub(crate) queue_name: TaskQueueName,
}

impl axum::response::IntoResponse for GetTaskQueueConfigResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

pub(crate) async fn set_task_queue_config<C: CatalogStore, A: Authorizer, S: SecretStore>(
    project_id: ArcProjectId,
    warehouse_id: Option<WarehouseId>,
    queue_name: &TaskQueueName,
    request: &SetTaskQueueConfigRequest,
    context: ApiContext<State<A, C, S>>,
) -> Result<()> {
    let task_queues = context.v1_state.registered_task_queues;

    if let Some(validate_config_fn) = task_queues.validate_config_fn(queue_name).await {
        validate_config_fn(request.queue_config.0.clone()).map_err(|e| {
            ErrorModel::bad_request(
                format!("Failed to deserialize queue config for queue-name '{queue_name}': '{e}'"),
                "InvalidQueueConfig",
                Some(Box::new(e)),
            )
        })?;
    } else {
        let mut existing_queue_names = task_queues.queue_names().await;
        existing_queue_names.sort_unstable();
        let existing_queue_names = existing_queue_names.iter().join(", ");
        return Err(ErrorModel::bad_request(
            format!("Queue '{queue_name}' not found! Existing queues: [{existing_queue_names}]"),
            "QueueNotFound",
            None,
        )
        .into());
    }
    let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
    C::set_task_queue_config(
        project_id,
        warehouse_id,
        queue_name,
        request,
        transaction.transaction(),
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

/// Request body for `POST .../task-queue/{queue_name}/schedule`.
///
/// Schedules a single new task on the named queue for the given entity.
/// The queue must have opted in via `TaskConfig::user_schedulable() = true`.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ScheduleTaskRequest {
    /// The entity to schedule a task for. Today only tables and views are
    /// accepted; warehouse- or project-scoped queues are not user-schedulable.
    pub entity: WarehouseTaskEntityId,
    /// When the task should run. `None` means immediately (the worker picks
    /// it up on its next poll). RFC 3339 / ISO 8601 format.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", schema(example = "2026-12-31T23:59:59Z"))]
    pub scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional queue-specific payload. Defaults to `{}` when omitted.
    /// Validated against the queue's payload schema.
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

/// Response from `POST .../task-queue/{queue_name}/schedule`.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ScheduleTaskResponse {
    /// The id of the newly scheduled task.
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub task_id: TaskId,
}

impl axum::response::IntoResponse for ScheduleTaskResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

pub(crate) async fn get_task_queue_config<C: CatalogStore, A: Authorizer, S: SecretStore>(
    filter: &TaskQueueConfigFilter,
    queue_name: &TaskQueueName,
    context: ApiContext<State<A, C, S>>,
) -> Result<GetTaskQueueConfigResponse> {
    let config = C::get_task_queue_config(filter, queue_name, context.v1_state.catalog)
        .await?
        .unwrap_or_else(|| GetTaskQueueConfigResponse {
            queue_config: QueueConfigResponse {
                config: serde_json::json!({}),
                queue_name: queue_name.clone(),
            },
            max_seconds_since_last_heartbeat: None,
        });
    Ok(config)
}

/// Schedule a single task on `queue_name` for a previously-resolved entity.
///
/// Callers must have performed warehouse/entity resolution and authz before
/// invoking this. The function gates on registry membership +
/// `user_schedulable`, then enqueues via `C::enqueue_task`. On the unique
/// `(warehouse_id, entity_type, entity_id, queue_name)` index conflict it
/// returns a 409.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn schedule_task<C: CatalogStore, A: Authorizer, S: SecretStore>(
    project_id: ArcProjectId,
    warehouse_id: WarehouseId,
    queue_name: &TaskQueueName,
    entity_id: WarehouseTaskEntityId,
    entity_name: Vec<String>,
    table_properties: HashMap<String, String>,
    request: ScheduleTaskRequest,
    context: ApiContext<State<A, C, S>>,
) -> Result<ScheduleTaskResponse> {
    let task_queues = &context.v1_state.registered_task_queues;
    let catalog_state = context.v1_state.catalog.clone();

    // ----- queue registration + schedulability gate -----
    match task_queues.is_user_schedulable(queue_name).await {
        Some(true) => {}
        Some(false) => {
            let schedulable = task_queues.user_schedulable_queue_names().await;
            let schedulable = schedulable.iter().join(", ");
            return Err(ErrorModel::bad_request(
                format!(
                    "Queue '{queue_name}' is not user-schedulable. \
                     Schedulable queues: [{schedulable}]"
                ),
                "QueueNotUserSchedulable",
                None,
            )
            .into());
        }
        None => {
            let mut existing = task_queues.queue_names().await;
            existing.sort_unstable();
            let existing = existing.iter().join(", ");
            return Err(ErrorModel::not_found(
                format!("Queue '{queue_name}' not found. Registered queues: [{existing}]"),
                "QueueNotFound",
                None,
            )
            .into());
        }
    }

    let queue_name_static: &'static TaskQueueName = task_queues
        .static_queue_name(queue_name)
        .await
        .ok_or_else(|| {
            ErrorModel::internal(
                format!("Queue '{queue_name}' vanished between schedulability check and enqueue"),
                "QueueRaceDuringSchedule",
                None,
            )
        })?;

    // ----- eligibility check (queue-specific; mirrors the hook's gates) -----
    // Fetches the warehouse's queue config and hands it, along with the
    // entity's table properties, to `TaskConfig::check_schedule_eligibility`.
    // Catches the "create then no-op at pickup" footgun for disabled queues
    // / `gc.enabled=false` / per-table veto.
    let raw_queue_config = C::get_task_queue_config(
        &TaskQueueConfigFilter::WarehouseId { warehouse_id },
        queue_name_static,
        catalog_state.clone(),
    )
    .await?
    .map_or_else(
        || serde_json::json!({}),
        |resp| resp.queue_config.config.clone(),
    );
    let eligibility_fn = task_queues
        .schedule_eligibility_fn(queue_name)
        .await
        .ok_or_else(|| {
            ErrorModel::internal(
                format!("Queue '{queue_name}' lost its eligibility fn between gate and check"),
                "EligibilityFnRaceDuringSchedule",
                None,
            )
        })?;
    eligibility_fn(raw_queue_config, table_properties, entity_id)?;

    // ----- build task input -----
    let metadata = crate::service::tasks::ScheduleTaskMetadata {
        project_id: project_id.clone(),
        parent_task_id: None,
        scheduled_for: request.scheduled_for,
        entity: crate::service::tasks::TaskEntity::EntityInWarehouse {
            warehouse_id,
            entity_id,
            entity_name,
        },
    };
    let payload = request.payload.unwrap_or_else(|| serde_json::json!({}));
    let task_input = crate::service::tasks::TaskInput {
        task_metadata: metadata,
        payload,
    };

    // ----- enqueue (idempotent via DB unique index) -----
    let mut tx = C::Transaction::begin_write(catalog_state.clone()).await?;
    let inserted = C::enqueue_task(queue_name_static, task_input, tx.transaction()).await?;
    tx.commit().await?;

    match inserted {
        Some(task_id) => Ok(ScheduleTaskResponse { task_id }),
        None => {
            // Look up the existing active task so the 409 body carries the
            // task_id; saves the operator a `task/list` round-trip when
            // chaining to `task/control RunNow` / `RunAt`. Best-effort: if
            // the lookup itself fails, log it and fall back to the generic
            // message — a misbehaving catalog should not be invisible.
            //
            // Tiny race: the unique-index conflict could have cleared by
            // the time the lookup runs (worker finished, task became
            // terminal). In that case the lookup returns no rows and the
            // 409 prints the generic message; the next schedule attempt
            // would succeed.
            let existing_task_id = match lookup_active_task_id::<C>(
                warehouse_id,
                project_id,
                queue_name_static,
                entity_id,
                catalog_state,
            )
            .await
            {
                Ok(found) => found,
                Err(e) => {
                    tracing::warn!(
                        warehouse_id = %warehouse_id,
                        queue = %queue_name,
                        "Failed to look up existing active task_id for 409 body: {e}"
                    );
                    None
                }
            };
            Err(format_task_already_active_error(queue_name, existing_task_id).into())
        }
    }
}

/// Build the 409 conflict body for "schedule attempted but a task is
/// already active on this `(warehouse, entity, queue)` triple". Public
/// only so the lifecycle/unit tests can pin both shapes (with and without
/// the looked-up `task_id`).
fn format_task_already_active_error(
    queue_name: &TaskQueueName,
    existing_task_id: Option<TaskId>,
) -> ErrorModel {
    let msg = match existing_task_id {
        Some(id) => format!(
            "A task on queue '{queue_name}' is already active for this entity (task-id={id}). \
             POST /task/control with this id to retime (run-now / run-at) or cancel it."
        ),
        None => format!(
            "A task on queue '{queue_name}' is already active for this entity. \
             Use POST /task/list to find it; POST /task/control to retime or cancel it."
        ),
    };
    ErrorModel::conflict(msg, "TaskAlreadyActive", None)
}

/// Find the single active task_id for `(warehouse, entity, queue)`, used to
/// enrich the 409 body returned by `schedule_task`. Best-effort: a lookup
/// failure is downgraded to "task_id unknown" in the error message rather
/// than failing the call.
///
/// Reads from the `task` table only, which by schema holds only active
/// rows (status ∈ `{scheduled, running, should-stop}`; terminal rows move
/// to `task_log` — see migration `20250523101407_tasks_store_their_state`).
/// The explicit status filter below is therefore redundant in steady state
/// but documents intent and protects against a future schema where
/// `task` could hold terminal rows.
///
/// Tiny race: if the conflicting row transitions to terminal and is moved
/// to `task_log` between `enqueue_task` returning `None` and this lookup
/// running, the lookup returns `None` and the 409 falls back to the
/// generic message. Acceptable — the next schedule attempt would succeed.
async fn lookup_active_task_id<C: CatalogStore>(
    warehouse_id: WarehouseId,
    project_id: ArcProjectId,
    queue_name: &'static TaskQueueName,
    entity_id: WarehouseTaskEntityId,
    catalog_state: C::State,
) -> std::result::Result<Option<TaskId>, ErrorModel> {
    use crate::api::management::v1::tasks::{
        ListTasksRequest, TaskStatus, WarehouseTaskEntityFilter,
    };

    let entity_filter = match entity_id {
        WarehouseTaskEntityId::Table { table_id } => {
            WarehouseTaskEntityFilter::Table { table_id }
        }
        WarehouseTaskEntityId::View { view_id } => WarehouseTaskEntityFilter::View { view_id },
    };
    let query = ListTasksRequest::builder()
        .status(Some(vec![
            TaskStatus::Scheduled,
            TaskStatus::Running,
            TaskStatus::Stopping,
        ]))
        .queue_name(Some(vec![queue_name.clone()]))
        .entities(Some(vec![entity_filter]))
        .build();
    let filter = TaskFilter::WarehouseId {
        warehouse_id,
        project_id,
    };
    let mut tx = C::Transaction::begin_read(catalog_state).await?;
    let tasks = C::list_tasks(&filter, &query, tx.transaction()).await?;
    tx.commit().await?;
    Ok(tasks.tasks.into_iter().next().map(|t| t.id.task_id))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn task_already_active_error_includes_task_id_when_found() {
        let queue: TaskQueueName = "remove_orphan_files".into();
        let id = TaskId::from(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());

        let err = format_task_already_active_error(&queue, Some(id));

        assert_eq!(err.code, 409);
        assert_eq!(err.r#type, "TaskAlreadyActive");
        assert!(
            err.message.contains("task-id=550e8400-e29b-41d4-a716-446655440000"),
            "409 body must include the existing task-id so operators can chain to /task/control without an extra /task/list round-trip; got: {}",
            err.message
        );
        assert!(err.message.contains("run-now"));
        assert!(err.message.contains("run-at"));
    }

    #[test]
    fn task_already_active_error_falls_back_to_generic_message_when_lookup_failed() {
        let queue: TaskQueueName = "remove_orphan_files".into();

        let err = format_task_already_active_error(&queue, None);

        assert_eq!(err.code, 409);
        assert_eq!(err.r#type, "TaskAlreadyActive");
        // No task-id in body, but the user is still pointed at the right next step.
        assert!(!err.message.contains("task-id="));
        assert!(err.message.contains("POST /task/list"));
        assert!(err.message.contains("POST /task/control"));
    }

    // Postgres-backed lifecycle coverage lives in
    // `tasks::test::schedule_lifecycle`, which uses the
    // `setup_and_registry` harness to register a `user_schedulable=true`
    // test queue and exercises the full schedule → 409 → RunNow chain.
}
