use std::sync::Arc;

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use lakekeeper::{
    ProjectId, WarehouseId,
    api::management::v1::{
        task_queue::{GetTaskQueueConfigResponse, QueueConfigResponse, SetTaskQueueConfigRequest},
        tasks::TaskStatus,
    },
    service::{
        ArcProjectId, CatalogBackendError, CatalogBackendErrorType, DatabaseIntegrityError,
        TableId, ViewId,
        task_configs::TaskQueueConfigFilter,
        tasks::{
            CancelTasksFilter, EnqueueOutcome, FailedAttempt, FailureKind, RetryDecision,
            RetryPolicy, ScheduleTaskMetadata, TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE,
            TASK_ATTEMPT_ALREADY_FAILED_ERROR_TYPE, Task, TaskAttemptId, TaskCheckState,
            TaskEntity, TaskId, TaskInput, TaskIntermediateStatus, TaskMetadata, TaskOutcome,
            TaskQueueDemand, TaskQueueDemandRequest, TaskQueueName, TaskResourceDemandRange,
            TaskScheduleId, WarehouseTaskEntityId, given_up_message,
        },
    },
};
use sqlx::{PgConnection, PgPool, postgres::types::PgInterval};
use uuid::Uuid;

use crate::dbutils::DBErrorHandler;

mod cleanup_task_logs_older_than;
mod get_task_details;
mod list_tasks;
mod resolve_tasks;
mod task_schedule;
pub(crate) use cleanup_task_logs_older_than::cleanup_task_logs_older_than;
pub(crate) use get_task_details::get_task_details;
pub(crate) use list_tasks::list_tasks;
pub(crate) use resolve_tasks::resolve_tasks;
pub(crate) use task_schedule::{
    delete_task_schedule, delete_warehouse_task_schedules, list_project_task_schedule_states,
    list_task_schedules, materialize_due_task_schedules, trigger_task_schedule,
    upsert_task_schedule,
};

/// Attempts of a write whose conflicting row is gone before it can be read back.
pub(crate) const MAX_WRITE_ATTEMPTS: usize = 3;

/// A write lost to a concurrent change in each of its attempts.
#[derive(thiserror::Error, Debug)]
#[error("Gave up after {attempts} attempts, each lost to a concurrent change")]
pub(crate) struct RetriesExhaustedError {
    attempts: usize,
}

impl RetriesExhaustedError {
    pub(crate) fn backend_error() -> CatalogBackendError {
        CatalogBackendError::new(
            Self {
                attempts: MAX_WRITE_ATTEMPTS,
            },
            CatalogBackendErrorType::ConcurrentModification,
        )
    }
}

#[derive(Debug)]
pub(crate) struct InsertResult {
    pub task_id: TaskId,
    #[cfg(test)]
    pub entity_id: Option<WarehouseTaskEntityId>,
}

#[derive(Debug, sqlx::Type, Clone, Copy, PartialEq, Eq, Hash)]
#[sqlx(type_name = "entity_type", rename_all = "kebab-case")]
enum TaskEntityTypeDB {
    Table,
    View,
    GenericTable,
    Project,
    Warehouse,
}

impl From<WarehouseTaskEntityId> for TaskEntityTypeDB {
    fn from(entity_id: WarehouseTaskEntityId) -> Self {
        match entity_id {
            WarehouseTaskEntityId::Table { .. } => Self::Table,
            WarehouseTaskEntityId::View { .. } => Self::View,
            WarehouseTaskEntityId::GenericTable { .. } => Self::GenericTable,
        }
    }
}

fn task_status_from_db(
    task_status: Option<TaskIntermediateStatus>,
    task_log_status: Option<TaskOutcome>,
) -> Result<TaskStatus, ErrorModel> {
    task_status
        .map(Into::into)
        .or(task_log_status.map(Into::into))
        .ok_or_else(|| {
            ErrorModel::internal(
                "Task has neither status nor log status.",
                "TaskStatusMissing",
                None,
            )
        })
}

/// A project- or warehouse-scoped task carries the id of the schedule that produced it.
fn task_schedule_id_from_db(
    entity_type: TaskEntityTypeDB,
    entity_id: Option<Uuid>,
) -> Option<TaskScheduleId> {
    match entity_type {
        TaskEntityTypeDB::Project | TaskEntityTypeDB::Warehouse => {
            entity_id.map(TaskScheduleId::from)
        }
        TaskEntityTypeDB::Table | TaskEntityTypeDB::View | TaskEntityTypeDB::GenericTable => None,
    }
}

/// The longest retry backoff, which every timestamp in range can be moved by. A longer one is
/// shortened to it; a negative one counts as zero.
const MAX_RETRY_BACKOFF: chrono::Duration = chrono::Duration::days(36_500);

#[derive(thiserror::Error, Debug)]
#[error("Duration does not fit a Postgres interval")]
struct IntervalOverflowError;

impl From<IntervalOverflowError> for IcebergErrorResponse {
    fn from(e: IntervalOverflowError) -> Self {
        ErrorModel::internal(
            "Could not convert duration into microseconds. Integer overflow, this is a bug.",
            "InternalError",
            Some(Box::new(e)),
        )
        .into()
    }
}

fn pg_interval(duration: chrono::Duration) -> Result<PgInterval, IntervalOverflowError> {
    Ok(PgInterval {
        months: 0,
        days: 0,
        microseconds: duration.num_microseconds().ok_or(IntervalOverflowError)?,
    })
}

/// The active task holding the unique key (project, warehouse, entity type, entity id, queue).
/// One query per shape of the key, so that each can use an index.
async fn lookup_active_task(
    conn: &mut PgConnection,
    project_id: &str,
    warehouse_id: Option<Uuid>,
    entity_type: TaskEntityTypeDB,
    entity_id: Option<Uuid>,
    queue_name: &str,
) -> Result<Option<(TaskId, TaskIntermediateStatus)>, CatalogBackendError> {
    let row = match (warehouse_id, entity_id) {
        (_, Some(entity_id)) => sqlx::query!(
            r#"
            SELECT task_id, status as "status: TaskIntermediateStatus"
            FROM task
            WHERE entity_id = $4
                AND project_id = $1
                AND warehouse_id IS NOT DISTINCT FROM $2
                AND entity_type = $3
                AND queue_name = $5
            "#,
            project_id,
            warehouse_id,
            entity_type as _,
            entity_id,
            queue_name,
        )
        .fetch_optional(conn)
        .await
        .map(|r| r.map(|r| (r.task_id, r.status))),
        (Some(warehouse_id), None) => sqlx::query!(
            r#"
            SELECT task_id, status as "status: TaskIntermediateStatus"
            FROM task
            WHERE project_id = $1
                AND warehouse_id = $2
                AND entity_type = $3
                AND entity_id IS NULL
                AND queue_name = $4
            "#,
            project_id,
            warehouse_id,
            entity_type as _,
            queue_name,
        )
        .fetch_optional(conn)
        .await
        .map(|r| r.map(|r| (r.task_id, r.status))),
        (None, None) => sqlx::query!(
            r#"
            SELECT task_id, status as "status: TaskIntermediateStatus"
            FROM task
            WHERE project_id = $1
                AND warehouse_id IS NULL
                AND entity_type = $2
                AND entity_id IS NULL
                AND queue_name = $3
            "#,
            project_id,
            entity_type as _,
            queue_name,
        )
        .fetch_optional(conn)
        .await
        .map(|r| r.map(|r| (r.task_id, r.status))),
    }
    .map_err(DBErrorHandler::into_catalog_backend_error)?;
    Ok(row.map(|(task_id, status)| (TaskId::from(task_id), status)))
}

/// The warehouse, entity type and entity id a task's entity is stored as.
fn task_entity_columns(entity: &TaskEntity) -> (Option<Uuid>, TaskEntityTypeDB, Option<Uuid>) {
    match entity {
        TaskEntity::Project => (None, TaskEntityTypeDB::Project, None),
        TaskEntity::Warehouse { warehouse_id } => {
            (Some(**warehouse_id), TaskEntityTypeDB::Warehouse, None)
        }
        TaskEntity::EntityInWarehouse {
            warehouse_id,
            entity_id,
            ..
        } => (
            Some(**warehouse_id),
            (*entity_id).into(),
            Some(entity_id.as_uuid()),
        ),
    }
}

/// Enqueue one task and report whether it was inserted or which active task holds its key.
pub(crate) async fn enqueue_task(
    conn: &mut PgConnection,
    queue_name: &TaskQueueName,
    task: TaskInput,
) -> Result<EnqueueOutcome, IcebergErrorResponse> {
    let project_id = task.task_metadata.project_id.to_string();
    let (warehouse_id, entity_type, entity_id) = task_entity_columns(&task.task_metadata.entity);
    for _ in 0..MAX_WRITE_ATTEMPTS {
        if let Some(inserted) = queue_task_batch(&mut *conn, queue_name, vec![task.clone()])
            .await?
            .pop()
        {
            return Ok(EnqueueOutcome::Inserted(inserted.task_id));
        }
        if let Some((task_id, status)) = lookup_active_task(
            &mut *conn,
            &project_id,
            warehouse_id,
            entity_type,
            entity_id,
            queue_name.as_str(),
        )
        .await
        .map_err(ErrorModel::from)?
        {
            return Ok(EnqueueOutcome::Existing { task_id, status });
        }
        // The active task finished between the insert and the lookup. Try again.
    }
    // Only reached if the active task keeps finishing between the insert and the lookup.
    Err(ErrorModel::from(
        RetriesExhaustedError::backend_error().append_detail(format!("Queue `{queue_name}`")),
    )
    .into())
}

/// Move a pending task of the queue to a new parent and replace its payload.
pub(crate) async fn reparent_pending_task(
    conn: &mut PgConnection,
    queue_name: &TaskQueueName,
    legacy_queue_names: &[&TaskQueueName],
    task_id: TaskId,
    new_parent: TaskId,
    task_data: serde_json::Value,
) -> Result<bool, IcebergErrorResponse> {
    let queue_names: Vec<String> = std::iter::once(queue_name)
        .chain(legacy_queue_names.iter().copied())
        .map(|n| n.as_str().to_string())
        .collect();
    let row = sqlx::query!(
        r#"
        UPDATE task
        SET parent_task_id = $2, task_data = $3
        WHERE task_id = $1 AND status = 'scheduled' AND queue_name = ANY($4)
        RETURNING task_id
        "#,
        *task_id,
        *new_parent,
        task_data,
        &queue_names,
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| e.into_error_model("Failed to re-parent task"))?;
    Ok(row.is_some())
}

fn task_entity_from_db(
    entity_type: TaskEntityTypeDB,
    warehouse_id: Option<Uuid>,
    entity_id: Option<Uuid>,
    entity_name: Option<Vec<String>>,
) -> Result<TaskEntity, DatabaseIntegrityError> {
    match entity_type {
        TaskEntityTypeDB::View | TaskEntityTypeDB::Table | TaskEntityTypeDB::GenericTable => {
            let warehouse_id = warehouse_id
                .ok_or_else(|| {
                    DatabaseIntegrityError::new("WarehouseId is missing for tabular scoped task.")
                })
                .map(WarehouseId::from)?;

            let entity_id = entity_id.ok_or_else(|| {
                DatabaseIntegrityError::new("EntityId is missing for tabular scoped task.")
            })?;

            let entity_id = match entity_type {
                TaskEntityTypeDB::View => WarehouseTaskEntityId::View {
                    view_id: ViewId::from(entity_id),
                },
                TaskEntityTypeDB::Table => WarehouseTaskEntityId::Table {
                    table_id: TableId::from(entity_id),
                },
                TaskEntityTypeDB::GenericTable => WarehouseTaskEntityId::GenericTable {
                    generic_table_id: lakekeeper::service::GenericTableId::from(entity_id),
                },
                _ => unreachable!(),
            };

            let entity_name = entity_name.ok_or_else(|| {
                DatabaseIntegrityError::new("Entity name is missing for tabular scoped task.")
            })?;

            Ok(TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name,
            })
        }
        TaskEntityTypeDB::Project => Ok(TaskEntity::Project),
        TaskEntityTypeDB::Warehouse => {
            let warehouse_id = warehouse_id
                .ok_or_else(|| {
                    DatabaseIntegrityError::new("WarehouseId is missing for warehouse scoped task.")
                })
                .map(WarehouseId::from)?;
            Ok(TaskEntity::Warehouse { warehouse_id })
        }
    }
}

/// The columns of tasks to insert, one entry per task.
struct TaskInsertColumns {
    task_ids: Vec<Uuid>,
    parent_task_ids: Vec<Option<Uuid>>,
    project_ids: Vec<String>,
    warehouse_ids: Vec<Option<Uuid>>,
    scheduled_fors: Vec<Option<chrono::DateTime<chrono::Utc>>>,
    entity_ids: Vec<Option<Uuid>>,
    entity_types: Vec<TaskEntityTypeDB>,
    payloads: Vec<serde_json::Value>,
    entity_names: Vec<Option<serde_json::Value>>,
    resource_demands: Vec<Option<i64>>,
}

impl TaskInsertColumns {
    fn new(tasks: Vec<TaskInput>) -> Result<Self, ErrorModel> {
        let mut columns = Self {
            task_ids: Vec::with_capacity(tasks.len()),
            parent_task_ids: Vec::with_capacity(tasks.len()),
            project_ids: Vec::with_capacity(tasks.len()),
            warehouse_ids: Vec::with_capacity(tasks.len()),
            scheduled_fors: Vec::with_capacity(tasks.len()),
            entity_ids: Vec::with_capacity(tasks.len()),
            entity_types: Vec::with_capacity(tasks.len()),
            payloads: Vec::with_capacity(tasks.len()),
            entity_names: Vec::with_capacity(tasks.len()),
            resource_demands: Vec::with_capacity(tasks.len()),
        };
        for TaskInput {
            task_metadata:
                ScheduleTaskMetadata {
                    resource_demand,
                    project_id,
                    parent_task_id,
                    scheduled_for,
                    entity: scope,
                },
            payload,
        } in tasks
        {
            let (warehouse_id, entity_type, entity_id) = task_entity_columns(&scope);
            let entity_name = match scope {
                TaskEntity::EntityInWarehouse { entity_name, .. } => Some(
                    serde_json::to_value(&entity_name).map_err(|e| {
                        ErrorModel::internal(
                            format!("Failed to serialize entity name when queuing task: {entity_name:?}, error: {e}"),
                            "InternalError",
                            None,
                        )
                    })?,
                ),
                TaskEntity::Project | TaskEntity::Warehouse { .. } => None,
            };
            columns.task_ids.push(Uuid::now_v7());
            columns
                .parent_task_ids
                .push(parent_task_id.as_deref().copied());
            columns.project_ids.push(project_id.to_string());
            columns.warehouse_ids.push(warehouse_id);
            columns.scheduled_fors.push(scheduled_for);
            columns.payloads.push(payload);
            columns.entity_types.push(entity_type);
            columns.entity_ids.push(entity_id);
            columns.entity_names.push(entity_name);
            columns
                .resource_demands
                .push(resource_demand.map(|demand| demand.max(0)));
        }
        Ok(columns)
    }
}

pub(crate) async fn queue_task_batch(
    conn: &mut PgConnection,
    queue_name: &TaskQueueName,
    tasks: Vec<TaskInput>,
) -> Result<Vec<InsertResult>, IcebergErrorResponse> {
    let queue_name = queue_name.as_str();
    let TaskInsertColumns {
        task_ids,
        parent_task_ids,
        project_ids,
        warehouse_ids,
        scheduled_fors,
        entity_ids,
        entity_types,
        payloads,
        entity_names,
        resource_demands,
    } = TaskInsertColumns::new(tasks)?;

    Ok(sqlx::query!(
        r#"WITH input_rows AS (
            SELECT
                task_id,
                parent_task_id,
                warehouse_id,
                scheduled_for,
                payload,
                entity_ids,
                entity_types,
                entity_name,
                project_id,
                resource_demand,
                $2 as queue_name
            FROM unnest(
                $1::uuid[],
                $3::uuid[],
                $4::uuid[],
                $5::timestamptz[],
                $6::jsonb[],
                $7::uuid[],
                $8::entity_type[],
                $10::jsonb[],
                $11::text[],
                $12::bigint[]
            ) AS t(
                task_id,
                parent_task_id,
                warehouse_id,
                scheduled_for,
                payload,
                entity_ids,
                entity_types,
                entity_name,
                project_id,
                resource_demand
            )
        )
        INSERT INTO task(
                task_id,
                queue_name,
                status,
                parent_task_id,
                warehouse_id,
                scheduled_for,
                task_data,
                entity_id,
                entity_type,
                entity_name,
                project_id,
                resource_demand
        )
        SELECT
            i.task_id,
            i.queue_name,
            $9,
            i.parent_task_id,
            i.warehouse_id,
            coalesce(i.scheduled_for, now()),
            i.payload,
            i.entity_ids,
            i.entity_types,
            CASE 
            WHEN entity_name IS NULL THEN NULL::text[]
                ELSE ARRAY(SELECT jsonb_array_elements_text(entity_name))
            END,
            i.project_id,
            i.resource_demand
        FROM input_rows i
        ON CONFLICT (project_id, warehouse_id, entity_type, entity_id, queue_name) DO NOTHING
        RETURNING task_id, queue_name, entity_id, entity_type as "entity_type: TaskEntityTypeDB""#,
        &task_ids,
        queue_name,
        &parent_task_ids as _,
        &warehouse_ids as _,
        &scheduled_fors
            .iter()
            .map(|t| t.as_ref())
            .collect::<Vec<_>>() as _,
        &payloads,
        &entity_ids as _,
        &entity_types as _,
        TaskIntermediateStatus::Scheduled as _,
        &entity_names as _,
        &project_ids,
        &resource_demands as _,
    )
    .fetch_all(conn)
    .await
    .map(|records| {
        records
            .into_iter()
            .map(|record| InsertResult {
                task_id: record.task_id.into(),
                #[cfg(test)]
                entity_id: match record.entity_type {
                    TaskEntityTypeDB::View => Some(WarehouseTaskEntityId::View {
                        view_id: record.entity_id.unwrap().into(),
                    }),
                    TaskEntityTypeDB::Table => Some(WarehouseTaskEntityId::Table {
                        table_id: record.entity_id.unwrap().into(),
                    }),
                    TaskEntityTypeDB::GenericTable => Some(WarehouseTaskEntityId::GenericTable {
                        generic_table_id: record.entity_id.unwrap().into(),
                    }),
                    TaskEntityTypeDB::Project | TaskEntityTypeDB::Warehouse => None,
                },
            })
            .collect_vec()
    })
    .map_err(|e| e.into_error_model("failed queueing tasks"))?)
}

/// Five attempts, each retried at once.
#[cfg(test)]
const TEST_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 5,
    retry_decision: |_| RetryDecision::retry(chrono::Duration::zero()),
};

/// [`record_failure`] that schedules a retry at once.
#[cfg(test)]
pub(crate) async fn record_failure_without_backoff(
    id: impl AsRef<TaskAttemptId>,
    max_retries: i32,
    details: &str,
    conn: &mut PgConnection,
) -> Result<(), IcebergErrorResponse> {
    record_failure(
        id,
        max_retries,
        &RetryDecision::retry(chrono::Duration::zero()),
        details,
        conn,
    )
    .await
}

/// [`pick_task_with_retry_policy`] with [`TEST_RETRY_POLICY`].
#[cfg(test)]
pub(crate) async fn pick_task(
    pool: &PgPool,
    queue_name: &TaskQueueName,
    legacy_queue_names: &[&TaskQueueName],
    default_max_time_since_last_heartbeat: chrono::Duration,
) -> Result<Option<Task>, IcebergErrorResponse> {
    pick_task_with_retry_policy(
        pool,
        queue_name,
        legacy_queue_names,
        default_max_time_since_last_heartbeat,
        TEST_RETRY_POLICY,
        TaskResourceDemandRange::default(),
    )
    .await
}

/// Pick the oldest due task of the queue whose resource demand lies in `resource_demand`.
///
/// First records at most one attempt of the queue whose heartbeat timed out as failed, whatever
/// its resource demand, so it is retried or finished by the same rules as any failed attempt.
///
/// `default_max_time_since_last_heartbeat` is only used if no task configuration is found
/// in the DB for the given `queue_name`, typically before a user has configured the value explicitly.
pub(crate) async fn pick_task_with_retry_policy(
    pool: &PgPool,
    queue_name: &TaskQueueName,
    legacy_queue_names: &[&TaskQueueName],
    default_max_time_since_last_heartbeat: chrono::Duration,
    retry_policy: RetryPolicy,
    resource_demand: TaskResourceDemandRange,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let (above, at_most) = demand_bounds(resource_demand);
    let queue_name = queue_name.as_str();
    let queue_names = all_queue_names(queue_name, legacy_queue_names);
    if let Err(e) = fail_timed_out_attempt(
        pool,
        &queue_names,
        queue_name,
        default_max_time_since_last_heartbeat,
        retry_policy,
    )
    .await
    {
        tracing::error!(?e, "Failed to record a timed-out attempt of '{queue_name}'");
    }
    if resource_demand.is_empty() {
        return Ok(None);
    }
    let x = sqlx::query!(
        r#"
        WITH picked_task AS (
            -- The oldest due task of each queue name, then the oldest of those: each name reads
            -- `task_queue_name_status_idx` in order and stops at its first match, where
            -- `queue_name = ANY($1)` would sort the whole due backlog. The pick locks at most one
            -- row per name until it commits.
            SELECT c.task_id, c.attempt, c.warehouse_id, c.project_id
            FROM unnest($1::text[]) AS q(queue_name)
            CROSS JOIN LATERAL (
                SELECT t.task_id, t.attempt, t.warehouse_id, t.project_id, t.scheduled_for
                FROM task t
                WHERE t.queue_name = q.queue_name AND t.scheduled_for <= now() AND t.status = 'scheduled'
                    -- A task without a resource demand counts as 0. Bounds are never NULL, so the
                    -- range stays an index condition in a generic plan.
                    AND COALESCE(t.resource_demand, 0) > $3
                    AND COALESCE(t.resource_demand, 0) <= $4
                -- task_id breaks ties of scheduled_for.
                ORDER BY t.scheduled_for, t.task_id
                -- Skips rows other pickers hold before LIMIT counts them, so a held row never
                -- hides the due rows behind it.
                FOR UPDATE OF t SKIP LOCKED
                LIMIT 1
            ) c
            ORDER BY c.scheduled_for, c.task_id
            LIMIT 1
        ),
        picked_config AS (
            -- Config belongs to the queue, not to a row's (possibly legacy)
            -- name. Prefer the canonical queue's config ($2) and fall back to a
            -- legacy-named config row only when the canonical one is unset, so a
            -- row enqueued under an old name still loads the right config.
            SELECT cfg.config
            FROM picked_task p
            JOIN task_config cfg
                ON cfg.queue_name = ANY($1)
                AND ((cfg.warehouse_id IS NULL AND p.warehouse_id IS NULL) OR (cfg.warehouse_id = p.warehouse_id))
                AND cfg.project_id = p.project_id
            ORDER BY (cfg.queue_name = $2) DESC
            LIMIT 1
        )
        UPDATE task
        SET status = 'running',
            progress = 0.0,
            execution_details = NULL,
            picked_up_at = now(),
            last_heartbeat_at = now(),
            attempt = task.attempt + 1
        FROM picked_task p
        WHERE task.task_id = p.task_id AND task.attempt = p.attempt
        RETURNING
            task.task_id,
            task.entity_id,
            task.entity_type as "entity_type: TaskEntityTypeDB",
            task.entity_name,
            task.warehouse_id,
            task.task_data,
            task.scheduled_for,
            task.status as "status: TaskIntermediateStatus",
            task.picked_up_at,
            task.attempt,
            task.parent_task_id,
            task.queue_name,
            (select config from picked_config),
            task.project_id,
            task.resource_demand
            "#,
        &queue_names,
        queue_name,
        above,
        at_most,
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to pick a task for '{queue_name}'");
        e.into_error_model(format!("Failed to pick a '{queue_name}' task"))
    })?;

    if let Some(task) = x {
        tracing::trace!("Picked up task: {:?}", task);
        let project_id = Arc::new(ProjectId::from_db_unchecked(task.project_id));
        let scope = task_entity_from_db(
            task.entity_type,
            task.warehouse_id,
            task.entity_id,
            task.entity_name.clone(),
        )
        .map_err(ErrorModel::from)?;
        return Ok(Some(Task {
            task_metadata: TaskMetadata {
                resource_demand: task.resource_demand,
                project_id,
                parent_task_id: task.parent_task_id.map(TaskId::from),
                scheduled_for: task.scheduled_for,
                entity: scope,
                schedule_id: task_schedule_id_from_db(task.entity_type, task.entity_id),
            },
            config: task.config,
            id: TaskAttemptId {
                task_id: task.task_id.into(),
                attempt: task.attempt,
            },
            status: task.status,
            queue_name: task.queue_name.into(),
            picked_up_at: task.picked_up_at,
            data: task.task_data,
        }));
    }

    Ok(None)
}

/// All names a queue answers to: the current name plus any pre-rename aliases. Rows enqueued
/// before a rename still carry the old name.
fn all_queue_names(queue_name: &str, legacy_queue_names: &[&TaskQueueName]) -> Vec<String> {
    std::iter::once(queue_name.to_string())
        .chain(legacy_queue_names.iter().map(|n| n.as_str().to_string()))
        .collect()
}

/// `(above, at_most)` of `range` without open bounds: resource demands are never negative.
fn demand_bounds(range: TaskResourceDemandRange) -> (i64, i64) {
    (range.above.unwrap_or(-1), range.at_most.unwrap_or(i64::MAX))
}

/// Count the due, heartbeat-expired and running tasks of a queue in a resource demand range.
pub(crate) async fn task_queue_demand(
    pool: &PgPool,
    request: TaskQueueDemandRequest<'_>,
) -> Result<TaskQueueDemand, CatalogBackendError> {
    let TaskQueueDemandRequest {
        queue_name,
        legacy_queue_names,
        default_max_time_since_last_heartbeat,
        resource_demand,
    } = request;
    if resource_demand.is_empty() {
        return Ok(TaskQueueDemand::default());
    }
    let (above, at_most) = demand_bounds(resource_demand);
    let queue_name = queue_name.as_str();
    let queue_names = all_queue_names(queue_name, legacy_queue_names);
    let counts = sqlx::query!(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE s.state = 'due') AS "due!",
            COUNT(*) FILTER (WHERE s.state = 'expired') AS "heartbeat_expired!",
            COUNT(*) FILTER (WHERE s.state = 'running') AS "running!"
        FROM (
            -- What a pick can take now, as in `pick_task_with_retry_policy`.
            SELECT 'due' AS state
            FROM task t
            WHERE t.queue_name = ANY($1) AND t.status = 'scheduled' AND t.scheduled_for <= now()
                AND COALESCE(t.resource_demand, 0) > $3
                AND COALESCE(t.resource_demand, 0) <= $4
            UNION ALL
            -- The heartbeat timeout as in `fail_timed_out_attempt`.
            SELECT CASE
                WHEN now() - t.last_heartbeat_at > COALESCE(tc.max_time_since_last_heartbeat, $5)
                THEN 'expired' ELSE 'running' END
            FROM task t
            LEFT JOIN LATERAL (
                SELECT cfg.max_time_since_last_heartbeat
                FROM task_config cfg
                WHERE cfg.queue_name = ANY($1)
                    AND ((cfg.warehouse_id IS NULL AND t.warehouse_id IS NULL) OR (cfg.warehouse_id = t.warehouse_id))
                    AND cfg.project_id = t.project_id
                ORDER BY (cfg.queue_name = $2) DESC
                LIMIT 1
            ) tc ON true
            WHERE t.queue_name = ANY($1) AND t.status IN ('running', 'should-stop')
                AND COALESCE(t.resource_demand, 0) > $3
                AND COALESCE(t.resource_demand, 0) <= $4
        ) s
        "#,
        &queue_names,
        queue_name,
        above,
        at_most,
        pg_interval(default_max_time_since_last_heartbeat)
            .map_err(CatalogBackendError::new_unexpected)?,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail(format!("Failed to count the '{queue_name}' tasks"))
    })?;
    Ok(TaskQueueDemand {
        due: counts.due,
        heartbeat_expired: counts.heartbeat_expired,
        running: counts.running,
    })
}

/// Record one attempt of the queue whose heartbeat timed out as failed, including one that was
/// asked to stop.
async fn fail_timed_out_attempt(
    pool: &PgPool,
    queue_names: &[String],
    queue_name: &str,
    default_max_time_since_last_heartbeat: chrono::Duration,
    retry_policy: RetryPolicy,
) -> Result<(), IcebergErrorResponse> {
    let max_time_since_last_heartbeat = pg_interval(default_max_time_since_last_heartbeat)?;
    let mut transaction = pool
        .begin()
        .await
        .map_err(|e| e.into_error_model("Failed to start transaction for timed-out attempts"))?;
    let timed_out = sqlx::query!(
        r#"
        SELECT
            t.task_id,
            t.attempt,
            t.entity_type as "entity_type: TaskEntityTypeDB",
            t.entity_id,
            t.resource_demand,
            t.execution_details
        FROM task t
        LEFT JOIN LATERAL (
            SELECT cfg.max_time_since_last_heartbeat
            FROM task_config cfg
            WHERE cfg.queue_name = ANY($1)
                AND ((cfg.warehouse_id IS NULL AND t.warehouse_id IS NULL) OR (cfg.warehouse_id = t.warehouse_id))
                AND cfg.project_id = t.project_id
            ORDER BY (cfg.queue_name = $3) DESC
            LIMIT 1
        ) tc ON true
        WHERE t.queue_name = ANY($1)
            AND t.status IN ('running', 'should-stop')
            AND (now() - t.last_heartbeat_at) > COALESCE(tc.max_time_since_last_heartbeat, $2)
        ORDER BY t.scheduled_for, t.task_id
        FOR UPDATE OF t SKIP LOCKED
        LIMIT 1
        "#,
        queue_names,
        max_time_since_last_heartbeat,
        queue_name
    )
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|e| e.into_error_model("Failed to find timed-out task attempts"))?;

    let Some(timed_out) = timed_out else {
        return Ok(());
    };
    let id = TaskAttemptId {
        task_id: timed_out.task_id.into(),
        attempt: timed_out.attempt,
    };
    let details = "Attempt timed out.";
    let failed = FailedAttempt {
        task_id: id.task_id,
        attempt: id.attempt,
        schedule_id: task_schedule_id_from_db(timed_out.entity_type, timed_out.entity_id),
        kind: FailureKind::TimedOut,
        error: details,
        resource_demand: timed_out.resource_demand,
        execution_details: timed_out.execution_details.as_ref(),
    };
    record_failure(
        id,
        retry_policy.max_retries,
        &retry_policy.decide(&failed),
        details,
        &mut transaction,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|e| e.into_error_model("Failed to commit timed-out task attempt"))?;
    Ok(())
}

/// Leaves a `scheduled` row alone: it waits for its next pick, so this attempt is closed.
pub(crate) async fn record_success(
    id: impl AsRef<TaskAttemptId>,
    pool: &mut PgConnection,
    message: Option<&str>,
) -> Result<(), IcebergErrorResponse> {
    let TaskAttemptId { task_id, attempt } = *id.as_ref();
    let (task_log_exists, log_inserted, conflicting_message, conflicting_created_at) = sqlx::query!(
        r#"
        WITH moved AS (
            DELETE FROM task WHERE task_id = $1 AND attempt = $3 AND status != 'scheduled' RETURNING *
        ),
        task_log_attempt AS (
            SELECT message, task_created_at FROM task_log WHERE task_id = $1 AND attempt = $3
        ),
        log_insert AS (
            INSERT INTO task_log(
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_type,
                entity_name,
                message,
                attempt,
                started_at,
                duration,
                progress,
                execution_details,
                attempt_scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                task_created_at,
                project_id,
                resource_demand
            )
            SELECT task_id,
                    warehouse_id,
                    queue_name,
                    task_data,
                    'success',
                    entity_id,
                    entity_type,
                    entity_name,
                    $2,
                    attempt,
                    picked_up_at,
                    now() - picked_up_at,
                    1.0000,
                    execution_details,
                    scheduled_for,
                    last_heartbeat_at,
                    parent_task_id,
                    created_at,
                    project_id,
                    resource_demand
            FROM moved
            ON CONFLICT (task_id, attempt) DO NOTHING
            RETURNING task_id as log_task_id
        )
        SELECT 
            (EXISTS(SELECT 1 FROM task_log_attempt) OR EXISTS(SELECT 1 FROM moved)) as "task_log_exists!",
            EXISTS(SELECT 1 FROM log_insert) as "log_inserted!",
            (SELECT message FROM task_log_attempt) as "conflicting_message",
            (SELECT task_created_at FROM task_log_attempt) as "conflicting_created_at"
        "#,
        *task_id,
        message,
        attempt
    )
    .fetch_one(pool)
    .await
    .map_err(|e| e.into_error_model("Error recording task success."))
    .map(|r| (r.task_log_exists, r.log_inserted, r.conflicting_message, r.conflicting_created_at))?;

    if task_log_exists && !log_inserted {
        let err = ErrorModel::conflict(
            format!(
                "Task {task_id} with attempt {attempt} has already been recorded as completed."
            ),
            TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE,
            None,
        )
        .append_detail("Error recording task success.");

        let err =
            if let (Some(msg), Some(created_at)) = (conflicting_message, conflicting_created_at) {
                err.append_detail(format!(
                    "Original attempt recorded at {created_at} with message: '{msg}'"
                ))
            } else {
                err
            };

        return Err(err.into());
    } else if !task_log_exists {
        // Task didn't exist
        return Err(ErrorModel::not_found(
            format!("Task {task_id} with attempt {attempt} not found in active tasks."),
            "TaskNotFound",
            None,
        )
        .append_detail("Error recording task success.")
        .into());
    }

    Ok(())
}

/// What recording a failed attempt found: whether the attempt is logged, whether this call logged
/// it, and the message and time of an attempt logged before.
struct FailureRecord {
    task_log_exists: bool,
    log_inserted: bool,
    conflicting_message: Option<String>,
    conflicting_created_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Leaves a `scheduled` row alone: it waits for its next pick, so this attempt is closed.
pub(crate) async fn record_failure(
    id: impl AsRef<TaskAttemptId>,
    max_retries: i32,
    retry_decision: &RetryDecision,
    details: &str,
    conn: &mut PgConnection,
) -> Result<(), IcebergErrorResponse> {
    let TaskAttemptId { task_id, attempt } = *id.as_ref();
    let record = match retry_decision {
        RetryDecision::GiveUp { reason, .. } => {
            fail_task(conn, task_id, attempt, &given_up_message(details, reason)).await
        }
        RetryDecision::Retry { .. } if attempt >= max_retries => {
            fail_task(conn, task_id, attempt, details).await
        }
        RetryDecision::Retry {
            backoff,
            resource_demand,
            ..
        } => {
            let backoff =
                pg_interval((*backoff).clamp(chrono::Duration::zero(), MAX_RETRY_BACKOFF))?;
            let resource_demand = resource_demand.map(|demand| demand.max(0));
            reschedule_failed_task(conn, task_id, attempt, details, backoff, resource_demand).await
        }
    }
    .map_err(|e| e.append_detail(format!("Task ID: {task_id}, Attempt: {attempt}")))?;

    if record.task_log_exists && !record.log_inserted {
        let err = ErrorModel::conflict(
            format!("Task {task_id} with attempt {attempt} has already been recorded as failed."),
            TASK_ATTEMPT_ALREADY_FAILED_ERROR_TYPE,
            None,
        )
        .append_detail("Error recording task attempt failure.");

        // Task existed but log insert failed due to conflict
        let err = if let (Some(msg), Some(created_at)) =
            (record.conflicting_message, record.conflicting_created_at)
        {
            err.append_detail(format!(
                "Original attempt recorded at {created_at} with message: '{msg}'"
            ))
        } else {
            err
        };

        return Err(err.into());
    } else if !record.task_log_exists {
        // Task didn't exist
        return Err(ErrorModel::not_found(
            format!("Task {task_id} with attempt {attempt} not found in active tasks."),
            "TaskNotFound",
            None,
        )
        .append_detail("Error recording task attempt failure.")
        .into());
    }

    Ok(())
}

/// Move the attempt to the log as failed and end the task.
async fn fail_task(
    conn: &mut PgConnection,
    task_id: TaskId,
    attempt: i32,
    details: &str,
) -> Result<FailureRecord, ErrorModel> {
    sqlx::query!(
        r#"
        WITH moved AS (
            DELETE FROM task WHERE task_id = $1 AND attempt = $2 AND status != 'scheduled'
            RETURNING *
        ),
        task_log_attempt AS (
            SELECT message, task_created_at FROM task_log WHERE task_id = $1 AND attempt = $2
        ),
        log_insert AS (
            INSERT INTO task_log(
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_type,
                entity_name,
                message,
                attempt,
                started_at,
                duration,
                progress,
                execution_details,
                attempt_scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                task_created_at,
                project_id,
                resource_demand
            )
            SELECT
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                'failed',
                entity_id,
                entity_type,
                entity_name,
                $3,
                attempt,
                picked_up_at,
                now() - picked_up_at,
                progress,
                execution_details,
                scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                created_at,
                project_id,
                resource_demand
            FROM moved
            ON CONFLICT (task_id, attempt) DO NOTHING
            RETURNING task_id as log_task_id
        )
        SELECT 
            (EXISTS(SELECT 1 FROM task_log_attempt) OR EXISTS(SELECT 1 FROM moved)) as "task_log_exists!",
            EXISTS(SELECT 1 FROM log_insert) as "log_inserted!",
            (SELECT message FROM task_log_attempt) as "conflicting_message",
            (SELECT task_created_at FROM task_log_attempt) as "conflicting_created_at"
        "#,
        *task_id,
        attempt,
        details
    )
    .fetch_one(conn)
    .await
    .map_err(|e| e.into_error_model("Error recording task attempt failure."))
    .map(|r| FailureRecord {
        task_log_exists: r.task_log_exists,
        log_inserted: r.log_inserted,
        conflicting_message: r.conflicting_message,
        conflicting_created_at: r.conflicting_created_at,
    })
}

/// Log the attempt as failed and schedule the task again after `retry_backoff`.
async fn reschedule_failed_task(
    conn: &mut PgConnection,
    task_id: TaskId,
    attempt: i32,
    details: &str,
    retry_backoff: PgInterval,
    retry_resource_demand: Option<i64>,
) -> Result<FailureRecord, ErrorModel> {
    sqlx::query!(
        r#"
        WITH locked AS (
            SELECT * FROM task
            WHERE task_id = $1 AND attempt = $3 AND status != 'scheduled'
            FOR UPDATE
        ),
        task_log_attempt AS (
            SELECT message, task_created_at FROM task_log WHERE task_id = $1 AND attempt = $3
        ),
        log_insert AS (
            INSERT INTO task_log(
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_type,
                entity_name,
                message,
                attempt,
                started_at,
                duration,
                progress,
                execution_details,
                attempt_scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                task_created_at,
                project_id,
                resource_demand
            )
            SELECT 
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                'failed',
                entity_id,
                entity_type,
                entity_name,
                $2,
                attempt,
                picked_up_at,
                now() - picked_up_at,
                progress,
                execution_details,
                scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                created_at,
                project_id,
                resource_demand
            FROM locked
            ON CONFLICT (task_id, attempt) DO NOTHING
            RETURNING task_id as log_task_id
        ),
        task_update AS (
            UPDATE task t
            SET 
                status = 'scheduled',
                -- A zero backoff keeps the task's place in the queue.
                scheduled_for = CASE WHEN $4 > interval '0' THEN now() + $4 ELSE t.scheduled_for END,
                resource_demand = COALESCE($5, t.resource_demand),
                progress = 0.0,
                picked_up_at = NULL,
                execution_details = NULL,
                last_heartbeat_at = NULL
            FROM locked
            WHERE t.task_id = locked.task_id AND t.attempt = locked.attempt
            RETURNING t.task_id as updated_task_id
        )
        SELECT 
            (EXISTS(SELECT 1 FROM task_log_attempt) OR EXISTS(SELECT 1 FROM log_insert)) as "task_log_exists!",
            EXISTS(SELECT 1 FROM log_insert) as "log_inserted!",
            (SELECT message FROM task_log_attempt) as "conflicting_message",
            (SELECT task_created_at FROM task_log_attempt) as "conflicting_created_at",
            EXISTS(SELECT 1 FROM task_update) as "task_updated!"
        "#,
        *task_id,
        details,
        attempt,
        retry_backoff,
        retry_resource_demand,
    )
    .fetch_one(conn)
    .await
    .map_err(|e| e.into_error_model("Error marking task as failed."))
    .map(|r| FailureRecord {
        task_log_exists: r.task_log_exists,
        log_inserted: r.log_inserted,
        conflicting_message: r.conflicting_message,
        conflicting_created_at: r.conflicting_created_at,
    })
}

#[derive(Debug, Clone)]
struct TaskConfigRow {
    config: serde_json::Value,
    max_time_since_last_heartbeat: Option<PgInterval>,
}

pub(crate) async fn get_task_queue_config<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    connection: E,
    filter: &TaskQueueConfigFilter,
    queue_name: &TaskQueueName,
) -> lakekeeper::api::Result<Option<GetTaskQueueConfigResponse>> {
    let data = match filter {
        TaskQueueConfigFilter::WarehouseId { warehouse_id } => {
            sqlx::query_as!(
                TaskConfigRow,
                r#"
                SELECT config, max_time_since_last_heartbeat
                FROM task_config
                WHERE warehouse_id = $1 AND queue_name = $2
                "#,
                **warehouse_id,
                queue_name.as_str()
            )
            .fetch_optional(connection)
            .await
        }
        TaskQueueConfigFilter::ProjectId { project_id } => {
            sqlx::query_as!(
                TaskConfigRow,
                r#"
                SELECT config, max_time_since_last_heartbeat
                FROM task_config
                WHERE project_id = $1 AND queue_name = $2 AND warehouse_id IS NULL
                "#,
                project_id.as_str(),
                queue_name.as_str(),
            )
            .fetch_optional(connection)
            .await
        }
    };

    let result = data.map_err(|e| {
        tracing::error!(?e, "Failed to get task queue config");
        e.into_error_model(format!("Failed to get task queue config for {queue_name}"))
    })?;
    let Some(result) = result else {
        return Ok(None);
    };
    Ok(Some(GetTaskQueueConfigResponse {
        queue_config: QueueConfigResponse {
            config: result.config,
            queue_name: queue_name.clone(),
        },
        max_seconds_since_last_heartbeat: result
            .max_time_since_last_heartbeat
            .map(|x| x.microseconds / 1_000_000),
    }))
}

pub(crate) async fn set_task_queue_config(
    transaction: &mut PgConnection,
    queue_name: &TaskQueueName,
    project_id: ArcProjectId,
    warehouse_id: Option<WarehouseId>,
    config: &SetTaskQueueConfigRequest,
) -> lakekeeper::api::Result<()> {
    let serialized = config.queue_config.as_json();
    let max_time_since_last_heartbeat = config
        .max_seconds_since_last_heartbeat
        .map(|seconds| pg_interval(chrono::Duration::seconds(seconds)))
        .transpose()?;
    sqlx::query!(
        r#"
        INSERT INTO task_config (queue_name, project_id, warehouse_id, config, max_time_since_last_heartbeat)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (queue_name, project_id, warehouse_id) DO UPDATE
        SET config = $4, max_time_since_last_heartbeat = COALESCE($5, task_config.max_time_since_last_heartbeat)
        "#,
        queue_name.as_str(),
        project_id.as_str(),
        warehouse_id.map(Uuid::from),
        serialized,
        max_time_since_last_heartbeat
    )
    .execute(transaction)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to set task queue config");
        e.into_error_model(format!("Failed to set task queue config for {queue_name}"))
    })?;

    Ok(())
}

pub(crate) async fn request_tasks_stop(
    transaction: &mut PgConnection,
    task_ids: &[TaskId],
) -> lakekeeper::api::Result<()> {
    sqlx::query!(
        r#"
        UPDATE task
        SET status = 'should-stop'
        WHERE task_id = ANY($1) 
            AND status = 'running'
        "#,
        &task_ids.iter().map(|s| **s).collect_vec(),
    )
    .execute(transaction)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to request task to stop");
        let task_ids_str = task_ids.iter().join(", ");
        e.into_error_model(format!("Failed to request task to stop: {task_ids_str}"))
    })?;

    Ok(())
}

// If scheduled_for is None, run immediately
pub(crate) async fn reschedule_tasks_for(
    transaction: &mut PgConnection,
    task_ids: &[TaskId],
    scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
) -> lakekeeper::api::Result<()> {
    let run_now = scheduled_for.is_none();
    let scheduled_not_null = scheduled_for.unwrap_or_else(chrono::Utc::now);
    sqlx::query!(
        r#"
        WITH reschedule_tasks AS (
            SELECT t.* FROM task t 
            WHERE task_id = ANY($1) AND status IN ('should-stop', 'scheduled')
            FOR UPDATE
        ),
        inserted AS (
            INSERT INTO task_log(
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_type,
                entity_name,
                message,
                attempt,
                started_at,
                duration,
                progress,
                execution_details,
                attempt_scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                task_created_at,
                project_id,
                resource_demand
            )
            SELECT
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                'failed',
                entity_id,
                entity_type,
                entity_name,
                'Task did not stop in time before being rescheduled.',
                attempt,
                picked_up_at,
                now() - picked_up_at,
                progress,
                execution_details,
                scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                created_at,
                project_id,
                resource_demand
            FROM reschedule_tasks
            WHERE status != 'scheduled'
            ON CONFLICT (task_id, attempt) DO NOTHING
        )
        UPDATE task
        SET 
            scheduled_for = (CASE WHEN $3 THEN now() ELSE $2 END),
            status = 'scheduled',
            progress = 0.0,
            execution_details = NULL,
            last_heartbeat_at = NULL,
            picked_up_at = NULL
        FROM reschedule_tasks r
        WHERE task.task_id = r.task_id AND task.attempt = r.attempt
        "#,
        &task_ids.iter().map(|s| **s).collect_vec(),
        scheduled_not_null,
        run_now
    )
    .execute(transaction)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to reschedule tasks");
        let time_str = if let Some(scheduled_for) = scheduled_for {
            format!("to run at {scheduled_for}")
        } else {
            "to run now".to_string()
        };
        e.into_error_model(format!("Failed to reschedule tasks {time_str}."))
    })?;

    Ok(())
}

pub(crate) async fn check_and_heartbeat_task(
    transaction: &mut PgConnection,
    id: impl AsRef<TaskAttemptId>,
    progress: f32,
    execution_details: Option<serde_json::Value>,
) -> lakekeeper::api::Result<TaskCheckState> {
    let TaskAttemptId { task_id, attempt } = *id.as_ref();
    Ok(sqlx::query!(
        r#"WITH heartbeat as (
            UPDATE task 
            SET last_heartbeat_at = now(), 
                progress = $2, 
                execution_details = $3 
            WHERE task_id = $1 AND attempt = $4
            RETURNING status
        )
        SELECT status as "status: TaskIntermediateStatus" FROM heartbeat"#,
        *task_id,
        progress,
        execution_details,
        attempt,
    )
    .fetch_optional(transaction)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to check task");
        e.into_error_model(format!("Failed to check task {task_id}"))
    })?
    .map_or(TaskCheckState::NotActive, |state| match state.status {
        // Back to scheduled means the was rescheduled, and thus the attempt should stop.
        TaskIntermediateStatus::ShouldStop | TaskIntermediateStatus::Scheduled => {
            TaskCheckState::Stop
        }
        TaskIntermediateStatus::Running => TaskCheckState::Continue,
    }))
}

/// Which queues a cancellation covers.
struct CancelQueues {
    /// The queue's current name plus any pre-rename aliases. Matched with
    /// `queue_name = ANY(...)` so tasks enqueued under an old name are cancelled
    /// too (e.g. undropping a tabular whose expiration was scheduled before the
    /// rename). Empty when no queue is given; `all` then matches all queues.
    names: Vec<String>,
    all: bool,
    /// For messages.
    label: String,
}

/// Cancel scheduled tasks.
/// If `force_delete_running_tasks` is true, "running" and "should-stop" tasks will also be cancelled.
/// If `queue_name` is `None`, tasks in all queues will be cancelled.
pub(crate) async fn cancel_scheduled_tasks(
    connection: &mut PgConnection,
    filter: CancelTasksFilter,
    queue_name: Option<&TaskQueueName>,
    legacy_queue_names: &[&TaskQueueName],
    force_delete_running_tasks: bool,
) -> lakekeeper::api::Result<()> {
    let all = queue_name.is_none();
    let names: Vec<String> = queue_name
        .into_iter()
        .chain(legacy_queue_names.iter().copied())
        .map(|n| n.as_str().to_string())
        .collect();
    let label = if names.is_empty() {
        "<all>".to_string()
    } else {
        names.join(", ")
    };
    let queues = CancelQueues { names, all, label };
    match filter {
        CancelTasksFilter::WarehouseId { warehouse_id } => {
            cancel_warehouse_tasks(
                connection,
                warehouse_id,
                &queues,
                force_delete_running_tasks,
            )
            .await
        }
        CancelTasksFilter::TaskIds(task_ids) => {
            cancel_tasks_by_id(connection, &task_ids, &queues, force_delete_running_tasks).await
        }
        CancelTasksFilter::ProjectId {
            project_id,
            include_sub_tasks,
        } => {
            cancel_project_tasks(
                connection,
                &project_id,
                include_sub_tasks,
                &queues,
                force_delete_running_tasks,
            )
            .await
        }
    }
}

async fn cancel_warehouse_tasks(
    connection: &mut PgConnection,
    warehouse_id: WarehouseId,
    queues: &CancelQueues,
    force_delete_running_tasks: bool,
) -> lakekeeper::api::Result<()> {
    let (queue_names, queue_name_is_none, queue_name) = (&queues.names, queues.all, &queues.label);
    sqlx::query!(
        r#"
                WITH deleted as (
                    DELETE FROM task
                    WHERE (status = $3 OR $5) AND warehouse_id = $1 AND (queue_name = ANY($2) OR $6)
                    RETURNING *
                )
                INSERT INTO task_log(task_id,
                                        warehouse_id,
                                        queue_name,
                                        task_data,
                                        entity_id,
                                        entity_type,
                                        entity_name,
                                        status,
                                        attempt,
                                        started_at,
                                        duration,
                                        progress,
                                        execution_details,
                                        attempt_scheduled_for,
                                        last_heartbeat_at,
                                        parent_task_id,
                                        task_created_at,
                                        project_id,
                                        resource_demand
                                    )
                SELECT task_id,
                        warehouse_id,
                        queue_name,
                        task_data,
                        entity_id,
                        entity_type,
                        entity_name,
                        $4,
                        -- A scheduled task that has run waits for its next attempt, which
                        -- logs the cancellation. Decided on the deleted row, which a
                        -- concurrent failure report updates before this delete reads it.
                        attempt + (deleted.status = 'scheduled' AND deleted.attempt > 0)::int,
                        picked_up_at,
                        case when picked_up_at is not null
                            then now() - picked_up_at
                            else null
                        end,
                        progress,
                        execution_details,
                        scheduled_for,
                        last_heartbeat_at,
                        parent_task_id,
                        created_at,
                        project_id,
                        resource_demand
                FROM deleted
                ON CONFLICT (task_id, attempt) DO NOTHING
                "#,
        *warehouse_id,
        queue_names,
        TaskIntermediateStatus::Scheduled as _,
        TaskOutcome::Cancelled as _,
        force_delete_running_tasks,
        queue_name_is_none
    )
    .execute(connection)
    .await
    .map_err(|e| {
        tracing::error!(
            ?e,
            "Failed to cancel {queue_name} Tasks for warehouse {warehouse_id}"
        );
        e.into_error_model(format!(
            "Failed to cancel {queue_name} Tasks for warehouse {warehouse_id}"
        ))
    })?;
    Ok(())
}

async fn cancel_tasks_by_id(
    connection: &mut PgConnection,
    task_ids: &[TaskId],
    queues: &CancelQueues,
    force_delete_running_tasks: bool,
) -> lakekeeper::api::Result<()> {
    let (queue_names, queue_name_is_none) = (&queues.names, queues.all);
    sqlx::query!(
        r#"
                WITH deleted as (
                    DELETE FROM task
                    WHERE (status = $3 OR $6) AND task_id = ANY($1) AND (queue_name = ANY($4) OR $5)
                    RETURNING *
                )
                INSERT INTO task_log(task_id,
                                        warehouse_id,
                                        queue_name,
                                        task_data,
                                        status,
                                        entity_id,
                                        entity_type,
                                        entity_name,
                                        attempt,
                                        started_at,
                                        duration,
                                        progress,
                                        execution_details,
                                        attempt_scheduled_for,
                                        last_heartbeat_at,
                                        parent_task_id,
                                        task_created_at,
                                        project_id,
                                        resource_demand
                                    )
                SELECT task_id,
                        warehouse_id,
                        queue_name,
                        task_data,
                        $2,
                        entity_id,
                        entity_type,
                        entity_name,
                        -- A scheduled task that has run waits for its next attempt, which
                        -- logs the cancellation. Decided on the deleted row, which a
                        -- concurrent failure report updates before this delete reads it.
                        attempt + (deleted.status = 'scheduled' AND deleted.attempt > 0)::int,
                        picked_up_at,
                        case when picked_up_at is not null
                            then now() - picked_up_at
                            else null
                        end,
                        progress,
                        execution_details,
                        scheduled_for,
                        last_heartbeat_at,
                        parent_task_id,
                        created_at,
                        project_id,
                        resource_demand
                FROM deleted
                ON CONFLICT (task_id, attempt) DO NOTHING
                "#,
        &task_ids.iter().map(|s| **s).collect_vec(),
        TaskOutcome::Cancelled as _,
        TaskIntermediateStatus::Scheduled as _,
        queue_names,
        queue_name_is_none,
        force_delete_running_tasks
    )
    .execute(connection)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to cancel Tasks for task_ids {task_ids:?}");
        e.into_error_model("Failed to cancel Tasks for specified ids")
    })?;
    Ok(())
}

async fn cancel_project_tasks(
    connection: &mut PgConnection,
    project_id: &ProjectId,
    include_sub_tasks: bool,
    queues: &CancelQueues,
    force_delete_running_tasks: bool,
) -> lakekeeper::api::Result<()> {
    let (queue_names, queue_name_is_none, queue_name) = (&queues.names, queues.all, &queues.label);
    sqlx::query!(
        r#"
                WITH deleted as (
                    DELETE FROM task
                    WHERE (status = $3 OR $5) AND project_id = $1 AND ($7 OR warehouse_id IS NULL) AND (queue_name = ANY($2) OR $6)
                    RETURNING *
                )
                INSERT INTO task_log(task_id,
                                        warehouse_id,
                                        queue_name,
                                        task_data,
                                        status,
                                        entity_id,
                                        entity_type,
                                        entity_name,
                                        attempt,
                                        started_at,
                                        duration,
                                        progress,
                                        execution_details,
                                        attempt_scheduled_for,
                                        last_heartbeat_at,
                                        parent_task_id,
                                        task_created_at,
                                        project_id,
                                        resource_demand)
                SELECT task_id,
                        warehouse_id,
                        queue_name,
                        task_data,
                        $4,
                        entity_id,
                        entity_type,
                        entity_name,
                        -- A scheduled task that has run waits for its next attempt, which
                        -- logs the cancellation. Decided on the deleted row, which a
                        -- concurrent failure report updates before this delete reads it.
                        attempt + (deleted.status = 'scheduled' AND deleted.attempt > 0)::int,
                        picked_up_at,
                        case when picked_up_at is not null
                            then now() - picked_up_at
                            else null
                        end,
                        progress,
                        execution_details,
                        scheduled_for,
                        last_heartbeat_at,
                        parent_task_id,
                        created_at,
                        project_id,
                        resource_demand
                FROM deleted
                ON CONFLICT (task_id, attempt) DO NOTHING
                "#,
        project_id.as_str(),
        queue_names,
        TaskIntermediateStatus::Scheduled as _,
        TaskOutcome::Cancelled as _,
        force_delete_running_tasks,
        queue_name_is_none,
        include_sub_tasks
    )
    .execute(connection)
    .await
    .map_err(|e| {
        tracing::error!(
            ?e,
            "Failed to cancel {queue_name} Tasks for project {project_id}"
        );
        e.into_error_model(format!(
            "Failed to cancel {queue_name} Tasks for project {project_id}"
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::LazyLock, vec};

    use chrono::{DateTime, Utc};
    #[cfg(feature = "open-api")]
    use lakekeeper::utoipa;
    use lakekeeper::{
        WarehouseId,
        api::management::v1::{
            task_queue::QueueConfig, tasks::TaskStatus as ApiTaskStatus,
            warehouse::TabularDeleteProfile,
        },
        service::{
            CatalogStore, Transaction,
            authz::AllowAllAuthorizer,
            tasks::{
                DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT, SpecializedTask, TaskConfig, TaskData,
                TaskDetailsScope, TaskExecutionDetails, TaskId, TaskInput, TaskIntermediateStatus,
                WarehouseTaskEntityId,
            },
        },
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{CatalogState, PostgresBackend, PostgresTransaction, tests::SetupTestCatalog};

    #[allow(clippy::too_many_arguments)]
    async fn queue_task(
        conn: &mut PgConnection,
        queue_name: &TaskQueueName,
        parent_task_id: Option<TaskId>,
        project_id: ArcProjectId,
        scheduled_for: Option<DateTime<Utc>>,
        payload: Option<serde_json::Value>,
        entity: TaskEntity,
    ) -> Result<Option<TaskId>, IcebergErrorResponse> {
        Ok(queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand: None,
                    project_id,
                    parent_task_id,
                    scheduled_for,
                    entity,
                },
                payload: payload.unwrap_or(serde_json::json!({})),
            }],
        )
        .await?
        .pop()
        .map(|x| x.task_id))
    }

    fn generate_tq_name() -> TaskQueueName {
        TaskQueueName::from(format!("test-{}", Uuid::now_v7()))
    }

    #[sqlx::test]
    async fn test_queue_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap();

        assert!(
            queue_task(
                &mut conn,
                &tq_name,
                None,
                project_id.clone(),
                None,
                None,
                TaskEntity::EntityInWarehouse {
                    warehouse_id,
                    entity_id,
                    entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
                },
            )
            .await
            .unwrap()
            .is_none()
        );

        let entity_id3 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id3 = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity_id3,
                entity_name: vec![format!("entity-{}", entity_id3.as_uuid())],
            },
        )
        .await
        .unwrap();

        assert_ne!(id, id3);
    }

    #[sqlx::test]
    async fn test_queue_task_batch_round_trips_generic_table_entity(pool: PgPool) {
        // Pins the TaskEntityTypeDB::GenericTable arm of the InsertResult
        // mapping at the queue_task_batch site: a GenericTable entity goes in,
        // a GenericTable entity must come back with the same generic_table_id.
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let generic_table_uuid = Uuid::now_v7();
        let entity_id = WarehouseTaskEntityId::GenericTable {
            generic_table_id: generic_table_uuid.into(),
        };
        let tq_name = generate_tq_name();

        let mut inserts = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand: None,
                    project_id: project_id.clone(),
                    parent_task_id: None,
                    scheduled_for: None,
                    entity: TaskEntity::EntityInWarehouse {
                        warehouse_id,
                        entity_id,
                        entity_name: vec!["ns".to_string(), format!("gt-{generic_table_uuid}")],
                    },
                },
                payload: serde_json::json!({"kind": "generic-table"}),
            }],
        )
        .await
        .unwrap();

        assert_eq!(inserts.len(), 1, "expected one InsertResult");
        let inserted = inserts.pop().unwrap();
        match inserted.entity_id {
            Some(WarehouseTaskEntityId::GenericTable { generic_table_id }) => {
                assert_eq!(*generic_table_id, generic_table_uuid);
            }
            other => panic!("expected InsertResult.entity_id = GenericTable, got {other:?}"),
        }
    }

    pub(crate) async fn setup_warehouse(pool: PgPool) -> (WarehouseId, ArcProjectId) {
        let prof = crate::tests::memory_io_profile();
        let (_, wh) = crate::tests::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            1,
            None,
        )
        .await;
        (wh.warehouse_id, wh.project_id)
    }

    pub(crate) async fn setup_two_warehouses(
        pool: PgPool,
    ) -> (ArcProjectId, WarehouseId, WarehouseId) {
        let prof = crate::tests::memory_io_profile();
        let (_, wh) = crate::tests::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            2,
            None,
        )
        .await;
        (
            wh.project_id,
            wh.warehouse_id,
            wh.additional_warehouses[0].1,
        )
    }

    #[sqlx::test]
    async fn test_pick_task_dual_reads_legacy_queue_name(pool: PgPool) {
        // A task enqueued under a queue's pre-rename name must still be picked
        // up when the worker polls under the current name plus the legacy
        // alias. This is what keeps in-flight soft-deletions finalizing across
        // the `tabular_expiration` -> `soft_deletion` rename.
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let legacy_name = TaskQueueName::from("tabular_expiration");
        let current_name = TaskQueueName::from("soft_deletion");
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id = queue_task(
            &mut conn,
            &legacy_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        // Polling under the current name alone must NOT match the legacy row.
        let none = pick_task(
            &pool,
            &current_name,
            &[],
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
        )
        .await
        .unwrap();
        assert!(
            none.is_none(),
            "legacy-named task must not match the current name without the alias"
        );

        // With the legacy alias supplied, the same poll picks it up.
        let task = pick_task(
            &pool,
            &current_name,
            &[&legacy_name],
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(task.task_id(), id);
        assert_eq!(&task.queue_name, &legacy_name);
    }

    /// Queues a task of `queue_name` for a new table, due at `scheduled_for`.
    async fn queue_due(
        pool: &PgPool,
        queue_name: &TaskQueueName,
        warehouse_id: WarehouseId,
        project_id: &ArcProjectId,
        scheduled_for: DateTime<Utc>,
    ) -> TaskId {
        let mut conn = pool.acquire().await.unwrap();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        queue_task(
            &mut conn,
            queue_name,
            None,
            project_id.clone(),
            Some(scheduled_for),
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap()
    }

    /// Picks tasks of `queue_name` and `legacy_name` until none is left.
    async fn pick_until_none(
        pool: &PgPool,
        queue_name: &TaskQueueName,
        legacy_name: &TaskQueueName,
    ) -> Vec<TaskId> {
        let mut picked = Vec::new();
        while let Some(task) = pick_task(
            pool,
            queue_name,
            &[legacy_name],
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
        )
        .await
        .unwrap()
        {
            picked.push(task.task_id());
        }
        picked
    }

    #[sqlx::test]
    async fn test_pick_takes_the_oldest_due_task_across_queue_names(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let current = generate_tq_name();
        let legacy = generate_tq_name();
        let now = Utc::now();
        let ago = |minutes| now - chrono::Duration::minutes(minutes);
        let current_3 = queue_due(&pool, &current, warehouse_id, &project_id, ago(3)).await;
        let current_1 = queue_due(&pool, &current, warehouse_id, &project_id, ago(1)).await;
        let legacy_2 = queue_due(&pool, &legacy, warehouse_id, &project_id, ago(2)).await;
        let legacy_1 = queue_due(&pool, &legacy, warehouse_id, &project_id, ago(1)).await;
        queue_due(&pool, &legacy, warehouse_id, &project_id, ago(-60)).await;

        // task_id breaks the tie of the two tasks due a minute ago.
        let mut tied = [current_1, legacy_1];
        tied.sort_by_key(|id| **id);
        assert_eq!(
            pick_until_none(&pool, &current, &legacy).await,
            vec![current_3, legacy_2, tied[0], tied[1]]
        );
    }

    #[sqlx::test]
    async fn test_pick_skips_the_tasks_another_picker_holds(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let current = generate_tq_name();
        let legacy = generate_tq_name();
        let now = Utc::now();
        let ago = |minutes| now - chrono::Duration::minutes(minutes);
        let current_3 = queue_due(&pool, &current, warehouse_id, &project_id, ago(3)).await;
        let current_1 = queue_due(&pool, &current, warehouse_id, &project_id, ago(1)).await;
        let legacy_2 = queue_due(&pool, &legacy, warehouse_id, &project_id, ago(2)).await;

        // Another picker holds the oldest task of each name.
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT task_id FROM task WHERE task_id = ANY($1) FOR UPDATE")
            .bind(vec![*current_3, *legacy_2])
            .fetch_all(&mut *holder)
            .await
            .unwrap();
        let picked = pick_task(
            &pool,
            &current,
            &[&legacy],
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(picked.task_id(), current_1);

        holder.rollback().await.unwrap();
        assert_eq!(
            pick_until_none(&pool, &current, &legacy).await,
            vec![current_3, legacy_2]
        );
    }

    #[sqlx::test]
    async fn test_concurrent_pickers_pick_each_task_once(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let current = generate_tq_name();
        let legacy = generate_tq_name();
        let now = Utc::now();
        let mut queued = Vec::new();
        for i in 0..50 {
            let queue_name = if i % 2 == 0 { &current } else { &legacy };
            // Five distinct due times, so the pickers also race over ties.
            let scheduled_for = now - chrono::Duration::minutes(i % 5);
            queued
                .push(queue_due(&pool, queue_name, warehouse_id, &project_id, scheduled_for).await);
        }

        let pickers = (0..8)
            .map(|_| {
                let (pool, current, legacy) = (pool.clone(), current.clone(), legacy.clone());
                tokio::spawn(async move { pick_until_none(&pool, &current, &legacy).await })
            })
            .collect::<Vec<_>>();
        let mut picked = Vec::new();
        for picker in pickers {
            picked.extend(picker.await.unwrap());
        }

        picked.sort_by_key(|id| **id);
        queued.sort_by_key(|id| **id);
        assert_eq!(picked, queued);
        let once: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task WHERE task_id = ANY($1) AND status = 'running' AND attempt = 1",
        )
        .bind(queued.iter().map(|id| **id).collect::<Vec<_>>())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(once, 50);
    }

    #[sqlx::test]
    async fn test_failed_tasks_retry_attempts(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_failure_without_backoff(
            &task,
            5,
            "test details",
            &mut pool.acquire().await.unwrap(),
        )
        .await
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_failure_without_backoff(&task, 2, "test", &mut pool.acquire().await.unwrap())
            .await
            .unwrap();

        assert!(
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[sqlx::test]
    async fn test_success_tasks_are_not_polled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let tq_name = generate_tq_name();

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        assert!(
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[sqlx::test]
    async fn test_success_tasks_can_be_reinserted_with_new_id(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let tq_name = generate_tq_name();

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"a": "a"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity,
                entity_name: vec![format!("entity-{}", entity.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"a": "a"}));

        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        let id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            Some(serde_json::json!({"b": "b"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity,
                entity_name: vec![format!("entity-{}", entity.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), id2);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_cancelled_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"a": "a"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity,
                entity_name: vec![format!("entity-{}", entity.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![id]),
            Some(&tq_name),
            &[],
            false,
        )
        .await
        .unwrap();

        let id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            Some(serde_json::json!({"b": "b"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity,
                entity_name: vec![format!("entity-{}", entity.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id2);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_failed_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let tq_name = generate_tq_name();

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"a": "a"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity,
                entity_name: vec![format!("entity-{}", entity.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"a": "a"}));

        record_failure_without_backoff(&task, 1, "failed", &mut pool.acquire().await.unwrap())
            .await
            .unwrap();

        let id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            Some(serde_json::json!({"b": "b"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity,
                entity_name: vec![format!("entity-{}", entity.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), id2);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_scheduled_tasks_are_polled_later(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let tq_name = generate_tq_name();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let scheduled_for = Utc::now() + chrono::Duration::milliseconds(500);
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            Some(scheduled_for),
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap(),
            None
        );

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
    }

    #[sqlx::test]
    async fn test_stale_tasks_are_picked_up_again(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, &tq_name, &[], chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(
            attempt_log(&pool, id).await,
            vec![(
                1,
                TaskOutcome::Failed,
                Some("Attempt timed out.".to_string())
            )]
        );
    }

    /// Queue one table task and pick it with a heartbeat timeout of 500 ms.
    async fn queue_and_pick_timing_out_task(
        pool: &PgPool,
        retry_policy: RetryPolicy,
    ) -> (TaskQueueName, Task) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();
        let task = pick_task_with_retry_policy(
            pool,
            &tq_name,
            &[],
            chrono::Duration::milliseconds(500),
            retry_policy,
            TaskResourceDemandRange::default(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(task.attempt(), 1);
        (tq_name, task)
    }

    async fn attempt_log(
        pool: &PgPool,
        task_id: TaskId,
    ) -> Vec<(i32, TaskOutcome, Option<String>)> {
        sqlx::query_as(
            "SELECT attempt, status, message FROM task_log WHERE task_id = $1 ORDER BY attempt",
        )
        .bind(*task_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// Moves the task's last heartbeat an hour back, past any heartbeat timeout.
    async fn expire_heartbeat(pool: &PgPool, task_id: TaskId) {
        sqlx::query(
            "UPDATE task SET last_heartbeat_at = now() - interval '1 hour' WHERE task_id = $1",
        )
        .bind(*task_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// How long after failed `attempt` was logged the task is due again.
    pub(crate) async fn retry_delay(pool: &PgPool, task_id: TaskId, attempt: i32) -> PgInterval {
        sqlx::query_scalar(
            "SELECT t.scheduled_for - l.created_at FROM task t \
             JOIN task_log l ON l.task_id = t.task_id AND l.attempt = $2 WHERE t.task_id = $1",
        )
        .bind(*task_id)
        .bind(attempt)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test]
    async fn test_timed_out_attempt_is_retried_after_its_backoff(pool: PgPool) {
        let policy = RetryPolicy {
            max_retries: 5,
            retry_decision: |failed| {
                RetryDecision::retry(chrono::Duration::minutes(i64::from(failed.attempt) * 10))
            },
        };
        let (tq_name, task) = queue_and_pick_timing_out_task(&pool, policy).await;
        expire_heartbeat(&pool, task.task_id()).await;

        assert!(
            pick_task_with_retry_policy(
                &pool,
                &tq_name,
                &[],
                chrono::Duration::milliseconds(500),
                policy,
                TaskResourceDemandRange::default(),
            )
            .await
            .unwrap()
            .is_none()
        );
        assert_eq!(
            retry_delay(&pool, task.task_id(), 1).await.microseconds,
            600_000_000
        );

        // The worker of attempt 1 reports after its attempt was recorded as timed out.
        let mut conn = pool.acquire().await.unwrap();
        let err = record_success(&task, &mut conn, None).await.unwrap_err();
        assert_eq!(
            (err.error.code, err.error.r#type.as_str()),
            (409, "TaskAttemptAlreadyCompleted")
        );
        let err = record_failure_without_backoff(&task, 5, "late", &mut conn)
            .await
            .unwrap_err();
        assert_eq!(
            (err.error.code, err.error.r#type.as_str()),
            (409, "TaskAttemptAlreadyFailed")
        );

        let (status, attempt): (TaskIntermediateStatus, i32) =
            sqlx::query_as("SELECT status, attempt FROM task WHERE task_id = $1")
                .bind(*task.task_id())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((status, attempt), (TaskIntermediateStatus::Scheduled, 1));
        assert_eq!(
            attempt_log(&pool, task.task_id()).await,
            vec![(
                1,
                TaskOutcome::Failed,
                Some("Attempt timed out.".to_string())
            )]
        );
    }

    #[sqlx::test]
    async fn test_timed_out_last_attempt_fails_the_task(pool: PgPool) {
        let policy = RetryPolicy {
            max_retries: 1,
            retry_decision: |_| RetryDecision::retry(chrono::Duration::zero()),
        };
        let (tq_name, task) = queue_and_pick_timing_out_task(&pool, policy).await;
        expire_heartbeat(&pool, task.task_id()).await;

        assert!(
            pick_task_with_retry_policy(
                &pool,
                &tq_name,
                &[],
                chrono::Duration::milliseconds(500),
                policy,
                TaskResourceDemandRange::default(),
            )
            .await
            .unwrap()
            .is_none()
        );
        let active: i64 = sqlx::query_scalar("SELECT count(*) FROM task WHERE task_id = $1")
            .bind(*task.task_id())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(active, 0);
        assert_eq!(
            attempt_log(&pool, task.task_id()).await,
            vec![(
                1,
                TaskOutcome::Failed,
                Some("Attempt timed out.".to_string())
            )]
        );
    }

    #[sqlx::test]
    async fn test_timed_out_attempt_asked_to_stop_is_retried(pool: PgPool) {
        let (tq_name, task) = queue_and_pick_timing_out_task(&pool, TEST_RETRY_POLICY).await;
        sqlx::query("UPDATE task SET status = 'should-stop' WHERE task_id = $1")
            .bind(*task.task_id())
            .execute(&pool)
            .await
            .unwrap();
        expire_heartbeat(&pool, task.task_id()).await;

        let retried = pick_task(&pool, &tq_name, &[], chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((retried.task_id(), retried.attempt()), (task.task_id(), 2));
        assert_eq!(
            attempt_log(&pool, task.task_id()).await,
            vec![(
                1,
                TaskOutcome::Failed,
                Some("Attempt timed out.".to_string())
            )]
        );
    }

    /// Fails a task's first attempt, then cancels it with the filter `filter` builds from it while
    /// it waits for its retry.
    async fn assert_cancelling_a_task_waiting_for_its_retry_logs_the_cancellation(
        pool: PgPool,
        filter: impl FnOnce(&Task) -> CancelTasksFilter,
    ) {
        let (_, task) = queue_and_pick_timing_out_task(&pool, TEST_RETRY_POLICY).await;
        let mut conn = pool.acquire().await.unwrap();
        record_failure(
            &task,
            5,
            &RetryDecision::retry(chrono::Duration::hours(1)),
            "boom",
            &mut conn,
        )
        .await
        .unwrap();

        cancel_scheduled_tasks(&mut conn, filter(&task), None, &[], false)
            .await
            .unwrap();
        let active: i64 = sqlx::query_scalar("SELECT count(*) FROM task WHERE task_id = $1")
            .bind(*task.task_id())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(active, 0);
        assert_eq!(
            attempt_log(&pool, task.task_id()).await,
            vec![
                (1, TaskOutcome::Failed, Some("boom".to_string())),
                (2, TaskOutcome::Cancelled, None),
            ]
        );
    }

    #[sqlx::test]
    async fn test_cancelling_by_id_a_task_waiting_for_its_retry_logs_the_cancellation(
        pool: PgPool,
    ) {
        assert_cancelling_a_task_waiting_for_its_retry_logs_the_cancellation(pool, |task| {
            CancelTasksFilter::TaskIds(vec![task.task_id()])
        })
        .await;
    }

    #[sqlx::test]
    async fn test_cancelling_by_warehouse_a_task_waiting_for_its_retry_logs_the_cancellation(
        pool: PgPool,
    ) {
        assert_cancelling_a_task_waiting_for_its_retry_logs_the_cancellation(pool, |task| {
            CancelTasksFilter::WarehouseId {
                warehouse_id: task.task_metadata.warehouse_id().unwrap(),
            }
        })
        .await;
    }

    #[sqlx::test]
    async fn test_cancelling_by_project_a_task_waiting_for_its_retry_logs_the_cancellation(
        pool: PgPool,
    ) {
        assert_cancelling_a_task_waiting_for_its_retry_logs_the_cancellation(pool, |task| {
            CancelTasksFilter::ProjectId {
                project_id: (**task.task_metadata.project_id()).clone(),
                include_sub_tasks: true,
            }
        })
        .await;
    }

    #[sqlx::test]
    async fn test_cancelling_a_running_task_while_its_failure_commits_logs_the_cancellation(
        pool: PgPool,
    ) {
        let (_, task) = queue_and_pick_timing_out_task(&pool, TEST_RETRY_POLICY).await;
        let mut failure = pool.begin().await.unwrap();
        record_failure(
            &task,
            5,
            &RetryDecision::retry(chrono::Duration::hours(1)),
            "boom",
            &mut failure,
        )
        .await
        .unwrap();

        let cancel = tokio::spawn({
            let pool = pool.clone();
            let task_id = task.task_id();
            async move {
                let mut conn = pool.acquire().await.unwrap();
                cancel_scheduled_tasks(
                    &mut conn,
                    CancelTasksFilter::TaskIds(vec![task_id]),
                    None,
                    &[],
                    true,
                )
                .await
            }
        });
        // The cancel waits for the failure's row lock.
        crate::warehouse::test::wait_for_a_lock_wait(&pool).await;
        failure.commit().await.unwrap();
        cancel.await.unwrap().unwrap();

        assert_eq!(
            attempt_log(&pool, task.task_id()).await,
            vec![
                (1, TaskOutcome::Failed, Some("boom".to_string())),
                (2, TaskOutcome::Cancelled, None),
            ]
        );
    }

    /// Queue a table task with `resource_demand`, due `minutes_ago` minutes ago.
    async fn queue_with_demand(
        pool: &PgPool,
        queue_name: &TaskQueueName,
        warehouse_id: WarehouseId,
        project_id: &ArcProjectId,
        resource_demand: Option<i64>,
        minutes_ago: i64,
    ) -> TaskId {
        let mut conn = pool.acquire().await.unwrap();
        queue_task_batch(
            &mut conn,
            queue_name,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand,
                    project_id: project_id.clone(),
                    parent_task_id: None,
                    scheduled_for: Some(Utc::now() - chrono::Duration::minutes(minutes_ago)),
                    entity: TaskEntity::EntityInWarehouse {
                        warehouse_id,
                        entity_id: WarehouseTaskEntityId::Table {
                            table_id: Uuid::now_v7().into(),
                        },
                        entity_name: vec!["t".to_string()],
                    },
                },
                payload: serde_json::json!({}),
            }],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
        .task_id
    }

    async fn pick_in(
        pool: &PgPool,
        queue_name: &TaskQueueName,
        retry_policy: RetryPolicy,
        range: TaskResourceDemandRange,
    ) -> Option<Task> {
        pick_task_with_retry_policy(
            pool,
            queue_name,
            &[],
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
            retry_policy,
            range,
        )
        .await
        .unwrap()
    }

    /// `resource_demand` of the task row and of each log entry, by attempt.
    async fn demands(
        pool: &PgPool,
        task_id: TaskId,
    ) -> (Vec<Option<i64>>, Vec<(i32, Option<i64>)>) {
        let task = sqlx::query_scalar("SELECT resource_demand FROM task WHERE task_id = $1")
            .bind(*task_id)
            .fetch_all(pool)
            .await
            .unwrap();
        let log = sqlx::query_as(
            "SELECT attempt, resource_demand FROM task_log WHERE task_id = $1 ORDER BY attempt",
        )
        .bind(*task_id)
        .fetch_all(pool)
        .await
        .unwrap();
        (task, log)
    }

    #[sqlx::test]
    async fn test_resource_demand_travels_with_the_task(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = generate_tq_name();
        let task_id = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(7), 3).await;
        let cancelled = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(8), 2).await;
        let failed = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(9), 1).await;
        let mut conn = pool.acquire().await.unwrap();
        let listed = || {
            let (pool, project_id, q) = (pool.clone(), project_id.clone(), q.clone());
            async move {
                list_tasks(
                    &lakekeeper::service::tasks::TaskFilter::WarehouseId {
                        warehouse_id,
                        project_id,
                    },
                    &lakekeeper::api::management::v1::tasks::ListTasksRequest::builder()
                        .queue_name(Some(vec![q]))
                        .build(),
                    &mut pool.acquire().await.unwrap(),
                )
                .await
                .unwrap()
                .tasks
                .into_iter()
                .map(|t| (t.task_id(), t.task_metadata.resource_demand()))
                .collect::<HashMap<_, _>>()
            }
        };
        let all = HashMap::from([(task_id, Some(7)), (cancelled, Some(8)), (failed, Some(9))]);
        assert_eq!(listed().await, all);
        let task = pick_task(&pool, &q, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (task.task_id(), task.task_metadata.resource_demand()),
            (task_id, Some(7))
        );
        let details = || {
            let (pool, project_id) = (pool.clone(), project_id.clone());
            async move {
                get_task_details(
                    task_id,
                    TaskDetailsScope::Warehouse {
                        project_id,
                        warehouse_id,
                    },
                    10,
                    &pool,
                )
                .await
                .unwrap()
                .unwrap()
                .task
                .task_metadata
                .resource_demand()
            }
        };
        assert_eq!(details().await, Some(7));

        // Every outcome logs the attempt's resource demand.
        record_success(&task, &mut conn, None).await.unwrap();
        assert_eq!(details().await, Some(7));
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![cancelled]),
            None,
            &[],
            false,
        )
        .await
        .unwrap();
        let task = pick_task(&pool, &q, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), failed);
        record_failure(
            &task,
            1,
            &RetryDecision::retry(chrono::Duration::zero()),
            "boom",
            &mut conn,
        )
        .await
        .unwrap();
        assert_eq!(
            [
                demands(&pool, task_id).await,
                demands(&pool, cancelled).await,
                demands(&pool, failed).await
            ],
            [
                (vec![], vec![(1, Some(7))]),
                (vec![], vec![(0, Some(8))]),
                (vec![], vec![(1, Some(9))])
            ]
        );
        assert_eq!(listed().await, all);
    }

    #[sqlx::test]
    async fn test_pick_takes_only_tasks_in_its_resource_demand_range(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let pick_all = |q: TaskQueueName, range: TaskResourceDemandRange| {
            let pool = pool.clone();
            async move {
                let mut picked = Vec::new();
                while let Some(task) = pick_in(&pool, &q, TEST_RETRY_POLICY, range).await {
                    picked.push(task.task_id());
                }
                picked
            }
        };

        // Up to 10, inclusive; a task without a resource demand counts as 0.
        let q = generate_tq_name();
        queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(11), 5).await;
        let unset = queue_with_demand(&pool, &q, warehouse_id, &project_id, None, 4).await;
        let ten = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(10), 3).await;
        let zero = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(0), 2).await;
        let five = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(5), 1).await;
        let up_to_ten = TaskResourceDemandRange::builder().at_most(10).build();
        assert_eq!(pick_all(q, up_to_ten).await, vec![unset, ten, zero, five]);

        // Above 10, exclusive; above 0 leaves out 0 and tasks without one.
        let q = generate_tq_name();
        let eleven = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(11), 5).await;
        queue_with_demand(&pool, &q, warehouse_id, &project_id, None, 4).await;
        let ten = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(10), 3).await;
        queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(0), 2).await;
        let above_ten = TaskResourceDemandRange::builder().above(10).build();
        assert_eq!(pick_all(q.clone(), above_ten).await, vec![eleven]);
        let above_zero = TaskResourceDemandRange::builder().above(0).build();
        assert_eq!(pick_all(q, above_zero).await, vec![ten]);

        // Both bounds.
        let q = generate_tq_name();
        queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(11), 4).await;
        let ten = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(10), 3).await;
        let six = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(6), 2).await;
        queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(5), 1).await;
        let six_to_ten = TaskResourceDemandRange::builder()
            .above(5)
            .at_most(10)
            .build();
        assert_eq!(pick_all(q.clone(), six_to_ten).await, vec![ten, six]);

        // An empty range picks nothing.
        let empty = TaskResourceDemandRange::builder()
            .above(5)
            .at_most(5)
            .build();
        assert!(empty.is_empty());
        assert_eq!(pick_all(q, empty).await, vec![]);
    }

    #[sqlx::test]
    async fn test_filtered_pick_closes_a_timed_out_attempt_outside_its_range(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = generate_tq_name();
        let task_id = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(20), 1).await;
        pick_task(&pool, &q, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        expire_heartbeat(&pool, task_id).await;

        let up_to_ten = TaskResourceDemandRange::builder().at_most(10).build();
        assert!(
            pick_in(&pool, &q, TEST_RETRY_POLICY, up_to_ten)
                .await
                .is_none()
        );
        assert_eq!(
            attempt_log(&pool, task_id).await,
            vec![(
                1,
                TaskOutcome::Failed,
                Some("Attempt timed out.".to_string())
            )]
        );
        assert_eq!(
            demands(&pool, task_id).await,
            (vec![Some(20)], vec![(1, Some(20))])
        );
    }

    #[sqlx::test]
    async fn test_a_retry_decision_can_give_up(pool: PgPool) {
        // Give up on a timed-out attempt above 10; retry anything else.
        let policy = RetryPolicy {
            max_retries: 5,
            retry_decision: |failed| {
                if failed.resource_demand > Some(10) {
                    RetryDecision::give_up("above the largest class")
                } else {
                    RetryDecision::retry(chrono::Duration::zero())
                }
            },
        };
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = generate_tq_name();
        let everything = TaskResourceDemandRange::default();
        let small = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(10), 2).await;
        let large = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(11), 1).await;
        for task_id in [small, large] {
            let task = pick_in(&pool, &q, policy, everything).await.unwrap();
            assert_eq!(task.task_id(), task_id);
        }
        expire_heartbeat(&pool, small).await;
        expire_heartbeat(&pool, large).await;

        // Each pick first closes one timed-out attempt: `small` is retried, `large` is not.
        let task = pick_in(&pool, &q, policy, everything).await.unwrap();
        assert_eq!((task.task_id(), task.attempt()), (small, 2));
        assert!(pick_in(&pool, &q, policy, everything).await.is_none());
        assert_eq!(
            attempt_log(&pool, large).await,
            vec![(
                1,
                TaskOutcome::Failed,
                Some("Attempt timed out.\nNot retried: above the largest class".to_string())
            )]
        );
        assert_eq!(demands(&pool, large).await, (vec![], vec![(1, Some(11))]));

        // A reported failure gives up with its reason, also on its last attempt.
        let mut conn = pool.acquire().await.unwrap();
        record_failure(&task, 2, &RetryDecision::give_up("no"), "boom", &mut conn)
            .await
            .unwrap();
        assert_eq!(
            attempt_log(&pool, small).await,
            vec![
                (
                    1,
                    TaskOutcome::Failed,
                    Some("Attempt timed out.".to_string())
                ),
                (
                    2,
                    TaskOutcome::Failed,
                    Some("boom\nNot retried: no".to_string())
                )
            ]
        );
        assert_eq!(
            demands(&pool, small).await,
            (vec![], vec![(1, Some(10)), (2, Some(10))])
        );
    }

    #[sqlx::test]
    async fn test_retry_policy_sets_the_resource_demand_of_a_timed_out_attempts_retry(
        pool: PgPool,
    ) {
        // Double the demand of an attempt that timed out without saying it was terminating.
        let policy = RetryPolicy {
            max_retries: 5,
            retry_decision: |failed| {
                let terminating = failed
                    .execution_details
                    .is_some_and(|d| d.get("terminating").is_some());
                let retry = RetryDecision::retry(chrono::Duration::zero());
                if failed.kind == FailureKind::TimedOut && !terminating {
                    retry.with_resource_demand(failed.resource_demand.unwrap_or(1) * 2)
                } else {
                    retry
                }
            },
        };
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = generate_tq_name();
        let task_id = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(8), 1).await;
        let everything = TaskResourceDemandRange::default();
        let mut conn = pool.acquire().await.unwrap();
        let heartbeat = |task: Task, details: serde_json::Value| {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.unwrap();
                let state = check_and_heartbeat_task(&mut conn, &task, 0.5, Some(details))
                    .await
                    .unwrap();
                assert_eq!(state, TaskCheckState::Continue);
            }
        };

        let task = pick_in(&pool, &q, policy, everything).await.unwrap();
        heartbeat(task, serde_json::json!({"rss": 1})).await;
        expire_heartbeat(&pool, task_id).await;
        let task = pick_in(&pool, &q, policy, everything).await.unwrap();
        assert_eq!(
            (task.attempt(), task.task_metadata.resource_demand()),
            (2, Some(16))
        );
        assert_eq!(
            demands(&pool, task_id).await,
            (vec![Some(16)], vec![(1, Some(8))])
        );

        // A terminating attempt keeps its demand.
        heartbeat(task, serde_json::json!({"terminating": true})).await;
        expire_heartbeat(&pool, task_id).await;
        let task = pick_in(&pool, &q, policy, everything).await.unwrap();
        assert_eq!(
            (task.attempt(), task.task_metadata.resource_demand()),
            (3, Some(16))
        );

        // A reported failure sets the demand it is recorded with; a negative one counts as 0.
        record_failure(
            &task,
            5,
            &RetryDecision::retry(chrono::Duration::zero()).with_resource_demand(32),
            "boom",
            &mut conn,
        )
        .await
        .unwrap();
        let task = pick_in(&pool, &q, policy, everything).await.unwrap();
        assert_eq!(task.task_metadata.resource_demand(), Some(32));
        record_failure(
            &task,
            5,
            &RetryDecision::retry(chrono::Duration::zero()).with_resource_demand(-5),
            "boom",
            &mut conn,
        )
        .await
        .unwrap();
        assert_eq!(
            demands(&pool, task_id).await,
            (
                vec![Some(0)],
                vec![(1, Some(8)), (2, Some(16)), (3, Some(16)), (4, Some(32))]
            )
        );

        // Task details show the demand of each attempt.
        let details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            (
                details.task.task_metadata.resource_demand(),
                details
                    .attempts
                    .iter()
                    .map(|a| (a.attempt, a.resource_demand))
                    .collect_vec()
            ),
            (
                Some(0),
                vec![(4, Some(32)), (3, Some(16)), (2, Some(16)), (1, Some(8))]
            )
        );
    }

    /// A queue's [`TaskConfig::retry_decision`] applies to the failures its tasks report.
    #[sqlx::test]
    async fn test_task_config_sets_the_resource_demand_of_a_reported_failures_retry(pool: PgPool) {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Config {}
        impl TaskConfig for Config {
            fn queue_name() -> &'static TaskQueueName {
                &QUEUE_NAME
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(3600)
            }
            fn retry_decision(failed: &FailedAttempt<'_>) -> RetryDecision {
                RetryDecision::retry(chrono::Duration::zero())
                    .with_resource_demand(failed.resource_demand.map(|demand| demand + 1))
            }
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct Empty {}
        impl TaskData for Empty {}
        impl TaskExecutionDetails for Empty {}
        static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| "test-retry-demand".into());
        type Specialized = SpecializedTask<Config, Empty, Empty>;

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let task_id =
            queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, Some(7), 1).await;
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let in_range = TaskResourceDemandRange::builder().at_most(7).build();

        let task = Specialized::pick_new_task_with_resource_demand::<PostgresBackend>(
            state.clone(),
            in_range,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(task.task_metadata.resource_demand(), Some(7));
        task.record_failure::<PostgresBackend>(state.clone(), "boom")
            .await;
        assert_eq!(
            demands(&pool, task_id).await,
            (vec![Some(8)], vec![(1, Some(7))])
        );
        // The retry is out of the range now.
        assert!(
            Specialized::pick_new_task_with_resource_demand::<PostgresBackend>(
                state.clone(),
                in_range
            )
            .await
            .unwrap()
            .is_none()
        );

        // Polling asks for the range before each pick.
        let mut ranges = vec![
            TaskResourceDemandRange::builder().at_most(8).build(),
            in_range,
        ];
        let task = Specialized::poll_for_new_task_with_resource_demand::<PostgresBackend>(
            state,
            || ranges.pop().unwrap(),
            &std::time::Duration::from_millis(1),
            lakekeeper::CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            (task.attempt(), task.task_metadata.resource_demand()),
            (2, Some(8))
        );
        assert_eq!(ranges, vec![]);
    }

    /// A failure recorded with a decision follows it, whatever the queue's
    /// [`TaskConfig::retry_decision`] says; [`TaskConfig::max_retries`] still caps retries.
    #[sqlx::test]
    async fn test_a_failure_recorded_with_a_decision_overrides_the_queues_retry_decision(
        pool: PgPool,
    ) {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Config {}
        impl TaskConfig for Config {
            fn queue_name() -> &'static TaskQueueName {
                &QUEUE_NAME
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(3600)
            }
            fn max_retries() -> i32 {
                2
            }
            // The queue retries at once and keeps the demand.
            fn retry_decision(_: &FailedAttempt<'_>) -> RetryDecision {
                RetryDecision::retry(chrono::Duration::zero())
            }
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct Empty {}
        impl TaskData for Empty {}
        impl TaskExecutionDetails for Empty {}
        static QUEUE_NAME: LazyLock<TaskQueueName> =
            LazyLock::new(|| "test-failure-with-decision".into());
        type Specialized = SpecializedTask<Config, Empty, Empty>;

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let given_up =
            queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, Some(7), 2).await;
        let retried =
            queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, Some(7), 1).await;
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let pick = || async {
            Specialized::pick_new_task::<PostgresBackend>(state.clone())
                .await
                .unwrap()
        };

        // Giving up ends the task as failed with the reason.
        let task = pick().await.unwrap();
        assert_eq!(
            task.id(),
            TaskAttemptId {
                task_id: given_up,
                attempt: 1
            }
        );
        task.record_failure_with_decision::<PostgresBackend>(
            state.clone(),
            "boom",
            RetryDecision::give_up("stopped at its time limit"),
        )
        .await;
        assert_eq!(
            attempt_log(&pool, given_up).await,
            vec![(
                1,
                TaskOutcome::Failed,
                Some("boom\nNot retried: stopped at its time limit".to_string())
            )]
        );
        assert_eq!(demands(&pool, given_up).await, (vec![], vec![(1, Some(7))]));

        // A retry is scheduled with the given backoff and resource demand.
        let task = pick().await.unwrap();
        assert_eq!(
            task.id(),
            TaskAttemptId {
                task_id: retried,
                attempt: 1
            }
        );
        task.record_failure_with_decision::<PostgresBackend>(
            state.clone(),
            "boom",
            RetryDecision::retry(chrono::Duration::minutes(10)).with_resource_demand(9),
        )
        .await;
        assert_eq!(
            attempt_log(&pool, retried).await,
            vec![(1, TaskOutcome::Failed, Some("boom".to_string()))]
        );
        let (status, attempt): (TaskIntermediateStatus, i32) =
            sqlx::query_as("SELECT status, attempt FROM task WHERE task_id = $1")
                .bind(*retried)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((status, attempt), (TaskIntermediateStatus::Scheduled, 1));
        assert_eq!(
            demands(&pool, retried).await,
            (vec![Some(9)], vec![(1, Some(7))])
        );
        let delay = retry_delay(&pool, retried, 1).await;
        assert_eq!(
            (delay.months, delay.days, delay.microseconds),
            (0, 0, 600_000_000)
        );
        assert!(pick().await.is_none());

        // After `max_retries` attempts a retry is ignored and the task fails.
        sqlx::query("UPDATE task SET scheduled_for = now() WHERE task_id = $1")
            .bind(*retried)
            .execute(&pool)
            .await
            .unwrap();
        let task = pick().await.unwrap();
        assert_eq!(
            task.id(),
            TaskAttemptId {
                task_id: retried,
                attempt: 2
            }
        );
        task.record_failure_with_decision::<PostgresBackend>(
            state.clone(),
            "boom",
            RetryDecision::retry(chrono::Duration::zero()),
        )
        .await;
        assert_eq!(
            attempt_log(&pool, retried).await,
            vec![
                (1, TaskOutcome::Failed, Some("boom".to_string())),
                (2, TaskOutcome::Failed, Some("boom".to_string()))
            ]
        );
        assert_eq!(
            demands(&pool, retried).await,
            (vec![], vec![(1, Some(7)), (2, Some(9))])
        );
    }

    #[sqlx::test]
    async fn test_a_negative_resource_demand_counts_as_zero(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = generate_tq_name();
        let task_id = queue_with_demand(&pool, &q, warehouse_id, &project_id, Some(-1), 1).await;
        assert_eq!(demands(&pool, task_id).await, (vec![Some(0)], vec![]));

        // The database refuses a negative one.
        let err = sqlx::query("UPDATE task SET resource_demand = -1 WHERE task_id = $1")
            .bind(*task_id)
            .execute(&pool)
            .await
            .unwrap_err();
        let db_err = err.as_database_error().unwrap();
        assert_eq!(db_err.code().as_deref(), Some("23514"));
        assert_eq!(db_err.constraint(), Some("task_resource_demand_check"));
    }

    #[sqlx::test]
    async fn test_a_retry_backoff_is_kept_between_zero_and_100_years(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (tq_name, task) = queue_and_pick_timing_out_task(&pool, TEST_RETRY_POLICY).await;

        // A negative backoff keeps the task's place in the queue.
        let scheduled_for = "SELECT scheduled_for FROM task WHERE task_id = $1";
        let before: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(scheduled_for)
            .bind(*task.task_id())
            .fetch_one(&pool)
            .await
            .unwrap();
        record_failure(
            &task,
            5,
            &RetryDecision::retry(chrono::Duration::days(-1)),
            "negative",
            &mut conn,
        )
        .await
        .unwrap();
        let after: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(scheduled_for)
            .bind(*task.task_id())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(after, before);

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure(
            &task,
            5,
            &RetryDecision::retry(chrono::Duration::MAX),
            "huge",
            &mut conn,
        )
        .await
        .unwrap();
        let delay = retry_delay(&pool, task.task_id(), 2).await;
        assert_eq!(
            (delay.months, delay.days, delay.microseconds),
            (0, 36_500, 0)
        );
    }

    #[sqlx::test]
    async fn test_multiple_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity_id2,
                entity_name: vec![format!("entity-{}", entity_id2.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task2 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert_eq!(task2.task_id(), id2);
        assert!(matches!(task2.status, TaskIntermediateStatus::Running));
        assert_eq!(task2.attempt(), 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, &tq_name);

        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
        record_success(&task2, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
    }

    #[sqlx::test]
    async fn test_queue_batch(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let task_1_entity_name = vec![String::from("entity-1")];
        let task_2_entity_name = vec![String::from("entity-2")];
        let ids = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        scheduled_for: None,
                        entity: TaskEntity::EntityInWarehouse {
                            entity_id: WarehouseTaskEntityId::Table {
                                table_id: Uuid::now_v7().into(),
                            },
                            entity_name: task_1_entity_name.clone(),
                            warehouse_id,
                        },
                    },

                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id,
                        parent_task_id: None,
                        scheduled_for: None,
                        entity: TaskEntity::EntityInWarehouse {
                            entity_id: WarehouseTaskEntityId::Table {
                                table_id: Uuid::now_v7().into(),
                            },
                            entity_name: task_2_entity_name.clone(),
                            warehouse_id,
                        },
                    },
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let id = ids[0].task_id;
        let id2 = ids[1].task_id;

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.task_metadata.entity_name(), Some(&task_1_entity_name));

        assert_eq!(task2.task_id(), id2);
        assert!(matches!(task2.status, TaskIntermediateStatus::Running));
        assert_eq!(task2.attempt(), 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, &tq_name);
        assert_eq!(task2.task_metadata.entity_name(), Some(&task_2_entity_name));

        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
        record_success(&task2, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
    }

    fn task_schedule_metadata(
        project_id: ArcProjectId,
        warehouse_id: WarehouseId,
        entity_id: WarehouseTaskEntityId,
    ) -> ScheduleTaskMetadata {
        ScheduleTaskMetadata {
            resource_demand: None,
            project_id,
            parent_task_id: None,
            scheduled_for: None,
            entity: TaskEntity::EntityInWarehouse {
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
                warehouse_id,
            },
        }
    }

    #[sqlx::test]
    async fn test_queue_batch_idempotency(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let idp1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let idp2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();
        let _ids = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    task_metadata: task_schedule_metadata(project_id.clone(), warehouse_id, idp1),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_schedule_metadata(project_id.clone(), warehouse_id, idp2),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert!(matches!(task2.status, TaskIntermediateStatus::Running));
        assert_eq!(task2.attempt(), 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, &tq_name);

        // Re-insert the first task with the same idempotency key
        // and a new idempotency key
        // This should create a new task with the new idempotency key
        let new_key = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let ids_second = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    task_metadata: task_schedule_metadata(project_id.clone(), warehouse_id, idp1),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_schedule_metadata(project_id, warehouse_id, new_key),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let new_id = ids_second[0].task_id;

        assert_eq!(ids_second.len(), 1);
        assert_eq!(ids_second[0].entity_id, Some(new_key));

        // move both old tasks to done which will clear them from task table and move them into
        // task_log
        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
        record_success(&task2, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        // pick one new task, one re-inserted task
        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), new_id);
        assert!(matches!(task.status, TaskIntermediateStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert!(
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none(),
            "There should be no tasks left, something is wrong."
        );

        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
    }

    #[sqlx::test]
    async fn test_set_get_task_config(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        assert!(
            get_task_queue_config(
                &mut *conn,
                &TaskQueueConfigFilter::WarehouseId { warehouse_id },
                &tq_name
            )
            .await
            .unwrap()
            .is_none()
        );

        let config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig::from_json(serde_json::json!({"max_attempts": 5})),
            max_seconds_since_last_heartbeat: Some(3600),
        };

        set_task_queue_config(
            &mut conn,
            &tq_name,
            project_id.clone(),
            Some(warehouse_id),
            &config,
        )
        .await
        .unwrap();

        let response = get_task_queue_config(
            &mut *conn,
            &TaskQueueConfigFilter::WarehouseId { warehouse_id },
            &tq_name,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(&response.queue_config.queue_name, &tq_name);
        assert_eq!(
            response.queue_config.config,
            serde_json::json!({"max_attempts": 5})
        );
        assert_eq!(response.max_seconds_since_last_heartbeat, Some(3600));
    }

    #[sqlx::test]
    async fn test_get_task_queue_config_with_project_filter(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let (warehouse_id, project_id) = setup_warehouse(pool).await;

        // Add warehouse-level task config
        let warehouse_task_queue_name = generate_tq_name();
        let warehouse_queue_config = serde_json::json!({"answer": 42});
        let warehouse_config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig::from_json(warehouse_queue_config),
            max_seconds_since_last_heartbeat: None,
        };
        set_task_queue_config(
            &mut conn,
            &warehouse_task_queue_name,
            project_id.clone(),
            Some(warehouse_id),
            &warehouse_config,
        )
        .await
        .unwrap();

        // Add project-level task config
        let project_task_queue_name = generate_tq_name();
        let project_queue_config = serde_json::json!({"message": "Hello, World!"});
        let project_config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig::from_json(project_queue_config.clone()),
            max_seconds_since_last_heartbeat: None,
        };
        set_task_queue_config(
            &mut conn,
            &project_task_queue_name,
            project_id.clone(),
            None,
            &project_config,
        )
        .await
        .unwrap();

        // Retrieve and verify projet-level task queue config
        let response = get_task_queue_config(
            &mut *conn,
            &TaskQueueConfigFilter::ProjectId { project_id },
            &project_task_queue_name,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.queue_config.config, project_queue_config);
    }

    #[sqlx::test]
    async fn test_get_task_queue_config_with_project_filter_and_all_have_same_queue_name(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();

        let (warehouse_id, project_id) = setup_warehouse(pool).await;

        let task_queue_name = generate_tq_name();

        // Add warehouse-level task config
        let warehouse_queue_config = serde_json::json!({"answer": 42});
        let warehouse_config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig::from_json(warehouse_queue_config),
            max_seconds_since_last_heartbeat: None,
        };
        set_task_queue_config(
            &mut conn,
            &task_queue_name,
            project_id.clone(),
            Some(warehouse_id),
            &warehouse_config,
        )
        .await
        .unwrap();

        // Add project-level task config
        let project_queue_config = serde_json::json!({"message": "Hello, World!"});
        let project_config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig::from_json(project_queue_config.clone()),
            max_seconds_since_last_heartbeat: None,
        };
        set_task_queue_config(
            &mut conn,
            &task_queue_name,
            project_id.clone(),
            None,
            &project_config,
        )
        .await
        .unwrap();

        // Retrieve and verify projet-level task queue config
        let response = get_task_queue_config(
            &mut *conn,
            &TaskQueueConfigFilter::ProjectId { project_id },
            &task_queue_name,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.queue_config.config, project_queue_config);
    }

    #[sqlx::test]
    async fn test_set_task_config_yields_a_task_with_config(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        let config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig::from_json(serde_json::json!({"max_attempts": 5})),
            max_seconds_since_last_heartbeat: Some(3600),
        };

        set_task_queue_config(
            &mut conn,
            &tq_name,
            project_id.clone(),
            Some(warehouse_id),
            &config,
        )
        .await
        .unwrap();
        let payload = serde_json::json!("our-task");
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let _task = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(payload.clone()),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.queue_name, tq_name);
        assert_eq!(task.config, Some(serde_json::json!({"max_attempts": 5})));
        assert_eq!(task.data, payload);

        let other_tq_name = generate_tq_name();
        let other_payload = serde_json::json!("other-task");
        let other_entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let _task = queue_task(
            &mut conn,
            &other_tq_name,
            None,
            project_id,
            None,
            Some(other_payload.clone()),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: other_entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(
            &pool,
            &other_tq_name,
            &[],
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(task.queue_name, other_tq_name);
        assert_eq!(task.config, None);
        assert_eq!(task.data, other_payload);
    }

    #[sqlx::test]
    async fn test_record_success_attempt(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"test": "data"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record success for non-existant attempt
        let mut id = picked_task.id();
        id.attempt += 1;
        record_success(id, &mut conn, Some("First success"))
            .await
            .unwrap_err();

        // Record success first time
        record_success(&picked_task, &mut conn, Some("First success"))
            .await
            .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Record success second time - should fail
        record_success(&picked_task, &mut conn, Some("Second success"))
            .await
            .unwrap_err();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist in task_log");

        // Should be marked as successful
        assert!(matches!(task_details.task.status, TaskStatus::Success));
        // Should have no historical attempts since it succeeded on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt(), 1);
    }

    #[sqlx::test]
    async fn test_record_success_conflict_on_duplicate(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record success for the first time
        record_success(
            &picked_task,
            &mut pool.acquire().await.unwrap(),
            Some("first success"),
        )
        .await
        .unwrap();

        // Try to record success again for the same attempt - should return 409 Conflict
        let result = record_success(
            &picked_task,
            &mut pool.acquire().await.unwrap(),
            Some("second success"),
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error.code, 409);
        assert_eq!(error.error.r#type, "TaskAttemptAlreadyCompleted");
        assert!(
            error
                .error
                .message
                .contains("has already been recorded as completed")
        );
    }

    #[sqlx::test]
    async fn test_record_success_not_found_error(pool: PgPool) {
        let task_attempt_id = TaskAttemptId {
            task_id: TaskId::from(Uuid::now_v7()),
            attempt: 1,
        };

        // Try to record success for non-existent task - should return 404 Not Found
        let result = record_success(
            &task_attempt_id,
            &mut pool.acquire().await.unwrap(),
            Some("success message"),
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error.code, 404);
        assert_eq!(error.error.r#type, "TaskNotFound");
        assert!(error.error.message.contains("not found in active tasks"));
    }

    #[sqlx::test]
    async fn test_record_success_normal_case(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record success should work fine
        let result = record_success(
            &picked_task,
            &mut pool.acquire().await.unwrap(),
            Some("success message"),
        )
        .await;
        assert!(result.is_ok());

        // Task should no longer be available for picking
        let no_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(no_task.is_none());
    }

    #[sqlx::test]
    async fn test_record_failure_attempts(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"test": "failure_data"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record failure for non-existant attempt
        let mut id = picked_task.id();
        id.attempt += 1;
        record_failure_without_backoff(id, 2, "First failure", &mut conn)
            .await
            .unwrap_err();

        // Record failure with max_retries=1 (should fail permanently)
        record_failure_without_backoff(&picked_task, 1, "First failure", &mut conn)
            .await
            .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Record failure second time
        record_failure_without_backoff(&picked_task, 1, "Second failure", &mut conn)
            .await
            .unwrap_err();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist in task_log");

        assert_eq!(task_details.attempts.len(), 0); // No retries, so no historical attempts

        // Should be marked as failed
        assert!(matches!(task_details.task.status, TaskStatus::Failed));
        // Should have no historical attempts since it failed permanently on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt(), 1);
    }

    #[sqlx::test]
    async fn test_record_failure_conflict_on_duplicate(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"test": "conflict_test"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record failure first time (should succeed)
        record_failure_without_backoff(&picked_task, 2, "First failure", &mut conn)
            .await
            .unwrap();
        // Get original task details, assert last_heartbeat is reset
        let original_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist in task_log");
        assert_eq!(original_details.task.last_heartbeat_at, None);

        // Task should be rescheduled for retry since max_retries=2 > attempt=1
        let rescheduled_task =
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(rescheduled_task.task_id(), task_id);
        assert_eq!(rescheduled_task.attempt(), 2);

        // Try to record failure again with the original attempt ID
        // This should return a 409 Conflict since the log entry already exists
        let error = record_failure_without_backoff(&picked_task, 2, "Duplicate failure", &mut conn)
            .await
            .unwrap_err();

        // Verify it's a conflict error, not a not found error
        let iceberg_error = error.error;
        assert_eq!(iceberg_error.r#type, "TaskAttemptAlreadyFailed");
        assert!(
            iceberg_error
                .message
                .contains("has already been recorded as failed")
        );

        // Verify the task is still active and can be processed
        let still_active = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(still_active.is_none()); // Already picked up above

        // Test with max_retries_exceeded scenario
        record_failure_without_backoff(&rescheduled_task, 2, "Second failure", &mut conn)
            .await
            .unwrap();

        // Task should now be permanently failed
        let no_more_tasks = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(no_more_tasks.is_none());

        // Try to record failure on the permanently failed task (second attempt)
        // This should also return a 409 Conflict
        let error2 =
            record_failure_without_backoff(&rescheduled_task, 2, "Another duplicate", &mut conn)
                .await
                .unwrap_err();

        let iceberg_error2 = error2.error;
        assert_eq!(iceberg_error2.r#type, "TaskAttemptAlreadyFailed");
        assert!(
            iceberg_error2
                .message
                .contains("has already been recorded as failed")
        );
    }

    #[sqlx::test]
    async fn test_record_failure_not_found_error(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (_warehouse_id, _project_id) = setup_warehouse(pool.clone()).await;
        let non_existent_task_id = TaskAttemptId {
            task_id: TaskId::from(Uuid::now_v7()),
            attempt: 1,
        };

        // Try to record failure for a non-existent task
        let error =
            record_failure_without_backoff(&non_existent_task_id, 2, "Should not work", &mut conn)
                .await
                .unwrap_err();

        // Verify it's a not found error, not a conflict error
        let iceberg_error = error.error;
        assert_eq!(iceberg_error.r#type, "TaskNotFound");
        assert_eq!(iceberg_error.code, 404);
        assert!(iceberg_error.message.contains("not found in active tasks"));
    }

    #[sqlx::test]
    async fn test_cancel_scheduled_tasks_idempotent(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue a task but don't pick it up (leave it scheduled)
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"test": "cancel_data"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        // Cancel the task first time
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            false,
        )
        .await
        .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Cancel the task second time - should be idempotent (no error)
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            false,
        )
        .await
        .unwrap();

        // Cancel the task third time - should still be idempotent
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            false,
        )
        .await
        .unwrap();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist in task_log");

        // Should be marked as cancelled
        assert!(matches!(task_details.task.status, TaskStatus::Cancelled));
        // Should have no historical attempts since it was cancelled while scheduled
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt(), 0);
    }

    #[sqlx::test]
    async fn test_cancel_running_tasks_idempotent_with_force_delete(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue and pick up a task (make it running)
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"test": "running_cancel_data"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);
        assert!(matches!(
            picked_task.status,
            TaskIntermediateStatus::Running
        ));

        // Cancel the running task with force_delete=true first time
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            true, // force_delete_running_tasks
        )
        .await
        .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Cancel the task second time - should be idempotent (no error)
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            true,
        )
        .await
        .unwrap();

        // Cancel the task third time - should still be idempotent
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            true,
        )
        .await
        .unwrap();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist in task_log");

        // Should be marked as cancelled
        assert!(matches!(task_details.task.status, TaskStatus::Cancelled));
        // Should have no historical attempts since it was cancelled while running
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt(), 1);
    }

    #[sqlx::test]
    async fn test_mixed_operations(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"test": "mixed_operations"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record success first
        record_success(&picked_task, &mut conn, Some("Task completed"))
            .await
            .unwrap();

        // Now try to record failure - success was already recorded, so this should error
        record_failure_without_backoff(
            &picked_task,
            5,
            "Attempting to fail completed task",
            &mut conn,
        )
        .await
        .unwrap_err();

        // Try to cancel the already completed task - should be idempotent
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            true,
        )
        .await
        .unwrap();

        // Try to cancel the already completed task - should be idempotent
        cancel_scheduled_tasks(
            &mut conn,
            CancelTasksFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            &[],
            true,
        )
        .await
        .unwrap();

        // Verify task is still marked as successful using get_task_details
        let task_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist in task_log");

        // Should remain marked as successful despite later operations
        assert!(matches!(task_details.task.status, TaskStatus::Success));
        // Should have no historical attempts since it succeeded on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt(), 1);
    }

    #[sqlx::test]
    async fn test_run_tasks_at_with_and_without_scheduled_for(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Test 1: run_tasks_at with None (should run immediately)
        let entity_id1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let future_time = Utc::now() + chrono::Duration::hours(2);

        let task_id1 = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            Some(future_time), // Schedule for 2 hours in the future
            Some(serde_json::json!({"test": "run_now"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity_id1,
                entity_name: vec![format!("entity-{}", entity_id1.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        // Verify task is scheduled for future (not pickable now)
        let task_before = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(task_before.is_none(), "Task should not be pickable yet");

        // Use run_tasks_at with None to run immediately
        reschedule_tasks_for(&mut conn, &[task_id1], None)
            .await
            .unwrap();

        // Now the task should be pickable immediately
        let task_after = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task_after.task_id(), task_id1);

        // Test 2: run_tasks_at with specific time
        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let specific_time = Utc::now() + chrono::Duration::minutes(30);

        let task_id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            Some(future_time), // Schedule for 2 hours in the future
            Some(serde_json::json!({"test": "run_at_specific_time"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: entity_id2,
                entity_name: vec![format!("entity-{}", entity_id2.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        // Use run_tasks_at with specific time
        reschedule_tasks_for(&mut conn, &[task_id2], Some(specific_time))
            .await
            .unwrap();

        // Verify the task's scheduled_for was updated to the specific time
        let details2 = get_task_details(
            task_id2,
            TaskDetailsScope::Warehouse {
                project_id: project_id.clone(),
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task 2 should exist");

        // The scheduled_for should be close to our specific_time (within a few seconds tolerance)
        let time_diff = (details2.task.scheduled_for() - specific_time)
            .num_seconds()
            .abs();
        assert!(
            time_diff <= 5,
            "Task should be scheduled for the specified time (within 5 second tolerance)"
        );

        // Test 3: Verify using get_task_details shows the updated scheduling
        let details1 = get_task_details(
            task_id1,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task 1 should exist");

        assert!(matches!(
            details1.task.status,
            TaskStatus::Running // Task was picked up
        ));
        assert!(matches!(details2.task.status, TaskStatus::Scheduled));

        // Task 2 should be scheduled for the specific time we set
        let task2_scheduled_diff = (details2.task.scheduled_for() - specific_time)
            .num_seconds()
            .abs();
        assert!(
            task2_scheduled_diff <= 10,
            "Task 2 should be scheduled for the specific time"
        );

        // Test 4: Test with tasks that don't exist (should not error)
        let non_existent_task_id = TaskId::from(Uuid::now_v7());
        reschedule_tasks_for(&mut conn, &[non_existent_task_id], None)
            .await
            .unwrap();
    }

    #[sqlx::test]
    async fn test_reschedule_running_task_creates_failed_attempt(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Step 1: Queue a new task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            Some(serde_json::json!({"test": "reschedule_running_task"})),
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        // Step 2: Pick that task (makes it running)
        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);
        assert_eq!(picked_task.status, TaskIntermediateStatus::Running);
        assert_eq!(picked_task.attempt(), 1);

        let original_attempt_id = picked_task.id();

        // Step 3: Reschedule the running task
        let future_time = Utc::now() + chrono::Duration::minutes(30);
        reschedule_tasks_for(&mut conn, &[task_id], Some(future_time))
            .await
            .unwrap();

        // Step 4: Verify task details - should not be running anymore and should have a failed attempt
        let task_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id: project_id.clone(),
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist");

        // Task should still be running, as rescheduling does not affect running tasks
        assert_eq!(task_details.task.status, TaskStatus::Running);

        // Stop task.
        request_tasks_stop(&mut conn, &[task_id]).await.unwrap();
        let future_time = Utc::now() + chrono::Duration::minutes(30);
        reschedule_tasks_for(&mut conn, &[task_id], Some(future_time))
            .await
            .unwrap();

        // Step 4: Verify task details - should not be running anymore and should have a failed attempt
        let task_details = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .expect("Task should exist");

        assert_eq!(task_details.task.status, TaskStatus::Scheduled);

        // Task should be rescheduled for the future time
        let time_diff = (task_details.task.scheduled_for() - future_time)
            .num_seconds()
            .abs();
        assert!(
            time_diff <= 5,
            "Task should be scheduled for the specified time (within 5 second tolerance)"
        );

        // Should have exactly one failed attempt (the original running attempt)
        assert_eq!(
            task_details.attempts.len(),
            1,
            "Should have one failed attempt"
        );
        let failed_attempt = &task_details.attempts[0];
        assert_eq!(failed_attempt.attempt, 1);
        assert_eq!(failed_attempt.status, ApiTaskStatus::Failed);
        assert_eq!(
            failed_attempt.message.as_deref(),
            Some("Task did not stop in time before being rescheduled.")
        );

        // The current task should be on attempt 2 (since the previous attempt was failed)
        assert_eq!(
            task_details.task.attempt(),
            1,
            "Task should still be on attempt 1 since it was rescheduled, not retried"
        );

        // Step 5: check_and_heartbeat on the original attempt should yield Stop
        let heartbeat_result = check_and_heartbeat_task(
            &mut conn,
            original_attempt_id,
            0.5,
            Some(serde_json::json!({"progress": "halfway"})),
        )
        .await
        .unwrap();

        assert_eq!(
            heartbeat_result,
            TaskCheckState::Stop,
            "Original attempt should be Stop since it was rescheduled"
        );

        // Step 6: Verify that the rescheduled task can be picked up again
        let rescheduled_task =
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap();

        // Should not be pickable yet since it's scheduled for the future
        assert!(
            rescheduled_task.is_none(),
            "Task should not be pickable yet since it's scheduled for the future"
        );

        // Step 7: Reschedule to run now and verify it can be picked up
        reschedule_tasks_for(&mut conn, &[task_id], None)
            .await
            .unwrap();

        let now_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(now_task.task_id(), task_id);
        assert_eq!(now_task.status, TaskIntermediateStatus::Running);
        assert_eq!(now_task.attempt(), 2); // Attempt 2 now as the task used to be running
    }

    /// `(status, attempt)` of the task row and `(attempt, status, message)` of its log entries.
    async fn task_and_log_rows(
        pool: &PgPool,
        task_id: TaskId,
    ) -> (Vec<(String, i32)>, Vec<(i32, String, Option<String>)>) {
        let task = sqlx::query_as("SELECT status::text, attempt FROM task WHERE task_id = $1")
            .bind(*task_id)
            .fetch_all(pool)
            .await
            .unwrap();
        let log = sqlx::query_as(
            "SELECT attempt, status::text, message FROM task_log WHERE task_id = $1 ORDER BY attempt",
        )
        .bind(*task_id)
        .fetch_all(pool)
        .await
        .unwrap();
        (task, log)
    }

    /// A stopped attempt that records its success after "run now" keeps the rescheduled task.
    #[sqlx::test]
    async fn test_record_success_in_transaction_keeps_rescheduled_task(pool: PgPool) {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Config {}
        impl TaskConfig for Config {
            fn queue_name() -> &'static TaskQueueName {
                &QUEUE_NAME
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(3600)
            }
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct Empty {}
        impl TaskData for Empty {}
        impl TaskExecutionDetails for Empty {}
        static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| "test-closed-attempt".into());

        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity = TaskEntity::EntityInWarehouse {
            warehouse_id,
            entity_id,
            entity_name: vec!["table".to_string()],
        };
        let task_id = queue_task(&mut conn, &QUEUE_NAME, None, project_id, None, None, entity)
            .await
            .unwrap()
            .unwrap();
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let picked = SpecializedTask::<Config, Empty, Empty>::pick_new_task::<PostgresBackend>(
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            picked.id(),
            TaskAttemptId {
                task_id,
                attempt: 1
            }
        );

        request_tasks_stop(&mut conn, &[task_id]).await.unwrap();
        reschedule_tasks_for(&mut conn, &[task_id], None)
            .await
            .unwrap();
        let expected_rows = (
            vec![("scheduled".to_string(), 1)],
            vec![(
                1,
                "failed".to_string(),
                Some("Task did not stop in time before being rescheduled.".to_string()),
            )],
        );
        assert_eq!(task_and_log_rows(&pool, task_id).await, expected_rows);

        let mut transaction = PostgresTransaction::begin_write(state).await.unwrap();
        picked
            .record_success_in_transaction::<PostgresBackend>(transaction.transaction(), None)
            .await;
        transaction.commit().await.unwrap();

        assert_eq!(task_and_log_rows(&pool, task_id).await, expected_rows);
    }

    async fn setup_project(state: CatalogState) -> lakekeeper::api::Result<ArcProjectId> {
        let project_id = Arc::new(ProjectId::new_random());
        let mut transaction = PostgresTransaction::begin_write(state).await?;
        PostgresBackend::create_project(
            &project_id,
            format!("Project {project_id}"),
            transaction.transaction(),
        )
        .await?;
        transaction.commit().await?;
        Ok(project_id)
    }

    async fn setup_task_config(
        conn: &mut PgConnection,
        queue_name: &TaskQueueName,
        config: Value,
        project_id: ArcProjectId,
        warehouse_id: Option<WarehouseId>,
    ) -> lakekeeper::api::Result<()> {
        set_task_queue_config(
            conn,
            queue_name,
            project_id,
            warehouse_id,
            &SetTaskQueueConfigRequest {
                queue_config: QueueConfig::from_json(config),
                max_seconds_since_last_heartbeat: None,
            },
        )
        .await
    }

    /// Test: Project-level tasks across different projects get picked correctly
    #[sqlx::test]
    async fn test_pick_task_finds_correct_config_for_project_level_tasks_on_different_projects(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        // 1. Set up two projects
        let project_id_1 = setup_project(state.clone()).await.unwrap();
        let project_id_2 = setup_project(state.clone()).await.unwrap();

        // 2. Store expected data per task_id in HashMap (pick order can be random)
        let mut config_expected = HashMap::with_capacity(2);
        config_expected.insert(
            project_id_1.clone(),
            serde_json::json!({"test": "Task in Project 1"}),
        );
        config_expected.insert(
            project_id_2.clone(),
            serde_json::json!({"test": "Task in Project 2"}),
        );

        // 3. Queue one project-level task per project (warehouse_id = None)
        let tq_name = generate_tq_name();

        setup_task_config(
            &mut conn,
            &tq_name,
            config_expected.get(&project_id_1).cloned().unwrap(),
            project_id_1.clone(),
            None,
        )
        .await
        .unwrap();
        setup_task_config(
            &mut conn,
            &tq_name,
            config_expected.get(&project_id_2).cloned().unwrap(),
            project_id_2.clone(),
            None,
        )
        .await
        .unwrap();

        let _ = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id_1.clone(),
            None,
            None,
            TaskEntity::Project,
        )
        .await
        .unwrap()
        .unwrap();
        let _ = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id_2.clone(),
            None,
            None,
            TaskEntity::Project,
        )
        .await
        .unwrap()
        .unwrap();

        // 4. Pick both tasks and verify:
        //    - data matches the project's data
        let mut picked_project_tasks = Vec::new();

        while let Some(picked_task) =
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
        {
            if picked_task.task_metadata.warehouse_id().is_some() {
                continue;
            }
            picked_project_tasks.push(picked_task.clone());
            let project_id = picked_task.task_metadata.project_id.clone();
            let config = picked_task.config.unwrap();
            assert_eq!(config_expected.get(&project_id).cloned().unwrap(), config);
        }
        assert_eq!(picked_project_tasks.len(), 2);
    }

    // Test: Project-level and warehouse-level tasks for same project are isolated
    #[sqlx::test]
    async fn test_pick_task_finds_correct_config_for_project_and_warehouse_level_tasks_in_same_project(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        // 1. Set up one project with one warehouse
        let project_id = setup_project(state).await.unwrap();
        let (_, warehouse) = SetupTestCatalog::builder()
            .project_id(Some(project_id.clone()))
            .pool(pool.clone())
            .authorizer(AllowAllAuthorizer::default())
            .build()
            .setup()
            .await;

        // 2. Store expected data
        let project_level_config = serde_json::json!({"level": "project"});
        let warehouse_level_config = serde_json::json!({"level": "warehouse"});

        // 3. Queue one project-level task and one warehouse-level task
        let tq_name = generate_tq_name();

        setup_task_config(
            &mut conn,
            &tq_name,
            project_level_config.clone(),
            project_id.clone(),
            None,
        )
        .await
        .unwrap();
        setup_task_config(
            &mut conn,
            &tq_name,
            warehouse_level_config.clone(),
            project_id.clone(),
            Some(warehouse.warehouse_id),
        )
        .await
        .unwrap();

        let _ = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id.clone(),
            None,
            None,
            TaskEntity::Project,
        )
        .await
        .unwrap()
        .unwrap();
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: TableId::new_random(),
        };
        let _ = queue_task(
            &mut conn,
            &tq_name,
            None,
            project_id,
            None,
            None,
            TaskEntity::EntityInWarehouse {
                warehouse_id: warehouse.warehouse_id,
                entity_id,
                entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
            },
        )
        .await
        .unwrap()
        .unwrap();

        // 4. Pick both tasks and verify:
        //    - data matches with the corresponding task
        let mut picked_tasks = Vec::new();

        while let Some(picked_task) =
            pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
        {
            picked_tasks.push(picked_task.clone());
            if picked_task.task_metadata.warehouse_id().is_none() {
                let config = picked_task.config.unwrap();
                assert_eq!(project_level_config, config);
            } else {
                let config = picked_task.config.unwrap();
                assert_eq!(warehouse_level_config, config);
            }
        }
        assert_eq!(picked_tasks.len(), 2);
    }

    /// The real poll under a runner: a picked task runs until the worker polls again, and a
    /// closed gate lets the poll pick nothing.
    #[sqlx::test]
    async fn test_poll_counts_picked_tasks_and_stops_at_the_drain_gate(pool: PgPool) {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Config {}
        impl TaskConfig for Config {
            fn queue_name() -> &'static TaskQueueName {
                &QUEUE_NAME
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(3600)
            }
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct Empty {}
        impl TaskData for Empty {}
        impl TaskExecutionDetails for Empty {}
        static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| "test-drain-gate".into());
        type Specialized = SpecializedTask<Config, Empty, Empty>;

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let first = queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, None, 3).await;
        let second =
            queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, None, 2).await;
        let third = queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, None, 1).await;
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let runner = lakekeeper::service::tasks::TestTaskRunner::default();
        let poll_interval = std::time::Duration::from_millis(10);
        let poll = |token: lakekeeper::CancellationToken| {
            Specialized::poll_for_new_task::<PostgresBackend>(state.clone(), &poll_interval, token)
        };

        let worker_runner = runner.clone();
        runner
            .run_as_worker(async move {
                let token = lakekeeper::CancellationToken::new();
                assert_eq!(worker_runner.running_tasks(), 0);
                let task = poll(token.clone()).await.unwrap();
                assert_eq!(task.task_id(), first);
                assert_eq!(worker_runner.running_tasks(), 1);
                // Polling again ends the first task and picks the second.
                let task = poll(token.clone()).await.unwrap();
                assert_eq!(task.task_id(), second);
                assert_eq!(worker_runner.running_tasks(), 1);

                // The running task learns of the drain through its token.
                let drain = lakekeeper::service::tasks::drain_token();
                assert!(!drain.is_cancelled());
                worker_runner.stop_picking();
                assert!(drain.is_cancelled());
                let cancel = token.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    cancel.cancel();
                });
                assert!(poll(token).await.is_none());
                assert_eq!(worker_runner.running_tasks(), 0);
            })
            .await;

        // The gate left the third task due.
        let task = pick_task(&pool, &QUEUE_NAME, &[], chrono::Duration::seconds(3600))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), third);
    }

    /// A worker that picks once and never polls again, like a run-once Job: its task runs until
    /// the worker ends, and a closed gate lets the pick return nothing.
    #[sqlx::test]
    async fn test_a_single_pick_in_a_worker_is_counted_and_gated(pool: PgPool) {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Config {}
        impl TaskConfig for Config {
            fn queue_name() -> &'static TaskQueueName {
                &QUEUE_NAME
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(3600)
            }
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct Empty {}
        impl TaskData for Empty {}
        impl TaskExecutionDetails for Empty {}
        static QUEUE_NAME: LazyLock<TaskQueueName> =
            LazyLock::new(|| "test-drain-single-pick".into());
        type Specialized = SpecializedTask<Config, Empty, Empty>;

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let first = queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, None, 2).await;
        let second =
            queue_with_demand(&pool, &QUEUE_NAME, warehouse_id, &project_id, None, 1).await;
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let runner = lakekeeper::service::tasks::TestTaskRunner::default();

        let worker_runner = runner.clone();
        let worker_state = state.clone();
        runner
            .run_as_worker(async move {
                let task = Specialized::pick_new_task::<PostgresBackend>(worker_state)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(task.task_id(), first);
                assert_eq!(worker_runner.running_tasks(), 1);
            })
            .await;
        assert_eq!(runner.running_tasks(), 0);

        runner.stop_picking();
        runner
            .run_as_worker(async move {
                let task = Specialized::pick_new_task::<PostgresBackend>(state)
                    .await
                    .unwrap();
                assert!(task.is_none());
            })
            .await;
        assert_eq!(runner.running_tasks(), 0);

        // The gate left the second task due.
        let task = pick_task(&pool, &QUEUE_NAME, &[], chrono::Duration::seconds(3600))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), second);
    }

    /// Marks a task as picked, with its last heartbeat `heartbeat_age` ago.
    async fn set_picked(
        pool: &PgPool,
        task_id: TaskId,
        status: &str,
        heartbeat_age: chrono::Duration,
    ) {
        sqlx::query(
            "UPDATE task SET status = $2::task_intermediate_status, attempt = 1, \
             picked_up_at = now(), last_heartbeat_at = now() - $3 WHERE task_id = $1",
        )
        .bind(*task_id)
        .bind(status)
        .bind(heartbeat_age)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn demand_in(
        pool: &PgPool,
        queue_name: &TaskQueueName,
        legacy_queue_names: &[&TaskQueueName],
        range: TaskResourceDemandRange,
    ) -> (i64, i64, i64) {
        let demand = task_queue_demand(
            pool,
            TaskQueueDemandRequest::builder()
                .queue_name(queue_name)
                .legacy_queue_names(legacy_queue_names)
                .default_max_time_since_last_heartbeat(chrono::Duration::minutes(5))
                .resource_demand(range)
                .build(),
        )
        .await
        .unwrap();
        (demand.due, demand.heartbeat_expired, demand.running)
    }

    #[sqlx::test]
    async fn test_task_queue_demand_counts_each_range(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = generate_tq_name();
        let legacy = generate_tq_name();
        let other = generate_tq_name();
        let queue = |name: &TaskQueueName, demand: Option<i64>, minutes_ago: i64| {
            let pool = pool.clone();
            let name = name.clone();
            let project_id = project_id.clone();
            async move {
                queue_with_demand(&pool, &name, warehouse_id, &project_id, demand, minutes_ago)
                    .await
            }
        };

        // Due, at the edges of the ranges (-, 10], (10, 20] and (20, -).
        queue(&q, None, 9).await;
        queue(&q, Some(0), 8).await;
        queue(&q, Some(10), 7).await;
        queue(&q, Some(11), 6).await;
        queue(&q, Some(20), 5).await;
        queue(&q, Some(21), 4).await;
        // Under the queue's earlier name, and in another queue.
        queue(&legacy, None, 3).await;
        queue(&other, Some(5), 3).await;
        // Not due yet.
        queue(&q, Some(5), -10).await;
        // Running with a live heartbeat, asked to stop, and with an expired heartbeat.
        let running = queue(&q, Some(5), 2).await;
        set_picked(&pool, running, "running", chrono::Duration::zero()).await;
        let stopping = queue(&q, Some(15), 2).await;
        set_picked(
            &pool,
            stopping,
            "should-stop",
            chrono::Duration::seconds(10),
        )
        .await;
        let expired = queue(&q, Some(10), 2).await;
        set_picked(&pool, expired, "running", chrono::Duration::hours(1)).await;
        let live = queue(&q, Some(30), 2).await;
        set_picked(&pool, live, "running", chrono::Duration::seconds(30)).await;

        // A second project whose heartbeat timeout is 1 second: the same 30 seconds are expired.
        let second_project = Arc::new(ProjectId::from_db_unchecked(Uuid::now_v7().to_string()));
        sqlx::query("INSERT INTO project (project_id, project_name) VALUES ($1, 'second')")
            .bind(second_project.as_str())
            .execute(&pool)
            .await
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        set_task_queue_config(
            &mut conn,
            &q,
            second_project.clone(),
            None,
            &SetTaskQueueConfigRequest {
                queue_config: QueueConfig::from_json(serde_json::json!({})),
                max_seconds_since_last_heartbeat: Some(1),
            },
        )
        .await
        .unwrap();
        let second = queue_task_batch(
            &mut conn,
            &q,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand: Some(25),
                    project_id: second_project,
                    parent_task_id: None,
                    scheduled_for: None,
                    entity: TaskEntity::Project,
                },
                payload: serde_json::json!({}),
            }],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
        .task_id;
        set_picked(&pool, second, "running", chrono::Duration::seconds(30)).await;

        let small = TaskResourceDemandRange::builder().at_most(10).build();
        let medium = TaskResourceDemandRange::builder()
            .above(10)
            .at_most(20)
            .build();
        let large = TaskResourceDemandRange::builder().above(20).build();
        let empty = TaskResourceDemandRange::builder()
            .above(5)
            .at_most(5)
            .build();
        for (range, legacy_names, expected) in [
            // (due, heartbeat expired, running)
            (small, vec![&legacy], (4, 1, 1)),
            (small, vec![], (3, 1, 1)),
            (medium, vec![&legacy], (2, 0, 1)),
            (large, vec![&legacy], (1, 1, 1)),
            (TaskResourceDemandRange::default(), vec![&legacy], (7, 2, 3)),
            (empty, vec![&legacy], (0, 0, 0)),
        ] {
            assert_eq!(
                demand_in(&pool, &q, &legacy_names, range).await,
                expected,
                "{range:?} {legacy_names:?}"
            );
        }

        // Each pick fails one of the two attempts counted as expired; with no backoff it is due again.
        for expired_left in [1, 0] {
            let picked = pick_task_with_retry_policy(
                &pool,
                &q,
                &[&legacy],
                chrono::Duration::minutes(5),
                TEST_RETRY_POLICY,
                TaskResourceDemandRange::builder().above(1000).build(),
            )
            .await
            .unwrap();
            assert!(picked.is_none());
            assert_eq!(
                demand_in(&pool, &q, &[&legacy], TaskResourceDemandRange::default()).await,
                (7 + 2 - expired_left, expired_left, 3)
            );
        }
    }

    /// Plan of `sql` as a generic plan, with sequential and bitmap scans off.
    async fn generic_plan(pool: &PgPool, sql: &str, args: &str) -> String {
        let mut transaction = pool.begin().await.unwrap();
        for setting in [
            "SET LOCAL plan_cache_mode = force_generic_plan",
            "SET LOCAL enable_seqscan = off",
            "SET LOCAL enable_bitmapscan = off",
        ] {
            sqlx::query(setting)
                .execute(&mut *transaction)
                .await
                .unwrap();
        }
        // `sql` and `args` are literals of the tests below.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("PREPARE demand_plan AS {sql}")))
            .execute(&mut *transaction)
            .await
            .unwrap();
        let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "EXPLAIN EXECUTE demand_plan{args}"
        )))
        .fetch_all(&mut *transaction)
        .await
        .unwrap();
        // A prepared statement outlives the transaction; the pool reuses the connection.
        sqlx::raw_sql("DEALLOCATE demand_plan")
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        rows.join("\n")
    }

    /// The pick and the due count find a resource demand range in `task_queue_name_status_idx`,
    /// also in a generic plan.
    #[sqlx::test]
    async fn test_resource_demand_ranges_are_index_conditions(pool: PgPool) {
        setup_warehouse(pool.clone()).await;
        let range_in_index = "Index Cond: ((queue_name = $1) AND (status = 'scheduled'::task_intermediate_status) \
             AND (scheduled_for <= now()) AND (COALESCE(resource_demand, '0'::bigint) > $2) \
             AND (COALESCE(resource_demand, '0'::bigint) <= $3))";
        // The inner select of the pick.
        let pick = generic_plan(
            &pool,
            "SELECT t.task_id FROM task t \
             WHERE t.queue_name = $1 AND t.scheduled_for <= now() AND t.status = 'scheduled' \
             AND COALESCE(t.resource_demand, 0) > $2 AND COALESCE(t.resource_demand, 0) <= $3 \
             ORDER BY t.scheduled_for, t.task_id FOR UPDATE OF t SKIP LOCKED LIMIT 1",
            "('compaction', 100, 200)",
        )
        .await;
        assert!(
            pick.contains("Index Scan using task_queue_name_status_idx"),
            "{pick}"
        );
        assert!(pick.contains(range_in_index), "{pick}");
        // The index order is the pick's order, also within one `scheduled_for`.
        assert!(!pick.contains("Sort"), "{pick}");
        assert!(!pick.contains("Filter:"), "{pick}");

        // The due branch of `task_queue_demand`.
        let count = generic_plan(
            &pool,
            "SELECT COUNT(*) FROM task t \
             WHERE t.queue_name = ANY($1) AND t.status = 'scheduled' AND t.scheduled_for <= now() \
             AND COALESCE(t.resource_demand, 0) > $2 AND COALESCE(t.resource_demand, 0) <= $3",
            "('{compaction}', 100, 200)",
        )
        .await;
        assert!(
            count.contains("Index Only Scan using task_queue_name_status_idx"),
            "{count}"
        );
        // An index-only scan prints the range on the index's expression column.
        let range_in_index_only = "Index Cond: ((queue_name = ANY ($1)) AND (status = 'scheduled'::task_intermediate_status) \
             AND (scheduled_for <= now()) AND ((COALESCE(resource_demand, '0'::bigint)) > $2) \
             AND ((COALESCE(resource_demand, '0'::bigint)) <= $3))";
        assert!(count.contains(range_in_index_only), "{count}");
        assert!(!count.contains("Filter:"), "{count}");
    }
}
