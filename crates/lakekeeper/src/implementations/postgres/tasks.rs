use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use sqlx::{postgres::types::PgInterval, PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    api::management::v1::warehouse::{
        GetTaskQueueConfigResponse, QueueConfigResponse, SetTaskQueueConfigRequest,
    },
    implementations::postgres::dbutils::DBErrorHandler,
    service::task_queue::{Task, TaskAttemptId, TaskFilter, TaskQueueName, TaskStatus},
    WarehouseId,
};

mod get_task_details;
mod list_tasks;
mod resolve_tasks;
pub(crate) use get_task_details::get_task_details;
pub(crate) use list_tasks::list_tasks;
pub(crate) use resolve_tasks::resolve_tasks;

#[derive(Debug)]
pub(crate) struct InsertResult {
    pub task_id: TaskId,
    #[cfg(test)]
    pub entity_id: EntityId,
}

#[derive(Debug, sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "entity_type", rename_all = "kebab-case")]
enum EntityType {
    Tabular,
}

impl From<EntityId> for EntityType {
    fn from(entity_id: EntityId) -> Self {
        match entity_id {
            EntityId::Tabular(_) => Self::Tabular,
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn queue_task_batch(
    conn: &mut PgConnection,
    queue_name: &TaskQueueName,
    tasks: Vec<TaskInput>,
) -> Result<Vec<InsertResult>, IcebergErrorResponse> {
    let queue_name = queue_name.as_str();
    let mut task_ids = Vec::with_capacity(tasks.len());
    let mut parent_task_ids = Vec::with_capacity(tasks.len());
    let mut warehouse_idents = Vec::with_capacity(tasks.len());
    let mut scheduled_fors = Vec::with_capacity(tasks.len());
    let mut entity_ids = Vec::with_capacity(tasks.len());
    let mut entity_types = Vec::with_capacity(tasks.len());
    let mut payloads = Vec::with_capacity(tasks.len());
    for TaskInput {
        task_metadata:
            TaskMetadata {
                parent_task_id,
                warehouse_id,
                schedule_for,
                entity_id,
            },
        payload,
    } in tasks
    {
        task_ids.push(Uuid::now_v7());
        parent_task_ids.push(parent_task_id.as_deref().copied());
        warehouse_idents.push(*warehouse_id);
        scheduled_fors.push(schedule_for);
        payloads.push(payload);
        entity_types.push(EntityType::from(entity_id));
        entity_ids.push(entity_id.to_uuid());
    }

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
                $2 as queue_name
            FROM unnest(
                $1::uuid[],
                $3::uuid[],
                $4::uuid[],
                $5::timestamptz[],
                $6::jsonb[],
                $7::uuid[],
                $8::entity_type[]
            ) AS t(
                task_id,
                parent_task_id,
                warehouse_id,
                scheduled_for,
                payload,
                entity_ids,
                entity_types
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
                entity_type)
        SELECT
            i.task_id,
            i.queue_name,
            $9,
            i.parent_task_id,
            i.warehouse_id,
            coalesce(i.scheduled_for, now()),
            i.payload,
            i.entity_ids,
            i.entity_types
        FROM input_rows i
        ON CONFLICT (warehouse_id, entity_type, entity_id, queue_name) DO NOTHING
        RETURNING task_id, queue_name, entity_id, entity_type as "entity_type: EntityType""#,
        &task_ids,
        queue_name,
        &parent_task_ids as _,
        &warehouse_idents,
        &scheduled_fors
            .iter()
            .map(|t| t.as_ref())
            .collect::<Vec<_>>() as _,
        &payloads,
        &entity_ids,
        &entity_types as _,
        TaskStatus::Scheduled as _,
    )
    .fetch_all(conn)
    .await
    .map(|records| {
        records
            .into_iter()
            .map(|record| InsertResult {
                task_id: record.task_id.into(),
                // queue_name: record.queue_name,
                #[cfg(test)]
                entity_id: match record.entity_type {
                    EntityType::Tabular => EntityId::Tabular(record.entity_id),
                },
            })
            .collect_vec()
    })
    .map_err(|e| e.into_error_model("failed queueing tasks"))?)
}

/// `default_max_time_since_last_heartbeat` is only used if no task configuration is found
/// in the DB for the given `queue_name`, typically before a user has configured the value explicitly.
#[allow(clippy::too_many_lines)]
pub(crate) async fn pick_task(
    pool: &PgPool,
    queue_name: &TaskQueueName,
    default_max_time_since_last_heartbeat: chrono::Duration,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let queue_name = queue_name.as_str();
    let max_time_since_last_heartbeat = PgInterval {
        months: 0,
        days: 0,
        microseconds: default_max_time_since_last_heartbeat
            .num_microseconds()
            .ok_or(ErrorModel::internal(
                "Could not convert max_age into microseconds. Integer overflow, this is a bug.",
                "InternalError",
                None,
            ))?,
    };
    let x = sqlx::query!(
        r#"
        WITH picked_task AS (
            SELECT t.*, config
            FROM task t
            LEFT JOIN task_config tc
                ON tc.queue_name = t.queue_name
                    AND tc.warehouse_id = t.warehouse_id
            WHERE (t.queue_name = $1 AND scheduled_for <= now()) 
                AND (
                    (status = 'scheduled') OR 
                    (status != 'scheduled' AND (now() - last_heartbeat_at) > COALESCE(tc.max_time_since_last_heartbeat, $2))
                )
            -- FOR UPDATE locks the row we select here, SKIP LOCKED makes us not wait for rows other
            -- transactions locked
            FOR UPDATE OF t SKIP LOCKED
            LIMIT 1
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
                message,
                attempt,
                started_at,
                duration,
                progress,
                execution_details,
                attempt_scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                task_created_at
            )
            SELECT task_id,
                    warehouse_id,
                    queue_name,
                    task_data,
                    'failed',
                    entity_id,
                    entity_type,
                    'Attempt timed out.',
                    attempt,
                    picked_up_at,
                    now() - picked_up_at,
                    progress,
                    execution_details,
                    scheduled_for,
                    last_heartbeat_at,
                    parent_task_id,
                    created_at
            FROM picked_task p
            WHERE p.status != 'scheduled'
            ON CONFLICT (task_id, attempt) DO NOTHING
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
            task.entity_type as "entity_type: EntityType",
            task.warehouse_id,
            task.task_data,
            task.scheduled_for,
            task.status as "status: TaskStatus",
            task.picked_up_at,
            task.attempt,
            task.parent_task_id,
            task.queue_name,
            (select config from picked_task)
            "#,
        queue_name,
        max_time_since_last_heartbeat
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to pick a task for '{queue_name}'");
        e.into_error_model(format!("Failed to pick a '{queue_name}' task"))
    })?;

    if let Some(task) = x {
        tracing::trace!("Picked up task: {:?}", task);
        return Ok(Some(Task {
            task_metadata: TaskMetadata {
                warehouse_id: task.warehouse_id.into(),
                entity_id: match task.entity_type {
                    EntityType::Tabular => EntityId::Tabular(task.entity_id),
                },
                parent_task_id: task.parent_task_id.map(TaskId::from),
                schedule_for: Some(task.scheduled_for),
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

pub(crate) async fn record_success(
    id: impl AsRef<TaskAttemptId>,
    pool: &mut PgConnection,
    message: Option<&str>,
) -> Result<(), IcebergErrorResponse> {
    let TaskAttemptId { task_id, attempt } = *id.as_ref();
    let inserted = sqlx::query!(
        r#"
        WITH moved AS (
            DELETE FROM task WHERE task_id = $1 AND attempt = $3 RETURNING *
        )
        INSERT INTO task_log(
            task_id,
            warehouse_id,
            queue_name,
            task_data,
            status,
            entity_id,
            entity_type,
            message,
            attempt,
            started_at,
            duration,
            progress,
            execution_details,
            attempt_scheduled_for,
            last_heartbeat_at,
            parent_task_id,
            task_created_at
        )
        SELECT task_id,
                warehouse_id,
                queue_name,
                task_data,
                'success',
                entity_id,
                entity_type,
                $2,
                attempt,
                picked_up_at,
                now() - picked_up_at,
                1.0000,
                execution_details,
                scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                created_at
        FROM moved
        ON CONFLICT (task_id, attempt) DO NOTHING
        RETURNING task_id
        "#,
        *task_id,
        message,
        attempt
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| e.into_error_model("Error recording task success."))?;

    if inserted.is_none() {
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

#[allow(clippy::too_many_lines)]
pub(crate) async fn record_failure(
    id: impl AsRef<TaskAttemptId>,
    max_retries: i32,
    details: &str,
    conn: &mut PgConnection,
) -> Result<(), IcebergErrorResponse> {
    let TaskAttemptId { task_id, attempt } = *id.as_ref();
    let max_retries_exceeded = attempt >= max_retries;
    let found = if max_retries_exceeded {
        sqlx::query_scalar!(
            r#"
            WITH moved AS (
                DELETE FROM task WHERE task_id = $1 AND attempt = $2 RETURNING *
            )
            INSERT INTO task_log(
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_type,
                message,
                attempt,
                started_at,
                duration,
                progress,
                execution_details,
                attempt_scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                task_created_at
            )
            SELECT
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                'failed',
                entity_id,
                entity_type,
                $3,
                attempt,
                picked_up_at,
                now() - picked_up_at,
                progress,
                execution_details,
                scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                created_at
            FROM moved
            ON CONFLICT (task_id, attempt) DO NOTHING
            RETURNING task_id
            "#,
            *task_id,
            attempt,
            details
        )
        .fetch_optional(conn)
        .await
        .map_err(|e| e.into_error_model("Error recording task attempt failure."))
    } else {
        sqlx::query_scalar!(
            r#"
            WITH locked AS (
                SELECT * FROM task WHERE task_id = $1 AND attempt = $3 FOR UPDATE
            ),
            ins as (
                INSERT INTO task_log(
                    task_id,
                    warehouse_id,
                    queue_name,
                    task_data,
                    status,
                    entity_id,
                    entity_type,
                    message,
                    attempt,
                    started_at,
                    duration,
                    progress,
                    execution_details,
                    attempt_scheduled_for,
                    last_heartbeat_at,
                    parent_task_id,
                    task_created_at
                )
                SELECT 
                    task_id,
                    warehouse_id,
                    queue_name,
                    task_data,
                    'failed',
                    entity_id,
                    entity_type,
                    $2,
                    attempt,
                    picked_up_at,
                    now() - picked_up_at,
                    progress,
                    execution_details,
                    scheduled_for,
                    last_heartbeat_at,
                    parent_task_id,
                    created_at
                FROM locked
                ON CONFLICT (task_id, attempt) DO NOTHING
            )
            UPDATE task t
            SET 
                status = 'scheduled',
                progress = 0.0,
                picked_up_at = NULL,
                execution_details = NULL
            FROM locked
                WHERE t.task_id = locked.task_id AND t.attempt = locked.attempt
            RETURNING t.task_id
            "#,
            *task_id,
            details,
            attempt
        )
        .fetch_optional(conn)
        .await
        .map_err(|e| e.into_error_model("Error marking task as failed."))
    }
    .map_err(|e| e.append_detail(format!("Task ID: {task_id}, Attempt: {attempt}")))?;

    if found.is_none() {
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

pub(crate) async fn get_task_queue_config(
    transaction: &mut PgConnection,
    warehouse_id: WarehouseId,
    queue_name: &TaskQueueName,
) -> crate::api::Result<Option<GetTaskQueueConfigResponse>> {
    let result = sqlx::query!(
        r#"
        SELECT config, max_time_since_last_heartbeat
        FROM task_config
        WHERE warehouse_id = $1 AND queue_name = $2
        "#,
        *warehouse_id,
        queue_name.as_str()
    )
    .fetch_optional(transaction)
    .await
    .map_err(|e| {
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
    warehouse_id: WarehouseId,
    config: SetTaskQueueConfigRequest,
) -> crate::api::Result<()> {
    let serialized = config.queue_config.0;
    let max_time_since_last_heartbeat =
        if let Some(max_seconds_since_last_heartbeat) = config.max_seconds_since_last_heartbeat {
            Some(PgInterval {
                months: 0,
                days: 0,
                microseconds: chrono::Duration::seconds(max_seconds_since_last_heartbeat)
                    .num_microseconds()
                    .ok_or(ErrorModel::internal(
                    "Could not convert max_age into microseconds. Integer overflow, this is a bug.",
                    "InternalError",
                    None,
                ))?,
            })
        } else {
            None
        };
    sqlx::query!(
        r#"
        INSERT INTO task_config (queue_name, warehouse_id, config, max_time_since_last_heartbeat)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (queue_name, warehouse_id) DO UPDATE
        SET config = $3, max_time_since_last_heartbeat = COALESCE($4, task_config.max_time_since_last_heartbeat )
        "#,
        queue_name.as_str(),
        *warehouse_id,
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
) -> crate::api::Result<()> {
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
) -> crate::api::Result<()> {
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
                message,
                attempt,
                started_at,
                duration,
                progress,
                execution_details,
                attempt_scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                task_created_at
            )
            SELECT
                task_id,
                warehouse_id,
                queue_name,
                task_data,
                'failed',
                entity_id,
                entity_type,
                'Task did not stop in time before being rescheduled.',
                attempt,
                picked_up_at,
                now() - picked_up_at,
                progress,
                execution_details,
                scheduled_for,
                last_heartbeat_at,
                parent_task_id,
                created_at
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
) -> crate::api::Result<TaskCheckState> {
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
        SELECT status as "status: TaskStatus" FROM heartbeat"#,
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
        TaskStatus::ShouldStop | TaskStatus::Scheduled => TaskCheckState::Stop,
        TaskStatus::Running => TaskCheckState::Continue,
    }))
}

use crate::service::task_queue::{
    EntityId, TaskCheckState, TaskId, TaskInput, TaskMetadata, TaskOutcome,
};

/// Cancel scheduled tasks.
/// If `force_delete_running_tasks` is true, "running" and "should-stop" tasks will also be cancelled.
/// If `queue_name` is `None`, tasks in all queues will be cancelled.
#[allow(clippy::too_many_lines)]
pub(crate) async fn cancel_scheduled_tasks(
    connection: &mut PgConnection,
    filter: TaskFilter,
    queue_name: Option<&TaskQueueName>,
    force_delete_running_tasks: bool,
) -> crate::api::Result<()> {
    let queue_name_is_none = queue_name.is_none();
    let queue_name = queue_name.map(crate::service::task_queue::TaskQueueName::as_str);
    let queue_name = queue_name.unwrap_or("");
    match filter {
        TaskFilter::WarehouseId(warehouse_id) => {
            sqlx::query!(
                r#"
                WITH deleted as (
                    DELETE FROM task
                    WHERE (status = $3 OR $5) AND warehouse_id = $1 AND (queue_name = $2 OR $6)
                    RETURNING *
                )
                INSERT INTO task_log(task_id,
                                        warehouse_id,
                                        queue_name,
                                        task_data,
                                        entity_id,
                                        entity_type,
                                        status,
                                        attempt,
                                        started_at,
                                        duration,
                                        progress,
                                        execution_details,
                                        attempt_scheduled_for,
                                        last_heartbeat_at,
                                        parent_task_id,
                                        task_created_at)
                SELECT task_id,
                        warehouse_id,
                        queue_name,
                        task_data,
                        entity_id,
                        entity_type,
                        $4,
                        attempt,
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
                        created_at
                FROM deleted
                ON CONFLICT (task_id, attempt) DO NOTHING
                "#,
                *warehouse_id,
                queue_name,
                TaskStatus::Scheduled as _,
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
        }
        TaskFilter::TaskIds(task_ids) => {
            sqlx::query!(
                r#"
                WITH deleted as (
                    DELETE FROM task
                    WHERE (status = $3 OR $6) AND task_id = ANY($1) AND (queue_name = $4 OR $5)
                    RETURNING *
                )
                INSERT INTO task_log(task_id,
                                        warehouse_id,
                                        queue_name,
                                        task_data,
                                        status,
                                        entity_id,
                                        entity_type,
                                        attempt,
                                        started_at,
                                        duration,
                                        progress,
                                        execution_details,
                                        attempt_scheduled_for,
                                        last_heartbeat_at,
                                        parent_task_id,
                                        task_created_at)
                SELECT task_id,
                        warehouse_id,
                        queue_name,
                        task_data,
                        $2,
                        entity_id,
                        entity_type,
                        attempt,
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
                        created_at
                FROM deleted
                ON CONFLICT (task_id, attempt) DO NOTHING
                "#,
                &task_ids.iter().map(|s| **s).collect_vec(),
                TaskOutcome::Cancelled as _,
                TaskStatus::Scheduled as _,
                queue_name,
                queue_name_is_none,
                force_delete_running_tasks
            )
            .execute(connection)
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to cancel Tasks for task_ids {task_ids:?}");
                e.into_error_model("Failed to cancel Tasks for specified ids")
            })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, Utc};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::management::v1::{
            tasks::TaskStatus as ApiTaskStatus,
            warehouse::{QueueConfig, TabularDeleteProfile},
        },
        service::{
            authz::AllowAllAuthorizer,
            task_queue::{
                EntityId, TaskId, TaskInput, TaskStatus, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
            },
        },
        WarehouseId,
    };

    async fn queue_task(
        conn: &mut PgConnection,
        queue_name: &TaskQueueName,
        parent_task_id: Option<TaskId>,
        entity_id: EntityId,
        warehouse_id: WarehouseId,
        schedule_for: Option<DateTime<Utc>>,
        payload: Option<serde_json::Value>,
    ) -> Result<Option<TaskId>, IcebergErrorResponse> {
        Ok(queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: TaskMetadata {
                    warehouse_id,
                    parent_task_id,
                    entity_id,
                    schedule_for,
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
        let warehouse_id = setup_warehouse(pool.clone()).await;

        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            None
        )
        .await
        .unwrap()
        .is_none());

        let id3 = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap();

        assert_ne!(id, id3);
    }

    pub(crate) async fn setup_warehouse(pool: PgPool) -> WarehouseId {
        let prof = crate::tests::memory_io_profile();
        let (_, wh) = crate::tests::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;
        wh.warehouse_id
    }

    pub(crate) async fn setup_two_warehouses(pool: PgPool) -> (WarehouseId, WarehouseId) {
        let prof = crate::tests::memory_io_profile();
        let (_, wh) = crate::tests::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            2,
        )
        .await;
        (wh.warehouse_id, wh.additional_warehouses[0].0)
    }

    #[sqlx::test]
    async fn test_failed_tasks_retry_attempts(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_failure(&task, 5, "test details", &mut pool.acquire().await.unwrap())
            .await
            .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_failure(&task, 2, "test", &mut pool.acquire().await.unwrap())
            .await
            .unwrap();

        assert!(
            pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[sqlx::test]
    async fn test_success_tasks_are_not_polled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let tq_name = generate_tq_name();

        let warehouse_id = setup_warehouse(pool.clone()).await;
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        assert!(
            pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[sqlx::test]
    async fn test_success_tasks_can_be_reinserted_with_new_id(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let tq_name = generate_tq_name();

        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
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
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"b": "b"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_cancelled_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![id]),
            Some(&tq_name),
            false,
        )
        .await
        .unwrap();

        let id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"b": "b"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id2);
        assert!(matches!(task.status, TaskStatus::Running));
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

        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"a": "a"}));

        record_failure(&task, 1, "failed", &mut pool.acquire().await.unwrap())
            .await
            .unwrap();

        let id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"b": "b"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), id2);
        assert!(matches!(task.status, TaskStatus::Running));
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
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let scheduled_for = Utc::now() + chrono::Duration::milliseconds(500);
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            Some(scheduled_for),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(
            pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
    }

    #[sqlx::test]
    async fn test_stale_tasks_are_picked_up_again(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, &tq_name, chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
    }

    #[sqlx::test]
    async fn test_multiple_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let id = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        let id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert_eq!(task2.task_id(), id2);
        assert!(matches!(task2.status, TaskStatus::Running));
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
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let ids = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    task_metadata: TaskMetadata {
                        entity_id: EntityId::Tabular(Uuid::now_v7()),
                        warehouse_id,
                        parent_task_id: None,
                        schedule_for: None,
                    },

                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: TaskMetadata {
                        entity_id: EntityId::Tabular(Uuid::now_v7()),
                        warehouse_id,
                        parent_task_id: None,
                        schedule_for: None,
                    },
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let id = ids[0].task_id;
        let id2 = ids[1].task_id;

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id(), id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert_eq!(task2.task_id(), id2);
        assert!(matches!(task2.status, TaskStatus::Running));
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

    fn task_metadata(warehouse_id: WarehouseId, entity_id: EntityId) -> TaskMetadata {
        TaskMetadata {
            entity_id,
            warehouse_id,
            parent_task_id: None,
            schedule_for: None,
        }
    }

    #[sqlx::test]
    async fn test_queue_batch_idempotency(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let idp1 = EntityId::Tabular(Uuid::now_v7());
        let idp2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();
        let _ids = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, idp1),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, idp2),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt(), 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, &tq_name);

        // Re-insert the first task with the same idempotency key
        // and a new idempotency key
        // This should create a new task with the new idempotency key
        let new_key = EntityId::Tabular(Uuid::now_v7());
        let ids_second = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, idp1),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, new_key),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let new_id = ids_second[0].task_id;

        assert_eq!(ids_second.len(), 1);
        assert_eq!(ids_second[0].entity_id, new_key);

        // move both old tasks to done which will clear them from task table and move them into
        // task_log
        record_success(&task, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
        record_success(&task2, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        // pick one new task, one re-inserted task
        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), new_id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt(), 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert!(
            pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        assert!(get_task_queue_config(&mut conn, warehouse_id, &tq_name)
            .await
            .unwrap()
            .is_none());

        let config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig(serde_json::json!({"max_attempts": 5})),
            max_seconds_since_last_heartbeat: Some(3600),
        };

        set_task_queue_config(&mut conn, &tq_name, warehouse_id, config)
            .await
            .unwrap();

        let response = get_task_queue_config(&mut conn, warehouse_id, &tq_name)
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
    async fn test_set_task_config_yields_a_task_with_config(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        let config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig(serde_json::json!({"max_attempts": 5})),
            max_seconds_since_last_heartbeat: Some(3600),
        };

        set_task_queue_config(&mut conn, &tq_name, warehouse_id, config)
            .await
            .unwrap();
        let payload = serde_json::json!("our-task");
        let _task = queue_task(
            &mut conn,
            &tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            Some(payload.clone()),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.queue_name, tq_name);
        assert_eq!(task.config, Some(serde_json::json!({"max_attempts": 5})));
        assert_eq!(task.data, payload);

        let other_tq_name = generate_tq_name();
        let other_payload = serde_json::json!("other-task");
        let _task = queue_task(
            &mut conn,
            &other_tq_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            Some(other_payload.clone()),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, &other_tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = EntityId::Tabular(Uuid::now_v7());

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "data"})),
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
        let active_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Record success second time - should fail
        record_success(&picked_task, &mut conn, Some("Second success"))
            .await
            .unwrap_err();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &pool)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should be marked as successful
        assert!(matches!(task_details.task.status, ApiTaskStatus::Success));
        // Should have no historical attempts since it succeeded on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_record_failure_attempts(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = EntityId::Tabular(Uuid::now_v7());

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "failure_data"})),
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record failure for non-existant attempt
        let mut id = picked_task.id();
        id.attempt += 1;
        record_failure(id, 2, "First failure", &mut conn)
            .await
            .unwrap_err();

        // Record failure with max_retries=1 (should fail permanently)
        record_failure(&picked_task, 1, "First failure", &mut conn)
            .await
            .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Record failure second time
        record_failure(&picked_task, 1, "Second failure", &mut conn)
            .await
            .unwrap_err();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &pool)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        assert_eq!(task_details.attempts.len(), 0); // No retries, so no historical attempts

        // Should be marked as failed
        assert!(matches!(task_details.task.status, ApiTaskStatus::Failed));
        // Should have no historical attempts since it failed permanently on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_cancel_scheduled_tasks_idempotent(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = EntityId::Tabular(Uuid::now_v7());

        // Queue a task but don't pick it up (leave it scheduled)
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "cancel_data"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Verify task is available
        let scheduled_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(scheduled_task.task_id(), task_id);

        // Put the task back to scheduled state for cancellation test
        sqlx::query!(
            "UPDATE task SET status = $1, picked_up_at = NULL WHERE task_id = $2",
            TaskStatus::Scheduled as _,
            *task_id
        )
        .execute(&mut conn as &mut PgConnection)
        .await
        .unwrap();

        // Cancel the task first time
        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            false,
        )
        .await
        .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Cancel the task second time - should be idempotent (no error)
        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            false,
        )
        .await
        .unwrap();

        // Cancel the task third time - should still be idempotent
        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            false,
        )
        .await
        .unwrap();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &pool)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should be marked as cancelled
        assert!(matches!(task_details.task.status, ApiTaskStatus::Cancelled));
        // Should have no historical attempts since it was cancelled while scheduled
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_cancel_running_tasks_idempotent_with_force_delete(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = EntityId::Tabular(Uuid::now_v7());

        // Queue and pick up a task (make it running)
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "running_cancel_data"})),
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);
        assert!(matches!(picked_task.status, TaskStatus::Running));

        // Cancel the running task with force_delete=true first time
        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            true, // force_delete_running_tasks
        )
        .await
        .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Cancel the task second time - should be idempotent (no error)
        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            true,
        )
        .await
        .unwrap();

        // Cancel the task third time - should still be idempotent
        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            true,
        )
        .await
        .unwrap();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &pool)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should be marked as cancelled
        assert!(matches!(task_details.task.status, ApiTaskStatus::Cancelled));
        // Should have no historical attempts since it was cancelled while running
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_mixed_operations(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = EntityId::Tabular(Uuid::now_v7());

        // Queue and pick up a task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "mixed_operations"})),
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);

        // Record success first
        record_success(&picked_task, &mut conn, Some("Task completed"))
            .await
            .unwrap();

        // Now try to record failure - success was already recorded, so this should error
        record_failure(
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
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            true,
        )
        .await
        .unwrap();

        // Try to cancel the already completed task - should be idempotent
        cancel_scheduled_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![task_id]),
            Some(&tq_name),
            true,
        )
        .await
        .unwrap();

        // Verify task is still marked as successful using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &pool)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should remain marked as successful despite later operations
        assert!(matches!(task_details.task.status, ApiTaskStatus::Success));
        // Should have no historical attempts since it succeeded on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_run_tasks_at_with_and_without_scheduled_for(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Test 1: run_tasks_at with None (should run immediately)
        let entity_id1 = EntityId::Tabular(Uuid::now_v7());
        let future_time = Utc::now() + chrono::Duration::hours(2);

        let task_id1 = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id1,
            warehouse_id,
            Some(future_time), // Schedule for 2 hours in the future
            Some(serde_json::json!({"test": "run_now"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Verify task is scheduled for future (not pickable now)
        let task_before = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(task_before.is_none(), "Task should not be pickable yet");

        // Use run_tasks_at with None to run immediately
        reschedule_tasks_for(&mut conn, &[task_id1], None)
            .await
            .unwrap();

        // Now the task should be pickable immediately
        let task_after = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task_after.task_id(), task_id1);

        // Test 2: run_tasks_at with specific time
        let entity_id2 = EntityId::Tabular(Uuid::now_v7());
        let specific_time = Utc::now() + chrono::Duration::minutes(30);

        let task_id2 = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id2,
            warehouse_id,
            Some(future_time), // Schedule for 2 hours in the future
            Some(serde_json::json!({"test": "run_at_specific_time"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Use run_tasks_at with specific time
        reschedule_tasks_for(&mut conn, &[task_id2], Some(specific_time))
            .await
            .unwrap();

        // Verify the task's scheduled_for was updated to the specific time
        let details2 = get_task_details(warehouse_id, task_id2, 10, &pool)
            .await
            .unwrap()
            .expect("Task 2 should exist");

        // The scheduled_for should be close to our specific_time (within a few seconds tolerance)
        let time_diff = (details2.task.scheduled_for - specific_time)
            .num_seconds()
            .abs();
        assert!(
            time_diff <= 5,
            "Task should be scheduled for the specified time (within 5 second tolerance)"
        );

        // Test 3: Verify using get_task_details shows the updated scheduling
        let details1 = get_task_details(warehouse_id, task_id1, 10, &pool)
            .await
            .unwrap()
            .expect("Task 1 should exist");

        assert!(matches!(
            details1.task.status,
            ApiTaskStatus::Running // Task was picked up
        ));
        assert!(matches!(details2.task.status, ApiTaskStatus::Scheduled));

        // Task 2 should be scheduled for the specific time we set
        let task2_scheduled_diff = (details2.task.scheduled_for - specific_time)
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
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let entity_id = EntityId::Tabular(Uuid::now_v7());

        // Step 1: Queue a new task
        let task_id = queue_task(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "reschedule_running_task"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Step 2: Pick that task (makes it running)
        let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id(), task_id);
        assert_eq!(picked_task.status, TaskStatus::Running);
        assert_eq!(picked_task.attempt(), 1);

        let original_attempt_id = picked_task.id();

        // Step 3: Reschedule the running task
        let future_time = Utc::now() + chrono::Duration::minutes(30);
        reschedule_tasks_for(&mut conn, &[task_id], Some(future_time))
            .await
            .unwrap();

        // Step 4: Verify task details - should not be running anymore and should have a failed attempt
        let task_details = get_task_details(warehouse_id, task_id, 10, &pool)
            .await
            .unwrap()
            .expect("Task should exist");

        // Task should still be running, as rescheduling does not affect running tasks
        assert_eq!(task_details.task.status, ApiTaskStatus::Running);

        // Stop task.
        request_tasks_stop(&mut conn, &[task_id]).await.unwrap();
        let future_time = Utc::now() + chrono::Duration::minutes(30);
        reschedule_tasks_for(&mut conn, &[task_id], Some(future_time))
            .await
            .unwrap();

        // Step 4: Verify task details - should not be running anymore and should have a failed attempt
        let task_details = get_task_details(warehouse_id, task_id, 10, &pool)
            .await
            .unwrap()
            .expect("Task should exist");

        assert_eq!(task_details.task.status, ApiTaskStatus::Scheduled);

        // Task should be rescheduled for the future time
        let time_diff = (task_details.task.scheduled_for - future_time)
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
            task_details.task.attempt, 1,
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
        let rescheduled_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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

        let now_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(now_task.task_id(), task_id);
        assert_eq!(now_task.status, TaskStatus::Running);
        assert_eq!(now_task.attempt(), 2); // Attempt 2 now as the task used to be running
    }
}
