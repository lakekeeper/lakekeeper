use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use sqlx::{postgres::types::PgInterval, PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    api::management::v1::warehouse::{
        GetTaskQueueConfigResponse, QueueConfigResponse, SetTaskQueueConfigRequest,
    },
    implementations::postgres::dbutils::DBErrorHandler,
    service::task_queue::{Task, TaskFilter, TaskQueueName, TaskStatus},
    WarehouseId,
};

mod list_pagination_token;

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
                unnest($1::uuid[]) as task_id,
                $2 as queue_name,
                unnest($3::uuid[]) as parent_task_id,
                unnest($4::uuid[]) as warehouse_id,
                unnest($5::timestamptz[]) as scheduled_for,
                unnest($6::jsonb[]) as payload,
                unnest($7::uuid[]) as entity_ids,
                unnest($8::entity_type[]) as entity_types
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
        r#"WITH updated_task AS (
        SELECT task_id, t.warehouse_id, config
        FROM task t
        LEFT JOIN task_config tc
            ON tc.queue_name = t.queue_name
                   AND tc.warehouse_id = t.warehouse_id
        WHERE (t.queue_name = $1 AND scheduled_for < now()) 
            AND (
                (status = $3) OR 
                (status != $3 AND (now() - last_heartbeat_at) > COALESCE(tc.max_time_since_last_heartbeat, $2))
            )
        -- FOR UPDATE locks the row we select here, SKIP LOCKED makes us not wait for rows other
        -- transactions locked, this is our queue right there.
        FOR UPDATE OF t SKIP LOCKED
        LIMIT 1
    )
    UPDATE task
    SET status = $4,
        progress = 0.0,
        execution_details = NULL,
        picked_up_at = now(),
        last_heartbeat_at = now(),
        attempt = task.attempt + 1
    FROM updated_task
    WHERE task.task_id = updated_task.task_id
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
        (select config from updated_task)
    "#,
        queue_name,
        max_time_since_last_heartbeat,
        TaskStatus::Scheduled as _,
        TaskStatus::Running as _,
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
            task_id: task.task_id.into(),
            status: task.status,
            queue_name: task.queue_name.into(),
            picked_up_at: task.picked_up_at,
            attempt: task.attempt,
            data: task.task_data,
        }));
    }

    Ok(None)
}

pub(crate) async fn record_success(
    task_id: TaskId,
    pool: &mut PgConnection,
    message: Option<&str>,
) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        WITH history as (
            INSERT INTO task_log(task_id,
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
                                 task_created_at)
                SELECT task_id,
                       warehouse_id,
                       queue_name,
                       task_data,
                       $3,
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
                FROM task
                WHERE task_id = $1
                ON CONFLICT (task_id, attempt) DO NOTHING
                RETURNING task_id
                )
        DELETE FROM task
        WHERE task_id = $1
        "#,
        *task_id,
        message,
        TaskOutcome::Success as _,
    )
    .execute(pool)
    .await
    .map_err(|e| e.into_error_model("failed to record task success"))?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn record_failure(
    conn: &mut PgConnection,
    task_id: TaskId,
    max_retries: i32,
    details: &str,
) -> Result<(), IcebergErrorResponse> {
    let should_fail = sqlx::query_scalar!(
        r#"
        SELECT attempt >= $1 as "should_fail!"
        FROM task
        WHERE task_id = $2
        "#,
        max_retries,
        *task_id
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| e.into_error_model("failed to check if task should fail"))?
    .unwrap_or(false);

    if should_fail {
        sqlx::query!(
            r#"
            WITH history as (
                INSERT INTO task_log(
                    task_id,
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
                    task_created_at
                )
                SELECT
                    task_id,
                    warehouse_id,
                    queue_name,
                    task_data,
                    $2,
                    entity_id,
                    entity_type,
                    attempt,
                    picked_up_at,
                    now() - picked_up_at,
                    progress,
                    execution_details,
                    scheduled_for,
                    last_heartbeat_at,
                    parent_task_id,
                    created_at
                FROM task WHERE task_id = $1
                ON CONFLICT (task_id, attempt) DO NOTHING
                RETURNING task_id
            )
            DELETE FROM task
            WHERE task_id = $1            
            "#,
            *task_id,
            TaskOutcome::Failed as _,
        )
        .execute(conn)
        .await
        .map_err(|e| e.into_error_model("failed to log and delete failed task"))?;
    } else {
        sqlx::query!(
            r#"
            WITH ins as (
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
                    task_created_at)
                SELECT 
                    task_id,
                    warehouse_id,
                    queue_name,
                    task_data,
                    $4,
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
                FROM task WHERE task_id = $1
                ON CONFLICT (task_id, attempt) DO NOTHING
                RETURNING task_id
            )
            UPDATE task t
            SET 
                status = $3, 
                progress = 0.0,
                picked_up_at = NULL,
                execution_details = NULL
            FROM ins
            WHERE t.task_id = ins.task_id
            "#,
            *task_id,
            details,
            TaskStatus::Scheduled as _,
            TaskOutcome::Failed as _,
        )
        .execute(conn)
        .await
        .map_err(|e| e.into_error_model("failed to update task status"))?;
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
    queue_name: &str,
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
        queue_name,
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
        SET status = $2
        WHERE task_id = ANY($1) 
            AND status = $3
        "#,
        &task_ids.iter().map(|s| **s).collect_vec(),
        TaskStatus::ShouldStop as _,
        TaskStatus::Running as _,
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

pub(crate) async fn check_and_heartbeat_task(
    transaction: &mut PgConnection,
    task_id: TaskId,
    attempt: i32,
    progress: f32,
    execution_details: Option<serde_json::Value>,
) -> crate::api::Result<TaskCheckState> {
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
        TaskStatus::ShouldStop => TaskCheckState::Stop,
        TaskStatus::Running | TaskStatus::Scheduled => TaskCheckState::Continue,
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
                r#"WITH log as (
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
                               end,
                                progress,
                                execution_details,
                                scheduled_for,
                                last_heartbeat_at,
                                parent_task_id,
                                created_at
                        FROM task
                        WHERE (status = $3 OR $5) AND warehouse_id = $1 AND (queue_name = $2 OR $6)
                        ON CONFLICT (task_id, attempt) DO NOTHING
                    )
                    DELETE FROM task
                    WHERE (status = $3 OR $5) AND warehouse_id = $1 AND (queue_name = $2 OR $6)
                "#,
                *warehouse_id,
                queue_name,
                TaskStatus::Scheduled as _,
                TaskOutcome::Cancelled as _,
                force_delete_running_tasks,
                queue_name_is_none
            )
            .fetch_all(connection)
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
                r#"WITH ins as (
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
                               end,
                                progress,
                                execution_details,
                                scheduled_for,
                                last_heartbeat_at,
                                parent_task_id,
                                created_at
                        FROM task
                        WHERE (status = $3 OR $6) AND task_id = ANY($1) AND (queue_name = $4 OR $5)
                        ON CONFLICT (task_id, attempt) DO NOTHING
                    )
                    DELETE FROM task t
                    WHERE (status = $3 OR $6) AND (queue_name = $4 OR $5)
                    AND task_id = ANY($1)
                "#,
                &task_ids.iter().map(|s| **s).collect_vec(),
                TaskOutcome::Cancelled as _,
                TaskStatus::Scheduled as _,
                queue_name,
                queue_name_is_none,
                force_delete_running_tasks
            )
            .fetch_all(connection)
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
        api::management::v1::warehouse::{QueueConfig, TabularDeleteProfile},
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
            AllowAllAuthorizer,
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
            AllowAllAuthorizer,
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_failure(&mut pool.acquire().await.unwrap(), id, 5, "test details")
            .await
            .unwrap();

        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_failure(&mut pool.acquire().await.unwrap(), id, 2, "test")
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        record_success(id, &mut pool.acquire().await.unwrap(), Some(""))
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"a": "a"}));

        record_success(task.task_id, &mut pool.acquire().await.unwrap(), Some(""))
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
        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
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

        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);
        assert_eq!(task.data, serde_json::json!({"a": "a"}));

        record_failure(
            &mut pool.acquire().await.unwrap(),
            task.task_id,
            1,
            "failed",
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
        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, &tq_name, chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, &tq_name);

        record_success(task.task_id, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
        record_success(id2, &mut pool.acquire().await.unwrap(), Some(""))
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, &tq_name);

        record_success(task.task_id, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
        record_success(id2, &mut pool.acquire().await.unwrap(), Some(""))
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
        let ids = queue_task_batch(
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

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, &tq_name);

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
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
        record_success(id, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();
        record_success(id2, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        // Re-insert two tasks with previously used idempotency keys
        let ids_third = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![TaskInput {
                task_metadata: task_metadata(warehouse_id, idp1),
                payload: serde_json::Value::default(),
            }],
        )
        .await
        .unwrap();

        assert_eq!(ids_third.len(), 1);
        let id = ids_third[0].task_id;

        record_success(id, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        // pick one new task, one re-inserted task
        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, new_id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
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

        record_success(task.task_id, &mut pool.acquire().await.unwrap(), Some(""))
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
    async fn test_record_success_idempotent(pool: PgPool) {
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
        assert_eq!(picked_task.task_id, task_id);

        // Record success first time
        record_success(task_id, &mut conn, Some("First success"))
            .await
            .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Record success second time - should be idempotent (no error)
        record_success(task_id, &mut conn, Some("Second success"))
            .await
            .unwrap();

        // Record success third time - should still be idempotent
        record_success(task_id, &mut conn, Some("Third success"))
            .await
            .unwrap();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should be marked as successful
        assert!(matches!(
            task_details.task.status,
            crate::api::management::v1::tasks::TaskStatus::Success
        ));
        // Should have no historical attempts since it succeeded on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_record_failure_idempotent_when_max_retries_exceeded(pool: PgPool) {
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
        assert_eq!(picked_task.task_id, task_id);

        // Record failure with max_retries=1 (should fail permanently)
        record_failure(&mut conn, task_id, 1, "First failure")
            .await
            .unwrap();

        // Verify task is no longer in active tasks table
        let active_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap();
        assert!(active_task.is_none());

        // Record failure second time - should be idempotent (no error)
        record_failure(&mut conn, task_id, 1, "Second failure")
            .await
            .unwrap();

        // Record failure third time - should still be idempotent
        record_failure(&mut conn, task_id, 1, "Third failure")
            .await
            .unwrap();

        // Verify task is in task_log using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should be marked as failed
        assert!(matches!(
            task_details.task.status,
            crate::api::management::v1::tasks::TaskStatus::Failed
        ));
        // Should have no historical attempts since it failed permanently on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_record_failure_idempotent_when_retries_available(pool: PgPool) {
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
            Some(serde_json::json!({"test": "retry_data"})),
        )
        .await
        .unwrap()
        .unwrap();

        let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(picked_task.task_id, task_id);
        assert_eq!(picked_task.attempt, 1);

        // Record failure with max_retries=5 (should retry)
        record_failure(&mut conn, task_id, 5, "First failure")
            .await
            .unwrap();

        // Task should be back in scheduled state
        let retry_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry_task.task_id, task_id);
        assert_eq!(retry_task.attempt, 2);

        // Record failure again for the same attempt - should be idempotent
        record_failure(&mut conn, task_id, 5, "Second failure for same attempt")
            .await
            .unwrap();

        // Record failure third time - should still be idempotent
        record_failure(&mut conn, task_id, 5, "Third failure for same attempt")
            .await
            .unwrap();

        // Task should still be available for retry
        let final_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_task.task_id, task_id);
        // Attempt number might vary depending on implementation, but should be at least 2
        assert!(final_task.attempt >= 2);

        // Verify we have log entries for the failures using get_task_details
        let task_details = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .expect("Task should exist");

        // Should have at least one historical attempt (the first failure)
        assert!(!task_details.attempts.is_empty());
        // Current task should be running (picked up for retry)
        assert!(matches!(
            task_details.task.status,
            crate::api::management::v1::tasks::TaskStatus::Running
        ));
        // Should be on attempt 2 or higher
        assert!(task_details.task.attempt >= 2);
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
        assert_eq!(scheduled_task.task_id, task_id);

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
        let task_details = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should be marked as cancelled
        assert!(matches!(
            task_details.task.status,
            crate::api::management::v1::tasks::TaskStatus::Cancelled
        ));
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
        assert_eq!(picked_task.task_id, task_id);
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
        let task_details = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should be marked as cancelled
        assert!(matches!(
            task_details.task.status,
            crate::api::management::v1::tasks::TaskStatus::Cancelled
        ));
        // Should have no historical attempts since it was cancelled while running
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }

    #[sqlx::test]
    async fn test_mixed_operations_idempotency(pool: PgPool) {
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
        assert_eq!(picked_task.task_id, task_id);

        // Record success first
        record_success(task_id, &mut conn, Some("Task completed"))
            .await
            .unwrap();

        // Now try to record failure - should be idempotent (no error)
        record_failure(&mut conn, task_id, 5, "Attempting to fail completed task")
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
        let task_details = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .expect("Task should exist in task_log");

        // Should remain marked as successful despite later operations
        assert!(matches!(
            task_details.task.status,
            crate::api::management::v1::tasks::TaskStatus::Success
        ));
        // Should have no historical attempts since it succeeded on first try
        assert!(task_details.attempts.is_empty());
        assert_eq!(task_details.task.attempt, 1);
    }
}
