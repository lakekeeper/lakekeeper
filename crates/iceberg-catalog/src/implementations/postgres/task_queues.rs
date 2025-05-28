use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use sqlx::{postgres::types::PgInterval, PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    api::management::v1::warehouse::{
        GetTaskQueueConfigResponse, QueueConfigResponse, SetTaskQueueConfigRequest,
    },
    implementations::postgres::dbutils::DBErrorHandler,
    service::task_queue::{Task, TaskFilter, TaskStatus},
    WarehouseId,
};

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
    queue_name: &'static str,
    tasks: Vec<TaskInput>,
) -> Result<Vec<InsertResult>, IcebergErrorResponse> {
    let mut task_ids = Vec::with_capacity(tasks.len());
    let mut parent_task_ids = Vec::with_capacity(tasks.len());
    let mut warehouse_idents = Vec::with_capacity(tasks.len());
    let mut scheduled_fors = Vec::with_capacity(tasks.len());
    let mut entity_ids = Vec::with_capacity(tasks.len());
    let mut entity_types = Vec::with_capacity(tasks.len());
    let mut states = Vec::with_capacity(tasks.len());
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
        states.push(payload);
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
        &states,
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

#[tracing::instrument]
pub(crate) async fn pick_task(
    pool: &PgPool,
    queue_name: &str,
    max_time_since_last_heartbeat: chrono::Duration,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let max_time_since_last_heartbeat = PgInterval {
        months: 0,
        days: 0,
        microseconds: max_time_since_last_heartbeat.num_microseconds().ok_or(
            ErrorModel::internal(
                "Could not convert max_age into microseconds. Integer overflow, this is a bug.",
                "InternalError",
                None,
            ),
        )?,
    };
    let x = sqlx::query!(
        r#"WITH updated_task AS (
        SELECT task_id, t.warehouse_id, config
        FROM task t
        LEFT JOIN task_config tc
            ON tc.queue_name = t.queue_name
                   AND tc.warehouse_id = t.warehouse_id
        WHERE (status = $3 AND t.queue_name = $1
                   AND scheduled_for < now() AT TIME ZONE 'UTC')
           OR (status = $4 AND (now() - last_heartbeat_at) > COALESCE(tc.max_time_since_last_heartbeat, $2))
        -- FOR UPDATE locks the row we select here, SKIP LOCKED makes us not wait for rows other
        -- transactions locked, this is our queue right there.
        FOR UPDATE OF t SKIP LOCKED
        LIMIT 1
    )
    UPDATE task
    SET status = $4,
        picked_up_at = now() AT TIME ZONE 'UTC',
        last_heartbeat_at = now() AT TIME ZONE 'UTC',
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
            queue_name: task.queue_name,
            picked_up_at: task.picked_up_at,
            attempt: task.attempt,
            state: task.task_data,
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
                                 duration)
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
                       now() - picked_up_at
                FROM task
                WHERE task_id = $1)
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
                INSERT INTO task_log(task_id, warehouse_id, queue_name, task_data, status, entity_id, entity_type, attempt, started_at, duration)
                SELECT task_id, warehouse_id, queue_name, task_data, $2, entity_id, entity_type, attempt, picked_up_at, now() - picked_up_at
                FROM task WHERE task_id = $1
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
            WITH task_log as (
                INSERT INTO task_log(task_id, warehouse_id, queue_name, task_data, status, entity_id, entity_type, message, attempt, started_at, duration)
                SELECT task_id, warehouse_id, queue_name, task_data, $4, entity_id, entity_type, $2, attempt, picked_up_at, now() - picked_up_at
                FROM task WHERE task_id = $1
            )
            UPDATE task
            SET status = $3
            WHERE task_id = $1
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
    queue_name: &str,
) -> crate::api::Result<Option<GetTaskQueueConfigResponse>> {
    let result = sqlx::query!(
        r#"
        SELECT config, max_time_since_last_heartbeat
        FROM task_config
        WHERE warehouse_id = $1 AND queue_name = $2
        "#,
        *warehouse_id,
        queue_name
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
            queue_name: queue_name.to_string(),
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

pub(crate) async fn stop_task(
    transaction: &mut PgConnection,
    task_id: TaskId,
) -> crate::api::Result<()> {
    sqlx::query!(
        r#"
        WITH heartbeat as (UPDATE task SET last_heartbeat_at = now() WHERE task_id = $1)
        UPDATE task
        SET status = $2
        WHERE task_id = $1
        "#,
        *task_id,
        TaskStatus::ShouldStop as _,
    )
    .execute(transaction)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to stop task");
        e.into_error_model(format!("Failed to stop task {task_id}"))
    })?;

    Ok(())
}

pub(crate) async fn check_task(
    transaction: &mut PgConnection,
    task_id: TaskId,
) -> crate::api::Result<Option<TaskCheckState>> {
    Ok(sqlx::query!(
        r#"WITH heartbeat as (UPDATE task SET last_heartbeat_at = now() WHERE task_id = $1)
        SELECT status as "status: TaskStatus" FROM task WHERE task_id = $1"#,
        *task_id
    )
    .fetch_optional(transaction)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to check task");
        e.into_error_model(format!("Failed to check task {task_id}"))
    })?
    .map(|state| match state.status {
        TaskStatus::ShouldStop => TaskCheckState::Stop,
        TaskStatus::Running | TaskStatus::Scheduled => TaskCheckState::Continue,
    }))
}

use crate::service::task_queue::{
    EntityId, TaskCheckState, TaskId, TaskInput, TaskMetadata, TaskOutcome,
};

/// Cancel pending tasks for a warehouse
/// If `task_ids` are provided in `filter` which are not pending, they are ignored
pub(crate) async fn cancel_tasks(
    connection: &mut PgConnection,
    filter: TaskFilter,
    queue_name: &str,
    force_delete_running_tasks: bool,
) -> crate::api::Result<()> {
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
                                             duration)
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
                               end
                        FROM task
                        WHERE (status = $3 OR $5) AND warehouse_id = $1 AND queue_name = $2
                    )
                    DELETE FROM task
                    WHERE (status = $3 OR $5) AND warehouse_id = $1 AND queue_name = $2
                "#,
                *warehouse_id,
                queue_name,
                TaskStatus::Scheduled as _,
                TaskOutcome::Cancelled as _,
                force_delete_running_tasks
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
                r#"WITH log as (
                        INSERT INTO task_log(task_id,
                                             warehouse_id,
                                             queue_name,
                                             task_data,
                                             status,
                                             entity_id,
                                             entity_type,
                                             attempt,
                                             started_at,
                                             duration)
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
                               end
                        FROM task
                        WHERE status = $3 AND task_id = ANY($1)
                    )
                    DELETE FROM task
                    WHERE status = $3
                    AND task_id = ANY($1)
                "#,
                &task_ids.iter().map(|s| **s).collect::<Vec<_>>(),
                TaskOutcome::Cancelled as _,
                TaskStatus::Scheduled as _,
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
        queue_name: &'static str,
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

    #[sqlx::test]
    async fn test_queue_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup(pool.clone()).await;

        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let id = queue_task(&mut conn, "test", None, entity_id, warehouse_id, None, None)
            .await
            .unwrap();

        assert!(
            queue_task(&mut conn, "test", None, entity_id, warehouse_id, None, None)
                .await
                .unwrap()
                .is_none()
        );

        let id3 = queue_task(
            &mut conn,
            "test",
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

    pub(crate) async fn setup(pool: PgPool) -> WarehouseId {
        let prof = crate::tests::test_io_profile();
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

    #[sqlx::test]
    async fn test_failed_tasks_are_put_back(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup(pool.clone()).await;
        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&mut pool.acquire().await.unwrap(), id, 5, "test")
            .await
            .unwrap();

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&mut pool.acquire().await.unwrap(), id, 2, "test")
            .await
            .unwrap();

        assert!(
            pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[sqlx::test]
    async fn test_success_task_arent_polled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let warehouse_id = setup(pool.clone()).await;
        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_success(id, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        assert!(
            pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[sqlx::test]
    async fn test_success_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let warehouse_id = setup(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"a": "a"}));

        record_success(task.task_id, &mut pool.acquire().await.unwrap(), Some(""))
            .await
            .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
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

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_cancelled_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        cancel_tasks(&mut conn, TaskFilter::TaskIds(vec![id]), "test", false)
            .await
            .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
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

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_failed_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let warehouse_id = setup(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"a": "a"}));

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
            "test",
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

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_scheduled_tasks_are_polled_later(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup(pool.clone()).await;
        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            Some(Utc::now() + chrono::Duration::milliseconds(500)),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(
            pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .is_none()
        );

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_stale_tasks_are_picked_up_again(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup(pool.clone()).await;
        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", chrono::Duration::milliseconds(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_multiple_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup(pool.clone()).await;
        let id = queue_task(
            &mut conn,
            "test",
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
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

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
        let warehouse_id = setup(pool.clone()).await;
        let ids = queue_task_batch(
            &mut conn,
            "test",
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

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

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
        let warehouse_id = setup(pool.clone()).await;
        let idp1 = EntityId::Tabular(Uuid::now_v7());
        let idp2 = EntityId::Tabular(Uuid::now_v7());
        let ids = queue_task_batch(
            &mut conn,
            "test",
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

        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        // Re-insert the first task with the same idempotency key
        // and a new idempotency key
        // This should create a new task with the new idempotency key
        let new_key = EntityId::Tabular(Uuid::now_v7());
        let ids_second = queue_task_batch(
            &mut conn,
            "test",
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
            "test",
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
        let task = pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, new_id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert!(
            pick_task(&pool, "test", DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
        let warehouse_id = setup(pool.clone()).await;
        let queue_name = "test_queue";

        assert!(get_task_queue_config(&mut conn, warehouse_id, queue_name)
            .await
            .unwrap()
            .is_none());

        let config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig(serde_json::json!({"max_attempts": 5})),
            max_seconds_since_last_heartbeat: Some(3600),
        };

        set_task_queue_config(&mut conn, queue_name, warehouse_id, config)
            .await
            .unwrap();

        let response = get_task_queue_config(&mut conn, warehouse_id, queue_name)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.queue_config.queue_name, queue_name);
        assert_eq!(
            response.queue_config.config,
            serde_json::json!({"max_attempts": 5})
        );
        assert_eq!(response.max_seconds_since_last_heartbeat, Some(3600));
    }

    #[sqlx::test]
    async fn test_set_task_config_yields_a_task_with_config(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup(pool.clone()).await;
        let queue_name = "test_queue";

        let config = SetTaskQueueConfigRequest {
            queue_config: QueueConfig(serde_json::json!({"max_attempts": 5})),
            max_seconds_since_last_heartbeat: Some(3600),
        };

        set_task_queue_config(&mut conn, queue_name, warehouse_id, config)
            .await
            .unwrap();
        let payload = serde_json::json!("our-task");
        let _task = queue_task(
            &mut conn,
            queue_name,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            Some(payload.clone()),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, queue_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.queue_name, queue_name);
        assert_eq!(task.config, Some(serde_json::json!({"max_attempts": 5})));
        assert_eq!(task.state, payload);

        let other_queue = "other-queue";
        let other_payload = serde_json::json!("other-task");
        let _task = queue_task(
            &mut conn,
            other_queue,
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            Some(other_payload.clone()),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, other_queue, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.queue_name, other_queue);
        assert_eq!(task.config, None);
        assert_eq!(task.state, other_payload);
    }
}
