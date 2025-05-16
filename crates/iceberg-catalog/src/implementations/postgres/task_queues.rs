use chrono::Utc;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    implementations::postgres::{dbutils::DBErrorHandler, ReadWrite},
    service::task_queue::{Task, TaskFilter, TaskQueue, TaskQueueConfig, TaskStatus},
    WarehouseId,
};

#[derive(Debug, Clone)]
pub struct PgQueue {
    pub read_write: ReadWrite,
    pub config: TaskQueueConfig,
    pub max_age: sqlx::postgres::types::PgInterval,
}

impl PgQueue {
    /// Creates a new `PgQueue` instance from the provided `ReadWrite` and `TaskQueueConfig`.
    ///
    /// # Errors
    /// Returns an error if the `max_age` duration in the `TaskQueueConfig` is invalid.
    pub fn from_config(read_write: ReadWrite, config: TaskQueueConfig) -> anyhow::Result<Self> {
        let microseconds = config
            .max_age
            .num_microseconds()
            .ok_or(anyhow::anyhow!("Invalid max age duration for task queues."))?;
        Ok(Self {
            read_write,
            config,
            max_age: sqlx::postgres::types::PgInterval {
                months: 0,
                days: 0,
                microseconds,
            },
        })
    }
}

#[async_trait::async_trait]
impl TaskQueue for PgQueue {
    async fn pick_new_task(&self, queue_name: &str) -> crate::api::Result<Option<Task>> {
        pick_task(&self.read_write.write_pool, queue_name, &self.max_age).await
    }

    async fn record_success(&self, id: Uuid) -> crate::api::Result<()> {
        record_success(id, &self.read_write.write_pool).await
    }

    async fn record_failure(&self, id: Uuid, error_details: &str) -> crate::api::Result<()> {
        record_failure(
            &self.read_write.write_pool,
            id,
            self.config.max_retries,
            error_details,
        )
        .await
    }
}

#[derive(Debug)]
pub struct InsertResult {
    pub task_id: Uuid,
    pub idempotency_key: Uuid,
}

pub trait InputTrait {
    fn warehouse_ident(&self) -> WarehouseId;
    fn suspend_until(&self) -> Option<chrono::DateTime<Utc>> {
        None
    }
}

pub(crate) async fn queue_task_batch(
    conn: &mut PgConnection,
    queue_name: &str,
    tasks: Vec<TaskInput>,
) -> Result<Vec<InsertResult>, IcebergErrorResponse> {
    let mut task_ids = Vec::with_capacity(tasks.len());
    let mut idempotency_keys = Vec::with_capacity(tasks.len());
    let mut parent_task_ids = Vec::with_capacity(tasks.len());
    let mut warehouse_idents = Vec::with_capacity(tasks.len());
    let mut suspend_untils = Vec::with_capacity(tasks.len());
    let mut tabular_ids = Vec::with_capacity(tasks.len());
    let mut states = Vec::with_capacity(tasks.len());
    for TaskInput {
        task_metadata:
            TaskMetadata {
                idempotency_key,
                tabular_id,
                parent_task_id,
                warehouse_id,
                suspend_until,
            },
        payload,
    } in tasks
    {
        task_ids.push(Uuid::now_v7());
        idempotency_keys.push(idempotency_key);
        parent_task_ids.push(parent_task_id);
        warehouse_idents.push(*warehouse_id);
        suspend_untils.push(suspend_until);
        states.push(payload);
        tabular_ids.push(tabular_id);
    }

    Ok(sqlx::query_as!(
        InsertResult,
        r#"WITH input_rows AS (
            SELECT
                unnest($1::uuid[]) as task_id,
                $2::text as queue_name,
                unnest($3::uuid[]) as parent_task_id,
                unnest($4::uuid[]) as idempotency_key,
                unnest($5::uuid[]) as warehouse_id,
                unnest($6::timestamptz[]) as suspend_until,
                unnest($7::jsonb[]) as payload,
                unnest($8::uuid[]) as tabular_id
        )
        INSERT INTO task(
                task_id,
                queue_name,
                status,
                parent_task_id,
                idempotency_key,
                warehouse_id,
                suspend_until,
                state,
                tabular_id)
        SELECT
            i.task_id,
            i.queue_name,
            'pending'::task_status,
            i.parent_task_id,
            i.idempotency_key,
            i.warehouse_id,
            i.suspend_until,
            i.payload,
            i.tabular_id
        FROM input_rows i
        ON CONFLICT ON CONSTRAINT unique_idempotency_key
        DO UPDATE SET
            status = EXCLUDED.status,
            state = EXCLUDED.state,
            suspend_until = EXCLUDED.suspend_until
        WHERE task.status = 'cancelled'
        RETURNING task_id, idempotency_key"#,
        &task_ids,
        queue_name,
        &parent_task_ids as _,
        &idempotency_keys,
        &warehouse_idents,
        &suspend_untils
            .iter()
            .map(|t| t.as_ref())
            .collect::<Vec<_>>() as _,
        &states,
        &tabular_ids as _
    )
    .fetch_all(conn)
    .await
    .map_err(|e| e.into_error_model("failed queueing tasks"))?)
}

#[tracing::instrument]
async fn pick_task(
    pool: &PgPool,
    queue_name: &str,
    max_age: &sqlx::postgres::types::PgInterval,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let x = sqlx::query!(
r#" WITH updated_task AS (
        SELECT task_id, warehouse_id
        FROM task
        WHERE (status = 'pending' AND queue_name = $1 AND ((suspend_until < now() AT TIME ZONE 'UTC') OR (suspend_until IS NULL)))
                OR (status = 'running' AND (now() - picked_up_at) > $3)
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ),
    cfg AS (SELECT config from task_config WHERE queue_name = $1 and warehouse_id = any(select warehouse_id from updated_task))
    UPDATE task
    SET status = 'running', picked_up_at = $2, attempt = task.attempt + 1
    FROM updated_task
    WHERE task.task_id = updated_task.task_id
    RETURNING task.task_id, task.tabular_id, task.warehouse_id, task.state, task.suspend_until, task.idempotency_key, task.status as "status: TaskStatus", task.picked_up_at, task.attempt, task.parent_task_id, task.queue_name, (select config from cfg)
    "#,
        queue_name,
        Utc::now(),
        max_age,
    )
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!(?e, "Failed to pick a task");
            e.into_error_model(format!("Failed to pick a '{queue_name}' task")) })?;

    if let Some(task) = x {
        tracing::info!("Picked up task: {:?}", task);
        return Ok(Some(Task {
            task_metadata: TaskMetadata {
                idempotency_key: task.idempotency_key,
                warehouse_id: task.warehouse_id.into(),
                tabular_id: task.tabular_id,
                parent_task_id: task.parent_task_id,
                suspend_until: task.suspend_until,
            },
            config: task.config,
            task_id: task.task_id,
            status: task.status,
            queue_name: task.queue_name,
            picked_up_at: task.picked_up_at,
            attempt: task.attempt,
            state: task.state,
        }));
    }

    Ok(None)
}

async fn record_success(id: Uuid, pool: &PgPool) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        WITH history as (
            INSERT INTO task_log(task_id, warehouse_id, queue_name, state, status)
                SELECT task_id, warehouse_id, queue_name, state, 'done' FROM task
                                                                        WHERE task_id = $1)
        DELETE FROM task
        WHERE task_id = $1
        "#,
        id
    )
    .execute(pool)
    .await
    .map_err(|e| e.into_error_model("failed to record task success"))?;
    Ok(())
}

async fn record_failure(
    conn: &PgPool,
    id: Uuid,
    n_retries: i32,
    details: &str,
) -> Result<(), IcebergErrorResponse> {
    let should_fail = sqlx::query_scalar!(
        r#"
        SELECT attempt >= $1 as "should_fail!"
        FROM task
        WHERE task_id = $2
        "#,
        n_retries,
        id
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| e.into_error_model("failed to check if task should fail"))?
    .unwrap_or(false);

    if should_fail {
        sqlx::query!(
            r#"
            WITH history as (
                INSERT INTO task_log(task_id, warehouse_id, queue_name, state, status)
                SELECT task_id, warehouse_id, queue_name, state, 'failed'
                FROM task WHERE task_id = $1
            )
            DELETE FROM task
            WHERE task_id = $1
            "#,
            id
        )
        .execute(conn)
        .await
        .map_err(|e| e.into_error_model("failed to log and delete failed task"))?;
    } else {
        sqlx::query!(
            r#"
            UPDATE task
            SET status = 'pending',
                last_error_details = $2
            WHERE task_id = $1
            "#,
            id,
            details
        )
        .execute(conn)
        .await
        .map_err(|e| e.into_error_model("failed to update task status"))?;
    }

    Ok(())
}

use crate::service::task_queue::{TaskInput, TaskMetadata};

/// Cancel pending tasks for a warehouse
/// If `task_ids` are provided in `filter` which are not pending, they are ignored
pub(crate) async fn cancel_pending_tasks(
    connection: &mut PgConnection,
    filter: TaskFilter,
    queue_name: &str,
) -> crate::api::Result<()> {
    match filter {
        TaskFilter::WarehouseId(warehouse_id) => {
            sqlx::query!(
                r#"WITH log as (
                        INSERT INTO task_log(task_id, warehouse_id, queue_name, state, status)
                        SELECT task_id, warehouse_id, queue_name, state, 'cancelled'
                        FROM task
                        WHERE status = 'pending' AND warehouse_id = $1 AND queue_name = $2
                    )
                    DELETE FROM task
                    WHERE status = 'pending' AND warehouse_id = $1 AND queue_name = $2
                "#,
                *warehouse_id,
                queue_name
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
                        INSERT INTO task_log(task_id, warehouse_id, queue_name, state, status)
                        SELECT task_id, warehouse_id, queue_name, state, 'cancelled'
                        FROM task
                        WHERE status = 'pending' AND task_id = ANY($1)
                    )
                    DELETE FROM task
                    WHERE status = 'pending'
                    AND task_id = ANY($1)
                "#,
                &task_ids.iter().map(|s| **s).collect::<Vec<_>>(),
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
    use chrono::DateTime;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::management::v1::warehouse::TabularDeleteProfile, service::authz::AllowAllAuthorizer,
        WarehouseId,
    };

    async fn queue_task(
        conn: &mut PgConnection,
        queue_name: &str,
        parent_task_id: Option<Uuid>,
        idempotency_key: Uuid,
        warehouse_id: WarehouseId,
        suspend_until: Option<DateTime<Utc>>,
    ) -> Result<Option<Uuid>, IcebergErrorResponse> {
        Ok(queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: TaskMetadata {
                    tabular_id: None,
                    idempotency_key,
                    warehouse_id,
                    parent_task_id,
                    suspend_until,
                },
                payload: serde_json::json!({}),
            }],
        )
        .await?
        .pop()
        .map(|x| x.task_id))
    }

    #[sqlx::test]
    async fn test_queue_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (_, warehouse_id) = setup(pool.clone(), config).await;

        let idempotency_key = Uuid::new_v5(&warehouse_id, b"test");

        let id = queue_task(&mut conn, "test", None, idempotency_key, warehouse_id, None)
            .await
            .unwrap();

        assert!(
            queue_task(&mut conn, "test", None, idempotency_key, warehouse_id, None,)
                .await
                .unwrap()
                .is_none()
        );

        let id3 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&warehouse_id, b"test2"),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        assert_ne!(id, id3);
    }

    pub(crate) async fn setup(pool: PgPool, config: TaskQueueConfig) -> (PgQueue, WarehouseId) {
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
        (
            PgQueue::from_config(ReadWrite::from_pools(pool.clone(), pool), config).unwrap(),
            wh.warehouse_id,
        )
    }

    #[sqlx::test]
    async fn test_failed_tasks_are_put_back(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;
        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&warehouse_id, b"test"),
            warehouse_id,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, id, 5, "test").await.unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, id, 2, "test").await.unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_success_task_arent_polled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&warehouse_id, b"test"),
            warehouse_id,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_success(id, &pool).await.unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_scheduled_tasks_are_polled_later(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&warehouse_id, b"test"),
            warehouse_id,
            Some(Utc::now() + chrono::Duration::milliseconds(500)),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", &queue.max_age)
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
        let config = TaskQueueConfig {
            max_age: chrono::Duration::milliseconds(500),
            ..Default::default()
        };
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&warehouse_id, b"test"),
            warehouse_id,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
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

        let task = pick_task(&pool, "test", &queue.max_age)
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
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&warehouse_id, b"test"),
            warehouse_id,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&warehouse_id, b"test2"),
            warehouse_id,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", &queue.max_age)
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

        record_success(task.task_id, &pool).await.unwrap();
        record_success(id2, &pool).await.unwrap();
    }

    #[sqlx::test]
    async fn test_queue_batch(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let ids = queue_task_batch(
            &mut conn,
            "test",
            vec![
                TaskInput {
                    task_metadata: TaskMetadata {
                        tabular_id: None,
                        idempotency_key: Uuid::now_v7(),
                        warehouse_id,
                        parent_task_id: None,
                        suspend_until: None,
                    },
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: TaskMetadata {
                        tabular_id: None,
                        idempotency_key: Uuid::now_v7(),
                        warehouse_id,
                        parent_task_id: None,
                        suspend_until: None,
                    },
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let id = ids[0].task_id;
        let id2 = ids[1].task_id;

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", &queue.max_age)
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

        record_success(task.task_id, &pool).await.unwrap();
        record_success(id2, &pool).await.unwrap();
    }

    fn task_metadata(idempotency_key: Uuid, warehouse_id: WarehouseId) -> TaskMetadata {
        TaskMetadata {
            tabular_id: None,
            idempotency_key,
            warehouse_id,
            parent_task_id: None,
            suspend_until: None,
        }
    }

    #[sqlx::test]
    async fn test_queue_batch_idempotency(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;
        let idp1 = Uuid::now_v7();
        let idp2 = Uuid::now_v7();
        let ids = queue_task_batch(
            &mut conn,
            "test",
            vec![
                TaskInput {
                    task_metadata: task_metadata(idp1, warehouse_id),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_metadata(idp2, warehouse_id),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();

        let id = ids[0].task_id;
        let id2 = ids[1].task_id;

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", &queue.max_age)
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
        let new_key = Uuid::now_v7();
        let ids_second = queue_task_batch(
            &mut conn,
            "test",
            vec![
                TaskInput {
                    task_metadata: task_metadata(idp1, warehouse_id),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_metadata(new_key, warehouse_id),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let new_id = ids_second[0].task_id;

        assert_eq!(ids_second.len(), 1);
        assert_eq!(ids_second[0].idempotency_key, new_key);

        // move both old tasks to done which will clear them from task table and move them into
        // task_log
        record_success(id, &pool).await.unwrap();
        record_success(id2, &pool).await.unwrap();

        // Re-insert two tasks with previously used idempotency keys
        let ids_third = queue_task_batch(
            &mut conn,
            "test",
            vec![TaskInput {
                task_metadata: task_metadata(idp1, warehouse_id),
                payload: serde_json::Value::default(),
            }],
        )
        .await
        .unwrap();

        assert_eq!(ids_third.len(), 1);
        let id = ids_third[0].task_id;
        assert_eq!(ids_third[0].idempotency_key, idp1);
        record_success(id, &pool).await.unwrap();

        // pick one new task, one re-inserted task
        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, new_id);
        assert_eq!(task.task_metadata.idempotency_key, new_key);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert!(
            pick_task(&pool, "test", &queue.max_age)
                .await
                .unwrap()
                .is_none(),
            "There should be no tasks left, something is wrong."
        );

        record_success(task.task_id, &pool).await.unwrap();
    }
}
