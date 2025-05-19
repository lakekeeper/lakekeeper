mod tabular_expiration_queue;
mod tabular_purge_queue;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use sqlx::{PgConnection, PgPool};
pub use tabular_expiration_queue::TabularExpirationQueue;
pub use tabular_purge_queue::TabularPurgeQueue;
use uuid::Uuid;

use crate::{
    implementations::postgres::{dbutils::DBErrorHandler, ReadWrite},
    service::task_queue::{Task, TaskFilter, TaskQueueConfig, TaskStatus},
    WarehouseId,
};

#[derive(Debug, Clone)]
struct PgQueue {
    pub read_write: ReadWrite,
    pub config: TaskQueueConfig,
    pub max_age: sqlx::postgres::types::PgInterval,
}

impl PgQueue {
    fn new(read_write: ReadWrite) -> Self {
        let config = TaskQueueConfig::default();
        let microseconds = config
            .max_age
            .num_microseconds()
            .expect("Invalid max age duration for task queues hard-coded in Default.");
        Self {
            read_write,
            config,
            max_age: sqlx::postgres::types::PgInterval {
                months: 0,
                days: 0,
                microseconds,
            },
        }
    }

    fn from_config(read_write: ReadWrite, config: TaskQueueConfig) -> anyhow::Result<Self> {
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

#[derive(Debug)]
pub struct TaskArg {
    parent: Option<Uuid>,
    warehouse_ident: WarehouseId,
    suspend_until: Option<DateTime<Utc>>,
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

fn preprocess_batch<T: InputTrait, F: Fn(&T) -> Uuid>(
    tasks: Vec<T>,
    idempotency_fn: F,
) -> crate::api::Result<(HashMap<Uuid, TaskArg>, HashMap<Uuid, T>)> {
    let mut idempotency2task = HashMap::with_capacity(tasks.len());
    let mut idempotency2specific = HashMap::with_capacity(tasks.len());
    for task in tasks {
        let idempotency_key = idempotency_fn(&task);
        if let Some(duplicate) = idempotency2task.insert(
            idempotency_key,
            TaskArg {
                parent: None,
                warehouse_ident: task.warehouse_ident(),
                suspend_until: task.suspend_until(),
            },
        ) {
            tracing::error!(?duplicate, "Duplicate Task in Batch.");
            // TODO: bad-request?
            return Err(ErrorModel::internal(
                "Duplicate Task in Batch",
                "InternalServerError",
                None,
            )
            .into());
        }
        idempotency2specific.insert(idempotency_key, task);
    }
    Ok((idempotency2task, idempotency2specific))
}

async fn queue_task_batch(
    conn: &mut PgConnection,
    queue_name: &str,
    tasks: &HashMap<Uuid, TaskArg>,
) -> Result<Vec<InsertResult>, IcebergErrorResponse> {
    let mut task_ids = Vec::with_capacity(tasks.len());
    let mut idempotency_keys = Vec::with_capacity(tasks.len());
    let mut parent_task_ids = Vec::with_capacity(tasks.len());
    let mut warehouse_idents = Vec::with_capacity(tasks.len());
    let mut suspend_untils = Vec::with_capacity(tasks.len());

    for (
        idempotency_key,
        TaskArg {
            parent,
            warehouse_ident,
            suspend_until,
        },
    ) in tasks
    {
        task_ids.push(Uuid::now_v7());
        idempotency_keys.push(*idempotency_key);
        parent_task_ids.push(*parent);
        warehouse_idents.push(**warehouse_ident);
        suspend_untils.push(*suspend_until);
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
                unnest($6::timestamptz[]) as suspend_until
        )
        INSERT INTO task(
                task_id,
                queue_name,
                status,
                parent_task_id,
                idempotency_key,
                warehouse_id,
                suspend_until)
        SELECT
            i.task_id,
            i.queue_name,
            'pending'::task_status,
            i.parent_task_id,
            i.idempotency_key,
            i.warehouse_id,
            i.suspend_until
        FROM input_rows i
        ON CONFLICT ON CONSTRAINT unique_idempotency_key
        DO UPDATE SET
            status = EXCLUDED.status,
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
            .collect::<Vec<_>>() as _
    )
    .fetch_all(conn)
    .await
    .map_err(|e| e.into_error_model("failed queueing tasks"))?)
}

async fn record_failure(
    conn: &PgPool,
    id: Uuid,
    n_retries: i32,
    details: &str,
) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        WITH cte as (
            SELECT attempt >= $2 as should_fail
            FROM task
            WHERE task_id = $1
        )
        UPDATE task
        SET status = CASE WHEN (select should_fail from cte) THEN 'failed'::task_status ELSE 'pending'::task_status END,
            last_error_details = $3
        WHERE task_id = $1
        "#,
        id,
        n_retries,
        details
    )
        .execute(conn)
        .await.map_err(|e| e.into_error_model("failed to record task failure"))?;
    Ok(())
}

#[tracing::instrument]
async fn pick_task(
    pool: &PgPool,
    queue_name: &'static str,
    max_age: &sqlx::postgres::types::PgInterval,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let x = sqlx::query_as!(
        Task,
        r#"
    WITH updated_task AS (
        SELECT task_id
        FROM task
        WHERE (status = 'pending' AND queue_name = $1 AND ((suspend_until < now() AT TIME ZONE 'UTC') OR (suspend_until IS NULL)))
                OR (status = 'running' AND (now() - picked_up_at) > $3)
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    UPDATE task
    SET status = 'running', picked_up_at = $2, attempt = task.attempt + 1
    FROM updated_task
    WHERE task.task_id = updated_task.task_id
    RETURNING task.task_id, task.status as "status: TaskStatus", task.picked_up_at, task.attempt, task.parent_task_id, task.queue_name
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

    if let Some(task) = x.as_ref() {
        tracing::info!("Picked up task: {:?}", task);
    }

    Ok(x)
}

async fn record_success(id: Uuid, pool: &PgPool) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        UPDATE task
        SET status = 'done'
        WHERE task_id = $1
        "#,
        id
    )
    .execute(pool)
    .await
    .map_err(|e| e.into_error_model("failed to record task success"))?;
    Ok(())
}

macro_rules! impl_pg_task_queue {
    ($name:ident) => {
        use crate::implementations::postgres::{task_queues::PgQueue, ReadWrite};

        #[derive(Debug, Clone)]
        pub struct $name {
            pg_queue: PgQueue,
        }

        impl $name {
            #[must_use]
            pub fn new(read_write: ReadWrite) -> Self {
                Self {
                    pg_queue: PgQueue::new(read_write),
                }
            }

            /// Create a new `$name` with the default configuration.
            ///
            /// # Errors
            /// Returns an error if the max age duration is invalid.
            pub fn from_config(
                read_write: ReadWrite,
                config: TaskQueueConfig,
            ) -> anyhow::Result<Self> {
                Ok(Self {
                    pg_queue: PgQueue::from_config(read_write, config)?,
                })
            }
        }
    };
}
use impl_pg_task_queue;

/// Cancel pending tasks for a warehouse
/// If `task_ids` are provided in `filter` which are not pending, they are ignored
async fn cancel_pending_tasks(
    queue: &PgQueue,
    filter: TaskFilter,
    queue_name: &'static str,
) -> crate::api::Result<()> {
    let mut transaction = queue
        .read_write
        .write_pool
        .begin()
        .await
        .map_err(|e| e.into_error_model("Failed to get transaction to cancel Task"))?;

    match filter {
        TaskFilter::WarehouseId(warehouse_id) => {
            sqlx::query!(
                r#"
                    UPDATE task SET status = 'cancelled'
                    WHERE status = 'pending'
                    AND warehouse_id = $1
                    AND queue_name = $2
                "#,
                *warehouse_id,
                queue_name
            )
            .fetch_all(&mut *transaction)
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
                    UPDATE task SET status = 'cancelled'
                    WHERE status = 'pending'
                    AND task_id = ANY($1)
                "#,
                &task_ids.iter().map(|s| **s).collect::<Vec<_>>(),
            )
            .fetch_all(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to cancel Tasks for task_ids {task_ids:?}");
                e.into_error_model("Failed to cancel Tasks for specified ids")
            })?;
        }
    }

    transaction.commit().await.map_err(|e| {
        tracing::error!(?e, "Failed to commit transaction to cancel Tasks");
        e.into_error_model("failed to commit transaction cancelling tasks.")
    })?;

    Ok(())
}

#[cfg(test)]
mod test {
    use maplit::hashmap;
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
        warehouse_ident: WarehouseId,
        suspend_until: Option<DateTime<Utc>>,
    ) -> Result<Option<Uuid>, IcebergErrorResponse> {
        Ok(queue_task_batch(
            conn,
            queue_name,
            &HashMap::from([(
                idempotency_key,
                TaskArg {
                    parent: parent_task_id,
                    warehouse_ident,
                    suspend_until,
                },
            )]),
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
            Some(config.clone()),
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
        assert!(task.parent_task_id.is_none());
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
        assert!(task.parent_task_id.is_none());
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
        assert!(task.parent_task_id.is_none());
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
        assert!(task.parent_task_id.is_none());
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
        assert!(task.parent_task_id.is_none());
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
        assert!(task.parent_task_id.is_none());
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
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.parent_task_id.is_none());
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
            &hashmap! {
                Uuid::new_v5(&warehouse_id, b"test") => TaskArg {
                    parent: None,
                    warehouse_ident: warehouse_id,
                    suspend_until: None,
                },
                Uuid::new_v5(&warehouse_id, b"test2") => TaskArg {
                    parent: None,
                    warehouse_ident: warehouse_id,
                    suspend_until: None,
                },
            },
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
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        record_success(task.task_id, &pool).await.unwrap();
        record_success(id2, &pool).await.unwrap();
    }

    #[sqlx::test]
    async fn test_queue_batch_idempotency(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let ids = queue_task_batch(
            &mut conn,
            "test",
            &hashmap! {
                Uuid::new_v5(&warehouse_id, b"test") => TaskArg {
                    parent: None,
                    warehouse_ident: warehouse_id,
                    suspend_until: None,
                },
                Uuid::new_v5(&warehouse_id, b"test2") => TaskArg {
                    parent: None,
                    warehouse_ident: warehouse_id,
                    suspend_until: None,
                },
            },
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
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        record_success(task.task_id, &pool).await.unwrap();
        record_success(id2, &pool).await.unwrap();

        let new_key = Uuid::new_v5(&warehouse_id, b"test3");

        let ids_second = queue_task_batch(
            &mut conn,
            "test",
            &hashmap! {
                Uuid::new_v5(&warehouse_id, b"test") => TaskArg {
                    parent: None,
                    warehouse_ident: warehouse_id,
                    suspend_until: None,
                },
                new_key => TaskArg {
                    parent: None,
                    warehouse_ident: warehouse_id,
                    suspend_until: None,
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(ids_second.len(), 1);
        let id = ids_second[0].task_id;
        let idempotency_key = ids_second[0].idempotency_key;
        assert_eq!(idempotency_key, new_key);

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert!(
            pick_task(&pool, "test", &queue.max_age)
                .await
                .unwrap()
                .is_none(),
            "There should be no tasks left, something is wrong."
        );

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_success(task.task_id, &pool).await.unwrap();
    }
}
