mod stats;
mod tabular_expiration_queue;
mod tabular_purge_queue;

use crate::api::iceberg::v1::{PaginationQuery, MAX_PAGE_SIZE};
use crate::api::management::v1::task::{
    ListTaskInstancesResponse, ListTasksRequest, ListTasksResponse,
};
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::pagination::{PaginateToken, V1PaginateToken};
use crate::implementations::postgres::ReadWrite;
use crate::service::task_queue::{
    Schedule, Scheduler, Task, TaskFilter, TaskId, TaskInstance, TaskInstanceStatus,
    TaskQueueConfig, TaskStatus,
};
use crate::ProjectIdent;
use async_trait::async_trait;
pub use stats::StatsQueue;
use std::str::FromStr;
pub use tabular_expiration_queue::TabularExpirationQueue;
pub use tabular_purge_queue::TabularPurgeQueue;

use chrono::Utc;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use sqlx::{Executor, PgConnection, PgPool};
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PgQueue {
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

macro_rules! impl_pg_task_queue {
    ($name:ident) => {
        use crate::implementations::postgres::task_queues::PgQueue;
        use crate::implementations::postgres::ReadWrite;

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

#[derive(Debug)]
pub struct PgScheduler {
    pub read_write: ReadWrite,
    pub config: TaskQueueConfig,
}

impl PgScheduler {
    /// Create a new `PgScheduler` with the given configuration.
    #[must_use]
    pub fn from_config(read_write: ReadWrite, config: TaskQueueConfig) -> Self {
        Self { read_write, config }
    }
}

#[async_trait]
impl Scheduler for PgScheduler {
    async fn schedule_task_instance(&self) -> Result<(), IcebergErrorResponse> {
        let mut conn = self.read_write.write_pool.acquire().await.map_err(|e| {
            e.into_error_model("Failed to acquire connection to schedule task instance")
        })?;
        schedule_task(&mut conn, None).await
    }

    fn config(&self) -> &TaskQueueConfig {
        &self.config
    }
}

async fn schedule_task(
    read_write: &mut PgConnection,
    single_task: Option<Uuid>,
) -> Result<(), IcebergErrorResponse> {
    // TODO: should we schedule more than one at a time?
    let task = sqlx::query!(
            r#"
            SELECT t.task_id, schedule, t.idempotency_key
            FROM task t
            WHERE ($1 = t.task_id or $1 is null) AND t.status = 'enabled' AND (next_tick < now() AT TIME ZONE 'UTC' AND next_tick is not null)
            ORDER BY next_tick
            FOR UPDATE SKIP LOCKED
            LIMIT 1"#,
            single_task,
        ).fetch_optional(&mut *read_write).await.map_err(|e| {
            tracing::error!(?e, "Failed to check for schedulable tasks.");
            e.into_error_model("Failed to check for schedulable tasks.")
        })?;

    if let Some(row) = task {
        let task_id = row.task_id;
        let sched = row
            .schedule
            .as_deref()
            .map(cron::Schedule::from_str)
            .transpose()
            .map_err(|e| {
                ErrorModel::internal(
                    "Failed to parse cron schedule from database.",
                    "InternalDatabaseError",
                    Some(Box::new(e)),
                )
            })?
            .map(|schedule| Schedule::Cron { schedule });

        schedule_task_instance(
            read_write,
            task_id,
            row.idempotency_key,
            sched.unwrap_or(Schedule::Immediate {}),
        )
        .await?;
    } else {
        tracing::debug!("No tasks to schedule.");
    }
    Ok(())
}

#[instrument(skip(conn))]
async fn schedule_task_instance(
    conn: &mut PgConnection,
    task_id: Uuid,
    idempotency_key: Uuid,
    schedule: Schedule,
) -> Result<(), IcebergErrorResponse> {
    let (next_tick, run_at, idempotency_key_data) = match schedule {
        Schedule::RunAt { date } => (None, Some(date), Some(date.to_rfc3339())),
        Schedule::Cron { schedule } => {
            let next = schedule.upcoming(Utc).next();
            (next, next, next.map(|dt| dt.to_rfc3339()))
        }
        Schedule::Immediate {} => (None, Some(Utc::now()), None),
    };

    let idempotency_key = if let Some(idempotency_key_data) = idempotency_key_data {
        Uuid::new_v5(&idempotency_key, idempotency_key_data.as_bytes())
    } else {
        idempotency_key
    };

    let has_next = next_tick.is_some();
    let task_instance_id = Uuid::now_v7();

    tracing::debug!("Scheduling task instance for task_id: '{task_id}', next_tick: '{next_tick:?}', run_at: '{run_at:?}', idempotency_key: '{idempotency_key}'");

    let it = sqlx::query_scalar!(
        r#"WITH updated_tasks AS (
           UPDATE task
           SET
               next_tick = $5,
               status = CASE
                   WHEN $6 THEN 'enabled'::schedule_status
                   ELSE 'disabled'::schedule_status
               END
           WHERE task_id = $1
           )
           INSERT INTO task_instance (
               task_id,
               task_instance_id,
               status,
               suspend_until,
               idempotency_key
           )
           VALUES ($1, $2, 'pending', $3, $4)
           ON CONFLICT ON CONSTRAINT task_instance_unique_idempotency_key DO NOTHING
           RETURNING task_instance_id"#,
        task_id,
        task_instance_id,
        run_at,
        idempotency_key,
        next_tick,
        has_next
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to schedule task instance");
        e.into_error_model("Failed to schedule task instance")
    })?;

    if let Some(it) = it {
        tracing::debug!("Scheduled task instance with id: '{it}'");
    } else {
        tracing::debug!("Task instance already exists.");
    }

    Ok(())
}

async fn queue_task(
    conn: &mut PgConnection,
    queue_name: &str,
    parent_task_id: Option<Uuid>,
    idempotency_key: Uuid,
    project_ident: ProjectIdent,
    schedul: Option<Schedule>,
) -> Result<Option<Uuid>, IcebergErrorResponse> {
    let sched = schedul.as_ref().and_then(|s| match s {
        Schedule::Cron { schedule } => Some(schedule.to_string()),
        _ => None,
    });

    let task_id = Uuid::now_v7();
    let inserted = sqlx::query_scalar!(
        r#"INSERT INTO task(
                task_id,
                queue_name,
                status,
                parent_task_id,
                idempotency_key,
                project_id,
                schedule)
        VALUES ($1, $2, 'enabled', $3, $4, $5, $6)
        ON CONFLICT ON CONSTRAINT unique_idempotency_key DO NOTHING
        RETURNING task_id"#,
        task_id,
        queue_name,
        parent_task_id,
        idempotency_key,
        *project_ident,
        sched,
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to queue task");
        e.into_error_model("failed queueing task")
    })?;

    if let Some(inserted) = inserted {
        tracing::debug!("Queued task with id '{inserted}'");
        schedule_task_instance(
            conn,
            inserted,
            idempotency_key,
            schedul.unwrap_or(Schedule::Immediate {}),
        )
        .await?;
    } else {
        tracing::debug!("Task already exists.");
    };
    Ok(inserted)
}

#[tracing::instrument]
async fn pick_task(
    pool: &PgPool,
    queue_name: &'static str,
    max_age: &sqlx::postgres::types::PgInterval,
) -> Result<Option<TaskInstance>, IcebergErrorResponse> {
    let x = sqlx::query_as!(
        TaskInstance,
        r#"
        WITH updated_task_instance AS (
            SELECT
                ti.task_id,
                ti.task_instance_id,
                t.queue_name,
                t.parent_task_id,
                t.project_id
            FROM task_instance ti
            JOIN task t ON ti.task_id = t.task_id
            WHERE (
                ti.status = 'pending'
                AND t.queue_name = $1
                AND (
                    (ti.suspend_until < now() AT TIME ZONE 'UTC')
                    OR (ti.suspend_until IS NULL)
                )
            ) OR (
                ti.status = 'running'
                AND (now() - ti.picked_up_at) > $2
            )
            ORDER BY ti.suspend_until NULLS FIRST
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE task_instance ti
        SET
            status = 'running',
            picked_up_at = now(),
            attempt = ti.attempt + 1
        FROM updated_task_instance
        WHERE ti.task_instance_id = updated_task_instance.task_instance_id
        RETURNING
            ti.task_id,
            ti.task_instance_id,
            ti.status as "status: TaskInstanceStatus",
            ti.picked_up_at,
            ti.attempt,
            (select parent_task_id from updated_task_instance),
            (select queue_name from updated_task_instance) as "queue_name!",
            (select project_id from updated_task_instance) as "project_ident!",
            '{}'::TEXT[] as "error_history!"
        "#,
        queue_name,
        max_age,
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to pick a task");
        e.into_error_model(format!("Failed to pick a '{queue_name}' task"))
    })?;

    if let Some(task) = x.as_ref() {
        tracing::info!("Picked up task: {:?}", task);
    }

    Ok(x)
}

async fn record_success(id: Uuid, pool: &PgPool) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"UPDATE task_instance
           SET status = 'success', completed_at = now()
           WHERE task_instance_id = $1
           RETURNING task_instance_id"#,
        id
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to record task success");
        e.into_error_model("failed to record task success")
    })?;

    Ok(())
}

async fn record_failure(
    conn: &PgPool,
    id: Uuid,
    n_retries: i32,
    details: &str,
) -> Result<(), IcebergErrorResponse> {
    let error_id = Uuid::now_v7();
    let _ = sqlx::query!(
        r#"
        WITH cte as (
            SELECT attempt >= $3 as should_fail
            FROM task_instance
            WHERE task_instance_id = $1
        ),
        cte2 as (
            INSERT INTO task_instance_error_history (task_instance_id, task_instance_error_history_id, error_details) VALUES ($1, $2, $4)
        )
        UPDATE task_instance
        SET status = CASE WHEN (select should_fail from cte) THEN 'failed'::task_status ELSE 'pending'::task_status END
        WHERE task_instance_id = $1
        RETURNING task_instance_id
        "#,
        id,
        error_id,
        n_retries,
        details
    )
    .fetch_one(conn)
    .await
    .map_err(|err| {
        tracing::error!(?err, "Failed to record task failure");
        err.into_error_model("failed to record task failure")
    })?;
    Ok(())
}

async fn delete_task(queue: &PgPool, filter: &TaskFilter) -> crate::api::Result<()> {
    match filter {
        TaskFilter::TaskIds(task_ids) => {
            let r = sqlx::query!(
                r#"DELETE FROM task
                    WHERE task_id = ANY($1)
                    AND (
                        -- Case 1: No task instances exist
                        NOT EXISTS (
                            SELECT 1
                            FROM task_instance
                            WHERE task_instance.task_id = task.task_id
                        )
                        OR
                        -- Case 2: All instances are in pending/failed/done state
                        NOT EXISTS (
                            SELECT 1
                            FROM task_instance
                            WHERE task_instance.task_id = task.task_id
                            AND status NOT IN ('pending', 'failed', 'success')
                        ));"#,
                &task_ids.iter().map(|s| **s).collect::<Vec<_>>(),
            )
            .execute(queue)
            .await
            .map_err(|e| {
                tracing::error!("Failed to disable Tasks for task_ids {task_ids:?}, {e:?}");
                e.into_error_model("Failed to disable Tasks for specified ids")
            })?;
            tracing::debug!("Deleted {}/{} tasks", r.rows_affected(), task_ids.len());
        }
    };

    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_tasks<'e, E: Executor<'e, Database = sqlx::Postgres>>(
    PaginationQuery {
        page_token,
        page_size,
    }: PaginationQuery,
    ListTasksRequest { project_ident }: ListTasksRequest,
    conn: E,
) -> crate::api::Result<ListTasksResponse> {
    let page_size = page_size.map_or(MAX_PAGE_SIZE, |i| i.clamp(1, MAX_PAGE_SIZE));

    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id): (_, Option<&Uuid>) = token
        .as_ref()
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
        .unzip();

    let r = sqlx::query!(
        r#"SELECT t.task_id as "task_id!",
       t.project_id as "project_id!",
       t.queue_name as "queue_name!",
       t.parent_task_id,
       t.created_at as "created_at!",
       t.updated_at,
       t.schedule,
       t.status as "status!: TaskStatus",
       CASE
           WHEN te.task_id IS NOT NULL THEN jsonb_build_object(
               'tabular_expirations',
               jsonb_build_object(
                   'task_data', row_to_json(te.*),
                   'tabular', (
                        SELECT row_to_json(subq)
                        FROM (
                            SELECT tab.name as tabular_name,
                                   n.namespace_name,
                                   w.warehouse_name
                            FROM tabular tab
                            JOIN namespace n ON tab.namespace_id = n.namespace_id
                            JOIN warehouse w ON n.warehouse_id = w.warehouse_id
                            WHERE tab.tabular_id = te.tabular_id
                        ) subq
                    )
               )
           )
           WHEN tp.task_id IS NOT NULL THEN jsonb_build_object(
               'tabular_purges',
               jsonb_build_object(
                   'task_data', row_to_json(tp.*),
                   'tabular', (
                        SELECT row_to_json(subq)
                        FROM (
                            SELECT tab.name as tabular_name,
                                   n.namespace_name,
                                   w.warehouse_name
                            FROM tabular tab
                            JOIN namespace n ON tab.namespace_id = n.namespace_id
                            JOIN warehouse w ON n.warehouse_id = w.warehouse_id
                            WHERE tab.tabular_id = te.tabular_id
                        ) subq
                    )
               )
           )
           WHEN s.task_id IS NOT NULL THEN jsonb_build_object(
               'statistics_task',
               jsonb_build_object(
                   'task_data', row_to_json(s.*),
                   'warehouse', (
                       SELECT row_to_json(ROW(w.warehouse_name))
                       FROM warehouse w
                       WHERE w.warehouse_id = s.warehouse_id
                   )
               )
           )
        END as details
        FROM task t
        LEFT JOIN tabular_expirations te ON t.task_id = te.task_id
        LEFT JOIN tabular_purges tp ON t.task_id = tp.task_id
        LEFT JOIN statistics_task s ON t.task_id = s.task_id
        WHERE (t.project_id = $1 OR $1 IS NULL)
              AND ((t.created_at > $2 OR $2 IS NULL) OR (t.created_at = $2 AND t.task_id > $3))
        LIMIT $4;"#,
        project_ident.map(Uuid::from),
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(conn)
    .await
    .map_err(|db| db.into_error_model("Failed to read tasks."))?;
    Ok(ListTasksResponse {
        tasks: r
            .into_iter()
            .map(|row| {
                Ok(Task {
                    task_id: row.task_id.into(),
                    queue_name: row.queue_name,
                    parent_task_id: row.parent_task_id,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    schedule: row.schedule.map(|s| s.parse()).transpose().map_err(|err| {
                        ErrorModel::internal(
                            "Failed to parse schedule from database.",
                            "InternalDatabaseError",
                            Some(Box::new(err)),
                        )
                    })?,
                    status: row.status,
                    project_id: row.project_id.into(),
                    details: row.details,
                })
            })
            .collect::<crate::api::Result<Vec<_>>>()?,
        continuation_token: None,
    })
}

pub(crate) async fn list_task_instances<'e, E: Executor<'e, Database = sqlx::Postgres>>(
    task_id: Option<TaskId>,
    PaginationQuery {
        page_token,
        page_size,
    }: PaginationQuery,
    conn: E,
) -> crate::api::Result<ListTaskInstancesResponse> {
    let page_size = page_size.map_or(MAX_PAGE_SIZE, |i| i.clamp(1, MAX_PAGE_SIZE));

    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id): (_, Option<&Uuid>) = token
        .as_ref()
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
        .unzip();
    tracing::debug!("Listing task instances for task_id: '{task_id:?}', page_size: '{page_size}'");
    let r = sqlx::query!(r#"SELECT
                                            ti.task_id,
                                            ti.task_instance_id,
                                            ti.attempt,
                                            ti.status as "status: TaskInstanceStatus",
                                            ti.last_error_details,
                                            ti.picked_up_at,
                                            ti.suspend_until,
                                            ti.completed_at,
                                            ti.created_at,
                                            ti.updated_at,
                                            t.project_id,
                                            t.queue_name,
                                            t.parent_task_id,
                                            array_agg(tieh.error_details) FILTER (WHERE tieh.error_details IS NOT NULL) as error_details
                                        FROM task_instance ti
                                        JOIN task t
                                            ON ti.task_id = t.task_id
                                        LEFT JOIN task_instance_error_history tieh
                                            ON ti.task_instance_id = tieh.task_instance_id
                                        WHERE
                                            (t.task_id = $1 OR $1 IS NULL)
                                            AND (
                                                (t.created_at > $2 OR $2 IS NULL)
                                                OR (t.created_at = $2 AND t.task_id > $3)
                                            )
                                        GROUP BY
                                            ti.task_id,
                                            ti.task_instance_id,
                                            ti.attempt,
                                            ti.status,
                                            ti.last_error_details,
                                            ti.picked_up_at,
                                            ti.suspend_until,
                                            ti.completed_at,
                                            ti.created_at,
                                            ti.updated_at,
                                            t.project_id,
                                            t.queue_name,
                                            t.parent_task_id
                                        LIMIT $4"#,
        task_id.map(Uuid::from),
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(conn)
    .await
    .map_err(|db| {
        tracing::debug!("Failed to read tasks. {db:?} {db}");
        db.into_error_model("Failed to read tasks.")
    })?;
    Ok(ListTaskInstancesResponse {
        tasks: r
            .into_iter()
            .map(|row| {
                Ok(TaskInstance {
                    task_id: row.task_id.into(),
                    task_instance_id: row.task_instance_id,
                    attempt: row.attempt,
                    status: row.status,
                    error_history: row.error_details.unwrap_or_default(),
                    picked_up_at: row.picked_up_at,
                    project_ident: row.project_id.into(),
                    queue_name: row.queue_name,
                    parent_task_id: row.parent_task_id,
                })
            })
            .collect::<crate::api::Result<Vec<_>>>()?,
        continuation_token: None,
    })
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    use crate::api::iceberg::types::PageToken;
    use crate::implementations::postgres::PostgresCatalog;
    use crate::service::Catalog;
    use crate::{WarehouseIdent, DEFAULT_PROJECT_ID};
    use sqlx::PgPool;
    use uuid::Uuid;

    const TEST_WAREHOUSE: WarehouseIdent = WarehouseIdent(Uuid::nil());

    pub(crate) async fn create_test_project(pool: PgPool) {
        let mut t = pool.begin().await.unwrap();
        PostgresCatalog::create_project(DEFAULT_PROJECT_ID.unwrap(), "bla".to_string(), &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_queue_task(pool: PgPool) {
        create_test_project(pool.clone()).await;
        let mut conn = pool.acquire().await.unwrap();

        let idempotency_key = Uuid::new_v5(&TEST_WAREHOUSE, b"test");

        let id = queue_task(
            &mut conn,
            "test",
            None,
            idempotency_key,
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap();
        let t = pick_task(
            &pool,
            "test",
            &sqlx::postgres::types::PgInterval {
                months: 0,
                days: 0,
                microseconds: 999_999,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(t.task_id, id.unwrap().into());
        assert!(queue_task(
            &mut conn,
            "test",
            None,
            idempotency_key,
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap()
        .is_none());

        let id3 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test2"),
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap();

        assert_ne!(id, id3);
    }

    pub(crate) fn setup(pool: PgPool, config: TaskQueueConfig) -> PgQueue {
        PgQueue::from_config(ReadWrite::from_pools(pool.clone(), pool), config).unwrap()
    }

    #[sqlx::test]
    async fn test_failed_tasks_are_put_back(pool: PgPool) {
        create_test_project(pool.clone()).await;
        let mut conn = pool.acquire().await.unwrap();

        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);
        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id.into());
        assert!(matches!(task.status, TaskInstanceStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, task.task_instance_id, 5, "test")
            .await
            .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id.into());
        assert!(matches!(task.status, TaskInstanceStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, task.task_instance_id, 2, "test")
            .await
            .unwrap();

        assert_eq!(
            pick_task(&pool, "test", &queue.max_age).await.unwrap(),
            None
        );
    }

    #[sqlx::test]
    async fn test_success_task_arent_polled(pool: PgPool) {
        create_test_project(pool.clone()).await;

        let mut conn = pool.acquire().await.unwrap();

        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id.into());
        assert!(matches!(task.status, TaskInstanceStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_success(task.task_instance_id, &pool).await.unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_scheduled_tasks_are_polled_later(pool: PgPool) {
        create_test_project(pool.clone()).await;
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            DEFAULT_PROJECT_ID.unwrap(),
            Some(Schedule::RunAt {
                date: Utc::now() + chrono::Duration::milliseconds(500),
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            pick_task(&pool, "test", &queue.max_age).await.unwrap(),
            None
        );
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id.into());
        assert!(matches!(task.status, TaskInstanceStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_stale_tasks_are_picked_up_again(pool: PgPool) {
        create_test_project(pool.clone()).await;
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig {
            max_age: chrono::Duration::milliseconds(500),
            ..Default::default()
        };
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id.into());
        assert!(matches!(task.status, TaskInstanceStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id.into());
        assert!(matches!(task.status, TaskInstanceStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_multiple_tasks(pool: PgPool) {
        create_test_project(pool.clone()).await;
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test2"),
            DEFAULT_PROJECT_ID.unwrap(),
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

        assert_eq!(task.task_id, id.into());
        assert!(matches!(task.status, TaskInstanceStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2.into());
        assert!(matches!(task2.status, TaskInstanceStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        record_success(task.task_instance_id, &pool).await.unwrap();
        record_success(task2.task_instance_id, &pool).await.unwrap();

        let tis = list_task_instances(
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            &pool,
        )
        .await
        .unwrap();

        assert_eq!(tis.tasks.len(), 2);
        assert_eq!(tis.tasks[0].status, TaskInstanceStatus::Success);
        assert_eq!(tis.tasks[1].status, TaskInstanceStatus::Success);
    }

    #[sqlx::test]
    async fn task_deletion_works(pool: PgPool) {
        create_test_project(pool.clone()).await;
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        delete_task(
            &queue.read_write.write_pool,
            &TaskFilter::TaskIds(vec![id.into()]),
        )
        .await
        .unwrap();

        assert_eq!(
            pick_task(&pool, "test", &queue.max_age).await.unwrap(),
            None
        );

        let tasks = list_tasks(
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            ListTasksRequest {
                project_ident: None,
            },
            &pool,
        )
        .await
        .unwrap();

        assert_eq!(tasks.tasks.len(), 0);

        let task_instances = list_task_instances(
            Some(id.into()),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            &pool,
        )
        .await
        .unwrap();

        assert_eq!(task_instances.tasks.len(), 0);
    }

    #[sqlx::test]
    async fn cannot_delete_task_with_running_instance(pool: PgPool) {
        create_test_project(pool.clone()).await;
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            DEFAULT_PROJECT_ID.unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let ti = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(ti.task_id, id.into());
        assert!(matches!(ti.status, TaskInstanceStatus::Running));

        let t = list_tasks(
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            ListTasksRequest {
                project_ident: None,
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(t.tasks.len(), 1);
        assert_eq!(t.tasks[0].task_id, id.into());
        let tis = list_task_instances(
            Some(id.into()),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(tis.tasks.len(), 1);
        assert_eq!(tis.tasks[0].task_id, id.into());
        assert_eq!(tis.tasks[0].status, TaskInstanceStatus::Running);
        assert_eq!(tis.tasks[0].task_instance_id, ti.task_instance_id);
    }
}
