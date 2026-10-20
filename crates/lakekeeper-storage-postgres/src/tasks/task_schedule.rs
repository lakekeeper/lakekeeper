use std::sync::Arc;

use chrono::{DateTime, SubsecRound, Utc};
use lakekeeper::{
    ProjectId, WarehouseId,
    service::{
        CatalogBackendError,
        tasks::{
            EnqueueOutcome, MaterializedTick, ProjectTaskScheduleState, TaskQueueName,
            TaskSchedule, TaskScheduleCondition, TaskScheduleConditionFailedError,
            TaskScheduleFilter, TaskScheduleId, TaskScheduleNotFoundError,
            TaskScheduleSpecVersionUnsupportedError, TaskScheduleStoreError, TaskScheduleUpsert,
            TriggerTaskScheduleError, UpsertTaskScheduleError, due_tick, first_tick_at_or_after,
        },
    },
};
use sqlx::PgConnection;
use uuid::Uuid;

use super::{MAX_WRITE_ATTEMPTS, RetriesExhaustedError, TaskEntityTypeDB, lookup_active_task};
use crate::dbutils::DBErrorHandler;

/// The `spec_version` this server writes, lists and materializes: interval and anchor.
const SPEC_VERSION: i16 = 1;

#[derive(Debug)]
struct TaskScheduleRow {
    schedule_id: Uuid,
    project_id: String,
    warehouse_id: Option<Uuid>,
    queue_name: String,
    schedule_key: String,
    interval_seconds: i64,
    anchor: DateTime<Utc>,
    payload: serde_json::Value,
    enabled: bool,
    next_run_at: DateTime<Utc>,
    missed_runs: i64,
    created_at: DateTime<Utc>,
    updated_at: Option<DateTime<Utc>>,
}

impl From<TaskScheduleRow> for TaskSchedule {
    fn from(row: TaskScheduleRow) -> Self {
        TaskSchedule {
            schedule_id: row.schedule_id.into(),
            project_id: Arc::new(ProjectId::from_db_unchecked(row.project_id)),
            warehouse_id: row.warehouse_id.map(WarehouseId::from),
            queue_name: row.queue_name.into(),
            schedule_key: row.schedule_key,
            interval: chrono::Duration::seconds(row.interval_seconds),
            anchor: row.anchor,
            payload: row.payload,
            enabled: row.enabled,
            next_run_at: row.next_run_at,
            missed_runs: row.missed_runs,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// The entity type of the tasks a schedule produces.
fn schedule_task_entity_type(warehouse_id: Option<Uuid>) -> TaskEntityTypeDB {
    if warehouse_id.is_some() {
        TaskEntityTypeDB::Warehouse
    } else {
        TaskEntityTypeDB::Project
    }
}

pub(crate) async fn upsert_task_schedule(
    conn: &mut PgConnection,
    schedule: TaskScheduleUpsert,
) -> Result<TaskSchedule, UpsertTaskScheduleError> {
    let TaskScheduleUpsert {
        project_id,
        warehouse_id,
        queue_name,
        schedule_key,
        interval,
        anchor,
        payload,
        enabled,
        condition,
    } = schedule;
    // Stored and compared at microsecond precision.
    let anchor = anchor.map(|anchor| anchor.trunc_subsecs(6));
    let interval_seconds = interval.num_seconds();
    let warehouse_id = warehouse_id.map(Uuid::from);
    for _ in 0..MAX_WRITE_ATTEMPTS {
        let stored = sqlx::query!(
            r#"
            SELECT
                schedule_id,
                anchor,
                interval_seconds,
                enabled,
                next_run_at,
                spec_version,
                now() AS "now!"
            FROM task_schedule
            WHERE schedule_id = task_schedule_id($1, $2, $3, $4)
            FOR UPDATE
            "#,
            project_id.as_str(),
            warehouse_id,
            queue_name.as_str(),
            &schedule_key,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;
        let now = match &stored {
            Some(stored) => stored.now,
            None => sqlx::query_scalar!(r#"SELECT now() AS "now!""#)
                .fetch_one(&mut *conn)
                .await
                .map_err(DBErrorHandler::into_catalog_backend_error)?,
        };
        if let Some(stored) = &stored
            && stored.spec_version > SPEC_VERSION
        {
            return Err(TaskScheduleSpecVersionUnsupportedError {
                queue_name,
                schedule_key,
                stack: Vec::new(),
            }
            .into());
        }
        let condition_holds = match (condition, &stored) {
            (None, _) => true,
            (Some(TaskScheduleCondition::Absent), stored) => stored.is_none(),
            (Some(TaskScheduleCondition::Interval(expected)), Some(stored)) => {
                stored.interval_seconds == expected.num_seconds()
            }
            (Some(TaskScheduleCondition::Interval(_)), None) => false,
        };
        if !condition_holds {
            return Err(TaskScheduleConditionFailedError {
                queue_name,
                schedule_key,
                stack: Vec::new(),
            }
            .into());
        }

        // A new schedule, a changed anchor or interval, or enabling a disabled schedule times
        // the next run to the first tick at or after now. Anything else keeps it.
        let new_anchor = anchor
            .or(stored.as_ref().map(|stored| stored.anchor))
            .unwrap_or(now);
        let next_run_at = match &stored {
            Some(stored)
                if stored.interval_seconds == interval_seconds
                    && stored.anchor == new_anchor
                    && (stored.enabled || enabled != Some(true)) =>
            {
                stored.next_run_at
            }
            _ => first_tick_at_or_after(new_anchor, interval_seconds, now)
                .map_err(CatalogBackendError::new_unexpected)?,
        };

        if let Some(stored) = stored {
            let row = sqlx::query_as!(
                TaskScheduleRow,
                r#"
                UPDATE task_schedule
                SET interval_seconds = $2,
                    anchor = $3,
                    payload = COALESCE($4, payload),
                    enabled = COALESCE($5, enabled),
                    next_run_at = $6,
                    spec_version = $7
                WHERE schedule_id = $1
                RETURNING
                    schedule_id,
                    project_id,
                    warehouse_id,
                    queue_name,
                    schedule_key,
                    interval_seconds,
                    anchor,
                    payload,
                    enabled,
                    next_run_at,
                    missed_runs,
                    created_at,
                    updated_at
                "#,
                stored.schedule_id,
                interval_seconds,
                new_anchor,
                payload,
                enabled,
                next_run_at,
                SPEC_VERSION,
            )
            .fetch_one(&mut *conn)
            .await
            .map_err(DBErrorHandler::into_catalog_backend_error)?;
            return Ok(row.into());
        }

        let row = sqlx::query_as!(
            TaskScheduleRow,
            r#"
            INSERT INTO task_schedule (
                schedule_id,
                project_id,
                warehouse_id,
                queue_name,
                schedule_key,
                interval_seconds,
                anchor,
                payload,
                enabled,
                next_run_at,
                spec_version
            )
            VALUES (
                task_schedule_id($1, $2, $3, $4),
                $1, $2, $3, $4, $5, $6,
                COALESCE($7, '{}'::jsonb),
                COALESCE($8, true),
                $9,
                $10
            )
            ON CONFLICT (schedule_id) DO NOTHING
            RETURNING
                schedule_id,
                project_id,
                warehouse_id,
                queue_name,
                schedule_key,
                interval_seconds,
                anchor,
                payload,
                enabled,
                next_run_at,
                missed_runs,
                created_at,
                updated_at
            "#,
            project_id.as_str(),
            warehouse_id,
            queue_name.as_str(),
            &schedule_key,
            interval_seconds,
            new_anchor,
            payload,
            enabled,
            next_run_at,
            SPEC_VERSION,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;
        if let Some(row) = row {
            return Ok(row.into());
        }
        // Created concurrently. Read it again, locked.
    }
    Err(RetriesExhaustedError::backend_error().into())
}

pub(crate) async fn list_task_schedules(
    conn: &mut PgConnection,
    filter: &TaskScheduleFilter,
) -> Result<Vec<TaskSchedule>, TaskScheduleStoreError> {
    let rows = sqlx::query_as!(
        TaskScheduleRow,
        r#"
        SELECT
            schedule_id,
            project_id,
            warehouse_id,
            queue_name,
            schedule_key,
            interval_seconds,
            anchor,
            payload,
            enabled,
            next_run_at,
            missed_runs,
            created_at,
            updated_at
        FROM task_schedule
        WHERE project_id = $1
            AND ($2::uuid IS NULL OR warehouse_id = $2)
            AND ($3::text IS NULL OR queue_name = $3)
            AND ($4::text IS NULL OR schedule_key = $4)
            AND spec_version <= $5
        ORDER BY schedule_id
        "#,
        filter.project_id.as_str(),
        filter.warehouse_id.map(Uuid::from),
        filter.queue_name.as_ref().map(|q| q.as_str()),
        filter.schedule_key.as_deref(),
        SPEC_VERSION,
    )
    .fetch_all(conn)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(rows.into_iter().map(Into::into).collect())
}

pub(crate) async fn delete_task_schedule(
    conn: &mut PgConnection,
    schedule_id: TaskScheduleId,
) -> Result<bool, TaskScheduleStoreError> {
    let deleted = sqlx::query!(
        "DELETE FROM task_schedule WHERE schedule_id = $1",
        *schedule_id
    )
    .execute(conn)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;
    Ok(deleted.rows_affected() == 1)
}

pub(crate) async fn list_project_task_schedule_states(
    conn: &mut PgConnection,
    queue_name: &TaskQueueName,
    schedule_key: &str,
) -> Result<Vec<ProjectTaskScheduleState>, TaskScheduleStoreError> {
    // Locked before the configs are read: a config change locks the schedule to apply itself, so
    // it either commits before the read below or waits for this transaction.
    sqlx::query!(
        r#"
        SELECT schedule_id FROM task_schedule
        WHERE queue_name = $1 AND schedule_key = $2 AND warehouse_id IS NULL
        ORDER BY schedule_id
        FOR UPDATE
        "#,
        queue_name.as_str(),
        schedule_key,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;
    let rows = sqlx::query!(
        r#"
        SELECT
            p.project_id,
            s.interval_seconds AS "interval_seconds?",
            c.config AS "queue_config?"
        FROM project p
        LEFT JOIN task_schedule s
            ON s.schedule_id = task_schedule_id(p.project_id, NULL, $1, $2)
        LEFT JOIN task_config c
            ON c.project_id = p.project_id AND c.warehouse_id IS NULL AND c.queue_name = $1
        ORDER BY p.project_id
        "#,
        queue_name.as_str(),
        schedule_key,
    )
    .fetch_all(conn)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;
    Ok(rows
        .into_iter()
        .map(|row| ProjectTaskScheduleState {
            project_id: Arc::new(ProjectId::from_db_unchecked(row.project_id)),
            interval: row.interval_seconds.map(chrono::Duration::seconds),
            queue_config: row.queue_config,
        })
        .collect())
}

pub(crate) async fn delete_warehouse_task_schedules(
    conn: &mut PgConnection,
    warehouse_id: WarehouseId,
) -> Result<(), TaskScheduleStoreError> {
    sqlx::query!(
        "DELETE FROM task_schedule WHERE warehouse_id = $1",
        *warehouse_id
    )
    .execute(conn)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;
    Ok(())
}

pub(crate) async fn trigger_task_schedule(
    conn: &mut PgConnection,
    schedule_id: TaskScheduleId,
) -> Result<EnqueueOutcome, TriggerTaskScheduleError> {
    for _ in 0..MAX_WRITE_ATTEMPTS {
        let row = sqlx::query!(
            r#"
            WITH schedule AS (
                SELECT schedule_id, project_id, warehouse_id, queue_name, payload
                FROM task_schedule
                WHERE schedule_id = $1
            ),
            inserted AS (
                INSERT INTO task (
                    task_id,
                    queue_name,
                    status,
                    warehouse_id,
                    scheduled_for,
                    task_data,
                    entity_id,
                    entity_type,
                    project_id
                )
                SELECT
                    $2,
                    s.queue_name,
                    'scheduled',
                    s.warehouse_id,
                    now(),
                    s.payload,
                    s.schedule_id,
                    CASE WHEN s.warehouse_id IS NULL THEN 'project'::entity_type
                        ELSE 'warehouse'::entity_type END,
                    s.project_id
                FROM schedule s
                ON CONFLICT (project_id, warehouse_id, entity_type, entity_id, queue_name) DO NOTHING
                RETURNING task_id
            )
            SELECT
                s.project_id,
                s.warehouse_id,
                s.queue_name,
                (SELECT task_id FROM inserted) AS inserted_task_id
            FROM schedule s
            "#,
            *schedule_id,
            Uuid::now_v7(),
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;

        let Some(row) = row else {
            return Err(TaskScheduleNotFoundError {
                schedule_id,
                stack: Vec::new(),
            }
            .into());
        };
        if let Some(task_id) = row.inserted_task_id {
            return Ok(EnqueueOutcome::Inserted(task_id.into()));
        }
        if let Some((task_id, status)) = lookup_active_task(
            &mut *conn,
            &row.project_id,
            row.warehouse_id,
            schedule_task_entity_type(row.warehouse_id),
            Some(*schedule_id),
            &row.queue_name,
        )
        .await?
        {
            return Ok(EnqueueOutcome::Existing { task_id, status });
        }
        // The active instance finished between the insert and the lookup. Try again.
    }
    Err(RetriesExhaustedError::backend_error()
        .append_detail(format!("Task schedule `{schedule_id}`"))
        .into())
}

// A project or warehouse deleted while this transaction holds a due schedule row can deadlock
// with it; Postgres aborts one side and the materializer retries on its next pass.
pub(crate) async fn materialize_due_task_schedules(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<MaterializedTick>, TaskScheduleStoreError> {
    // A schedule whose previous instance is active is not claimed, so it stays due. Claimed rows
    // stay locked until the transaction ends.
    let due = sqlx::query!(
        r#"
        SELECT s.schedule_id, s.anchor, s.interval_seconds, s.next_run_at, now() AS "now!"
        FROM task_schedule s
        WHERE s.enabled
            AND s.next_run_at <= now()
            AND s.spec_version <= $2
            -- The schedule id is derived from the scope, so it identifies the instance.
            AND NOT EXISTS (
                SELECT 1
                FROM task t
                WHERE t.entity_id = s.schedule_id
                    AND t.entity_type IN ('project', 'warehouse')
                    AND t.queue_name = s.queue_name
            )
        ORDER BY s.next_run_at, s.schedule_id
        LIMIT $1
        FOR UPDATE OF s SKIP LOCKED
        "#,
        limit.max(0),
        SPEC_VERSION,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;
    if due.is_empty() {
        return Ok(Vec::new());
    }

    // One run covers every tick since `next_run_at`; it is scheduled for the latest of them.
    let mut schedule_ids = Vec::with_capacity(due.len());
    let mut task_ids = Vec::with_capacity(due.len());
    let mut ticks = Vec::with_capacity(due.len());
    let mut following = Vec::with_capacity(due.len());
    let mut missed_runs = Vec::with_capacity(due.len());
    for row in due {
        let due_tick = due_tick(row.anchor, row.interval_seconds, row.next_run_at, row.now)
            .map_err(|e| {
                CatalogBackendError::new_unexpected(e)
                    .append_detail(format!("Task schedule `{}`", row.schedule_id))
            })?;
        schedule_ids.push(row.schedule_id);
        task_ids.push(Uuid::now_v7());
        ticks.push(due_tick.tick);
        following.push(due_tick.following);
        missed_runs.push(due_tick.missed_runs);
    }

    let rows = sqlx::query!(
        r#"
        WITH due AS (
            SELECT *
            FROM unnest($1::uuid[], $2::uuid[], $3::timestamptz[], $4::timestamptz[], $5::bigint[])
                AS d(schedule_id, task_id, tick, following, missed)
        ),
        inserted AS (
            INSERT INTO task (
                task_id,
                queue_name,
                status,
                warehouse_id,
                scheduled_for,
                task_data,
                entity_id,
                entity_type,
                project_id
            )
            SELECT
                d.task_id,
                s.queue_name,
                'scheduled',
                s.warehouse_id,
                d.tick,
                s.payload,
                s.schedule_id,
                CASE WHEN s.warehouse_id IS NULL THEN 'project'::entity_type
                    ELSE 'warehouse'::entity_type END,
                s.project_id
            FROM due d
            JOIN task_schedule s ON s.schedule_id = d.schedule_id
            ON CONFLICT (project_id, warehouse_id, entity_type, entity_id, queue_name) DO NOTHING
            RETURNING task_id, entity_id, scheduled_for
        )
        UPDATE task_schedule s
        SET next_run_at = d.following,
            missed_runs = s.missed_runs + d.missed
        FROM due d
        JOIN inserted i ON i.entity_id = d.schedule_id
        WHERE s.schedule_id = d.schedule_id
        RETURNING
            s.schedule_id,
            s.next_run_at,
            i.task_id,
            i.scheduled_for AS tick,
            d.missed AS "missed!"
        "#,
        &schedule_ids,
        &task_ids,
        &ticks,
        &following,
        &missed_runs,
    )
    .fetch_all(conn)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(rows
        .into_iter()
        .map(|r| MaterializedTick {
            schedule_id: r.schedule_id.into(),
            task_id: r.task_id.into(),
            tick: r.tick,
            missed_runs: r.missed,
            next_run_at: r.next_run_at,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashSet};

    use lakekeeper::{
        WarehouseId,
        service::{
            ArcProjectId, CatalogTaskOps, Transaction,
            tasks::{
                DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT, EnqueueOutcome, FailedAttempt, FailureKind,
                ScheduleTaskMetadata, TaskEntity, TaskId, TaskInput, TaskIntermediateStatus,
                TaskQueueName, TaskSchedule, TaskScheduleFilter, TaskScheduleId,
                TaskScheduleIntervalOutOfRangeError, TaskScheduleUpsert, TriggerTaskScheduleError,
                UpsertTaskScheduleError, WarehouseTaskEntityId, default_retry_backoff,
            },
        },
    };
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        PostgresBackend, PostgresTransaction,
        tasks::{
            pick_task, record_failure, record_success,
            test::{retry_delay, setup_two_warehouses, setup_warehouse},
        },
    };

    fn queue() -> TaskQueueName {
        TaskQueueName::from(format!("test-schedule-{}", Uuid::now_v7()))
    }

    /// Start from no schedules: project creation in the test setup adds a cleanup schedule.
    async fn clear_schedules(pool: &PgPool) {
        sqlx::query("DELETE FROM task_schedule")
            .execute(pool)
            .await
            .unwrap();
    }

    /// Moves every schedule one step closer to its ticks, as if `seconds` passed.
    async fn advance(pool: &PgPool, seconds: i64) {
        sqlx::query(
            "UPDATE task_schedule SET anchor = anchor - make_interval(secs => $1), \
             next_run_at = next_run_at - make_interval(secs => $1)",
        )
        .bind(seconds as f64)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn upsert(
        pool: &PgPool,
        project_id: &ArcProjectId,
        warehouse_id: Option<WarehouseId>,
        queue_name: &TaskQueueName,
        key: &str,
        interval: chrono::Duration,
    ) -> TaskSchedule {
        let mut t = pool.begin().await.unwrap();
        let schedule = upsert_task_schedule(
            &mut t,
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .warehouse_id(warehouse_id)
                .queue_name(queue_name.clone())
                .schedule_key(key)
                .interval(interval)
                .build(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        schedule
    }

    async fn materialize_ticks(pool: &PgPool) -> Vec<MaterializedTick> {
        let mut t = pool.begin().await.unwrap();
        let ticks = materialize_due_task_schedules(&mut t, 1000).await.unwrap();
        t.commit().await.unwrap();
        ticks
    }

    async fn materialize(pool: &PgPool) -> BTreeMap<TaskScheduleId, TaskId> {
        materialize_ticks(pool)
            .await
            .into_iter()
            .map(|t| (t.schedule_id, t.task_id))
            .collect()
    }

    async fn get(pool: &PgPool, schedule: &TaskSchedule) -> TaskSchedule {
        let mut conn = pool.acquire().await.unwrap();
        let mut found = list_task_schedules(
            &mut conn,
            &TaskScheduleFilter::builder()
                .project_id(schedule.project_id.clone())
                .warehouse_id(schedule.warehouse_id)
                .queue_name(Some(schedule.queue_name.clone()))
                .schedule_key(schedule.schedule_key.clone())
                .build(),
        )
        .await
        .unwrap();
        assert_eq!(found.len(), 1);
        found.pop().unwrap()
    }

    async fn active_task_ids(pool: &PgPool, schedule_id: TaskScheduleId) -> Vec<TaskId> {
        sqlx::query_scalar::<_, Uuid>("SELECT task_id FROM task WHERE entity_id = $1")
            .bind(*schedule_id)
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(TaskId::from)
            .collect()
    }

    async fn complete(pool: &PgPool, queue_name: &TaskQueueName, expected: TaskId) {
        let task = pick_task(pool, queue_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), expected);
        record_success(&task, &mut pool.acquire().await.unwrap(), None)
            .await
            .unwrap();
    }

    /// Pick and complete `n` tasks of the queue; returns their ids.
    async fn complete_all(pool: &PgPool, queue_name: &TaskQueueName, n: usize) -> HashSet<TaskId> {
        let mut done = HashSet::new();
        for _ in 0..n {
            let task = pick_task(pool, queue_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();
            record_success(&task, &mut pool.acquire().await.unwrap(), None)
                .await
                .unwrap();
            done.insert(task.task_id());
        }
        done
    }

    #[sqlx::test]
    async fn test_schedule_tasks_are_warehouse_scoped_and_name_their_schedule(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let schedule = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "a",
            chrono::Duration::minutes(1),
        )
        .await;
        let ticks = materialize(&pool).await;
        let task_id = ticks[&schedule.schedule_id];

        let task = pick_task(&pool, &q, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), task_id);
        assert_eq!(
            task.task_metadata.entity,
            TaskEntity::Warehouse { warehouse_id }
        );
        assert_eq!(task.task_metadata.schedule_id, Some(schedule.schedule_id));
        assert_eq!(task.task_metadata.parent_task_id, None);
        assert_eq!(task.data, serde_json::json!({}));

        // The warehouse entity filter lists it.
        let state = crate::CatalogState::from_pools(pool.clone(), pool.clone());
        let mut t = PostgresTransaction::begin_read(state).await.unwrap();
        let listed = PostgresBackend::list_tasks(
            &lakekeeper::service::tasks::TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &lakekeeper::api::management::v1::tasks::ListTasksRequest::builder()
                .entities(Some(vec![
                    lakekeeper::api::management::v1::tasks::WarehouseTaskEntityFilter::Warehouse,
                ]))
                .queue_name(Some(vec![q]))
                .build(),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(
            listed
                .tasks
                .iter()
                .map(|task| (task.task_id(), task.task_metadata.schedule_id))
                .collect::<Vec<_>>(),
            vec![(task_id, Some(schedule.schedule_id))]
        );
    }

    #[sqlx::test]
    async fn test_two_schedules_on_one_queue_in_one_warehouse_run_at_their_own_intervals(
        pool: PgPool,
    ) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let every_minute = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "every-minute",
            chrono::Duration::minutes(1),
        )
        .await;
        let every_two = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "every-two-minutes",
            chrono::Duration::minutes(2),
        )
        .await;

        // t = 0: both are due.
        let ticks = materialize(&pool).await;
        assert_eq!(
            ticks.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([every_minute.schedule_id, every_two.schedule_id])
        );
        let a0 = ticks[&every_minute.schedule_id];
        let b0 = ticks[&every_two.schedule_id];
        assert_ne!(a0, b0);
        assert_eq!(
            active_task_ids(&pool, every_minute.schedule_id).await,
            vec![a0]
        );
        assert_eq!(
            active_task_ids(&pool, every_two.schedule_id).await,
            vec![b0]
        );
        assert_eq!(complete_all(&pool, &q, 2).await, HashSet::from([a0, b0]));
        // Within the same minute nothing more is due.
        assert_eq!(materialize(&pool).await, BTreeMap::new());

        // t = 1 min: only the one-minute schedule is due.
        advance(&pool, 60).await;
        let ticks = materialize(&pool).await;
        assert_eq!(
            ticks.keys().copied().collect::<Vec<_>>(),
            vec![every_minute.schedule_id]
        );
        let a1 = ticks[&every_minute.schedule_id];
        assert_eq!(
            active_task_ids(&pool, every_minute.schedule_id).await,
            vec![a1]
        );
        assert_eq!(active_task_ids(&pool, every_two.schedule_id).await, vec![]);
        complete(&pool, &q, a1).await;

        // t = 2 min: both again.
        advance(&pool, 60).await;
        let ticks = materialize(&pool).await;
        assert_eq!(
            ticks.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([every_minute.schedule_id, every_two.schedule_id])
        );
        assert_eq!(
            active_task_ids(&pool, every_minute.schedule_id).await,
            vec![ticks[&every_minute.schedule_id]]
        );
        assert_eq!(
            active_task_ids(&pool, every_two.schedule_id).await,
            vec![ticks[&every_two.schedule_id]]
        );
    }

    #[sqlx::test]
    async fn test_interval_below_one_minute_is_refused(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let mut t = PostgresTransaction::begin_write(crate::CatalogState::from_pools(
            pool.clone(),
            pool.clone(),
        ))
        .await
        .unwrap();
        let err = PostgresBackend::upsert_task_schedule(
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .warehouse_id(Some(warehouse_id))
                .queue_name(queue())
                .schedule_key("out-of-range")
                .interval(chrono::Duration::seconds(59))
                .build(),
            t.transaction(),
        )
        .await
        .unwrap_err();
        let UpsertTaskScheduleError::TaskScheduleIntervalOutOfRangeError(
            TaskScheduleIntervalOutOfRangeError {
                interval_seconds,
                minimum_seconds,
                maximum_seconds,
                ..
            },
        ) = err
        else {
            panic!("expected TaskScheduleIntervalOutOfRangeError, got {err:?}");
        };
        assert_eq!(
            (interval_seconds, minimum_seconds, maximum_seconds),
            (59, 60, 3_153_600_000)
        );
    }

    #[sqlx::test]
    async fn test_concurrent_first_upserts_create_one_schedule(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = queue();
        let spec = |minutes| {
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .warehouse_id(Some(warehouse_id))
                .queue_name(q.clone())
                .schedule_key("concurrent")
                .interval(chrono::Duration::minutes(minutes))
                .build()
        };
        let mut first = pool.begin().await.unwrap();
        let created = upsert_task_schedule(&mut first, spec(5)).await.unwrap();

        // The second create waits on the first one's uncommitted row.
        let second = tokio::spawn({
            let pool = pool.clone();
            let spec = spec(10);
            async move {
                let mut t = pool.begin().await.unwrap();
                let schedule = upsert_task_schedule(&mut t, spec).await.unwrap();
                t.commit().await.unwrap();
                schedule
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity \
                     WHERE datname = current_database() AND wait_event_type = 'Lock'",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                if waiting == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second upsert never waited on the first");
        first.commit().await.unwrap();

        // It finds the committed row and updates it.
        let updated = second.await.unwrap();
        assert_eq!(
            (updated.schedule_id, updated.interval, updated.created_at),
            (
                created.schedule_id,
                chrono::Duration::minutes(10),
                created.created_at
            )
        );
        assert_eq!(get(&pool, &created).await, updated);
    }

    #[sqlx::test]
    async fn test_rows_load_with_an_empty_search_path(pool: PgPool) {
        // `pg_restore` loads rows with an empty search path, so the id check must not depend on
        // the caller's.
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let mut t = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL search_path = ''")
            .execute(&mut *t)
            .await
            .unwrap();
        let id: Uuid = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            r#"INSERT INTO "{schema}".task_schedule (schedule_id, project_id, warehouse_id,
             queue_name, schedule_key, interval_seconds, anchor, payload, next_run_at)
             VALUES ("{schema}".task_schedule_id($1, $2, 'q', 'k'), $1, $2, 'q', 'k', 60, now(),
             '{{}}', now()) RETURNING schedule_id"#
        )))
        .bind(project_id.as_str())
        .bind(*warehouse_id)
        .fetch_one(&mut *t)
        .await
        .unwrap();
        t.commit().await.unwrap();
        let expected: Uuid = sqlx::query_scalar("SELECT task_schedule_id($1, $2, 'q', 'k')")
            .bind(project_id.as_str())
            .bind(*warehouse_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(id, expected);
    }

    #[sqlx::test]
    async fn test_interval_check_refuses_rows_over_100_years(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let err = sqlx::query(
            "INSERT INTO task_schedule (schedule_id, project_id, warehouse_id, queue_name, \
             schedule_key, interval_seconds, anchor, payload, next_run_at) \
             VALUES (task_schedule_id($1, $2, 'q', 'k'), $1, $2, 'q', 'k', 3153600001, now(), \
             '{}', now())",
        )
        .bind(project_id.as_str())
        .bind(*warehouse_id)
        .execute(&pool)
        .await
        .unwrap_err();
        let db_err = err.as_database_error().unwrap();
        assert_eq!(db_err.code().as_deref(), Some("23514"));
        assert_eq!(db_err.constraint(), Some("task_schedule_interval_check"));
    }

    #[sqlx::test]
    async fn test_id_check_refuses_an_id_not_derived_from_the_scope(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let err = sqlx::query(
            "INSERT INTO task_schedule (schedule_id, project_id, warehouse_id, queue_name, \
             schedule_key, interval_seconds, anchor, payload, next_run_at) \
             VALUES ($1, $2, $3, 'q', 'k', 60, now(), '{}', now())",
        )
        .bind(Uuid::now_v7())
        .bind(project_id.as_str())
        .bind(*warehouse_id)
        .execute(&pool)
        .await
        .unwrap_err();
        let db_err = err.as_database_error().unwrap();
        assert_eq!(db_err.code().as_deref(), Some("23514"));
        assert_eq!(db_err.constraint(), Some("task_schedule_id_check"));
    }

    #[sqlx::test]
    async fn test_warehouse_schedule_must_belong_to_its_warehouses_project(pool: PgPool) {
        let (warehouse_id, _) = setup_warehouse(pool.clone()).await;
        sqlx::query("INSERT INTO project (project_id, project_name) VALUES ('other', 'other')")
            .execute(&pool)
            .await
            .unwrap();
        let err = sqlx::query(
            "INSERT INTO task_schedule (schedule_id, project_id, warehouse_id, queue_name, \
             schedule_key, interval_seconds, anchor, payload, next_run_at) \
             VALUES (task_schedule_id('other', $1, 'q', 'k'), 'other', $1, 'q', 'k', 60, now(), \
             '{}', now())",
        )
        .bind(*warehouse_id)
        .execute(&pool)
        .await
        .unwrap_err();
        let db_err = err.as_database_error().unwrap();
        assert_eq!(db_err.code().as_deref(), Some("23503"));
        assert_eq!(db_err.constraint(), Some("task_schedule_warehouse_fk"));
    }

    #[sqlx::test]
    async fn test_upsert_writes_only_when_its_condition_holds(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = queue();
        let write = |interval: chrono::Duration, condition| {
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .warehouse_id(Some(warehouse_id))
                .queue_name(q.clone())
                .schedule_key("k")
                .interval(interval)
                .condition(Some(condition))
                .build()
        };
        let hours = chrono::Duration::hours;
        let mut t = pool.begin().await.unwrap();
        let err = upsert_task_schedule(
            &mut t,
            write(hours(1), TaskScheduleCondition::Interval(hours(1))),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                UpsertTaskScheduleError::TaskScheduleConditionFailedError(_)
            ),
            "{err:?}"
        );
        let created = upsert_task_schedule(&mut t, write(hours(1), TaskScheduleCondition::Absent))
            .await
            .unwrap();
        assert_eq!(created.interval, hours(1));
        for refused in [
            write(hours(2), TaskScheduleCondition::Absent),
            write(hours(2), TaskScheduleCondition::Interval(hours(3))),
        ] {
            let err = upsert_task_schedule(&mut t, refused).await.unwrap_err();
            let UpsertTaskScheduleError::TaskScheduleConditionFailedError(err) = err else {
                panic!("expected TaskScheduleConditionFailedError, got {err:?}");
            };
            assert_eq!(
                (err.queue_name, err.schedule_key.as_str()),
                (q.clone(), "k")
            );
        }
        let updated = upsert_task_schedule(
            &mut t,
            write(hours(2), TaskScheduleCondition::Interval(hours(1))),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(updated.interval, hours(2));
        assert_eq!(get(&pool, &created).await.interval, hours(2));
    }

    #[sqlx::test]
    async fn test_project_schedule_states_list_every_project_once(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q = queue();
        upsert(
            &pool,
            &project_id,
            None,
            &q,
            "k",
            chrono::Duration::hours(2),
        )
        .await;
        // A warehouse schedule with the same queue and key is not the project's.
        upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "k",
            chrono::Duration::hours(3),
        )
        .await;
        sqlx::query(
            "INSERT INTO task_config (task_config_id, project_id, warehouse_id, queue_name, config) \
             VALUES ($1, $2, NULL, $3, '{\"a\": 1}')",
        )
        .bind(Uuid::now_v7())
        .bind(project_id.as_str())
        .bind(q.as_str())
        .execute(&pool)
        .await
        .unwrap();
        // Neither is a warehouse's config of the queue.
        sqlx::query(
            "INSERT INTO task_config (task_config_id, project_id, warehouse_id, queue_name, config) \
             VALUES ($1, $2, $3, $4, '{\"a\": 2}')",
        )
        .bind(Uuid::now_v7())
        .bind(project_id.as_str())
        .bind(*warehouse_id)
        .bind(q.as_str())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO project (project_id, project_name) VALUES ('zzz', 'zzz')")
            .execute(&pool)
            .await
            .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let states = list_project_task_schedule_states(&mut conn, &q, "k")
            .await
            .unwrap();
        // Ordered by project id; `zzz` sorts after every UUID.
        assert_eq!(
            states,
            vec![
                ProjectTaskScheduleState {
                    project_id: project_id.clone(),
                    interval: Some(chrono::Duration::hours(2)),
                    queue_config: Some(serde_json::json!({"a": 1})),
                },
                ProjectTaskScheduleState {
                    project_id: Arc::new(ProjectId::from_db_unchecked("zzz".to_string())),
                    interval: None,
                    queue_config: None,
                },
            ]
        );
    }

    #[sqlx::test]
    async fn test_deleting_a_warehouses_schedules_includes_newer_versions(pool: PgPool) {
        let (project_id, warehouse_id, other_warehouse_id) =
            setup_two_warehouses(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let project_schedule = upsert(
            &pool,
            &project_id,
            None,
            &q,
            "k",
            chrono::Duration::hours(1),
        )
        .await;
        upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "k",
            chrono::Duration::hours(1),
        )
        .await;
        upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "newer",
            chrono::Duration::hours(1),
        )
        .await;
        sqlx::query("UPDATE task_schedule SET spec_version = 99 WHERE schedule_key = 'newer'")
            .execute(&pool)
            .await
            .unwrap();
        let other_schedule = upsert(
            &pool,
            &project_id,
            Some(other_warehouse_id),
            &q,
            "k",
            chrono::Duration::hours(1),
        )
        .await;

        let mut conn = pool.acquire().await.unwrap();
        delete_warehouse_task_schedules(&mut conn, warehouse_id)
            .await
            .unwrap();
        let left: BTreeSet<Uuid> = sqlx::query_scalar("SELECT schedule_id FROM task_schedule")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            left,
            BTreeSet::from([*project_schedule.schedule_id, *other_schedule.schedule_id])
        );
    }

    #[sqlx::test]
    async fn test_schedule_ids_keep_their_encoding(pool: PgPool) {
        let ids: (Uuid, Uuid) = sqlx::query_as(
            "SELECT task_schedule_id('p', NULL, 'q', 'k'), \
             task_schedule_id('p', '00000000-0000-0000-0000-000000000001', 'q', 'k')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            ids,
            (
                Uuid::parse_str("71bd68a1-86d8-57be-8be6-530692575c1f").unwrap(),
                Uuid::parse_str("e3482f16-8729-5c58-98a3-5d221c807be7").unwrap(),
            )
        );
    }

    #[sqlx::test]
    async fn test_tick_while_instance_active_waits_and_its_run_covers_the_missed_ticks(
        pool: PgPool,
    ) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let schedule = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "slow",
            chrono::Duration::minutes(1),
        )
        .await;
        let first = materialize(&pool).await[&schedule.schedule_id];
        let before = get(&pool, &schedule).await;

        // The first instance is still pending when the next tick comes: the tick stays due.
        advance(&pool, 60).await;
        assert_eq!(materialize(&pool).await, BTreeMap::new());
        let waiting = get(&pool, &schedule).await;
        assert_eq!(
            waiting.next_run_at,
            before.next_run_at - chrono::Duration::minutes(1)
        );
        assert_eq!(waiting.missed_runs, 0);

        // A second tick falls due before the instance finishes.
        advance(&pool, 60).await;
        complete(&pool, &q, first).await;

        // One run for both ticks, scheduled for the later one.
        let ticks = materialize_ticks(&pool).await;
        assert_eq!(ticks.len(), 1);
        let tick = ticks[0];
        assert_eq!(tick.schedule_id, schedule.schedule_id);
        assert_ne!(tick.task_id, first);
        assert_eq!(tick.missed_runs, 1);
        assert_eq!(tick.tick, before.next_run_at - chrono::Duration::minutes(1));
        assert_eq!(tick.next_run_at, before.next_run_at);
        let after = get(&pool, &schedule).await;
        assert_eq!(after.missed_runs, 1);
        assert_eq!(after.next_run_at, before.next_run_at);
        let scheduled_for: DateTime<Utc> =
            sqlx::query_scalar("SELECT scheduled_for FROM task WHERE task_id = $1")
                .bind(*tick.task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scheduled_for, tick.tick);
    }

    #[sqlx::test]
    async fn test_recreated_schedule_keeps_its_id_and_waits_for_its_active_instance(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let schedule = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "group-1",
            chrono::Duration::minutes(1),
        )
        .await;
        let first = materialize(&pool).await[&schedule.schedule_id];

        let mut conn = pool.acquire().await.unwrap();
        assert!(
            delete_task_schedule(&mut conn, schedule.schedule_id)
                .await
                .unwrap()
        );
        let recreated = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "group-1",
            chrono::Duration::minutes(1),
        )
        .await;
        assert_eq!(recreated.schedule_id, schedule.schedule_id);

        // Due at once, but the instance of the deleted schedule is still active.
        assert_eq!(materialize(&pool).await, BTreeMap::new());
        assert_eq!(
            active_task_ids(&pool, schedule.schedule_id).await,
            vec![first]
        );
        complete(&pool, &q, first).await;
        let second = materialize(&pool).await[&schedule.schedule_id];
        assert_ne!(second, first);
    }

    #[sqlx::test]
    async fn test_enabling_a_disabled_schedule_moves_its_next_run_to_its_next_tick(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let anchor = Utc::now().trunc_subsecs(6) + chrono::Duration::minutes(30);
        let mut conn = pool.acquire().await.unwrap();
        let input = TaskScheduleUpsert::builder()
            .project_id(project_id.clone())
            .warehouse_id(Some(warehouse_id))
            .queue_name(q.clone())
            .schedule_key("hourly")
            .interval(chrono::Duration::hours(1))
            .anchor(Some(anchor))
            .build();
        let mut disable = input.clone();
        disable.enabled = Some(false);
        let disabled = upsert_task_schedule(&mut conn, disable).await.unwrap();
        assert_eq!((disabled.enabled, disabled.next_run_at), (false, anchor));

        // Two ticks pass while it is disabled.
        advance(&pool, 2 * 3600).await;
        assert_eq!(materialize(&pool).await, BTreeMap::new());
        let stored = get(&pool, &disabled).await;
        assert_eq!(stored.next_run_at, anchor - chrono::Duration::hours(2));

        // Enabling it skips the ticks that passed. The anchor is the one `advance` moved.
        let mut enable = input.clone();
        enable.anchor = None;
        enable.enabled = Some(true);
        let enabled = upsert_task_schedule(&mut conn, enable.clone())
            .await
            .unwrap();
        assert!(enabled.enabled);
        assert_eq!(enabled.anchor, anchor - chrono::Duration::hours(2));
        assert_eq!(enabled.next_run_at, anchor);
        assert_eq!(materialize(&pool).await, BTreeMap::new());

        // Enabling an enabled schedule keeps its next run.
        advance(&pool, 3600).await;
        let again = upsert_task_schedule(&mut conn, enable).await.unwrap();
        assert_eq!(again.next_run_at, anchor - chrono::Duration::hours(1));
        assert_eq!(
            materialize(&pool).await.keys().copied().collect::<Vec<_>>(),
            vec![disabled.schedule_id]
        );
    }

    #[sqlx::test]
    async fn test_schedule_of_a_newer_spec_version_is_left_alone(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let schedule = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "calendar",
            chrono::Duration::minutes(1),
        )
        .await;
        sqlx::query("UPDATE task_schedule SET spec_version = 2 WHERE schedule_id = $1")
            .bind(*schedule.schedule_id)
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(materialize(&pool).await, BTreeMap::new());
        let mut conn = pool.acquire().await.unwrap();
        assert_eq!(
            list_task_schedules(
                &mut conn,
                &TaskScheduleFilter::builder()
                    .project_id(project_id.clone())
                    .build(),
            )
            .await
            .unwrap(),
            vec![]
        );
        let err = upsert_task_schedule(
            &mut conn,
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .warehouse_id(Some(warehouse_id))
                .queue_name(q.clone())
                .schedule_key("calendar")
                .interval(chrono::Duration::hours(1))
                .build(),
        )
        .await
        .unwrap_err();
        let UpsertTaskScheduleError::TaskScheduleSpecVersionUnsupportedError(e) = err else {
            panic!("expected TaskScheduleSpecVersionUnsupportedError, got {err:?}");
        };
        assert_eq!(
            (e.queue_name, e.schedule_key.as_str()),
            (q.clone(), "calendar")
        );
        let interval: i64 =
            sqlx::query_scalar("SELECT interval_seconds FROM task_schedule WHERE schedule_id = $1")
                .bind(*schedule.schedule_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(interval, 60);
    }

    #[sqlx::test]
    async fn test_trigger_adds_a_run_and_leaves_next_run_untouched(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let mut conn = pool.acquire().await.unwrap();
        // Due at once.
        let schedule = upsert_task_schedule(
            &mut conn,
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .warehouse_id(Some(warehouse_id))
                .queue_name(q.clone())
                .schedule_key("hourly")
                .interval(chrono::Duration::hours(1))
                .payload(Some(serde_json::json!({"group": "g1"})))
                .build(),
        )
        .await
        .unwrap();
        assert_eq!(schedule.next_run_at, schedule.anchor);

        let outcome = trigger_task_schedule(&mut conn, schedule.schedule_id)
            .await
            .unwrap();
        let EnqueueOutcome::Inserted(task_id) = outcome else {
            panic!("expected Inserted, got {outcome:?}");
        };
        assert_eq!(get(&pool, &schedule).await, schedule);
        // The triggered run holds the due tick back.
        assert_eq!(materialize(&pool).await, BTreeMap::new());

        // A second trigger while the first run is pending reports it.
        assert_eq!(
            trigger_task_schedule(&mut conn, schedule.schedule_id)
                .await
                .unwrap(),
            EnqueueOutcome::Existing {
                task_id,
                status: TaskIntermediateStatus::Scheduled,
            }
        );

        let task = pick_task(&pool, &q, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), task_id);
        assert_eq!(task.data, serde_json::json!({"group": "g1"}));
        assert_eq!(
            trigger_task_schedule(&mut conn, schedule.schedule_id)
                .await
                .unwrap(),
            EnqueueOutcome::Existing {
                task_id,
                status: TaskIntermediateStatus::Running,
            }
        );
        assert_eq!(get(&pool, &schedule).await, schedule);

        let missing = TaskScheduleId::from(Uuid::now_v7());
        let err = trigger_task_schedule(&mut conn, missing).await.unwrap_err();
        let TriggerTaskScheduleError::TaskScheduleNotFoundError(e) = err else {
            panic!("expected TaskScheduleNotFoundError, got {err:?}");
        };
        assert_eq!(e.schedule_id, missing);
    }

    #[sqlx::test]
    async fn test_upsert_keeps_next_run_unless_timing_changes(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let anchor = Utc::now().trunc_subsecs(6) + chrono::Duration::hours(1);
        let mut conn = pool.acquire().await.unwrap();
        let input = TaskScheduleUpsert::builder()
            .project_id(project_id.clone())
            .warehouse_id(Some(warehouse_id))
            .queue_name(q.clone())
            .schedule_key("k")
            .interval(chrono::Duration::hours(1))
            .anchor(Some(anchor))
            .build();
        let created = upsert_task_schedule(&mut conn, input.clone())
            .await
            .unwrap();

        // Same timing, new payload, no anchor: the next run stays where it is, two hours back,
        // although re-timing would move it ahead.
        advance(&pool, 2 * 3600).await;
        let mut changed_payload = input.clone();
        changed_payload.anchor = None;
        changed_payload.payload = Some(serde_json::json!({"v": 2}));
        let updated = upsert_task_schedule(&mut conn, changed_payload)
            .await
            .unwrap();
        let moved = anchor - chrono::Duration::hours(2);
        assert_eq!(
            (
                updated.schedule_id,
                updated.anchor,
                updated.next_run_at,
                updated.payload
            ),
            (
                created.schedule_id,
                moved,
                moved,
                serde_json::json!({"v": 2})
            )
        );

        // A new anchor re-times the schedule.
        let mut new_anchor = input.clone();
        new_anchor.anchor = Some(anchor + chrono::Duration::minutes(30));
        let retimed = upsert_task_schedule(&mut conn, new_anchor).await.unwrap();
        assert_eq!(retimed.schedule_id, created.schedule_id);
        assert_eq!(retimed.next_run_at, anchor + chrono::Duration::minutes(30));

        // A new interval re-times it from the stored anchor, which now lies about an hour back.
        advance(&pool, 150 * 60).await;
        let mut new_interval = input.clone();
        new_interval.anchor = None;
        new_interval.interval = chrono::Duration::hours(2);
        let retimed = upsert_task_schedule(&mut conn, new_interval).await.unwrap();
        let stored_anchor = anchor - chrono::Duration::hours(2);
        assert_eq!(
            (retimed.anchor, retimed.next_run_at),
            (stored_anchor, stored_anchor + chrono::Duration::hours(2))
        );

        // Delete finds the schedule once.
        assert_eq!(
            (
                delete_task_schedule(&mut conn, created.schedule_id)
                    .await
                    .unwrap(),
                delete_task_schedule(&mut conn, created.schedule_id)
                    .await
                    .unwrap()
            ),
            (true, false)
        );
    }

    #[sqlx::test]
    async fn test_failed_attempts_back_off_then_go_terminal_without_stopping_the_schedule(
        pool: PgPool,
    ) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        clear_schedules(&pool).await;
        let q = queue();
        let schedule = upsert(
            &pool,
            &project_id,
            Some(warehouse_id),
            &q,
            "flaky",
            chrono::Duration::minutes(1),
        )
        .await;
        let task_id = materialize(&pool).await[&schedule.schedule_id];
        let max_retries = 5;

        for attempt in 1..=max_retries {
            let task = pick_task(&pool, &q, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((task.task_id(), task.attempt()), (task_id, attempt));
            let backoff = default_retry_backoff(&FailedAttempt {
                task_id,
                attempt,
                schedule_id: Some(schedule.schedule_id),
                kind: FailureKind::Error,
                error: "boom",
            });
            record_failure(
                &task,
                max_retries,
                backoff,
                "boom",
                &mut pool.acquire().await.unwrap(),
            )
            .await
            .unwrap();
            if attempt == max_retries {
                break;
            }
            // The retry is due exactly `backoff` after the failure was logged, and not before.
            assert_eq!(
                retry_delay(&pool, task_id, attempt).await.microseconds,
                backoff.num_microseconds().unwrap(),
                "attempt {attempt}"
            );
            assert!(
                pick_task(&pool, &q, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                    .await
                    .unwrap()
                    .is_none(),
                "attempt {attempt} must not be retried before its backoff"
            );
            sqlx::query("UPDATE task SET scheduled_for = now() WHERE task_id = $1")
                .bind(*task_id)
                .execute(&pool)
                .await
                .unwrap();
        }

        // Terminal: gone from `task`, five failed attempts in the log.
        assert_eq!(active_task_ids(&pool, schedule.schedule_id).await, vec![]);
        let failed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_log WHERE task_id = $1 AND status = 'failed'",
        )
        .bind(*task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(failed, 5);

        // The schedule carries on.
        advance(&pool, 60).await;
        let next = materialize(&pool).await[&schedule.schedule_id];
        assert_ne!(next, task_id);
        assert_eq!(get(&pool, &schedule).await.missed_runs, 0);
    }

    #[sqlx::test]
    async fn test_enqueue_reports_inserted_then_existing(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q: &'static TaskQueueName = Box::leak(Box::new(queue()));
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let input = TaskInput {
            task_metadata: ScheduleTaskMetadata {
                project_id: project_id.clone(),
                parent_task_id: None,
                scheduled_for: None,
                entity: TaskEntity::EntityInWarehouse {
                    warehouse_id,
                    entity_id,
                    entity_name: vec!["t".to_string()],
                },
            },
            payload: serde_json::json!({}),
        };
        let state = crate::CatalogState::from_pools(pool.clone(), pool.clone());
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first = PostgresBackend::enqueue_task(q, input.clone(), t.transaction())
            .await
            .unwrap();
        let EnqueueOutcome::Inserted(task_id) = first else {
            panic!("expected Inserted, got {first:?}");
        };
        let second = PostgresBackend::enqueue_task(q, input.clone(), t.transaction())
            .await
            .unwrap();
        assert_eq!(
            second,
            EnqueueOutcome::Existing {
                task_id,
                status: TaskIntermediateStatus::Scheduled,
            }
        );
        t.commit().await.unwrap();

        // A project-scoped task without an entity id has the same semantics.
        let project_input = TaskInput {
            task_metadata: ScheduleTaskMetadata {
                project_id: project_id.clone(),
                parent_task_id: None,
                scheduled_for: None,
                entity: TaskEntity::Project,
            },
            payload: serde_json::json!({}),
        };
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first = PostgresBackend::enqueue_task(q, project_input.clone(), t.transaction())
            .await
            .unwrap();
        let EnqueueOutcome::Inserted(project_task_id) = first else {
            panic!("expected Inserted, got {first:?}");
        };
        assert_eq!(
            PostgresBackend::enqueue_task(q, project_input, t.transaction())
                .await
                .unwrap(),
            EnqueueOutcome::Existing {
                task_id: project_task_id,
                status: TaskIntermediateStatus::Scheduled,
            }
        );
        t.commit().await.unwrap();

        // So does a warehouse-scoped task without an entity id.
        let warehouse_input = TaskInput {
            task_metadata: ScheduleTaskMetadata {
                project_id,
                parent_task_id: None,
                scheduled_for: None,
                entity: TaskEntity::Warehouse { warehouse_id },
            },
            payload: serde_json::json!({}),
        };
        let mut t = PostgresTransaction::begin_write(state).await.unwrap();
        let first = PostgresBackend::enqueue_task(q, warehouse_input.clone(), t.transaction())
            .await
            .unwrap();
        let EnqueueOutcome::Inserted(warehouse_task_id) = first else {
            panic!("expected Inserted, got {first:?}");
        };
        assert_eq!(
            PostgresBackend::enqueue_task(q, warehouse_input, t.transaction())
                .await
                .unwrap(),
            EnqueueOutcome::Existing {
                task_id: warehouse_task_id,
                status: TaskIntermediateStatus::Scheduled,
            }
        );
        t.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_reparent_moves_pending_task_and_payload_together(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q: &'static TaskQueueName = Box::leak(Box::new(queue()));
        let state = crate::CatalogState::from_pools(pool.clone(), pool.clone());
        let old_parent = TaskId::from(Uuid::now_v7());
        let new_parent = TaskId::from(Uuid::now_v7());
        let enqueue_child = |payload: serde_json::Value| TaskInput {
            task_metadata: ScheduleTaskMetadata {
                project_id: project_id.clone(),
                parent_task_id: Some(old_parent),
                scheduled_for: None,
                entity: TaskEntity::EntityInWarehouse {
                    warehouse_id,
                    entity_id: WarehouseTaskEntityId::Table {
                        table_id: Uuid::now_v7().into(),
                    },
                    entity_name: vec!["t".to_string()],
                },
            },
            payload,
        };
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let pending = PostgresBackend::enqueue_task(
            q,
            enqueue_child(serde_json::json!({"run": 1})),
            t.transaction(),
        )
        .await
        .unwrap()
        .inserted()
        .unwrap();
        let running = PostgresBackend::enqueue_task(
            q,
            enqueue_child(serde_json::json!({"run": 1})),
            t.transaction(),
        )
        .await
        .unwrap()
        .inserted()
        .unwrap();
        t.commit().await.unwrap();
        // Make `running` the one a worker holds.
        sqlx::query("UPDATE task SET status = 'running' WHERE task_id = $1")
            .bind(*running)
            .execute(&pool)
            .await
            .unwrap();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        assert!(
            !PostgresBackend::reparent_pending_task(
                &TaskQueueName::from("another-queue"),
                &[],
                pending,
                new_parent,
                serde_json::json!({"run": 2}),
                t.transaction(),
            )
            .await
            .unwrap()
        );
        // The queue's legacy names count as its own.
        let renamed: &'static TaskQueueName = Box::leak(Box::new(queue()));
        assert!(
            PostgresBackend::reparent_pending_task(
                renamed,
                &[q],
                pending,
                new_parent,
                serde_json::json!({"run": 2}),
                t.transaction(),
            )
            .await
            .unwrap()
        );
        assert!(
            !PostgresBackend::reparent_pending_task(
                q,
                &[],
                running,
                new_parent,
                serde_json::json!({"run": 2}),
                t.transaction(),
            )
            .await
            .unwrap()
        );
        t.commit().await.unwrap();

        let rows: Vec<(Uuid, Option<Uuid>, serde_json::Value)> = sqlx::query_as(
            "SELECT task_id, parent_task_id, task_data FROM task WHERE task_id = ANY($1) \
             ORDER BY task_id",
        )
        .bind(vec![*pending, *running])
        .fetch_all(&pool)
        .await
        .unwrap();
        let mut expected = vec![
            (*pending, Some(*new_parent), serde_json::json!({"run": 2})),
            (*running, Some(*old_parent), serde_json::json!({"run": 1})),
        ];
        expected.sort_by_key(|r| r.0);
        assert_eq!(rows, expected);
    }

    fn child_input(
        project_id: &ArcProjectId,
        warehouse_id: WarehouseId,
        parent: Option<TaskId>,
    ) -> TaskInput {
        TaskInput {
            task_metadata: ScheduleTaskMetadata {
                project_id: project_id.clone(),
                parent_task_id: parent,
                scheduled_for: None,
                entity: TaskEntity::EntityInWarehouse {
                    warehouse_id,
                    entity_id: WarehouseTaskEntityId::Table {
                        table_id: Uuid::now_v7().into(),
                    },
                    entity_name: vec!["t".to_string()],
                },
            },
            payload: serde_json::json!({}),
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_by_parent_spans_task_and_task_log(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let q: &'static TaskQueueName = Box::leak(Box::new(queue()));
        let state = crate::CatalogState::from_pools(pool.clone(), pool.clone());
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let mut enqueue = async |parent| {
            PostgresBackend::enqueue_task(
                q,
                child_input(&project_id, warehouse_id, parent),
                t.transaction(),
            )
            .await
            .unwrap()
            .inserted()
            .unwrap()
        };
        let parent = enqueue(None).await;
        let other_parent = enqueue(None).await;
        let child_1 = enqueue(Some(parent)).await;
        let child_2 = enqueue(Some(parent)).await;
        let _foreign_child = enqueue(Some(other_parent)).await;
        t.commit().await.unwrap();

        // Finish `child_1`, so it lives in `task_log` only.
        sqlx::query(
            "UPDATE task SET scheduled_for = now() + interval '1 hour' WHERE task_id <> $1",
        )
        .bind(*child_1)
        .execute(&pool)
        .await
        .unwrap();
        complete(&pool, q, child_1).await;

        let mut t = PostgresTransaction::begin_read(state).await.unwrap();
        let listed = PostgresBackend::list_tasks(
            &lakekeeper::service::tasks::TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &lakekeeper::api::management::v1::tasks::ListTasksRequest::builder()
                .parent_task_id(Some(parent))
                .build(),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        let listed = listed
            .tasks
            .iter()
            .map(|task| (task.task_id(), task.parent_task_id()))
            .collect::<HashSet<_>>();
        assert_eq!(
            listed,
            HashSet::from([(child_1, Some(parent)), (child_2, Some(parent))])
        );
    }

    const TASK_SCHEDULES_MIGRATION: i64 = 20_261_008_120_000;

    async fn apply_migrations(pool: &PgPool, filter: impl Fn(i64) -> bool) {
        let migrator = sqlx::migrate!();
        for migration in migrator.iter().filter(|m| filter(m.version)) {
            sqlx::raw_sql(migration.sql.clone())
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("migration {} failed: {e}", migration.version));
        }
    }

    #[sqlx::test(migrations = false)]
    async fn test_migration_applies_to_a_database_with_tasks(pool: PgPool) {
        apply_migrations(&pool, |v| v < TASK_SCHEDULES_MIGRATION).await;

        sqlx::query(
            "INSERT INTO project (project_id, project_name) VALUES ('p-legacy', 'a'), ('p-new', 'b')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let legacy_task = Uuid::now_v7();
        let other_task = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO task (task_id, project_id, queue_name, status, scheduled_for, task_data, \
             entity_type) VALUES \
             ($1, 'p-legacy', 'task_log_cleanup', 'scheduled', now(), '{}', 'project'), \
             ($2, 'p-new', 'other', 'scheduled', now(), '{}', 'project')",
        )
        .bind(legacy_task)
        .bind(other_task)
        .execute(&pool)
        .await
        .unwrap();

        apply_migrations(&pool, |v| v >= TASK_SCHEDULES_MIGRATION).await;

        // Schedules come from the post-migration hooks, not from the migration.
        let schedules: i64 = sqlx::query_scalar("SELECT count(*) FROM task_schedule")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(schedules, 0);

        // Existing tasks are untouched.
        let tasks: Vec<(Uuid, Option<Uuid>)> =
            sqlx::query_as("SELECT task_id, entity_id FROM task ORDER BY task_id")
                .fetch_all(&pool)
                .await
                .unwrap();
        let mut expected = vec![(legacy_task, None), (other_task, None)];
        expected.sort();
        assert_eq!(tasks, expected);

        // The relaxed checks: a project task may name its schedule, but still has no name.
        sqlx::query(
            "INSERT INTO task (task_id, project_id, queue_name, status, task_data, entity_type, \
             entity_id) VALUES ($1, 'p-new', 'q', 'scheduled', '{}', 'project', $2)",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap();
        let err = sqlx::query(
            "INSERT INTO task (task_id, project_id, queue_name, status, task_data, entity_type, \
             entity_id, entity_name) VALUES ($1, 'p-new', 'q2', 'scheduled', '{}', 'project', $2, \
             ARRAY['x'])",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap_err();
        let db_err = err.as_database_error().unwrap();
        assert_eq!(db_err.code().as_deref(), Some("23514"));
        assert_eq!(db_err.constraint(), Some("task_entity_check"));

        let validated: Vec<(String, bool)> = sqlx::query_as(
            "SELECT conname::text, convalidated FROM pg_constraint \
             WHERE conname IN ('task_entity_check', 'task_log_entity_check') ORDER BY conname",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            validated,
            vec![
                ("task_entity_check".to_string(), false),
                ("task_log_entity_check".to_string(), false),
            ]
        );
    }
}
