//! Task log cleanup runs from a task schedule.

use std::time::Duration;

use lakekeeper::{
    api::management::v1::warehouse::TabularDeleteProfile,
    service::{
        ArcProjectId, CatalogStore, CatalogTaskOps, Transaction,
        authz::AllowAllAuthorizer,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, TaskQueueName, TaskSchedule, TaskScheduleFilter,
            first_tick_at_or_after,
            task_log_cleanup_queue::{
                QUEUE_NAME, SCHEDULE_KEY, TaskLogCleanupPayload, TaskLogCleanupTask,
            },
            task_schedule::materialize_due_task_schedules,
        },
    },
};
use lakekeeper_storage_postgres::{CatalogState, PostgresBackend};
use sqlx::PgPool;
use uuid::Uuid;

type Context = lakekeeper::api::ApiContext<
    lakekeeper::service::State<
        AllowAllAuthorizer,
        PostgresBackend,
        lakekeeper_storage_postgres::SecretsState,
    >,
>;

/// The message of a cleanup task that an older server enqueued.
const SUPERSEDED: &str = "Superseded by the project's task log cleanup schedule.";

async fn setup(pool: &PgPool) -> (Context, CatalogState, ArcProjectId) {
    let (ctx, warehouse) = lakekeeper_integration_tests::setup(
        pool.clone(),
        lakekeeper_integration_tests::memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let state = ctx.v1_state.catalog.clone();
    (ctx, state, warehouse.project_id)
}

async fn cleanup_schedules(state: CatalogState, project_id: &ArcProjectId) -> Vec<TaskSchedule> {
    let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_read(state)
        .await
        .unwrap();
    let schedules = PostgresBackend::list_task_schedules(
        &TaskScheduleFilter::builder()
            .project_id(project_id.clone())
            .queue_name(Some(TaskQueueName::from(QUEUE_NAME.as_str())))
            .build(),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    schedules
}

/// A new cleanup schedule runs first at its first tick at or after its creation, within one day.
fn assert_first_run_within_a_day(schedule: &TaskSchedule) {
    assert_eq!(
        schedule.next_run_at,
        first_tick_at_or_after(schedule.anchor, 86_400, schedule.created_at).unwrap()
    );
    assert!(schedule.next_run_at - schedule.created_at <= chrono::Duration::days(1));
}

/// Moves the schedule's anchor and next run an hour into the past, so exactly that tick is due.
async fn make_due(pool: &PgPool, schedule: &TaskSchedule) {
    sqlx::query(
        "UPDATE task_schedule SET anchor = now() - interval '1 hour', \
         next_run_at = now() - interval '1 hour' WHERE schedule_id = $1",
    )
    .bind(*schedule.schedule_id)
    .execute(pool)
    .await
    .unwrap();
}

/// Insert a log entry past the 90-day retention; returns its task id.
async fn insert_stale_log(pool: &PgPool, project_id: &ArcProjectId) -> Uuid {
    let stale = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO task_log (task_id, attempt, queue_name, task_data, status, \
         attempt_scheduled_for, task_created_at, entity_type, project_id, created_at) \
         VALUES ($1, 1, 'other', '{}', 'success', now(), now() - interval '100 days', 'project', \
         $2, now() - interval '100 days')",
    )
    .bind(stale)
    .bind(project_id.as_str())
    .execute(pool)
    .await
    .unwrap();
    stale
}

async fn log_count(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM task_log WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn active_cleanup_tasks(pool: &PgPool, project_id: &ArcProjectId) -> Vec<Uuid> {
    sqlx::query_scalar(
        "SELECT task_id FROM task WHERE project_id = $1 AND queue_name = 'task_log_cleanup'",
    )
    .bind(project_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Run the built-in workers until `task_id` has a log entry; returns its status and message.
async fn run_until_logged(ctx: &Context, pool: &PgPool, task_id: Uuid) -> (String, Option<String>) {
    let cancellation_token = lakekeeper::CancellationToken::new();
    let handle = lakekeeper_integration_tests::spawn_build_in_queues(
        ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;
    let mut logged = None;
    for _ in 0..100 {
        logged = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT status::text, message FROM task_log WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .unwrap();
        if logged.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancellation_token.cancel();
    handle.await.unwrap();
    logged.expect("task was not processed")
}

async fn enqueue_legacy_cleanup(state: CatalogState, project_id: &ArcProjectId) -> Uuid {
    let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(state)
        .await
        .unwrap();
    let task_id = TaskLogCleanupTask::schedule_task::<PostgresBackend>(
        ScheduleTaskMetadata {
            project_id: project_id.clone(),
            parent_task_id: None,
            scheduled_for: None,
            entity: TaskEntity::Project,
        },
        TaskLogCleanupPayload::new(),
        t.transaction(),
    )
    .await
    .unwrap()
    .inserted()
    .unwrap();
    t.commit().await.unwrap();
    *task_id
}

#[sqlx::test]
async fn test_task_log_cleanup_runs_from_its_schedule(pool: PgPool) {
    let (ctx, state, project_id) = setup(&pool).await;

    // Creating the project created its schedule.
    let schedules = cleanup_schedules(state.clone(), &project_id).await;
    assert_eq!(schedules.len(), 1);
    let schedule = schedules[0].clone();
    assert_eq!(
        (
            schedule.schedule_key.as_str(),
            schedule.warehouse_id,
            schedule.interval,
        ),
        (SCHEDULE_KEY, None, chrono::Duration::days(1))
    );
    assert_first_run_within_a_day(&schedule);
    assert_eq!(
        active_cleanup_tasks(&pool, &project_id).await,
        Vec::<Uuid>::new()
    );

    let stale = insert_stale_log(&pool, &project_id).await;

    make_due(&pool, &schedule).await;
    let ticks = materialize_due_task_schedules::<PostgresBackend>(state.clone())
        .await
        .unwrap();
    let tick = ticks
        .iter()
        .find(|t| t.schedule_id == schedule.schedule_id)
        .unwrap();
    let task_id = *tick.task_id;
    assert_eq!(
        active_cleanup_tasks(&pool, &project_id).await,
        vec![task_id]
    );

    assert_eq!(
        run_until_logged(&ctx, &pool, task_id).await,
        ("success".to_string(), None)
    );
    assert_eq!(log_count(&pool, stale).await, 0);

    // No successor: the schedule owns the cadence.
    assert_eq!(
        active_cleanup_tasks(&pool, &project_id).await,
        Vec::<Uuid>::new()
    );
    let after = cleanup_schedules(state.clone(), &project_id).await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].schedule_id, schedule.schedule_id);
    assert_eq!(
        after[0].next_run_at,
        after[0].anchor + chrono::Duration::days(1)
    );
}

/// A cleanup task without a schedule comes from a server that predates schedules, for example an
/// older server finishing a scheduled run during a rolling upgrade. It is completed without a
/// successor, so it never becomes a second cleanup chain.
#[sqlx::test]
async fn test_legacy_cleanup_task_is_absorbed_by_the_schedule(pool: PgPool) {
    let (ctx, state, project_id) = setup(&pool).await;
    let schedule = cleanup_schedules(state.clone(), &project_id).await[0].clone();

    // The schedule's own run is active while the legacy row arrives.
    make_due(&pool, &schedule).await;
    let ticks = materialize_due_task_schedules::<PostgresBackend>(state.clone())
        .await
        .unwrap();
    let scheduled_run = *ticks
        .iter()
        .find(|t| t.schedule_id == schedule.schedule_id)
        .unwrap()
        .task_id;
    let legacy = enqueue_legacy_cleanup(state.clone(), &project_id).await;
    let mut active = active_cleanup_tasks(&pool, &project_id).await;
    active.sort();
    let mut expected = vec![scheduled_run, legacy];
    expected.sort();
    assert_eq!(active, expected);
    // A stale log entry, and settings someone else chose for the schedule.
    let stale = insert_stale_log(&pool, &project_id).await;
    sqlx::query(
        "UPDATE task_schedule SET enabled = false, payload = '{\"kept\": true}' \
         WHERE schedule_id = $1",
    )
    .bind(*schedule.schedule_id)
    .execute(&pool)
    .await
    .unwrap();

    // The legacy task cleans up and enqueues no successor; both runs keep the settings.
    assert_eq!(
        run_until_logged(&ctx, &pool, legacy).await,
        ("success".to_string(), Some(SUPERSEDED.to_string()))
    );
    assert_eq!(log_count(&pool, stale).await, 0);
    assert_eq!(
        run_until_logged(&ctx, &pool, scheduled_run).await,
        ("success".to_string(), None)
    );
    assert_eq!(
        active_cleanup_tasks(&pool, &project_id).await,
        Vec::<Uuid>::new()
    );
    let after = cleanup_schedules(state.clone(), &project_id).await;
    assert_eq!(after.len(), 1);
    assert_eq!(
        (
            after[0].schedule_id,
            after[0].enabled,
            after[0].payload.clone()
        ),
        (
            schedule.schedule_id,
            false,
            serde_json::json!({"kept": true})
        )
    );

    // A project created by an older server has no schedule; its legacy task creates one.
    sqlx::query("DELETE FROM task_schedule WHERE schedule_id = $1")
        .bind(*schedule.schedule_id)
        .execute(&pool)
        .await
        .unwrap();
    let legacy = enqueue_legacy_cleanup(state.clone(), &project_id).await;
    assert_eq!(
        run_until_logged(&ctx, &pool, legacy).await,
        ("success".to_string(), Some(SUPERSEDED.to_string()))
    );
    assert_eq!(
        active_cleanup_tasks(&pool, &project_id).await,
        Vec::<Uuid>::new()
    );
    let recreated = cleanup_schedules(state.clone(), &project_id).await;
    assert_eq!(recreated.len(), 1);
    assert_eq!(
        (
            recreated[0].schedule_key.as_str(),
            recreated[0].interval,
            recreated[0].next_run_at,
        ),
        (SCHEDULE_KEY, chrono::Duration::days(1), recreated[0].anchor)
    );
}

#[sqlx::test]
async fn test_changing_the_cleanup_period_applies_to_the_schedule(pool: PgPool) {
    use lakekeeper::api::management::v1::{
        ApiServer, project::Service as _, task_queue::SetTaskQueueConfigRequest,
    };

    let (ctx, state, project_id) = setup(&pool).await;
    let before = cleanup_schedules(state.clone(), &project_id).await;
    assert_eq!(before.len(), 1);

    let request: SetTaskQueueConfigRequest = serde_json::from_value(serde_json::json!({
        "queue-config": {"cleanup-period": "P7D", "retention-period": "P90D"},
    }))
    .unwrap();
    let mut request_metadata = lakekeeper_integration_tests::random_request_metadata();
    request_metadata.with_project_id((*project_id).clone());
    ApiServer::<PostgresBackend, AllowAllAuthorizer, lakekeeper_storage_postgres::SecretsState>::set_project_task_queue_config(
        &QUEUE_NAME,
        request,
        ctx.clone(),
        request_metadata,
    )
    .await
    .unwrap();

    let after = cleanup_schedules(state, &project_id).await;
    assert_eq!(after.len(), 1);
    assert_eq!(
        (after[0].schedule_id, after[0].interval, after[0].anchor),
        (
            before[0].schedule_id,
            chrono::Duration::days(7),
            before[0].anchor
        )
    );
}

#[sqlx::test]
async fn test_post_migration_hooks_create_missing_cleanup_schedules(pool: PgPool) {
    let (_ctx, state, project_id) = setup(&pool).await;
    let existing = cleanup_schedules(state.clone(), &project_id).await;
    assert_eq!(existing.len(), 1);
    sqlx::query("DELETE FROM task_schedule WHERE project_id = $1")
        .bind(project_id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    lakekeeper::service::run_post_migration_hooks::<PostgresBackend>(
        state.clone(),
        Vec::new(),
        Default::default(),
    )
    .await
    .unwrap();

    let created = cleanup_schedules(state, &project_id).await;
    assert_eq!(created.len(), 1);
    assert_eq!(
        (created[0].schedule_id, created[0].interval),
        (existing[0].schedule_id, chrono::Duration::days(1))
    );
    assert_first_run_within_a_day(&created[0]);
}
