//! A queued import under task control: progress, stop and cancel.
//!
//! Each test picks the import's task itself and changes its state before running
//! it, so the import meets the stop or cancel at its first heartbeat — a point
//! the test controls — and not at whatever instant a worker happened to be at.
use std::time::Duration;

use http::StatusCode;
use lakekeeper::{
    CancellationToken,
    api::data::v1::datasets::{DatasetService as _, ImportDatasetRequest},
    server::CatalogServer,
    service::{
        CatalogStore, CatalogTaskOps, Transaction,
        tasks::{
            CancelTasksFilter, TaskId,
            dataset_import_queue::{DatasetImportTask, run_dataset_import_task},
        },
    },
};
use lakekeeper_integration_tests::{CommitGate, TestDataset, random_request_metadata};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde_json::Value;
use sqlx::PgPool;

/// More objects than one staging batch, so the listing heartbeats before it ends.
const OBJECTS: usize = 6_000;

async fn make_dataset(pool: PgPool) -> TestDataset {
    let ds = TestDataset::imported(pool).await;
    for i in 0..OBJECTS {
        ds.write(&format!("f{i:05}"), b"x").await;
    }
    ds
}

/// Queue an import and pick its task up, as a worker would.
async fn picked_import(ds: &TestDataset) -> DatasetImportTask {
    picked(ds, ImportDatasetRequest::default()).await
}

async fn picked(ds: &TestDataset, request: ImportDatasetRequest) -> DatasetImportTask {
    CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            queued: Some(true),
            ..request
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    DatasetImportTask::poll_for_new_task::<PostgresBackend>(
        ds.ctx.v1_state.catalog.clone(),
        &Duration::from_millis(10),
        CancellationToken::new(),
    )
    .await
    .expect("the queued import is picked up")
}

async fn run(ds: &TestDataset, task: &DatasetImportTask) {
    run_dataset_import_task::<PostgresBackend, SecretsState>(
        task,
        &ds.ctx.v1_state.secrets,
        None,
        ds.ctx.v1_state.catalog.clone(),
    )
    .await;
}

/// Run `task` up to its publish, which is held at commit while `meanwhile`
/// changes the task's state, then let it finish. Returns once the run has
/// returned, within three seconds of the publish: recording success on a task
/// that is gone would retry for four.
async fn run_changing_the_task_at_publish(
    ds: &TestDataset,
    task: DatasetImportTask,
    meanwhile: impl AsyncFnOnce(&mut <PostgresBackend as CatalogStore>::Transaction),
) {
    let gate = CommitGate::install(&ds.pool, "dataset_ref", "UPDATE").await;
    let running = tokio::spawn({
        let (secrets, catalog) = (
            ds.ctx.v1_state.secrets.clone(),
            ds.ctx.v1_state.catalog.clone(),
        );
        async move {
            run_dataset_import_task::<PostgresBackend, SecretsState>(
                &task, &secrets, None, catalog,
            )
            .await;
        }
    });
    gate.wait_for_a_held_commit().await;
    let mut t = ds.begin_write().await;
    meanwhile(&mut t).await;
    t.commit().await.unwrap();
    gate.release().await;
    tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .expect("the run returns once it has published")
        .unwrap();
}

async fn snapshots(ds: &TestDataset) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot")
        .fetch_one(&ds.pool)
        .await
        .unwrap()
}

/// The task's current status, or `None` once its row is gone.
async fn task_status(ds: &TestDataset, task_id: TaskId) -> Option<String> {
    sqlx::query_scalar("SELECT status::text FROM task WHERE task_id = $1")
        .bind(*task_id)
        .fetch_optional(&ds.pool)
        .await
        .unwrap()
}

/// The execution details the task's first attempt ended with.
async fn first_attempt_details(ds: &TestDataset, task_id: TaskId) -> Value {
    sqlx::query_scalar("SELECT execution_details FROM task_log WHERE task_id = $1 AND attempt = 1")
        .bind(*task_id)
        .fetch_one(&ds.pool)
        .await
        .unwrap()
}

#[sqlx::test]
async fn test_a_stopped_import_publishes_nothing_and_runs_again(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = picked_import(&ds).await;
    let mut t = ds.begin_write().await;
    PostgresBackend::stop_tasks(&[task.task_id()], t.transaction())
        .await
        .unwrap();
    t.commit().await.unwrap();

    run(&ds, &task).await;

    assert_eq!(snapshots(&ds).await, 0, "nothing published, nothing staged");
    // A stop is graceful: the task goes back on the queue.
    assert_eq!(
        task_status(&ds, task.task_id()).await.as_deref(),
        Some("scheduled")
    );
    // Caught at the listing's first heartbeat, not only at the publish.
    let details = first_attempt_details(&ds, task.task_id()).await;
    assert_eq!(details["phase"], "listing", "{details}");
    assert_eq!(details["objects_listed"], 5_000, "{details}");

    // The stop applied to that attempt only: the next one runs to the end.
    let again = DatasetImportTask::poll_for_new_task::<PostgresBackend>(
        ds.ctx.v1_state.catalog.clone(),
        &Duration::from_millis(10),
        CancellationToken::new(),
    )
    .await
    .expect("the stopped import is picked up again");
    assert_eq!(again.task_id(), task.task_id());
    run(&ds, &again).await;
    assert_eq!(snapshots(&ds).await, 1);
    assert_eq!(task_status(&ds, task.task_id()).await, None, "finished");
}

/// A stop reaches an import whose scope admits none of what it lists: heartbeats
/// count what the listing returns, not what the scope keeps.
#[sqlx::test]
async fn test_a_stop_reaches_an_import_that_admits_nothing(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = picked(
        &ds,
        ImportDatasetRequest {
            include: Some(vec!["elsewhere/**".to_string()]),
            ..Default::default()
        },
    )
    .await;
    let mut t = ds.begin_write().await;
    PostgresBackend::stop_tasks(&[task.task_id()], t.transaction())
        .await
        .unwrap();
    t.commit().await.unwrap();

    run(&ds, &task).await;

    assert_eq!(
        task_status(&ds, task.task_id()).await.as_deref(),
        Some("scheduled"),
        "the stop was heard, so the task goes back on the queue"
    );
    let details = first_attempt_details(&ds, task.task_id()).await;
    assert_eq!(details["phase"], "listing", "{details}");
    assert_eq!(details["objects_listed"], 0, "{details}");
}

#[sqlx::test]
async fn test_a_cancelled_import_publishes_nothing(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = picked_import(&ds).await;
    let mut t = ds.begin_write().await;
    PostgresBackend::cancel_scheduled_tasks(
        None,
        &[],
        CancelTasksFilter::TaskIds(vec![task.task_id()]),
        true,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    run(&ds, &task).await;

    assert_eq!(snapshots(&ds).await, 0, "nothing published, nothing staged");
    assert_eq!(
        task_status(&ds, task.task_id()).await,
        None,
        "the task stays gone"
    );
}

/// A cancel that lands once the import has published undoes nothing, and the run
/// ends there: there is no task left to record the outcome on.
#[sqlx::test]
async fn test_a_cancel_after_the_publish_ends_the_run(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = picked_import(&ds).await;
    let task_id = task.task_id();

    run_changing_the_task_at_publish(&ds, task, async |t| {
        PostgresBackend::cancel_scheduled_tasks(
            None,
            &[],
            CancelTasksFilter::TaskIds(vec![task_id]),
            true,
            t.transaction(),
        )
        .await
        .unwrap();
    })
    .await;

    assert_eq!(snapshots(&ds).await, 1, "published");
    assert_eq!(task_status(&ds, task_id).await, None, "the task stays gone");
}

/// A stop that lands once the import has published undoes nothing: the task
/// finishes, as if the stop had come too late.
#[sqlx::test]
async fn test_a_stop_after_the_publish_lets_the_task_finish(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = picked_import(&ds).await;
    let task_id = task.task_id();

    run_changing_the_task_at_publish(&ds, task, async |t| {
        PostgresBackend::stop_tasks(&[task_id], t.transaction())
            .await
            .unwrap();
    })
    .await;

    assert_eq!(snapshots(&ds).await, 1, "published");
    assert_eq!(task_status(&ds, task_id).await, None, "finished");
    let details = first_attempt_details(&ds, task_id).await;
    assert_eq!(details["phase"], "done", "{details}");
    assert_eq!(details["imported"], OBJECTS, "{details}");
}

/// One queued import runs for a dataset at a time: another is refused while the
/// first is queued or running, so two never publish over each other's listing.
#[sqlx::test]
async fn test_a_second_queued_import_is_refused_while_one_runs(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = picked_import(&ds).await;

    let err = CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            queued: Some(true),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::CONFLICT, "{err:?}");
    assert_eq!(err.error.r#type, "DatasetImportAlreadyRunning");

    run(&ds, &task).await;
    picked_import(&ds).await;
}

#[sqlx::test]
async fn test_a_finished_import_records_where_it_got(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = picked_import(&ds).await;

    run(&ds, &task).await;

    assert_eq!(snapshots(&ds).await, 1);
    let details = first_attempt_details(&ds, task.task_id()).await;
    assert_eq!(details["phase"], "done", "{details}");
    assert_eq!(details["objects_listed"], OBJECTS, "{details}");
    assert_eq!(details["imported"], OBJECTS, "{details}");
}
