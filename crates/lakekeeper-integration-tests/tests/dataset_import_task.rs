//! A queued import under task control: progress, stop and cancel.
//!
//! Each test picks the import's task itself and changes its state before running
//! it, so the import meets the stop or cancel at its first heartbeat — a point
//! the test controls — and not at whatever instant a worker happened to be at.
use std::time::Duration;

use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::datasets::{DatasetParameters, DatasetService as _, ImportDatasetRequest},
        iceberg::types::Prefix,
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        CatalogStore, CatalogTaskOps, State, Transaction,
        authz::AllowAllAuthorizer,
        tasks::{
            CancelTasksFilter, TaskId,
            dataset_import_queue::{DatasetImportTask, run_dataset_import_task},
        },
    },
};
use lakekeeper_integration_tests::{
    CommitGate, create_dataset, create_ns, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const DS: &str = "images";

/// More objects than one staging batch, so the listing heartbeats before it ends.
const OBJECTS: usize = 6_000;

struct Fixture {
    ctx: TestApiContext,
    pool: PgPool,
    prefix: String,
    ns: String,
}

async fn make_dataset(pool: PgPool) -> Fixture {
    let (ctx, warehouse) = setup(
        pool.clone(),
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
    let created = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();
    // The warehouse's memory profile reads the same thread-local store.
    let storage = lakekeeper_io::memory::MemoryStorage::new();
    for i in 0..OBJECTS {
        storage
            .write(
                &format!("{}/f{i:05}", created.dataset.location),
                bytes::Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
    }
    Fixture {
        ctx,
        pool,
        prefix,
        ns,
    }
}

impl Fixture {
    fn ds_params(&self) -> DatasetParameters {
        DatasetParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
        }
    }

    /// Queue an import and pick its task up, as a worker would.
    async fn picked_import(&self) -> DatasetImportTask {
        self.picked(ImportDatasetRequest::default()).await
    }

    async fn picked(&self, request: ImportDatasetRequest) -> DatasetImportTask {
        CatalogServer::import_dataset(
            self.ds_params(),
            ImportDatasetRequest {
                queued: Some(true),
                ..request
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        DatasetImportTask::poll_for_new_task::<PostgresBackend>(
            self.ctx.v1_state.catalog.clone(),
            &Duration::from_millis(10),
            lakekeeper::CancellationToken::new(),
        )
        .await
        .expect("the queued import is picked up")
    }

    async fn run(&self, task: &DatasetImportTask) {
        run_dataset_import_task::<PostgresBackend, SecretsState>(
            task,
            &self.ctx.v1_state.secrets,
            None,
            self.ctx.v1_state.catalog.clone(),
        )
        .await;
    }

    /// Run `task` up to its publish, which is held at commit while `meanwhile`
    /// changes the task's state, then let it finish. Returns once the run has
    /// returned, within three seconds of the publish: recording success on a task
    /// that is gone would retry for four.
    async fn run_changing_the_task_at_publish(
        &self,
        task: DatasetImportTask,
        meanwhile: impl AsyncFnOnce(&mut <PostgresBackend as CatalogStore>::Transaction),
    ) {
        let gate = CommitGate::install(&self.pool, "dataset_ref", "UPDATE").await;
        let running = tokio::spawn({
            let (secrets, catalog) = (
                self.ctx.v1_state.secrets.clone(),
                self.ctx.v1_state.catalog.clone(),
            );
            async move {
                run_dataset_import_task::<PostgresBackend, SecretsState>(
                    &task, &secrets, None, catalog,
                )
                .await;
            }
        });
        gate.wait_for_a_held_commit().await;
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        meanwhile(&mut t).await;
        t.commit().await.unwrap();
        gate.release().await;
        tokio::time::timeout(Duration::from_secs(3), running)
            .await
            .expect("the run returns once it has published")
            .unwrap();
    }

    async fn snapshots(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// The task's current status, or `None` once its row is gone.
    async fn task_status(&self, task_id: TaskId) -> Option<String> {
        sqlx::query_scalar("SELECT status::text FROM task WHERE task_id = $1")
            .bind(*task_id)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    /// The execution details the task's first attempt ended with.
    async fn first_attempt_details(&self, task_id: TaskId) -> serde_json::Value {
        sqlx::query_scalar(
            "SELECT execution_details FROM task_log WHERE task_id = $1 AND attempt = 1",
        )
        .bind(*task_id)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }
}

#[sqlx::test]
async fn test_a_stopped_import_publishes_nothing_and_runs_again(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = ds.picked_import().await;
    let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
        ds.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    PostgresBackend::stop_tasks(&[task.task_id()], t.transaction())
        .await
        .unwrap();
    t.commit().await.unwrap();

    ds.run(&task).await;

    assert_eq!(ds.snapshots().await, 0, "nothing published, nothing staged");
    // A stop is graceful: the task goes back on the queue.
    assert_eq!(
        ds.task_status(task.task_id()).await.as_deref(),
        Some("scheduled")
    );
    // Caught at the listing's first heartbeat, not only at the publish.
    let details = ds.first_attempt_details(task.task_id()).await;
    assert_eq!(details["phase"], "listing", "{details}");
    assert_eq!(details["objects_listed"], 5_000, "{details}");

    // The stop applied to that attempt only: the next one runs to the end.
    let again = DatasetImportTask::poll_for_new_task::<PostgresBackend>(
        ds.ctx.v1_state.catalog.clone(),
        &Duration::from_millis(10),
        lakekeeper::CancellationToken::new(),
    )
    .await
    .expect("the stopped import is picked up again");
    assert_eq!(again.task_id(), task.task_id());
    ds.run(&again).await;
    assert_eq!(ds.snapshots().await, 1);
    assert_eq!(ds.task_status(task.task_id()).await, None, "finished");
}

/// A stop reaches an import whose scope admits none of what it lists: heartbeats
/// count what the listing returns, not what the scope keeps.
#[sqlx::test]
async fn test_a_stop_reaches_an_import_that_admits_nothing(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = ds
        .picked(ImportDatasetRequest {
            include: Some(vec!["elsewhere/**".to_string()]),
            ..Default::default()
        })
        .await;
    let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
        ds.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    PostgresBackend::stop_tasks(&[task.task_id()], t.transaction())
        .await
        .unwrap();
    t.commit().await.unwrap();

    ds.run(&task).await;

    assert_eq!(
        ds.task_status(task.task_id()).await.as_deref(),
        Some("scheduled"),
        "the stop was heard, so the task goes back on the queue"
    );
    let details = ds.first_attempt_details(task.task_id()).await;
    assert_eq!(details["phase"], "listing", "{details}");
    assert_eq!(details["objects_listed"], 0, "{details}");
}

#[sqlx::test]
async fn test_a_cancelled_import_publishes_nothing(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = ds.picked_import().await;
    let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
        ds.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
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

    ds.run(&task).await;

    assert_eq!(ds.snapshots().await, 0, "nothing published, nothing staged");
    assert_eq!(
        ds.task_status(task.task_id()).await,
        None,
        "the task stays gone"
    );
}

/// A cancel that lands once the import has published undoes nothing, and the run
/// ends there: there is no task left to record the outcome on.
#[sqlx::test]
async fn test_a_cancel_after_the_publish_ends_the_run(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = ds.picked_import().await;
    let task_id = task.task_id();

    ds.run_changing_the_task_at_publish(task, async |t| {
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

    assert_eq!(ds.snapshots().await, 1, "published");
    assert_eq!(ds.task_status(task_id).await, None, "the task stays gone");
}

/// A stop that lands once the import has published undoes nothing: the task
/// finishes, as if the stop had come too late.
#[sqlx::test]
async fn test_a_stop_after_the_publish_lets_the_task_finish(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = ds.picked_import().await;
    let task_id = task.task_id();

    ds.run_changing_the_task_at_publish(task, async |t| {
        PostgresBackend::stop_tasks(&[task_id], t.transaction())
            .await
            .unwrap();
    })
    .await;

    assert_eq!(ds.snapshots().await, 1, "published");
    assert_eq!(ds.task_status(task_id).await, None, "finished");
    let details = ds.first_attempt_details(task_id).await;
    assert_eq!(details["phase"], "done", "{details}");
    assert_eq!(details["imported"], OBJECTS, "{details}");
}

/// One queued import runs for a dataset at a time: another is refused while the
/// first is queued or running, so two never publish over each other's listing.
#[sqlx::test]
async fn test_a_second_queued_import_is_refused_while_one_runs(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = ds.picked_import().await;

    let err = CatalogServer::import_dataset(
        ds.ds_params(),
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

    ds.run(&task).await;
    ds.picked_import().await;
}

#[sqlx::test]
async fn test_a_finished_import_records_where_it_got(pool: PgPool) {
    let ds = make_dataset(pool).await;
    let task = ds.picked_import().await;

    ds.run(&task).await;

    assert_eq!(ds.snapshots().await, 1);
    let details = ds.first_attempt_details(task.task_id()).await;
    assert_eq!(details["phase"], "done", "{details}");
    assert_eq!(details["objects_listed"], OBJECTS, "{details}");
    assert_eq!(details["imported"], OBJECTS, "{details}");
}
