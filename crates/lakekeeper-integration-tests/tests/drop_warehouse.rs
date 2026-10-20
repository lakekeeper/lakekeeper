use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext,
        iceberg::{
            types::Prefix,
            v1::{
                NamespaceParameters,
                namespace::{NamespaceDropFlags, NamespaceService},
            },
        },
        management::v1::{
            ApiServer, DeleteWarehouseQuery,
            warehouse::{CreateWarehouseRequest, Service, TabularDeleteProfile},
        },
    },
    server::CatalogServer,
    service::{
        ArcProjectId, CatalogStore, CatalogTaskOps, State, Transaction,
        authz::AllowAllAuthorizer,
        tasks::{
            CancelTasksFilter, OnWarehouseDelete, QueueRegistration, QueueScope,
            ScheduleTaskMetadata, SpecializedTask, TaskCheckState, TaskConfig, TaskData,
            TaskEntity, TaskExecutionDetails, TaskId, TaskInput, TaskQueueName, TaskQueueRegistry,
            TaskScheduleUpsert, UserScheduling, WarehouseTaskEntityId,
        },
    },
};
use lakekeeper_integration_tests::{
    TestWarehouseResponse, create_ns, create_table, eventually, memory_io_profile,
    random_request_metadata, setup_with_registry, spawn_build_in_queues,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_cannot_drop_warehouse_before_purge_tasks_completed(pool: PgPool) {
    let storage_profile = lakekeeper_integration_tests::memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();

    let (api_context, _) = lakekeeper_integration_tests::setup(
        pool.clone(),
        storage_profile.clone(),
        None,
        authorizer,
        TabularDeleteProfile::default(),
        None,
        1,
        None,
    )
    .await;

    // Create a warehouse
    let warehouse_name = format!("test_warehouse_{}", Uuid::now_v7());
    let warehouse = ApiServer::create_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(warehouse_name.clone())
            .storage_profile(storage_profile)
            .build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create namespace
    let ns_name = NamespaceIdent::new(format!("test_namespace_{}", Uuid::now_v7()));
    let prefix = Some(Prefix(warehouse.warehouse_id().to_string()));
    let _ = CatalogServer::create_namespace(
        prefix.clone(),
        CreateNamespaceRequest {
            namespace: ns_name.clone(),
            properties: None,
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create tables
    for i in 0..2 {
        let table_name = format!("table_{i}");
        let _ = lakekeeper_integration_tests::create_table(
            api_context.clone(),
            &warehouse.warehouse_id().to_string(),
            &ns_name.to_string(),
            &table_name,
            false,
        )
        .await
        .unwrap();
    }

    // Delete namespace recursively with purge
    CatalogServer::drop_namespace(
        NamespaceParameters {
            prefix: prefix.clone(),
            namespace: ns_name.clone(),
        },
        NamespaceDropFlags::builder().recursive().purge().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Drop warehouse - this should fail due to purge tasks
    let err = ApiServer::delete_warehouse(
        warehouse.warehouse_id(),
        DeleteWarehouseQuery::builder().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("Warehouse deletion should fail due to purge tasks");
    assert_eq!(err.error.r#type, "WarehouseHasUnfinishedTasks");
    assert_eq!(err.error.code, 409);

    // Spawn task queue workers
    let cancellation_token = lakekeeper::CancellationToken::new();
    let queues_handle = spawn_build_in_queues(
        &api_context,
        Some(std::time::Duration::from_secs(1)),
        cancellation_token.clone(),
    )
    .await;

    // Drop warehouse — poll until purge tasks complete to avoid flakiness
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match ApiServer::delete_warehouse(
            warehouse.warehouse_id(),
            DeleteWarehouseQuery::builder().build(),
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        {
            Ok(()) => break,
            Err(_e) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => panic!("Warehouse deletion did not complete within 5s: {e:?}"),
        }
    }
    cancellation_token.cancel();
    queues_handle.await.unwrap();
}

type TestContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

static CANCEL_QUEUE: LazyLock<TaskQueueName> =
    LazyLock::new(|| "test_cancel_on_warehouse_delete".into());
static CANCEL_QUEUE_LEGACY: LazyLock<TaskQueueName> =
    LazyLock::new(|| "test_cancel_on_warehouse_delete_legacy".into());
static BLOCK_QUEUE: LazyLock<TaskQueueName> =
    LazyLock::new(|| "test_block_on_warehouse_delete".into());

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(lakekeeper::utoipa::ToSchema))]
struct CancelQueueConfig {}

impl TaskConfig for CancelQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &CANCEL_QUEUE
    }
    fn legacy_queue_names() -> Vec<&'static TaskQueueName> {
        vec![&CANCEL_QUEUE_LEGACY]
    }
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(60)
    }
    fn on_warehouse_delete() -> OnWarehouseDelete {
        OnWarehouseDelete::Cancel
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(lakekeeper::utoipa::ToSchema))]
struct BlockQueueConfig {}

impl TaskConfig for BlockQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &BLOCK_QUEUE
    }
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(60)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Empty {}
impl TaskData for Empty {}
impl TaskExecutionDetails for Empty {}

type CancelQueueTask = SpecializedTask<CancelQueueConfig, Empty, Empty>;

async fn setup_with_test_queues(
    pool: PgPool,
    number_of_warehouses: usize,
) -> (TestContext, TestWarehouseResponse) {
    let (ctx, warehouse, registry) = setup_with_registry(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        number_of_warehouses,
        None,
    )
    .await;
    register_without_workers::<CancelQueueConfig>(&registry).await;
    register_without_workers::<BlockQueueConfig>(&registry).await;
    (ctx, warehouse)
}

async fn register_without_workers<T: TaskConfig>(registry: &TaskQueueRegistry) {
    registry
        .register_queue::<T, Empty>(QueueRegistration {
            queue_name: T::queue_name(),
            worker_fn: Arc::new(|_| Box::pin(async {})),
            num_workers: 0,
            scope: QueueScope::Warehouse,
            user_scheduling: UserScheduling::Disabled,
        })
        .await;
}

fn table_entity(warehouse_id: WarehouseId) -> TaskEntity {
    let table_id = Uuid::now_v7();
    TaskEntity::EntityInWarehouse {
        warehouse_id,
        entity_id: WarehouseTaskEntityId::Table {
            table_id: table_id.into(),
        },
        entity_name: vec![format!("table-{table_id}")],
    }
}

fn task_input(project_id: &ArcProjectId, entity: TaskEntity) -> TaskInput {
    TaskInput {
        task_metadata: ScheduleTaskMetadata {
            project_id: project_id.clone(),
            parent_task_id: None,
            entity,
            scheduled_for: None,
        },
        payload: serde_json::json!({}),
    }
}

async fn enqueue_entity(
    ctx: &TestContext,
    queue_name: &'static TaskQueueName,
    project_id: &ArcProjectId,
    entity: TaskEntity,
) -> TaskId {
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let task_id = PostgresBackend::enqueue_task(
        queue_name,
        task_input(project_id, entity),
        transaction.transaction(),
    )
    .await
    .unwrap()
    .inserted()
    .unwrap();
    transaction.commit().await.unwrap();
    task_id
}

async fn enqueue(
    ctx: &TestContext,
    queue_name: &'static TaskQueueName,
    warehouse: &TestWarehouseResponse,
) {
    enqueue_entity(
        ctx,
        queue_name,
        &warehouse.project_id,
        table_entity(warehouse.warehouse_id),
    )
    .await;
}

/// Enqueues two running, a scheduled and a legacy-named task of [`CANCEL_QUEUE`]. Returns the
/// running ones.
async fn enqueue_cancel_queue_tasks(
    ctx: &TestContext,
    warehouse: &TestWarehouseResponse,
) -> [CancelQueueTask; 2] {
    enqueue(ctx, &CANCEL_QUEUE, warehouse).await;
    enqueue(ctx, &CANCEL_QUEUE, warehouse).await;
    let mut running = Vec::new();
    for _ in 0..2 {
        running.push(pick(ctx).await);
    }
    enqueue(ctx, &CANCEL_QUEUE, warehouse).await;
    enqueue(ctx, &CANCEL_QUEUE_LEGACY, warehouse).await;
    running.try_into().unwrap()
}

async fn pick(ctx: &TestContext) -> CancelQueueTask {
    CancelQueueTask::pick_new_task::<PostgresBackend>(ctx.v1_state.catalog.clone())
        .await
        .unwrap()
        .unwrap()
}

async fn delete_warehouse(
    ctx: &TestContext,
    warehouse_id: WarehouseId,
    query: DeleteWarehouseQuery,
) -> lakekeeper::api::Result<()> {
    ApiServer::delete_warehouse(warehouse_id, query, ctx.clone(), random_request_metadata()).await
}

/// `(queue_name, status)` of every task row of the warehouse, sorted.
async fn task_rows(pool: &PgPool, warehouse_id: WarehouseId) -> Vec<(String, String)> {
    sqlx::query_as(
        r#"SELECT queue_name, status::text FROM task WHERE warehouse_id = $1
           ORDER BY queue_name COLLATE "C", status::text COLLATE "C""#,
    )
    .bind(*warehouse_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

fn row(queue_name: &TaskQueueName, status: &str) -> (String, String) {
    (queue_name.to_string(), status.to_string())
}

async fn wait_for_a_lock_wait(pool: &PgPool) {
    eventually("a session waits for a lock", || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await
        .unwrap()
            > 0
    })
    .await;
}

#[sqlx::test]
async fn test_delete_warehouse_cancels_tasks_of_cancel_queues(pool: PgPool) {
    let (ctx, warehouse) = setup_with_test_queues(pool.clone(), 1).await;
    let [succeeding, failing] = enqueue_cancel_queue_tasks(&ctx, &warehouse).await;
    assert_eq!(
        task_rows(&pool, warehouse.warehouse_id).await,
        vec![
            row(&CANCEL_QUEUE, "running"),
            row(&CANCEL_QUEUE, "running"),
            row(&CANCEL_QUEUE, "scheduled"),
            row(&CANCEL_QUEUE_LEGACY, "scheduled"),
        ]
    );

    let query = DeleteWarehouseQuery::builder().build();
    delete_warehouse(&ctx, warehouse.warehouse_id, query)
        .await
        .unwrap();
    assert_eq!(task_rows(&pool, warehouse.warehouse_id).await, vec![]);

    // Workers of the cancelled running tasks find them gone (404) and stop. A retrying call
    // sleeps 4 x 1 s, so 3 s leaves headroom for slow machines.
    let started = Instant::now();
    succeeding
        .record_success::<PostgresBackend>(ctx.v1_state.catalog.clone(), None)
        .await;
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(3), "took {elapsed:?}");
    let started = Instant::now();
    failing
        .record_failure::<PostgresBackend>(ctx.v1_state.catalog.clone(), "failed")
        .await;
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(3), "took {elapsed:?}");
}

#[sqlx::test]
async fn test_block_queue_task_fails_warehouse_delete(pool: PgPool) {
    let (ctx, warehouse) = setup_with_test_queues(pool.clone(), 1).await;
    enqueue_cancel_queue_tasks(&ctx, &warehouse).await;
    enqueue(&ctx, &BLOCK_QUEUE, &warehouse).await;
    let expected_rows = vec![
        row(&BLOCK_QUEUE, "scheduled"),
        row(&CANCEL_QUEUE, "running"),
        row(&CANCEL_QUEUE, "running"),
        row(&CANCEL_QUEUE, "scheduled"),
        row(&CANCEL_QUEUE_LEGACY, "scheduled"),
    ];
    assert_eq!(
        task_rows(&pool, warehouse.warehouse_id).await,
        expected_rows
    );

    let query = DeleteWarehouseQuery::builder().build();
    let err = delete_warehouse(&ctx, warehouse.warehouse_id, query)
        .await
        .unwrap_err();
    assert_eq!(err.error.r#type, "WarehouseHasUnfinishedTasks");
    assert_eq!(err.error.code, 409);
    // The cancellation rolled back with the failed delete.
    assert_eq!(
        task_rows(&pool, warehouse.warehouse_id).await,
        expected_rows
    );
}

#[sqlx::test]
async fn test_delete_warehouse_cancels_only_tasks_of_that_warehouse(pool: PgPool) {
    let (ctx, warehouse) = setup_with_test_queues(pool.clone(), 2).await;
    let (_, other_warehouse_id, _) = warehouse.additional_warehouses[0].clone();
    let project_id = &warehouse.project_id;
    enqueue(&ctx, &CANCEL_QUEUE, &warehouse).await;
    let deleted_warehouse = TaskEntity::Warehouse {
        warehouse_id: warehouse.warehouse_id,
    };
    enqueue_entity(&ctx, &CANCEL_QUEUE, project_id, deleted_warehouse).await;
    let other_warehouse = TaskEntity::Warehouse {
        warehouse_id: other_warehouse_id,
    };
    enqueue_entity(&ctx, &CANCEL_QUEUE, project_id, other_warehouse).await;
    enqueue_entity(&ctx, &CANCEL_QUEUE, project_id, TaskEntity::Project).await;
    let scheduled = vec![row(&CANCEL_QUEUE, "scheduled")];
    assert_eq!(
        task_rows(&pool, warehouse.warehouse_id).await,
        vec![
            row(&CANCEL_QUEUE, "scheduled"),
            row(&CANCEL_QUEUE, "scheduled")
        ]
    );

    let query = DeleteWarehouseQuery::builder().build();
    delete_warehouse(&ctx, warehouse.warehouse_id, query)
        .await
        .unwrap();

    assert_eq!(task_rows(&pool, warehouse.warehouse_id).await, vec![]);
    assert_eq!(task_rows(&pool, other_warehouse_id).await, scheduled);
    // Filtered by queue: bootstrapping creates a project task of another queue.
    let project_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT queue_name, status::text FROM task WHERE warehouse_id IS NULL AND queue_name = $1",
    )
    .bind(CANCEL_QUEUE.as_str())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(project_rows, scheduled);
}

/// The cancel runs before the warehouse row is locked, so a worker that holds its task row and
/// then enqueues its next run in the same transaction finishes without deadlocking.
#[sqlx::test]
async fn test_delete_warehouse_cancels_before_locking_the_warehouse(pool: PgPool) {
    let (ctx, warehouse) = setup_with_test_queues(pool.clone(), 1).await;
    enqueue(&ctx, &CANCEL_QUEUE, &warehouse).await;
    let running = pick(&ctx).await;

    // The heartbeat locks the task row until the worker commits.
    let mut worker =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let state = running
        .heartbeat_in_transaction::<PostgresBackend>(worker.transaction(), 0.5, None)
        .await
        .unwrap();
    assert_eq!(state, TaskCheckState::Continue);

    let delete = tokio::spawn({
        let ctx = ctx.clone();
        let warehouse_id = warehouse.warehouse_id;
        let query = DeleteWarehouseQuery::builder().build();
        async move { delete_warehouse(&ctx, warehouse_id, query).await }
    });
    wait_for_a_lock_wait(&pool).await;

    // The next run's foreign-key check needs the warehouse row.
    let next_run = task_input(&warehouse.project_id, table_entity(warehouse.warehouse_id));
    tokio::time::timeout(
        Duration::from_secs(5),
        PostgresBackend::enqueue_task(&CANCEL_QUEUE, next_run, worker.transaction()),
    )
    .await
    .expect("enqueueing does not wait for the delete")
    .unwrap()
    .inserted()
    .unwrap();
    worker.commit().await.unwrap();

    // The next run was created after the cancel: a retryable conflict.
    let err = delete.await.unwrap().unwrap_err();
    assert_eq!(err.error.r#type, "WarehouseHasUnfinishedTasks");
    assert_eq!(err.error.code, 409);
    let query = DeleteWarehouseQuery::builder().build();
    delete_warehouse(&ctx, warehouse.warehouse_id, query)
        .await
        .unwrap();
    assert_eq!(task_rows(&pool, warehouse.warehouse_id).await, vec![]);
}

/// Schedules are deleted before the warehouse row is locked, so a materializer that holds a
/// schedule row and then inserts its task finishes without deadlocking.
#[sqlx::test]
async fn test_delete_warehouse_deletes_schedules_before_locking_the_warehouse(pool: PgPool) {
    let (ctx, warehouse) = setup_with_test_queues(pool.clone(), 1).await;
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let schedule = PostgresBackend::upsert_task_schedule(
        TaskScheduleUpsert::builder()
            .project_id(warehouse.project_id.clone())
            .warehouse_id(Some(warehouse.warehouse_id))
            .queue_name(CANCEL_QUEUE.clone())
            .schedule_key("nightly")
            .interval(chrono::Duration::hours(1))
            .build(),
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    // The materializer locks the schedule row until it commits.
    let mut materializer =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    sqlx::query("SELECT 1 FROM task_schedule WHERE schedule_id = $1 FOR UPDATE")
        .bind(*schedule.schedule_id)
        .execute(&mut **materializer.transaction())
        .await
        .unwrap();

    let delete = tokio::spawn({
        let ctx = ctx.clone();
        let warehouse_id = warehouse.warehouse_id;
        let query = DeleteWarehouseQuery::builder().build();
        async move { delete_warehouse(&ctx, warehouse_id, query).await }
    });
    wait_for_a_lock_wait(&pool).await;

    // The task's foreign-key check needs the warehouse row.
    let tick = task_input(&warehouse.project_id, table_entity(warehouse.warehouse_id));
    tokio::time::timeout(
        Duration::from_secs(5),
        PostgresBackend::enqueue_task(&CANCEL_QUEUE, tick, materializer.transaction()),
    )
    .await
    .expect("enqueueing does not wait for the delete")
    .unwrap()
    .inserted()
    .unwrap();
    materializer.commit().await.unwrap();

    delete.await.unwrap().unwrap();
    assert_eq!(task_rows(&pool, warehouse.warehouse_id).await, vec![]);
    let schedules: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_schedule WHERE warehouse_id = $1")
            .bind(*warehouse.warehouse_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(schedules, 0);
}

#[sqlx::test]
async fn test_protected_warehouse_delete_keeps_cancel_queue_tasks(pool: PgPool) {
    let (ctx, warehouse) = setup_with_test_queues(pool.clone(), 1).await;
    enqueue_cancel_queue_tasks(&ctx, &warehouse).await;
    let expected_rows = vec![
        row(&CANCEL_QUEUE, "running"),
        row(&CANCEL_QUEUE, "running"),
        row(&CANCEL_QUEUE, "scheduled"),
        row(&CANCEL_QUEUE_LEGACY, "scheduled"),
    ];
    assert_eq!(
        task_rows(&pool, warehouse.warehouse_id).await,
        expected_rows
    );
    ApiServer::set_warehouse_protection(
        warehouse.warehouse_id,
        true,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let query = DeleteWarehouseQuery::builder().build();
    let err = delete_warehouse(&ctx, warehouse.warehouse_id, query)
        .await
        .unwrap_err();
    assert_eq!(err.error.r#type, "WarehouseProtected");
    assert_eq!(err.error.code, 409);
    assert_eq!(
        task_rows(&pool, warehouse.warehouse_id).await,
        expected_rows
    );

    let query = DeleteWarehouseQuery::builder().force().build();
    delete_warehouse(&ctx, warehouse.warehouse_id, query)
        .await
        .unwrap();
    assert_eq!(task_rows(&pool, warehouse.warehouse_id).await, vec![]);
}

/// `(task_id, status, attempt)` of every task row of the warehouse, sorted by status.
async fn task_attempts(pool: &PgPool, warehouse_id: WarehouseId) -> Vec<(Uuid, String, i32)> {
    sqlx::query_as(
        r#"SELECT task_id, status::text, attempt FROM task WHERE warehouse_id = $1
           ORDER BY status::text COLLATE "C""#,
    )
    .bind(*warehouse_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[sqlx::test]
async fn test_non_empty_warehouse_delete_keeps_cancel_queue_tasks(pool: PgPool) {
    let (ctx, warehouse) = setup_with_test_queues(pool.clone(), 1).await;
    let prefix = warehouse.warehouse_id.to_string();
    create_ns(ctx.clone(), prefix.clone(), "ns".to_string()).await;
    create_table(ctx.clone(), prefix, "ns", "table", false)
        .await
        .unwrap();
    let project_id = &warehouse.project_id;
    let entity = table_entity(warehouse.warehouse_id);
    enqueue_entity(&ctx, &CANCEL_QUEUE, project_id, entity).await;
    let running = pick(&ctx).await.id.task_id;
    let entity = table_entity(warehouse.warehouse_id);
    let scheduled = enqueue_entity(&ctx, &CANCEL_QUEUE, project_id, entity).await;
    let expected_rows = vec![
        (*running, "running".to_string(), 1),
        (*scheduled, "scheduled".to_string(), 0),
    ];
    assert_eq!(
        task_attempts(&pool, warehouse.warehouse_id).await,
        expected_rows
    );

    let query = DeleteWarehouseQuery::builder().build();
    let err = delete_warehouse(&ctx, warehouse.warehouse_id, query)
        .await
        .unwrap_err();
    assert_eq!(err.error.r#type, "WarehouseNotEmpty");
    assert_eq!(err.error.code, 409);
    // The cancellation rolled back with the failed delete.
    assert_eq!(
        task_attempts(&pool, warehouse.warehouse_id).await,
        expected_rows
    );
}

/// Every queue the built-in registration adds keeps the default: its tasks block the delete.
#[sqlx::test]
async fn test_built_in_queue_tasks_block_warehouse_delete(pool: PgPool) {
    let (ctx, warehouse, _registry) = setup_with_registry(
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
    let built_in_queues = ctx.v1_state.registered_task_queues.queue_names().await;
    assert_eq!(
        built_in_queues
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["soft_deletion", "tabular_purge", "task_log_cleanup"]
    );

    let warehouse_entity = TaskEntity::Warehouse {
        warehouse_id: warehouse.warehouse_id,
    };
    for queue_name in built_in_queues {
        let task_id = enqueue_entity(
            &ctx,
            queue_name,
            &warehouse.project_id,
            warehouse_entity.clone(),
        )
        .await;
        let query = DeleteWarehouseQuery::builder().build();
        let err = delete_warehouse(&ctx, warehouse.warehouse_id, query)
            .await
            .unwrap_err();
        assert_eq!(
            (err.error.r#type.as_str(), err.error.code),
            ("WarehouseHasUnfinishedTasks", 409),
            "{queue_name}"
        );

        let mut transaction = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        PostgresBackend::cancel_scheduled_tasks(
            None,
            &[],
            CancelTasksFilter::TaskIds(vec![task_id]),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(
            task_rows(&pool, warehouse.warehouse_id).await,
            Vec::<(String, String)>::new(),
            "{queue_name}"
        );
    }
}
