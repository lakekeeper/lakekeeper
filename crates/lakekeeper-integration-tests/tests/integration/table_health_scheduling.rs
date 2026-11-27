//! A table commit schedules a table health measurement in the commit's transaction, due an hour
//! later. A commit that lands while a measurement runs leads to another measurement. A measurement
//! stores its result in the same transaction that records the task's success.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use iceberg::{TableIdent, TableUpdate};
use iceberg_ext::catalog::rest::{CommitTableRequest, CommitTransactionRequest};
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{DataAccess, NamespaceParameters, tables::TablesService as _},
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        ArcProjectId, CatalogStore, NamedEntity as _, State, TableId, Transaction, WarehouseId,
        authz::AllowAllAuthorizer,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            table_health_queue::{HealthQueuePayload, HealthRunReason, TableHealthTask},
        },
    },
};
use lakekeeper_integration_tests::{
    create_ns, create_table_request, memory_io_profile, setup_simple, spawn_build_in_queues,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

struct Setup {
    ctx: Ctx,
    ns_params: NamespaceParameters,
    project_id: ArcProjectId,
    warehouse_id: WarehouseId,
}

async fn setup(pool: PgPool) -> Setup {
    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let namespace = create_ns(ctx.clone(), prefix.clone(), "ns".to_string())
        .await
        .namespace;
    Setup {
        ctx,
        ns_params: NamespaceParameters {
            prefix: Some(Prefix(prefix)),
            namespace,
        },
        project_id: warehouse.project_id,
        warehouse_id: warehouse.warehouse_id,
    }
}

async fn create_table(setup: &Setup, name: &str) -> TableId {
    let table = CatalogServer::create_table(
        setup.ns_params.clone(),
        create_table_request(Some(name.to_string()), Some(false)),
        DataAccess::not_specified(),
        setup.ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    TableId::from(table.metadata.uuid())
}

/// Set a property on each named table, in one `commitTransaction`.
async fn commit(setup: &Setup, names: &[&str]) {
    CatalogServer::commit_transaction(
        setup.ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: names
                .iter()
                .map(|name| CommitTableRequest {
                    identifier: Some(TableIdent::new(
                        setup.ns_params.namespace.clone(),
                        (*name).to_string(),
                    )),
                    requirements: vec![],
                    updates: vec![TableUpdate::SetProperties {
                        updates: HashMap::from([("key".to_string(), Uuid::now_v7().to_string())]),
                    }],
                })
                .collect(),
        },
        setup.ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
}

#[derive(Debug, PartialEq)]
struct HealthTask {
    task_id: Uuid,
    entity_id: Uuid,
    scheduled_for: DateTime<Utc>,
    payload: serde_json::Value,
}

/// The active `table_health` tasks, by table id.
async fn health_tasks(pool: &PgPool) -> Vec<HealthTask> {
    sqlx::query_as::<_, (Uuid, Uuid, DateTime<Utc>, serde_json::Value)>(
        "SELECT task_id, entity_id, scheduled_for, task_data FROM task \
         WHERE queue_name = 'table_health' ORDER BY entity_id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|(task_id, entity_id, scheduled_for, payload)| HealthTask {
        task_id,
        entity_id,
        scheduled_for,
        payload,
    })
    .collect()
}

/// Final status of a task, once it has one.
async fn final_status(pool: &PgPool, task_id: Uuid) -> Option<String> {
    sqlx::query_scalar::<_, String>("SELECT status::text FROM task_log WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .unwrap()
}

async fn wait_for_final_status(pool: &PgPool, task_id: Uuid) -> String {
    for _ in 0..300 {
        if let Some(status) = final_status(pool, task_id).await {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("task {task_id} did not finish");
}

#[derive(Debug, PartialEq)]
struct HealthRow {
    measurement_version: i32,
    status: String,
    next_not_before: DateTime<Utc>,
    /// Live data, delete and metadata bytes.
    sizes: Option<(i64, i64, i64)>,
    result: serde_json::Value,
}

/// The stored health of a table.
async fn health_row(pool: &PgPool, table_id: TableId) -> Option<HealthRow> {
    sqlx::query_as::<
        _,
        (
            i32,
            String,
            DateTime<Utc>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            serde_json::Value,
        ),
    >(
        "SELECT measurement_version, status::text, next_not_before, live_data_bytes, \
         live_delete_bytes, live_metadata_bytes, result \
         FROM health_iceberg_table WHERE table_id = $1",
    )
    .bind(*table_id)
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(
        |(measurement_version, status, next_not_before, data, delete, metadata, result)| {
            HealthRow {
                measurement_version,
                status,
                next_not_before,
                sizes: data
                    .zip(delete)
                    .zip(metadata)
                    .map(|((data, delete), metadata)| (data, delete, metadata)),
                result,
            }
        },
    )
}

/// Execution details of a finished task.
async fn execution_details(pool: &PgPool, task_id: Uuid) -> serde_json::Value {
    sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT execution_details FROM task_log WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn metadata_location(pool: &PgPool, table_id: TableId) -> String {
    sqlx::query_scalar::<_, String>("SELECT metadata_location FROM tabular WHERE tabular_id = $1")
        .bind(*table_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Wait until a database session waits for a row lock.
async fn wait_for_lock_waiter(pool: &PgPool) {
    for _ in 0..300 {
        let waiting = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE wait_event_type = 'Lock' AND datname = current_database()",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no session waited for a lock");
}

/// Schedule a measurement of the table that is due now.
async fn schedule_measurement_now(setup: &Setup, table_id: TableId, name: &str) -> Uuid {
    let mut transaction = <PostgresBackend as CatalogStore>::Transaction::begin_write(
        setup.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let outcome = TableHealthTask::schedule_task::<PostgresBackend>(
        ScheduleTaskMetadata {
            project_id: setup.project_id.clone(),
            parent_task_id: None,
            scheduled_for: None,
            entity: TaskEntity::EntityInWarehouse {
                warehouse_id: setup.warehouse_id,
                entity_id: WarehouseTaskEntityId::Table { table_id },
                entity_name: TableIdent::new(setup.ns_params.namespace.clone(), name.to_string())
                    .into_name_parts(),
            },
            resource_demand: None,
        },
        HealthQueuePayload::new(HealthRunReason::Manual),
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    *outcome.inserted().unwrap()
}

#[lakekeeper_storage_postgres::db_test]
async fn test_commit_schedules_one_measurement_per_table(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let table_1 = create_table(&setup, "t1").await;
    let table_2 = create_table(&setup, "t2").await;
    assert_eq!(health_tasks(&pool).await, vec![]);

    let before = Utc::now();
    commit(&setup, &["t1", "t2"]).await;
    let after = Utc::now();

    let tasks = health_tasks(&pool).await;
    let mut table_ids = vec![*table_1, *table_2];
    table_ids.sort();
    assert_eq!(
        tasks.iter().map(|t| t.entity_id).collect::<Vec<_>>(),
        table_ids
    );
    for task in &tasks {
        assert!(task.scheduled_for >= before + chrono::TimeDelta::hours(1));
        assert!(task.scheduled_for <= after + chrono::TimeDelta::hours(1));
        assert_eq!(task.payload, serde_json::json!({ "reason": "commit" }));
    }

    // A second commit within the delay keeps the waiting task and its time.
    commit(&setup, &["t1"]).await;
    assert_eq!(health_tasks(&pool).await, tasks);
}

#[lakekeeper_storage_postgres::db_test]
async fn test_finish_without_commit_schedules_nothing(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let table_id = create_table(&setup, "t").await;
    let task_id = schedule_measurement_now(&setup, table_id, "t").await;

    let cancellation_token = lakekeeper::CancellationToken::new();
    let _queues = spawn_build_in_queues(
        &setup.ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;

    assert_eq!(wait_for_final_status(&pool, task_id).await, "success");
    assert_eq!(health_tasks(&pool).await, vec![]);

    // The table has no snapshot, so only metadata.json counts.
    let row = health_row(&pool, table_id).await.unwrap();
    assert_eq!(
        (row.measurement_version, row.status.as_str()),
        (1, "measured")
    );
    let (_, _, metadata_bytes) = row.sizes.unwrap();
    assert!(metadata_bytes > 0);
    assert_eq!(row.sizes, Some((0, 0, metadata_bytes)));
    assert_eq!(
        row.result["metadata-location"],
        serde_json::json!(metadata_location(&pool, table_id).await)
    );
    let details = execution_details(&pool, task_id).await;
    assert_eq!(
        (&details["measurement-version"], &details["manifests-read"]),
        (&serde_json::json!(1), &serde_json::json!(0))
    );
    cancellation_token.cancel();
}

#[lakekeeper_storage_postgres::db_test]
async fn test_commit_during_measurement_schedules_follow_up(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let table_id = create_table(&setup, "t").await;
    let task_id = schedule_measurement_now(&setup, table_id, "t").await;

    // Stands in for a commit that runs while the table is measured: it locks the table row
    // like a commit and changes the metadata location.
    let mut commit_transaction = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE tabular SET metadata_location = metadata_location || '.next' \
         WHERE tabular_id = $1",
    )
    .bind(*table_id)
    .execute(&mut *commit_transaction)
    .await
    .unwrap();

    let cancellation_token = lakekeeper::CancellationToken::new();
    let _queues = spawn_build_in_queues(
        &setup.ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;

    // The measurement reads the committed location, then its finish waits for the row lock.
    wait_for_lock_waiter(&pool).await;
    assert_eq!(final_status(&pool, task_id).await, None);
    let before = Utc::now();
    commit_transaction.commit().await.unwrap();

    assert_eq!(wait_for_final_status(&pool, task_id).await, "success");
    let tasks = health_tasks(&pool).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].entity_id, *table_id);
    assert!(tasks[0].scheduled_for >= before + chrono::TimeDelta::hours(1));
    assert_eq!(tasks[0].payload, serde_json::json!({ "reason": "commit" }));
    // The stored result is of the location before the commit, and its next run is the follow-up.
    let row = health_row(&pool, table_id).await.unwrap();
    assert_eq!(row.next_not_before, tasks[0].scheduled_for);
    assert_ne!(
        row.result["metadata-location"],
        serde_json::json!(metadata_location(&pool, table_id).await)
    );
    cancellation_token.cancel();
}

#[lakekeeper_storage_postgres::db_test]
async fn test_table_deleted_during_measurement_schedules_nothing(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let table_id = create_table(&setup, "t").await;
    let task_id = schedule_measurement_now(&setup, table_id, "t").await;

    let mut drop_transaction = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM tabular WHERE tabular_id = $1")
        .bind(*table_id)
        .execute(&mut *drop_transaction)
        .await
        .unwrap();

    let cancellation_token = lakekeeper::CancellationToken::new();
    let _queues = spawn_build_in_queues(
        &setup.ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;

    wait_for_lock_waiter(&pool).await;
    drop_transaction.commit().await.unwrap();

    assert_eq!(wait_for_final_status(&pool, task_id).await, "success");
    assert_eq!(health_tasks(&pool).await, vec![]);
    assert_eq!(health_row(&pool, table_id).await, None);
    cancellation_token.cancel();
}

/// Point the table at a metadata.json that does not exist. Returns the new location.
async fn point_at_missing_metadata(pool: &PgPool, table_id: TableId) -> String {
    sqlx::query_scalar::<_, String>(
        "UPDATE tabular SET metadata_location = metadata_location || '.missing' \
         WHERE tabular_id = $1 RETURNING metadata_location",
    )
    .bind(*table_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn task_message(pool: &PgPool, task_id: Uuid) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>("SELECT message FROM task_log WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[lakekeeper_storage_postgres::db_test]
async fn test_missing_metadata_is_stored_as_not_measurable(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let table_id = create_table(&setup, "t").await;
    let missing = point_at_missing_metadata(&pool, table_id).await;
    let task_id = schedule_measurement_now(&setup, table_id, "t").await;

    let cancellation_token = lakekeeper::CancellationToken::new();
    let _queues = spawn_build_in_queues(
        &setup.ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;

    assert_eq!(wait_for_final_status(&pool, task_id).await, "success");
    assert_eq!(health_tasks(&pool).await, vec![]);
    let row = health_row(&pool, table_id).await.unwrap();
    assert_eq!((row.status.as_str(), row.sizes), ("not-measurable", None));
    assert_eq!(
        row.result["not-measurable"],
        serde_json::json!({ "reason": "metadata-missing", "path": missing })
    );
    cancellation_token.cancel();
}

#[lakekeeper_storage_postgres::db_test]
async fn test_missing_metadata_after_a_commit_is_not_stored(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let table_id = create_table(&setup, "t").await;
    point_at_missing_metadata(&pool, table_id).await;
    let task_id = schedule_measurement_now(&setup, table_id, "t").await;

    // Stands in for a commit that runs while the table is measured, see
    // `test_commit_during_measurement_schedules_follow_up`.
    let mut commit_transaction = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE tabular SET metadata_location = metadata_location || '.next' \
         WHERE tabular_id = $1",
    )
    .bind(*table_id)
    .execute(&mut *commit_transaction)
    .await
    .unwrap();

    let cancellation_token = lakekeeper::CancellationToken::new();
    let _queues = spawn_build_in_queues(
        &setup.ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;

    wait_for_lock_waiter(&pool).await;
    commit_transaction.commit().await.unwrap();

    assert_eq!(wait_for_final_status(&pool, task_id).await, "success");
    assert_eq!(
        task_message(&pool, task_id).await.as_deref(),
        Some("Not stored: a file was missing and a commit changed the table during the run.")
    );
    assert_eq!(health_row(&pool, table_id).await, None);
    let tasks = health_tasks(&pool).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].payload, serde_json::json!({ "reason": "commit" }));
    cancellation_token.cancel();
}
