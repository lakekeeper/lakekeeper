//! The table health routes: the ranking shows only tables whose metadata the caller may read,
//! and the detail shows the table's task only to a caller who may see its tasks.

use std::{collections::HashMap, time::Duration};

use iceberg::{TableIdent, TableUpdate};
use iceberg_ext::catalog::rest::{CommitTableRequest, CommitTransactionRequest};
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{DataAccess, NamespaceParameters, tables::TablesService as _},
        },
        management::v1::{
            ApiServer,
            health::{
                GetTableHealthResponse, ListTableHealthQuery, TableHealthOrderBy,
                TableHealthService as _, TableHealthStatus, TableHealthTask,
            },
            tasks::TaskStatus,
            warehouse::TabularDeleteProfile,
        },
    },
    server::CatalogServer,
    service::{
        CatalogStore, CatalogTaskOps as _, State, TableId, Transaction, WarehouseId,
        authz::tests::HidingAuthorizer,
    },
};
use lakekeeper_integration_tests::{
    create_ns, create_table_request, memory_io_profile, setup_simple, spawn_build_in_queues,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

type Ctx = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;
type Api = ApiServer<PostgresBackend, HidingAuthorizer, SecretsState>;

struct Setup {
    ctx: Ctx,
    authz: HidingAuthorizer,
    ns_params: NamespaceParameters,
    warehouse_id: WarehouseId,
}

async fn setup(pool: PgPool) -> Setup {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        authz.clone(),
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
        authz,
        ns_params: NamespaceParameters {
            prefix: Some(Prefix(prefix)),
            namespace,
        },
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

/// Commits a property change to the table, which schedules a measurement.
async fn commit(setup: &Setup, name: &str) {
    CatalogServer::commit_transaction(
        setup.ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(TableIdent::new(
                    setup.ns_params.namespace.clone(),
                    name.to_string(),
                )),
                requirements: vec![],
                updates: vec![TableUpdate::SetProperties {
                    updates: HashMap::from([("key".to_string(), Uuid::now_v7().to_string())]),
                }],
            }],
        },
        setup.ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
}

/// Stores a health row as a run of measurement version 1 would. `sizes` are the live data,
/// delete and metadata bytes, `None` for a table that can't be measured.
async fn insert_health(
    pool: &PgPool,
    warehouse_id: WarehouseId,
    table_id: TableId,
    sizes: Option<(i64, i64, i64)>,
) {
    let (status, max_severity, result) = match sizes {
        Some(_) => (
            "measured",
            0_i16,
            json!({"not-measurable": null, "findings": []}),
        ),
        None => (
            "not-measurable",
            2,
            json!({"not-measurable": {"reason": "encrypted"}, "findings": []}),
        ),
    };
    sqlx::query(
        "INSERT INTO health_iceberg_table (warehouse_id, table_id, measurement_version, status, \
         measured_at, next_not_before, max_severity, finding_count, result, live_data_bytes, \
         live_delete_bytes, live_metadata_bytes) \
         VALUES ($1, $2, 1, $3::health_status, now(), now(), $4, 0, $5, $6, $7, $8)",
    )
    .bind(*warehouse_id)
    .bind(*table_id)
    .bind(status)
    .bind(max_severity)
    .bind(result)
    .bind(sizes.map(|sizes| sizes.0))
    .bind(sizes.map(|sizes| sizes.1))
    .bind(sizes.map(|sizes| sizes.2))
    .execute(pool)
    .await
    .unwrap();
}

/// The ranking, read in pages of one.
async fn ranking(setup: &Setup) -> Vec<TableId> {
    let mut ids = vec![];
    let mut page_token = None;
    loop {
        let page = Api::list_table_health(
            setup.warehouse_id,
            ListTableHealthQuery {
                order_by: TableHealthOrderBy::Severity,
                include_soft_deleted: false,
                page_token: page_token.clone(),
                page_size: Some(1),
            },
            setup.ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        ids.extend(page.tables.iter().map(|table| table.table_id));
        match page.next_page_token {
            Some(token) => page_token = Some(token),
            None => return ids,
        }
    }
}

async fn detail(setup: &Setup, table_id: TableId) -> GetTableHealthResponse {
    Api::get_table_health(
        setup.warehouse_id,
        table_id,
        setup.ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap()
}

/// A hidden table is left out and the page refilled. `ListEverything` on the warehouse doesn't
/// skip the `GetMetadata` check.
#[lakekeeper_storage_postgres::db_test]
async fn test_ranking_lists_only_tables_whose_metadata_the_caller_may_read(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let mut tables = vec![];
    for (name, bytes) in [("large", 300), ("medium", 200), ("small", 100)] {
        let table_id = create_table(&setup, name).await;
        insert_health(&pool, setup.warehouse_id, table_id, Some((bytes, 0, 1))).await;
        tables.push(table_id);
    }
    assert_eq!(ranking(&setup).await, tables);

    setup
        .authz
        .hide(&format!("table:{}/{}", setup.warehouse_id, tables[1]));
    assert_eq!(ranking(&setup).await, vec![tables[0], tables[2]]);

    setup.authz.block_action("table:GetMetadata");
    assert_eq!(ranking(&setup).await, vec![]);
}

/// A table without a result shows `not-measured` and its waiting task. After the run, it shows
/// the result and the task's outcome, and then the next task together with that outcome. Without
/// the right to see tasks, there is no task.
#[lakekeeper_storage_postgres::db_test]
async fn test_detail_shows_the_result_and_the_task(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let table_id = create_table(&setup, "t").await;
    commit(&setup, "t").await;
    let (task_id, scheduled_for) = sqlx::query_as::<_, (Uuid, chrono::DateTime<chrono::Utc>)>(
        "SELECT task_id, scheduled_for FROM task WHERE queue_name = 'table_health'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let waiting = detail(&setup, table_id).await;
    assert_eq!(
        (
            waiting.status,
            waiting.namespace,
            waiting.name,
            waiting.task
        ),
        (
            TableHealthStatus::NotMeasured,
            vec!["ns".to_string()],
            "t".to_string(),
            Some(TableHealthTask {
                task_id: Some(task_id.into()),
                status: Some(TaskStatus::Scheduled),
                scheduled_for: Some(scheduled_for),
                last_outcome: None,
            })
        )
    );

    // `GetTasks` on the table is enough.
    setup.authz.block_action("warehouse:GetAllTasks");
    assert!(detail(&setup, table_id).await.task.is_some());
    setup.authz.block_action("table:GetTasks");
    assert_eq!(detail(&setup, table_id).await.task, None);
    setup.authz.unblock_action("warehouse:GetAllTasks");
    assert!(detail(&setup, table_id).await.task.is_some());
    setup.authz.unblock_action("table:GetTasks");

    let mut transaction = <PostgresBackend as CatalogStore>::Transaction::begin_write(
        setup.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    PostgresBackend::run_tasks_at(&[task_id.into()], None, transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let cancellation_token = lakekeeper::CancellationToken::new();
    let _queues = spawn_build_in_queues(
        &setup.ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;
    let mut measured = detail(&setup, table_id).await;
    for _ in 0..300 {
        if measured
            .task
            .as_ref()
            .and_then(|task| task.last_outcome)
            .is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        measured = detail(&setup, table_id).await;
    }
    cancellation_token.cancel();

    // The table has no snapshot, so only metadata.json counts.
    let metadata_bytes = measured.live_metadata_bytes.unwrap();
    assert!(metadata_bytes > 0);
    assert_eq!(
        (
            measured.status,
            (measured.live_data_bytes, measured.live_delete_bytes),
            measured.measurement_version,
            measured.manifests_read,
            measured.findings,
            measured.task,
        ),
        (
            TableHealthStatus::Measured,
            (Some(0), Some(0)),
            Some(1),
            Some(0),
            vec![],
            Some(TableHealthTask {
                task_id: None,
                status: None,
                scheduled_for: None,
                last_outcome: Some(TaskStatus::Success),
            }),
        )
    );

    // The next commit's task shows together with the last outcome.
    commit(&setup, "t").await;
    let (next_task_id, next_scheduled_for) =
        sqlx::query_as::<_, (Uuid, chrono::DateTime<chrono::Utc>)>(
            "SELECT task_id, scheduled_for FROM task WHERE queue_name = 'table_health'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        detail(&setup, table_id).await.task,
        Some(TableHealthTask {
            task_id: Some(next_task_id.into()),
            status: Some(TaskStatus::Scheduled),
            scheduled_for: Some(next_scheduled_for),
            last_outcome: Some(TaskStatus::Success),
        })
    );

    // A table the caller may not see is not found.
    setup
        .authz
        .hide(&format!("table:{}/{table_id}", setup.warehouse_id));
    let error = Api::get_table_health(
        setup.warehouse_id,
        table_id,
        setup.ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.error.code, 404);
}
