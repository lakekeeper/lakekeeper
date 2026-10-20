//! Cache invalidation across instances: a change another instance logged evicts this
//! instance's cached entry.

use lakekeeper::{
    CancellationToken,
    service::{
        CatalogWarehouseOps, WarehouseStatus, authz::AllowAllAuthorizer,
        cache_invalidation::background_tasks,
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, eventually, memory_io_profile};
use lakekeeper_storage_postgres::PostgresBackend;
use sqlx::PgPool;

#[sqlx::test]
async fn a_change_logged_by_another_instance_evicts_the_cached_warehouse(pool: PgPool) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let catalog = ctx.v1_state.catalog.clone();
    let warehouse_id = warehouse.warehouse_id;
    let read_name = || async {
        PostgresBackend::get_warehouse_by_id(
            warehouse_id,
            WarehouseStatus::active(),
            catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap()
        .name
        .clone()
    };
    let cancellation_token = CancellationToken::new();
    for (_, task) in background_tasks::<PostgresBackend>(catalog.clone(), &cancellation_token).await
    {
        tokio::spawn(task);
    }
    assert_eq!(read_name().await, warehouse.warehouse_name);

    // Another instance renames the warehouse. Without a logged change, this instance keeps
    // its cached entry.
    let mut rename = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL lakekeeper_test.unrecorded_write = on")
        .execute(&mut *rename)
        .await
        .unwrap();
    sqlx::query("UPDATE warehouse SET warehouse_name = 'renamed' WHERE warehouse_id = $1::uuid")
        .bind(warehouse_id.to_string())
        .execute(&mut *rename)
        .await
        .unwrap();
    rename.commit().await.unwrap();
    assert_eq!(read_name().await, warehouse.warehouse_name);

    sqlx::query("SELECT lakekeeper_record_cache_change('warehouse', ARRAY[$1])")
        .bind(warehouse_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    eventually("the logged change evicts the cached warehouse", || async {
        read_name().await == "renamed"
    })
    .await;
    cancellation_token.cancel();
}

/// The test setup refuses a warehouse write that recorded no cache change, so a write path
/// that forgets to record fails its tests.
#[sqlx::test]
async fn the_test_setup_refuses_an_unrecorded_warehouse_write(pool: PgPool) {
    let (_, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let error = sqlx::query(
        "UPDATE warehouse SET warehouse_name = 'renamed' WHERE warehouse_id = $1::uuid",
    )
    .bind(warehouse.warehouse_id.to_string())
    .execute(&pool)
    .await
    .unwrap_err();

    assert_eq!(
        error.as_database_error().map(|e| e.message().to_string()),
        Some("a write to warehouse recorded no warehouse cache change".to_string())
    );
}
