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
    sqlx::query("UPDATE warehouse SET warehouse_name = 'renamed' WHERE warehouse_id = $1::uuid")
        .bind(warehouse_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
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
