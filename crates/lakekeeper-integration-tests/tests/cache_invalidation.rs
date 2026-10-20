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
use lakekeeper_storage_postgres::{PostgresBackend, test_utils::create_ns};
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

/// The test setup refuses a warehouse or namespace write that recorded no cache change, so a
/// write path that forgets to record fails its tests.
#[sqlx::test]
async fn the_test_setup_refuses_unrecorded_writes(pool: PgPool) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    create_ns(
        ctx,
        warehouse.warehouse_id.to_string(),
        "unrecorded".to_string(),
    )
    .await;
    let refused = |statement: &'static str, id: String| {
        let pool = pool.clone();
        async move {
            sqlx::query(statement)
                .bind(id)
                .execute(&pool)
                .await
                .unwrap_err()
                .as_database_error()
                .map(|e| e.message().to_string())
        }
    };

    assert_eq!(
        (
            refused(
                "UPDATE warehouse SET warehouse_name = 'renamed' WHERE warehouse_id = $1::uuid",
                warehouse.warehouse_id.to_string(),
            )
            .await,
            refused(
                "UPDATE namespace SET namespace_properties = '{}' WHERE warehouse_id = $1::uuid",
                warehouse.warehouse_id.to_string(),
            )
            .await,
        ),
        (
            Some(format!(
                "a write to warehouse recorded no warehouse cache change for {}",
                warehouse.warehouse_id
            )),
            Some("a write to namespace recorded no namespace cache change".to_string()),
        )
    );

    let new_warehouse_id = uuid::Uuid::now_v7();
    assert_eq!(
        refused(
            "INSERT INTO warehouse (warehouse_id, project_id, warehouse_name, status, tabular_delete_mode) \
             SELECT $1::uuid, project_id, 'inserted', 'active', 'hard' FROM warehouse LIMIT 1",
            new_warehouse_id.to_string(),
        )
        .await,
        Some(format!(
            "a write to warehouse recorded no warehouse cache change for {new_warehouse_id}"
        ))
    );

    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SELECT lakekeeper_record_cache_change('warehouse', ARRAY[$1])")
        .bind(uuid::Uuid::now_v7().to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("UPDATE warehouse SET warehouse_name = 'renamed' WHERE warehouse_id = $1")
        .bind(*warehouse.warehouse_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let other_id_recorded = transaction
        .commit()
        .await
        .unwrap_err()
        .as_database_error()
        .map(|e| e.message().to_string());
    assert_eq!(
        other_id_recorded,
        Some(format!(
            "a write to warehouse recorded no warehouse cache change for {}",
            warehouse.warehouse_id
        ))
    );
}
