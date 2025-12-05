use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    ProjectId, SecretId, WarehouseId,
    api::{
        RequestMetadata,
        management::v1::{
            ApiServer,
            warehouse::{
                RenameWarehouseRequest, Service, TabularDeleteProfile,
                UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest,
            },
        },
    },
    implementations::postgres::PostgresBackend,
    service::{
        CachePolicy, CatalogStore, CatalogWarehouseOps, Transaction, WarehouseStatus,
        authz::AllowAllAuthorizer, warehouse_cache::WAREHOUSE_CACHE,
    },
    tests::{SetupTestCatalog, memory_io_profile, random_request_metadata},
};

/// Test basic warehouse creation
#[sqlx::test]
async fn test_create_warehouse(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    // Create warehouse
    let warehouse = PostgresBackend::create_warehouse(
        format!("test-warehouse-{}", Uuid::now_v7()),
        &project_id,
        storage_profile.clone(),
        TabularDeleteProfile::Hard {},
        None,
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Verify warehouse was created
    assert_eq!(warehouse.project_id, project_id);
    assert_eq!(warehouse.status, WarehouseStatus::Active);
    assert!(matches!(
        warehouse.tabular_delete_profile,
        TabularDeleteProfile::Hard {}
    ));
    assert_eq!(warehouse.storage_secret_id, None);
    assert!(!warehouse.protected);
}

/// Test creating warehouse with storage secret
#[sqlx::test]
async fn test_create_warehouse_with_secret(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());
    let secret_id = SecretId::from(Uuid::now_v7());
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    // Create warehouse with secret
    let warehouse = PostgresBackend::create_warehouse(
        format!("test-warehouse-secret-{}", Uuid::now_v7()),
        &project_id,
        storage_profile.clone(),
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::TimeDelta::seconds(86400),
        },
        Some(secret_id),
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    assert_eq!(warehouse.storage_secret_id, Some(secret_id));
    assert!(matches!(
        warehouse.tabular_delete_profile,
        TabularDeleteProfile::Soft { .. }
    ));
}

/// Test that creating warehouse with duplicate name fails
#[sqlx::test]
async fn test_create_warehouse_duplicate_name(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    // Try to create warehouse with same name
    let result = PostgresBackend::create_warehouse(
        warehouse_resp.warehouse_name.clone(),
        &project_id,
        storage_profile.clone(),
        TabularDeleteProfile::Hard {},
        None,
        transaction.transaction(),
    )
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(
        err,
        crate::service::CatalogCreateWarehouseError::WarehouseAlreadyExists(_)
    ));
}

#[sqlx::test]
async fn test_get_warehouse_by_id(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Get warehouse by ID
    let warehouse = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(warehouse.is_some());
    let warehouse = warehouse.unwrap();
    assert_eq!(warehouse.warehouse_id, warehouse_resp.warehouse_id);
    assert_eq!(warehouse.name, warehouse_resp.warehouse_name);
}

#[sqlx::test]
async fn test_get_warehouse_by_id_not_found(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let non_existent_id = WarehouseId::new_random();

    // Get warehouse by non-existent ID
    let warehouse = PostgresBackend::get_warehouse_by_id(
        non_existent_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(warehouse.is_none());
}

#[sqlx::test]
async fn test_require_warehouse_by_id(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Require warehouse (should succeed)
    let warehouse = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("Warehouse should exist");

    assert_eq!(warehouse.warehouse_id, warehouse_resp.warehouse_id);

    // Require non-existent warehouse (should fail)
    let non_existent_id = WarehouseId::new_random();
    let result = PostgresBackend::get_warehouse_by_id(
        non_existent_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(result.is_none());
}

#[sqlx::test]
async fn test_get_warehouse_by_name(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());

    // Get warehouse by name
    let warehouse = PostgresBackend::get_warehouse_by_name(
        &warehouse_resp.warehouse_name,
        &project_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(warehouse.is_some());
    let warehouse = warehouse.unwrap();
    assert_eq!(warehouse.name, warehouse_resp.warehouse_name);
    assert_eq!(warehouse.warehouse_id, warehouse_resp.warehouse_id);
}

#[sqlx::test]
async fn test_get_warehouse_by_name_not_found(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());

    // Get warehouse by non-existent name
    let warehouse = PostgresBackend::get_warehouse_by_name(
        "non-existent-warehouse",
        &project_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(warehouse.is_none());
}

#[sqlx::test]
async fn test_list_warehouses(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(3)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());

    // List all active warehouses
    let warehouses =
        PostgresBackend::list_warehouses(&project_id, None, ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    assert_eq!(warehouses.len(), 3);

    // Verify all warehouses are active
    for warehouse in &warehouses {
        assert_eq!(warehouse.status, WarehouseStatus::Active);
        assert_eq!(warehouse.project_id, project_id);
    }

    // Verify main warehouse is in the list
    assert!(
        warehouses
            .iter()
            .any(|w| w.warehouse_id == warehouse_resp.warehouse_id)
    );
}

#[sqlx::test]
async fn test_list_warehouses_include_inactive(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(2)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());

    // Set one warehouse to inactive
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::set_warehouse_status(
        warehouse_resp.warehouse_id,
        WarehouseStatus::Inactive,
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    // List only active warehouses
    let active_warehouses =
        PostgresBackend::list_warehouses(&project_id, None, ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    assert_eq!(active_warehouses.len(), 1);
    assert!(
        active_warehouses
            .iter()
            .all(|w| w.status == WarehouseStatus::Active)
    );

    // List all warehouses (active and inactive)
    let all_warehouses = PostgresBackend::list_warehouses(
        &project_id,
        Some(vec![WarehouseStatus::Active, WarehouseStatus::Inactive]),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert_eq!(all_warehouses.len(), 2);
}

#[sqlx::test]
async fn test_rename_warehouse_not_found(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let non_existent_id = WarehouseId::new_random();
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    // Try to rename non-existent warehouse
    let result =
        PostgresBackend::rename_warehouse(non_existent_id, "new-name", transaction.transaction())
            .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(
        err,
        crate::service::CatalogRenameWarehouseError::WarehouseIdNotFound(_)
    ));
}

#[sqlx::test]
async fn test_set_warehouse_status(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    // Set warehouse to inactive
    let updated_warehouse = PostgresBackend::set_warehouse_status(
        warehouse_resp.warehouse_id,
        WarehouseStatus::Inactive,
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    assert_eq!(updated_warehouse.status, WarehouseStatus::Inactive);
    assert_eq!(updated_warehouse.warehouse_id, warehouse_resp.warehouse_id);

    // Verify the status persisted
    let warehouses = PostgresBackend::list_warehouses(
        &updated_warehouse.project_id,
        Some(vec![WarehouseStatus::Inactive]),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    let warehouse = warehouses
        .into_iter()
        .find(|w| w.warehouse_id == warehouse_resp.warehouse_id)
        .unwrap();

    assert_eq!(warehouse.status, WarehouseStatus::Inactive);

    // Verify get respects status
    let warehouse = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::inactive(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("Warehouse should exist");

    assert_eq!(warehouse.status, WarehouseStatus::Inactive);

    let warehouse_none = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(warehouse_none.is_none());

    let warehouse = PostgresBackend::get_warehouse_by_name(
        &warehouse_resp.warehouse_name,
        &updated_warehouse.project_id,
        WarehouseStatus::active_and_inactive(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("Warehouse should exist");

    assert_eq!(warehouse.status, WarehouseStatus::Inactive);
}

#[sqlx::test]
async fn test_set_warehouse_status_reactivate(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Set to inactive
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let inactive_warehouse = PostgresBackend::set_warehouse_status(
        warehouse_resp.warehouse_id,
        WarehouseStatus::Inactive,
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let version = inactive_warehouse.version;

    // Set back to active
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let updated_warehouse = PostgresBackend::set_warehouse_status(
        warehouse_resp.warehouse_id,
        WarehouseStatus::Active,
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(updated_warehouse.status, WarehouseStatus::Active);
    assert_eq!(*updated_warehouse.version, *version + 1);

    let warehouse = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Skip,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(warehouse.status, WarehouseStatus::Active);
    assert_eq!(*warehouse.version, *version + 1);
}

#[sqlx::test]
async fn test_set_warehouse_deletion_profile(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .delete_profile(TabularDeleteProfile::Hard {})
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Change to soft delete
    let new_profile = TabularDeleteProfile::Soft {
        expiration_seconds: chrono::TimeDelta::seconds(3600),
    };
    let updated_warehouse = ApiServer::update_warehouse_delete_profile(
        warehouse_resp.warehouse_id,
        UpdateWarehouseDeleteProfileRequest {
            delete_profile: new_profile,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(matches!(
        updated_warehouse.delete_profile,
        TabularDeleteProfile::Soft { .. }
    ));

    // Verify the profile persisted
    let warehouse = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(matches!(
        warehouse.tabular_delete_profile,
        TabularDeleteProfile::Soft { .. }
    ));
    assert_eq!(*warehouse.version, 1);
}

#[sqlx::test]
async fn test_update_storage_profile(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let new_secret_id = SecretId::from(Uuid::now_v7());
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    // Update storage profile with a secret
    let updated_warehouse = PostgresBackend::update_storage_profile(
        warehouse_resp.warehouse_id,
        storage_profile.clone(),
        Some(new_secret_id),
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    assert_eq!(updated_warehouse.storage_secret_id, Some(new_secret_id));

    // Verify the update persisted
    let warehouse = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Skip,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(warehouse.storage_secret_id, Some(new_secret_id));
    assert_eq!(*warehouse.version, 1);
}

#[sqlx::test]
async fn test_set_warehouse_protection(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Protect warehouse
    let updated_warehouse = ApiServer::set_warehouse_protection(
        warehouse_resp.warehouse_id,
        true,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert!(updated_warehouse.protected);

    // Protect warehouse
    let updated_warehouse = ApiServer::set_warehouse_protection(
        warehouse_resp.warehouse_id,
        false,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert!(!updated_warehouse.protected);

    let warehouse = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(!warehouse.protected);
    assert_eq!(*warehouse.version, 2);
}

/// Test operations on non-existent warehouse return appropriate errors
#[sqlx::test]
async fn test_operations_on_nonexistent_warehouse(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let non_existent_id = WarehouseId::new_random();

    // Test set_warehouse_status
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let result = PostgresBackend::set_warehouse_status(
        non_existent_id,
        WarehouseStatus::Inactive,
        transaction.transaction(),
    )
    .await;
    assert!(result.is_err());

    // Test set_warehouse_deletion_profile
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let result = PostgresBackend::set_warehouse_deletion_profile(
        non_existent_id,
        &TabularDeleteProfile::Hard {},
        transaction.transaction(),
    )
    .await;
    assert!(result.is_err());

    // Test update_storage_profile
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let result = PostgresBackend::update_storage_profile(
        non_existent_id,
        storage_profile.clone(),
        None,
        transaction.transaction(),
    )
    .await;
    assert!(result.is_err());

    // Test set_warehouse_protected
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let result =
        PostgresBackend::set_warehouse_protected(non_existent_id, true, transaction.transaction())
            .await;
    assert!(result.is_err());
}

#[sqlx::test]
async fn test_warehouse_cache_populated_by_get_id(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Clear cache first
    WAREHOUSE_CACHE
        .invalidate(&warehouse_resp.warehouse_id)
        .await;

    // Verify cache is empty
    let cached_before = WAREHOUSE_CACHE.get(&warehouse_resp.warehouse_id).await;
    assert!(cached_before.is_none());

    // Get warehouse by ID - should populate cache
    let warehouse = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Verify cache is now populated
    let cached_after = WAREHOUSE_CACHE.get(&warehouse_resp.warehouse_id).await;
    assert!(cached_after.is_some());

    // Verify by getting the warehouse again - should hit cache
    let warehouse2 = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(warehouse2.warehouse_id, warehouse.warehouse_id);
    assert_eq!(warehouse2.name, warehouse.name);
}

#[sqlx::test]
async fn test_warehouse_cache_populated_by_list(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(2)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());

    // Clear cache
    WAREHOUSE_CACHE
        .invalidate(&warehouse_resp.warehouse_id)
        .await;
    for (_, wh_id, _) in &warehouse_resp.additional_warehouses {
        WAREHOUSE_CACHE.invalidate(wh_id).await;
    }

    // List warehouses - should populate cache
    let warehouses =
        PostgresBackend::list_warehouses(&project_id, None, ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    assert_eq!(warehouses.len(), 2);

    // Verify all warehouses are now in cache by checking cache contains them
    for warehouse in &warehouses {
        let cached = WAREHOUSE_CACHE.get(&warehouse.warehouse_id).await;
        assert!(
            cached.is_some(),
            "Warehouse {} should be in cache",
            warehouse.warehouse_id
        );
    }
}

/// Test cache respects version
#[sqlx::test]
async fn test_cache_respects_min_version(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // First get - populates cache
    let warehouse1 = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    let original_version = *warehouse1.version;

    // Update warehouse (this increments version)
    // Because we are using the DB method, this does not update the cache
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::set_warehouse_deletion_profile(
        warehouse_resp.warehouse_id,
        &TabularDeleteProfile::Soft {
            expiration_seconds: chrono::TimeDelta::seconds(7200),
        },
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    // Get warehouse using the cache, should return stale data
    let warehouse_cached = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(*warehouse_cached.version, original_version);

    // Get with CachePolicy::RequireMinimumVersion using a version higher than original
    // This should fetch fresh data since the warehouse was updated
    let warehouse_fresh = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::RequireMinimumVersion(original_version + 1),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // version should be incremented
    assert_eq!(*warehouse_fresh.version, original_version + 1);
    assert_eq!(
        warehouse_fresh.tabular_delete_profile,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::TimeDelta::seconds(7200)
        }
    );
}

#[sqlx::test]
async fn test_cache_policy_skip_bypasses_cache(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Populate cache
    let original_warehouse = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Update warehouse directly (simulating external update)
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let updated_deletion_profile = TabularDeleteProfile::Soft {
        expiration_seconds: chrono::TimeDelta::seconds(7200),
    };
    PostgresBackend::set_warehouse_deletion_profile(
        warehouse_resp.warehouse_id,
        &updated_deletion_profile,
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let cached_warehouse = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(cached_warehouse.version, original_warehouse.version);

    let warehouse_fresh = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Skip,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(*warehouse_fresh.version, *original_warehouse.version + 1);
    assert_eq!(
        warehouse_fresh.tabular_delete_profile,
        updated_deletion_profile
    );
}

#[sqlx::test]
async fn test_get_by_name_uses_cache(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());

    let warehouse1 = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(warehouse1.status, WarehouseStatus::Active);

    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let updated_deletion_profile = TabularDeleteProfile::Soft {
        expiration_seconds: chrono::TimeDelta::seconds(7200),
    };
    PostgresBackend::set_warehouse_deletion_profile(
        warehouse_resp.warehouse_id,
        &updated_deletion_profile,
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    // Get by name - should return STALE cached data
    let warehouse2 = PostgresBackend::get_warehouse_by_name(
        &warehouse_resp.warehouse_name,
        &project_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Should still have old status from cache
    assert_eq!(warehouse2.warehouse_id, warehouse_resp.warehouse_id);
    assert_eq!(
        warehouse2.tabular_delete_profile,
        TabularDeleteProfile::Hard {}
    );
}

#[sqlx::test]
async fn test_version_not_updated_if_nothing_changed(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Get initial warehouse
    let warehouse_before = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    let original_version = *warehouse_before.version;

    // Update storage profile with the same values
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let updated_warehouse = PostgresBackend::update_storage_profile(
        warehouse_resp.warehouse_id,
        storage_profile.clone(),
        None,
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    // Version should remain unchanged
    assert_eq!(*updated_warehouse.version, original_version);

    // Verify from cache
    let warehouse_after = PostgresBackend::get_warehouse_by_id_cache_aware(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Skip,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(*warehouse_after.version, original_version);
}

// ==================== Cache Invalidation Tests using ApiServer ====================
/// Test cache invalidation when renaming warehouse via `ApiServer`
#[sqlx::test]
async fn test_cache_invalidation_on_api_rename(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Populate cache
    let warehouse_before = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    let old_name = warehouse_before.name.clone();

    // Verify cache is populated
    assert!(
        WAREHOUSE_CACHE
            .get(&warehouse_resp.warehouse_id)
            .await
            .is_some()
    );

    // Rename via ApiServer (this triggers hooks)
    let new_name = format!("renamed-{}", Uuid::now_v7());
    ApiServer::rename_warehouse(
        warehouse_resp.warehouse_id,
        RenameWarehouseRequest {
            new_name: new_name.clone(),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Get from cache - should have updated name
    let warehouse_after = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(warehouse_after.name, new_name);
    assert_ne!(warehouse_after.name, old_name);
}

/// Test cache invalidation when updating warehouse storage via `ApiServer`
#[sqlx::test]
async fn test_cache_invalidation_on_api_update_storage(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Populate cache
    let warehouse_before = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(warehouse_before.storage_secret_id, None);

    // Update storage via ApiServer
    let updated_storage_profile = memory_io_profile();
    ApiServer::update_storage(
        warehouse_resp.warehouse_id,
        UpdateWarehouseStorageRequest {
            storage_profile: updated_storage_profile.clone(),
            storage_credential: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Get from cache - should have fresh data with updated_at timestamp changed
    let warehouse_after = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // updated_at should be newer after the update
    assert_eq!(warehouse_after.storage_profile, updated_storage_profile);
}

/// Test cache invalidation when updating delete profile via `ApiServer`
#[sqlx::test]
async fn test_cache_invalidation_on_api_update_delete_profile(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .delete_profile(TabularDeleteProfile::Hard {})
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Populate cache
    let warehouse_before = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(matches!(
        warehouse_before.tabular_delete_profile,
        TabularDeleteProfile::Hard {}
    ));

    // Update delete profile via ApiServer
    let new_profile = TabularDeleteProfile::Soft {
        expiration_seconds: chrono::TimeDelta::seconds(7200),
    };
    ApiServer::update_warehouse_delete_profile(
        warehouse_resp.warehouse_id,
        UpdateWarehouseDeleteProfileRequest {
            delete_profile: new_profile,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Get from cache - should have updated profile
    let warehouse_after = PostgresBackend::get_warehouse_by_id(
        warehouse_resp.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog,
    )
    .await
    .unwrap()
    .unwrap();

    assert!(matches!(
        warehouse_after.tabular_delete_profile,
        TabularDeleteProfile::Soft { .. }
    ));
}
