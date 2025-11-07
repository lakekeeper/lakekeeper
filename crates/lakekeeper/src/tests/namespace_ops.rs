use std::sync::Arc;

use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::UpdateNamespacePropertiesRequest;
use sqlx::PgPool;

use crate::{
    api::{
        iceberg::v1::{namespace::NamespaceService, NamespaceParameters},
        management::v1::{namespace::NamespaceManagementService as _, ApiServer},
        RequestMetadata,
    },
    implementations::postgres::PostgresBackend,
    server::CatalogServer,
    service::{
        authz::AllowAllAuthorizer, namespace_cache::NAMESPACE_CACHE, CachePolicy,
        CatalogNamespaceOps, CatalogStore, CreateNamespaceRequest, NamespaceId, NamespaceVersion,
        Transaction,
    },
    tests::{memory_io_profile, random_request_metadata, SetupTestCatalog},
};

/// Test basic namespace creation
#[sqlx::test]
async fn test_create_namespace(pool: PgPool) {
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

    let namespace_ident = NamespaceIdent::from_vec(vec!["test_namespace".to_string()]).unwrap();

    // Create namespace
    let namespace = PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        NamespaceId::new_random(),
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Verify namespace was created
    assert_eq!(namespace.namespace_ident(), &namespace_ident);
    assert!(!namespace.is_protected());
    assert_eq!(namespace.properties(), None);
    assert_eq!(*namespace.version(), 0);
}

/// Test creating namespace with properties
#[sqlx::test]
async fn test_create_namespace_with_properties(pool: PgPool) {
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

    let namespace_ident = NamespaceIdent::from_vec(vec!["test_ns_props".to_string()]).unwrap();
    let properties = std::collections::HashMap::from([
        ("owner".to_string(), "test-user".to_string()),
        ("created_by".to_string(), "test-system".to_string()),
    ]);

    // Create namespace with properties
    let namespace = PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        NamespaceId::new_random(),
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: Some(properties.clone()),
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    assert_eq!(namespace.properties(), Some(&properties));
}

/// Test that creating namespace with duplicate name fails
#[sqlx::test]
async fn test_create_namespace_duplicate_name(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let namespace_ident = NamespaceIdent::from_vec(vec!["duplicate_ns".to_string()]).unwrap();

    // Create first namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        NamespaceId::new_random(),
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Try to create duplicate namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let result = PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        NamespaceId::new_random(),
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(
        err,
        crate::service::CatalogCreateNamespaceError::NamespaceAlreadyExists(_)
    ));
}

/// Test get namespace by ID
#[sqlx::test]
async fn test_get_namespace_by_id(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let namespace_ident = NamespaceIdent::from_vec(vec!["test_get_by_id".to_string()]).unwrap();
    let namespace_id = NamespaceId::new_random();

    // Create namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Get namespace by ID
    let namespace_hierarchy = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(namespace_hierarchy.is_some());
    let namespace_hierarchy = namespace_hierarchy.unwrap();
    assert_eq!(namespace_hierarchy.namespace_id(), namespace_id);
    assert_eq!(namespace_hierarchy.namespace_ident(), &namespace_ident);
}

/// Test get namespace by name
#[sqlx::test]
async fn test_get_namespace_by_name(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let namespace_ident = NamespaceIdent::from_vec(vec!["test_get_by_name".to_string()]).unwrap();
    let namespace_id = NamespaceId::new_random();

    // Create namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Get namespace by name
    let namespace_hierarchy = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        &namespace_ident,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(namespace_hierarchy.is_some());
    let namespace_hierarchy = namespace_hierarchy.unwrap();
    assert_eq!(namespace_hierarchy.namespace_id(), namespace_id);
    assert_eq!(namespace_hierarchy.namespace_ident(), &namespace_ident);
}

/// Test get non-existent namespace
#[sqlx::test]
async fn test_get_namespace_not_found(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let non_existent_id = NamespaceId::new_random();

    // Get non-existent namespace by ID
    let namespace = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        non_existent_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(namespace.is_none());

    // Get non-existent namespace by name
    let namespace = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        NamespaceIdent::from_vec(vec!["nonexistent".to_string()]).unwrap(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(namespace.is_none());
}

// ==================== Cache Tests ====================

/// Test cache is populated by `get_namespace`
#[sqlx::test]
async fn test_namespace_cache_populated_by_get_id(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let namespace_ident = NamespaceIdent::from_vec(vec!["cache_test".to_string()]).unwrap();
    let namespace_id = NamespaceId::new_random();

    // Create namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Clear cache first
    NAMESPACE_CACHE.invalidate(&namespace_id).await;

    // Verify cache is empty
    let cached_before = NAMESPACE_CACHE.get(&namespace_id).await;
    assert!(cached_before.is_none());

    // Get namespace by ID - should populate cache
    let namespace = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Verify cache is now populated
    let cached_after = NAMESPACE_CACHE.get(&namespace_id).await;
    assert!(cached_after.is_some());

    // Verify by getting the namespace again - should hit cache
    let namespace2 = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(namespace2.namespace_id(), namespace.namespace_id());
    assert_eq!(namespace2.namespace_ident(), namespace.namespace_ident());
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

    let namespace_ident = NamespaceIdent::from_vec(vec!["version_test".to_string()]).unwrap();
    let namespace_id = NamespaceId::new_random();

    // Create namespace via PostgresBackend (does not run hooks)
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // First get - populates cache
    let namespace1 = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    let original_version = *namespace1.version();

    // Update namespace properties via PostgresBackend (does not update cache)
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let new_props = std::collections::HashMap::from([("updated".to_string(), "true".to_string())]);

    PostgresBackend::update_namespace_properties(
        warehouse_resp.warehouse_id,
        namespace_id,
        new_props.clone(),
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Get namespace using the cache, should return stale data
    let namespace_cached = PostgresBackend::get_namespace_cache_aware(
        warehouse_resp.warehouse_id,
        namespace_id,
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(*namespace_cached.version(), original_version);

    // Get with CachePolicy::RequireMinimumVersion using a version higher than original
    // This should fetch fresh data since the namespace was updated
    let namespace_fresh = PostgresBackend::get_namespace_cache_aware(
        warehouse_resp.warehouse_id,
        namespace_id,
        CachePolicy::RequireMinimumVersion(original_version + 1),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // version should be incremented
    assert_eq!(*namespace_fresh.version(), original_version + 1);
    assert_eq!(namespace_fresh.properties(), Some(&new_props));
}

/// Test `CachePolicy::Skip` bypasses cache
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

    let namespace_ident = NamespaceIdent::from_vec(vec!["skip_cache".to_string()]).unwrap();
    let namespace_id = NamespaceId::new_random();

    // Create namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Populate cache
    let original_namespace = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Update namespace directly (simulating external update)
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let updated_props =
        std::collections::HashMap::from([("external_update".to_string(), "yes".to_string())]);

    PostgresBackend::update_namespace_properties(
        warehouse_resp.warehouse_id,
        namespace_id,
        updated_props.clone(),
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    let cached_namespace = PostgresBackend::get_namespace_cache_aware(
        warehouse_resp.warehouse_id,
        namespace_id,
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(cached_namespace.version(), original_namespace.version());

    let namespace_fresh = PostgresBackend::get_namespace_cache_aware(
        warehouse_resp.warehouse_id,
        namespace_id,
        CachePolicy::Skip,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        *namespace_fresh.version(),
        *original_namespace.version() + 1
    );
    assert_eq!(namespace_fresh.properties(), Some(&updated_props));
}

/// Test cache invalidation when updating namespace properties via `ApiServer`
#[sqlx::test]
async fn test_cache_invalidation_on_api_update_properties(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let namespace_ident = NamespaceIdent::from_vec(vec!["api_update".to_string()]).unwrap();

    // Create namespace via ApiServer
    let namespace = CatalogServer::<PostgresBackend, _, _>::create_namespace(
        Some(warehouse_resp.warehouse_id.to_string().into()),
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(&namespace.namespace, &namespace_ident);

    // Update properties via ApiServer (this triggers hooks and updates cache)
    let new_props = std::collections::HashMap::from([("key1".to_string(), "value1".to_string())]);

    let update_response = CatalogServer::<PostgresBackend, _, _>::update_namespace_properties(
        NamespaceParameters {
            prefix: Some(warehouse_resp.warehouse_id.to_string().into()),
            namespace: namespace_ident.clone(),
        },
        UpdateNamespacePropertiesRequest {
            removals: None,
            updates: Some(new_props.clone()),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(update_response.updated, vec!["key1".to_string()]);

    // Get from cache - should have updated properties
    let namespace_after = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_ident,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(namespace_after.properties().unwrap()["key1"], "value1");
}

/// Test cache invalidation when setting namespace protection via `ApiServer`
#[sqlx::test]
async fn test_cache_invalidation_on_api_set_protection(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let namespace_ident = NamespaceIdent::from_vec(vec!["protect_test".to_string()]).unwrap();

    // Create namespace via ApiServer
    let namespace = CatalogServer::<PostgresBackend, _, _>::create_namespace(
        Some(warehouse_resp.warehouse_id.to_string().into()),
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(&namespace.namespace, &namespace_ident);

    // Populate cache
    let namespace_before = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_ident.clone(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(!namespace_before.is_protected());

    // Set protection via ApiServer (this triggers hooks)
    ApiServer::set_namespace_protection(
        namespace_before.namespace_id(),
        warehouse_resp.warehouse_id,
        true,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Get from cache - should have updated protection
    let namespace_after = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_before.namespace_id(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(namespace_after.is_protected());

    // Unset protection
    ApiServer::set_namespace_protection(
        namespace_before.namespace_id(),
        warehouse_resp.warehouse_id,
        false,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let namespace_final = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_ident,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(!namespace_final.is_protected());
}

/// Test `get_by_name` uses cache
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

    let namespace_ident = NamespaceIdent::from_vec(vec!["name_cache".to_string()]).unwrap();
    let namespace_id = NamespaceId::new_random();

    // Create namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        CreateNamespaceRequest {
            namespace: namespace_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    let namespace1 = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        namespace_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(!namespace1.is_protected());

    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let new_props = std::collections::HashMap::from([("db_update".to_string(), "yes".to_string())]);

    PostgresBackend::update_namespace_properties(
        warehouse_resp.warehouse_id,
        namespace_id,
        new_props.clone(),
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Get by name - should return STALE cached data
    let namespace2 = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        &namespace_ident,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Should still have no properties from cache
    assert_eq!(namespace2.namespace_id(), namespace_id);
    assert_eq!(namespace2.properties(), None);
}

/// Test cache with hierarchical namespaces
#[sqlx::test]
async fn test_cache_with_hierarchical_namespaces(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Create parent namespace
    let parent_ident = NamespaceIdent::from_vec(vec!["parent".to_string()]).unwrap();
    let parent_id = NamespaceId::new_random();

    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        parent_id,
        CreateNamespaceRequest {
            namespace: parent_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Create child namespace
    let child_ident =
        NamespaceIdent::from_vec(vec!["parent".to_string(), "child".to_string()]).unwrap();
    let child_id = NamespaceId::new_random();

    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::create_namespace(
        warehouse_resp.warehouse_id,
        child_id,
        CreateNamespaceRequest {
            namespace: child_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Clear cache
    NAMESPACE_CACHE.invalidate(&parent_id).await;
    NAMESPACE_CACHE.invalidate(&child_id).await;

    // Get child namespace - should populate cache with both parent and child
    let child_hierarchy = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        child_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(child_hierarchy.depth(), 1);
    assert_eq!(child_hierarchy.namespace_id(), child_id);
    assert_eq!(child_hierarchy.parent().unwrap().namespace_id(), parent_id);

    // Verify both are now in cache
    let cached_child = NAMESPACE_CACHE.get(&child_id).await;
    assert!(cached_child.is_some());

    let cached_parent = NAMESPACE_CACHE.get(&parent_id).await;
    assert!(cached_parent.is_some());
}

/// Test that cache is invalidated when parent version is stale
#[sqlx::test]
async fn test_namespace_cache_parent_version_staleness(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let warehouse_id = warehouse_resp.warehouse_id;

    // Create parent namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let parent_ident = NamespaceIdent::from_vec(vec!["parent_stale".to_string()]).unwrap();
    let parent_id = NamespaceId::new_random();

    let parent_ns = PostgresBackend::create_namespace(
        warehouse_id,
        parent_id,
        CreateNamespaceRequest {
            namespace: parent_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    let parent_version = parent_ns.namespace.version;
    transaction.commit().await.unwrap();

    // Create child namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let child_ident =
        NamespaceIdent::from_vec(vec!["parent_stale".to_string(), "child".to_string()]).unwrap();
    let child_id = NamespaceId::new_random();

    let child_ns = PostgresBackend::create_namespace(
        warehouse_id,
        child_id,
        CreateNamespaceRequest {
            namespace: child_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Insert child into cache
    NAMESPACE_CACHE.insert(child_id, child_ns.clone()).await;

    // Insert parent with OLDER version (stale) into cache
    let mut stale_parent_ns = (*parent_ns.namespace).clone();
    stale_parent_ns.version = NamespaceVersion::from(*parent_version - 1);

    let stale_parent = crate::service::NamespaceWithParent {
        namespace: Arc::new(stale_parent_ns),
        parent: None,
    };

    NAMESPACE_CACHE.insert(parent_id, stale_parent).await;

    // Try to get child - should fail because parent version is stale
    let hierarchy =
        PostgresBackend::get_namespace(warehouse_id, child_id, ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .unwrap();

    if let Some(parent) = hierarchy.parent() {
        assert!(
            parent.namespace.version >= parent_version,
            "Should not use stale cached parent"
        );
    }

    // Verify child was invalidated due to stale parent
    let cached_child_after = NAMESPACE_CACHE.get(&child_id).await;
    // Child should either be invalidated or refetched with correct data
    if let Some(cached) = cached_child_after {
        // If still cached, parent must be correct version
        if let Some((_, cached_parent_version)) = cached.parent {
            assert!(
                cached_parent_version >= parent_version,
                "Cached child should not reference stale parent version"
            );
        }
    }
}

/// Test deep namespace hierarchies (4+ levels)
#[sqlx::test]
async fn test_namespace_deep_hierarchy(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let warehouse_id = warehouse_resp.warehouse_id;

    // Create 5-level hierarchy: a.b.c.d.e
    let levels = vec!["a", "b", "c", "d", "e"];
    let mut namespace_ids = Vec::new();

    for i in 0..levels.len() {
        let mut transaction = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();

        let ident_parts: Vec<String> = levels[0..=i].iter().map(|s| s.to_string()).collect();
        let ident = NamespaceIdent::from_vec(ident_parts).unwrap();
        let id = NamespaceId::new_random();

        PostgresBackend::create_namespace(
            warehouse_id,
            id,
            CreateNamespaceRequest {
                namespace: ident.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();
        namespace_ids.push(id);
    }

    // Clear cache and get deepest namespace
    for id in &namespace_ids {
        NAMESPACE_CACHE.invalidate(id).await;
    }

    let deepest_hierarchy = PostgresBackend::get_namespace(
        warehouse_id,
        namespace_ids[4],
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Verify all 4 parents are present
    assert_eq!(deepest_hierarchy.depth(), 4, "Should have depth 4");

    // Verify hierarchy is correct
    assert_eq!(deepest_hierarchy.namespace_id(), namespace_ids[4]);
    assert_eq!(deepest_hierarchy.parents.len(), 4);

    // Check that parent order is correct
    let mut all_namespaces = vec![deepest_hierarchy.namespace];
    all_namespaces.extend(deepest_hierarchy.parents);
    for (i, ns) in all_namespaces.iter().enumerate() {
        assert_eq!(
            ns.namespace_id(),
            namespace_ids[4 - i],
            "Namespace ID at level {} should match",
            i
        );
        // Validate that parent linkage is correct
        if i < all_namespaces.len() - 1 {
            let expected_parent_id = namespace_ids[4 - (i + 1)];
            if let Some((parent_id, _)) = ns.parent {
                assert_eq!(parent_id, expected_parent_id);
            } else {
                panic!("Namespace at level {} should have a parent", i);
            }
        }
    }

    // Verify all are cached
    for id in &namespace_ids {
        let cached = NAMESPACE_CACHE.get(id).await;
        assert!(
            cached.is_some(),
            "All namespaces in hierarchy should be cached"
        );
    }
}

/// Test that parent version is correctly tracked when child is created
#[sqlx::test]
async fn test_parent_version_tracking(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let warehouse_id = warehouse_resp.warehouse_id;

    // Create parent
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let parent_ident = NamespaceIdent::from_vec(vec!["track_parent".to_string()]).unwrap();
    let parent_id = NamespaceId::new_random();

    let parent_ns = PostgresBackend::create_namespace(
        warehouse_id,
        parent_id,
        CreateNamespaceRequest {
            namespace: parent_ident.clone(),
            properties: Some(std::collections::HashMap::from([(
                "version".to_string(),
                "1".to_string(),
            )])),
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    let initial_parent_version = parent_ns.namespace.version;
    transaction.commit().await.unwrap();

    // Create first child - should capture initial parent version
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let child1_ident =
        NamespaceIdent::from_vec(vec!["track_parent".to_string(), "child1".to_string()]).unwrap();

    let child1_ns = PostgresBackend::create_namespace(
        warehouse_id,
        NamespaceId::new_random(),
        CreateNamespaceRequest {
            namespace: child1_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Verify first child has initial parent version
    assert!(child1_ns.parent.is_some(), "Child should have parent info");
    let (captured_parent_id, captured_parent_version) = child1_ns.parent.as_ref().unwrap();
    assert_eq!(*captured_parent_id, parent_id);
    assert_eq!(*captured_parent_version, initial_parent_version);

    // Update parent (increments version)
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    PostgresBackend::update_namespace_properties(
        warehouse_id,
        parent_id,
        std::collections::HashMap::from([("version".to_string(), "2".to_string())]),
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Create second child - should capture UPDATED parent version
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let child2_ident =
        NamespaceIdent::from_vec(vec!["track_parent".to_string(), "child2".to_string()]).unwrap();

    let child2_ns = PostgresBackend::create_namespace(
        warehouse_id,
        NamespaceId::new_random(),
        CreateNamespaceRequest {
            namespace: child2_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Verify second child has NEWER parent version
    let (_, captured_parent_version2) = child2_ns.parent.as_ref().unwrap();
    assert!(
        *captured_parent_version2 > initial_parent_version,
        "Second child should capture updated parent version"
    );

    // Verify first child still references original version (snapshot at creation)
    assert_eq!(
        *captured_parent_version, initial_parent_version,
        "First child's parent version should remain unchanged"
    );
}

/// Test cache eviction invalidates ident-to-id mapping
#[sqlx::test]
async fn test_cache_eviction_invalidates_mapping(pool: PgPool) {
    use crate::service::namespace_cache::IDENT_TO_ID_CACHE;

    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let warehouse_id = warehouse_resp.warehouse_id;

    // Create namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let ident = NamespaceIdent::from_vec(vec!["evict_test".to_string()]).unwrap();
    let id = NamespaceId::new_random();

    PostgresBackend::create_namespace(
        warehouse_id,
        id,
        CreateNamespaceRequest {
            namespace: ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Populate cache by getting namespace
    PostgresBackend::get_namespace(warehouse_id, id, ctx.v1_state.catalog.clone())
        .await
        .unwrap();

    // Verify both caches have the entry
    assert!(NAMESPACE_CACHE.get(&id).await.is_some());

    let cache_key: Vec<unicase::UniCase<String>> = ident
        .inner()
        .into_iter()
        .map(unicase::UniCase::new)
        .collect();
    assert!(IDENT_TO_ID_CACHE
        .get(&(warehouse_id, cache_key.clone()))
        .await
        .is_some());

    // Invalidate main cache entry
    NAMESPACE_CACHE.invalidate(&id).await;

    // Give eviction listener time to run
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify ident-to-id mapping is also invalidated
    assert!(
        IDENT_TO_ID_CACHE
            .get(&(warehouse_id, cache_key))
            .await
            .is_none(),
        "Ident-to-id mapping should be invalidated when main entry is evicted"
    );
}

/// Test root namespaces (no parent) are handled correctly
#[sqlx::test]
async fn test_root_namespace_null_parent(pool: PgPool) {
    let storage_profile = memory_io_profile();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(storage_profile.clone())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let warehouse_id = warehouse_resp.warehouse_id;

    // Create root namespace
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let root_ident = NamespaceIdent::from_vec(vec!["root_test".to_string()]).unwrap();
    let root_id = NamespaceId::new_random();

    let root_ns = PostgresBackend::create_namespace(
        warehouse_id,
        root_id,
        CreateNamespaceRequest {
            namespace: root_ident.clone(),
            properties: None,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    // Verify root has no parent
    assert!(
        root_ns.parent.is_none(),
        "Root namespace should have no parent"
    );

    // Verify it can be cached and retrieved
    let hierarchy =
        PostgresBackend::get_namespace(warehouse_id, root_id, ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .unwrap();

    assert_eq!(hierarchy.depth(), 0, "Root should have depth 0");
    assert!(hierarchy.parent().is_none(), "Root should have no parent");
}
