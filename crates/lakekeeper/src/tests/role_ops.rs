use sqlx::PgPool;

use crate::{
    ProjectId,
    api::{
        RequestMetadata,
        iceberg::v1::PaginationQuery,
        management::v1::{
            ApiServer,
            role::{CreateRoleRequest, Service as _, UpdateRoleRequest},
        },
    },
    implementations::postgres::PostgresBackend,
    service::{
        ArcProjectId, CachePolicy, CatalogCreateRoleRequest, CatalogListRolesByIdFilter,
        CatalogRoleOps, CatalogStore, RoleId, RoleProviderId, RoleSourceId, Transaction,
        authn::Actor, authz::AllowAllAuthorizer, role_cache::ROLE_CACHE,
    },
    tests::{SetupTestCatalog, memory_io_profile, random_request_metadata},
};

fn request_metadata_with_project(project_id: &ProjectId) -> RequestMetadata {
    RequestMetadata::new_test(
        None,
        None,
        Actor::Anonymous,
        Some(project_id.clone()),
        None,
        http::Method::default(),
    )
}

fn make_provider() -> RoleProviderId {
    RoleProviderId::try_new("lakekeeper").unwrap()
}

fn make_source_id(s: &str) -> RoleSourceId {
    RoleSourceId::try_new(s).unwrap()
}

/// Create a role directly via `PostgresBackend` (no events fired, no cache update).
async fn db_create_role(
    ctx: &crate::api::ApiContext<
        crate::service::State<
            AllowAllAuthorizer,
            PostgresBackend,
            crate::implementations::postgres::SecretsState,
        >,
    >,
    project_id: &ProjectId,
    role_name: &str,
    source_id: &str,
) -> std::sync::Arc<crate::service::Role> {
    let provider_id = make_provider();
    let sid = make_source_id(source_id);
    let role_id = RoleId::new_random();

    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let role = PostgresBackend::create_role(
        project_id,
        CatalogCreateRoleRequest::builder()
            .role_id(role_id)
            .role_name(role_name)
            .source_id(&sid)
            .provider_id(&provider_id)
            .build(),
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    role
}

// ==================== Basic CRUD tests ====================

/// Test basic role creation via `PostgresBackend`
#[sqlx::test]
async fn test_create_role(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "my-role", "src-create").await;

    assert_eq!(role.name(), "my-role");
    assert_eq!(*role.project_id(), warehouse_resp.project_id);
    assert_eq!(*role.version, 0);
}

/// Test `list_roles` returns the created role
#[sqlx::test]
async fn test_list_roles(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "list-role", "src-list").await;
    let project_id = std::sync::Arc::new(warehouse_resp.project_id.clone());

    let result = PostgresBackend::list_roles(
        project_id.clone(),
        CatalogListRolesByIdFilter::builder().build(),
        PaginationQuery::new_with_page_size(100),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(result.roles.iter().any(|r| r.id() == role.id()));
}

/// Test `delete_role` removes the role
#[sqlx::test]
async fn test_delete_role(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "del-role", "src-del").await;
    let role_id = role.id();

    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::delete_role(&warehouse_resp.project_id, role_id, tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let err = PostgresBackend::get_role_by_id(
        &warehouse_resp.project_id,
        role_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        crate::service::GetRoleInProjectError::RoleIdNotFoundInProject(_)
    ));
}

/// Test `update_role` changes the name and increments version
#[sqlx::test]
async fn test_update_role(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "upd-role", "src-upd").await;
    let original_version = *role.version;

    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let updated = PostgresBackend::update_role(
        &warehouse_resp.project_id,
        role.id(),
        "upd-role-v2",
        Some("new desc"),
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(updated.name(), "upd-role-v2");
    assert_eq!(updated.description.as_deref(), Some("new desc"));
    assert_eq!(*updated.version, original_version + 1);
}

// ==================== Cache population tests ====================

/// Test that `get_role_by_id` populates `ROLE_CACHE`
#[sqlx::test]
async fn test_role_cache_populated_by_get_id(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(
        &ctx,
        &warehouse_resp.project_id,
        "cache-get",
        "src-cache-get",
    )
    .await;
    let role_id = role.id();

    // Clear cache
    ROLE_CACHE.invalidate(&role_id).await;
    assert!(ROLE_CACHE.get(&role_id).await.is_none());

    // get_role_by_id should populate cache
    let fetched = PostgresBackend::get_role_by_id(
        &warehouse_resp.project_id,
        role_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(fetched.id(), role_id);

    // Cache should now have the entry
    let cached = ROLE_CACHE.get(&role_id).await;
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().id(), role_id);

    // Second call should hit cache (same result)
    let fetched2 = PostgresBackend::get_role_by_id(
        &warehouse_resp.project_id,
        role_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(fetched2.id(), role_id);
    assert_eq!(fetched2.name(), fetched.name());
}

/// Test that `get_role_by_ident` populates `ROLE_CACHE`
#[sqlx::test]
async fn test_role_cache_populated_by_get_ident(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(
        &ctx,
        &warehouse_resp.project_id,
        "ident-role",
        "src-ident-get",
    )
    .await;
    let role_id = role.id();

    // Clear cache
    ROLE_CACHE.invalidate(&role_id).await;
    assert!(ROLE_CACHE.get(&role_id).await.is_none());

    let project_id: ArcProjectId = std::sync::Arc::new(warehouse_resp.project_id.clone());

    // get_role_by_ident should populate ROLE_CACHE (and IDENT_TO_ID_CACHE internally)
    let fetched = PostgresBackend::get_role_by_ident(
        project_id.clone(),
        role.ident_arc(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(fetched.id(), role_id);

    // Primary cache should now have the entry
    let cached = ROLE_CACHE.get(&role_id).await;
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().id(), role_id);
}

/// Test that `get_role_by_ident` returns stale data from cache when DB is updated
#[sqlx::test]
async fn test_get_role_by_ident_uses_cache(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(
        &ctx,
        &warehouse_resp.project_id,
        "ident-cache",
        "src-ident-cache",
    )
    .await;
    let project_id: ArcProjectId = std::sync::Arc::new(warehouse_resp.project_id.clone());

    // Populate cache via get_role_by_ident
    let v1 = PostgresBackend::get_role_by_ident(
        project_id.clone(),
        role.ident_arc(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(v1.name(), "ident-cache");

    // Update name in DB directly (bypasses cache)
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::update_role(
        &warehouse_resp.project_id,
        role.id(),
        "ident-cache-v2",
        None,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // get_role_by_ident should still return stale cached data
    let v2 = PostgresBackend::get_role_by_ident(
        project_id.clone(),
        role.ident_arc(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(v2.name(), "ident-cache");
    assert_eq!(*v2.version, 0);
}

// ==================== CachePolicy tests ====================

/// Test `CachePolicy::Use` returns stale data and
/// `CachePolicy::RequireMinimumVersion` fetches fresh
#[sqlx::test]
async fn test_cache_respects_min_version(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "ver-role", "src-ver").await;

    // First get — populates cache
    let v0 = PostgresBackend::get_role_by_id(
        &warehouse_resp.project_id,
        role.id(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let original_version = *v0.version;

    // Update in DB only (no cache event)
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::update_role(
        &warehouse_resp.project_id,
        role.id(),
        "ver-role-updated",
        None,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // CachePolicy::Use — should return stale cached data
    let stale = PostgresBackend::get_role_by_id_cache_aware(
        &warehouse_resp.project_id,
        role.id(),
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(*stale.version, original_version);
    assert_eq!(stale.name(), "ver-role");

    // CachePolicy::RequireMinimumVersion — should fetch fresh data
    let fresh = PostgresBackend::get_role_by_id_cache_aware(
        &warehouse_resp.project_id,
        role.id(),
        CachePolicy::RequireMinimumVersion(original_version + 1),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(*fresh.version, original_version + 1);
    assert_eq!(fresh.name(), "ver-role-updated");
}

/// Test `CachePolicy::Skip` bypasses cache read but still re-populates cache after DB fetch
#[sqlx::test]
async fn test_cache_policy_skip_bypasses_cache(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "skip-role", "src-skip").await;

    // Populate cache
    let original = PostgresBackend::get_role_by_id(
        &warehouse_resp.project_id,
        role.id(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    // Update in DB only (no cache event)
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::update_role(
        &warehouse_resp.project_id,
        role.id(),
        "skip-role-v2",
        None,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // CachePolicy::Use returns stale
    let cached = PostgresBackend::get_role_by_id_cache_aware(
        &warehouse_resp.project_id,
        role.id(),
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(cached.version, original.version);

    // CachePolicy::Skip goes to DB and re-populates cache with fresh data
    let fresh = PostgresBackend::get_role_by_id_cache_aware(
        &warehouse_resp.project_id,
        role.id(),
        CachePolicy::Skip,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(*fresh.version, *original.version + 1);
    assert_eq!(fresh.name(), "skip-role-v2");

    // After Skip, CachePolicy::Use should now return the fresh cached data
    let now_fresh = PostgresBackend::get_role_by_id_cache_aware(
        &warehouse_resp.project_id,
        role.id(),
        CachePolicy::Use,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(*now_fresh.version, *original.version + 1);
    assert_eq!(now_fresh.name(), "skip-role-v2");
}

// ==================== List from cache tests ====================

/// Test that `list_roles` with `role_ids` filter serves results from cache on second call
#[sqlx::test]
async fn test_list_roles_with_role_ids_served_from_cache(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role1 = db_create_role(&ctx, &warehouse_resp.project_id, "list-cache-1", "src-lc1").await;
    let role2 = db_create_role(&ctx, &warehouse_resp.project_id, "list-cache-2", "src-lc2").await;
    let role_ids = [role1.id(), role2.id()];

    let project_id: ArcProjectId = std::sync::Arc::new(warehouse_resp.project_id.clone());

    // Clear cache entries
    ROLE_CACHE.invalidate(&role1.id()).await;
    ROLE_CACHE.invalidate(&role2.id()).await;

    // First call — goes to DB, populates cache
    let result1 = PostgresBackend::list_roles(
        project_id.clone(),
        CatalogListRolesByIdFilter::builder()
            .role_ids(Some(&role_ids))
            .build(),
        PaginationQuery::new_with_page_size(100),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(result1.roles.len(), 2);

    // Both should now be in cache
    assert!(ROLE_CACHE.get(&role1.id()).await.is_some());
    assert!(ROLE_CACHE.get(&role2.id()).await.is_some());

    // Update one role in DB without updating cache
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::update_role(
        &warehouse_resp.project_id,
        role1.id(),
        "list-cache-1-updated",
        None,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Second call — should be served from cache (stale data)
    let result2 = PostgresBackend::list_roles(
        project_id.clone(),
        CatalogListRolesByIdFilter::builder()
            .role_ids(Some(&role_ids))
            .build(),
        PaginationQuery::new_with_page_size(100),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    // Should still see old name from cache
    let r1_cached = result2.roles.iter().find(|r| r.id() == role1.id()).unwrap();
    assert_eq!(r1_cached.name(), "list-cache-1");
}

/// Test `list_roles_across_projects` with `role_ids` filter populates cache
#[sqlx::test]
async fn test_list_roles_across_projects_cache_populated(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "cross-proj", "src-cross").await;

    ROLE_CACHE.invalidate(&role.id()).await;
    assert!(ROLE_CACHE.get(&role.id()).await.is_none());

    let role_ids = [role.id()];
    let result = PostgresBackend::list_roles_across_projects(
        CatalogListRolesByIdFilter::builder()
            .role_ids(Some(&role_ids))
            .build(),
        PaginationQuery::new_with_page_size(100),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert_eq!(result.roles.len(), 1);
    assert!(ROLE_CACHE.get(&role.id()).await.is_some());
}

// ==================== API event-driven cache tests ====================

/// Test that `ApiServer::update_role` fires an event that updates the cache
#[sqlx::test]
async fn test_cache_updated_on_api_update(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Create via API (fires create event → populates cache)
    let created = ApiServer::create_role(
        CreateRoleRequest {
            name: "api-upd-role".to_string(),
            description: None,
            project_id: Some(warehouse_resp.project_id.clone()),
            provider_id: None,
            source_id: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let role_id = created.id;

    // Give the async event handler time to run
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cache should be populated
    let before = ROLE_CACHE.get(&role_id).await;
    assert!(before.is_some());
    let before_version = *before.unwrap().version;

    // Update via ApiServer (fires update event → cache updated)
    ApiServer::update_role(
        ctx.clone(),
        request_metadata_with_project(&warehouse_resp.project_id),
        role_id,
        UpdateRoleRequest {
            name: "api-upd-role-v2".to_string(),
            description: Some("updated".to_string()),
        },
    )
    .await
    .unwrap();

    // Give the async event handler time to run
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cache should now contain the updated role
    let after = ROLE_CACHE.get(&role_id).await;
    assert!(after.is_some());
    let after_role = after.unwrap();
    assert_eq!(after_role.name(), "api-upd-role-v2");
    assert_eq!(*after_role.version, before_version + 1);
}

/// Test that `ApiServer::delete_role` fires an event that invalidates the cache
#[sqlx::test]
async fn test_cache_invalidated_on_api_delete(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Create via API (fires create event → populates cache)
    let created = ApiServer::create_role(
        CreateRoleRequest {
            name: "api-del-role".to_string(),
            description: None,
            project_id: Some(warehouse_resp.project_id.clone()),
            provider_id: None,
            source_id: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let role_id = created.id;

    // Give the async event handler time to run
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify cache is populated
    assert!(ROLE_CACHE.get(&role_id).await.is_some());

    // Also populate IDENT_TO_ID_CACHE by doing a get_role_by_ident
    let project_id: ArcProjectId = std::sync::Arc::new(warehouse_resp.project_id.clone());
    let ident = ROLE_CACHE.get(&role_id).await.unwrap().ident_arc();
    PostgresBackend::get_role_by_ident(
        project_id.clone(),
        ident.clone(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    // Delete via ApiServer (fires delete event → cache invalidated)
    ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(&warehouse_resp.project_id),
        role_id,
    )
    .await
    .unwrap();

    // Give the async event handler time to run
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Primary cache should be empty
    assert!(ROLE_CACHE.get(&role_id).await.is_none());

    // After eviction, get_role_by_ident should return not-found (goes to DB, role deleted)
    let result =
        PostgresBackend::get_role_by_ident(project_id, ident, ctx.v1_state.catalog.clone()).await;
    assert!(result.is_err());
}

/// Test that invalidating `ROLE_CACHE` cascades to the secondary ident-to-id cache,
/// so subsequent `get_role_by_ident` lookups re-fetch from DB rather than serving a
/// stale ident mapping.
#[sqlx::test]
async fn test_cache_eviction_invalidates_ident_lookup(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(&ctx, &warehouse_resp.project_id, "evict-role", "src-evict").await;
    let project_id: ArcProjectId = std::sync::Arc::new(warehouse_resp.project_id.clone());
    let ident = role.ident_arc();

    // Populate both caches via get_role_by_ident
    let v1 = PostgresBackend::get_role_by_ident(
        project_id.clone(),
        ident.clone(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(*v1.version, 0);

    // Update name in DB (version bumped to 1)
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::update_role(
        &warehouse_resp.project_id,
        role.id(),
        "evict-role-v2",
        None,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Explicitly invalidate the primary cache entry (simulates eviction)
    crate::service::role_cache::role_cache_invalidate(role.id()).await;

    // Give the eviction listener time to cascade to IDENT_TO_ID_CACHE
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Primary cache should be empty
    assert!(ROLE_CACHE.get(&role.id()).await.is_none());

    // get_role_by_ident should now go to DB (both caches are clear) and return fresh
    let v2 = PostgresBackend::get_role_by_ident(
        project_id.clone(),
        ident.clone(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(*v2.version, 1);
    assert_eq!(v2.name(), "evict-role-v2");
}

/// Test that `get_role_by_id_across_projects` populates `ROLE_CACHE`
#[sqlx::test]
async fn test_role_cache_populated_by_get_across_projects(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role = db_create_role(
        &ctx,
        &warehouse_resp.project_id,
        "cross-proj-get",
        "src-cpg",
    )
    .await;

    // Clear cache
    ROLE_CACHE.invalidate(&role.id()).await;
    assert!(ROLE_CACHE.get(&role.id()).await.is_none());

    // get_role_by_id_across_projects should populate cache
    let fetched =
        PostgresBackend::get_role_by_id_across_projects(role.id(), ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    assert_eq!(fetched.id(), role.id());

    // Cache should now have the entry
    assert!(ROLE_CACHE.get(&role.id()).await.is_some());
}

/// Test `list_roles` with `role_ids` and `source_ids` filters applies post-cache filtering
#[sqlx::test]
async fn test_list_roles_cache_source_id_filter(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role_a = db_create_role(
        &ctx,
        &warehouse_resp.project_id,
        "flt-role-a",
        "src-filter-a",
    )
    .await;
    let role_b = db_create_role(
        &ctx,
        &warehouse_resp.project_id,
        "flt-role-b",
        "src-filter-b",
    )
    .await;
    let role_ids = [role_a.id(), role_b.id()];
    let project_id: ArcProjectId = std::sync::Arc::new(warehouse_resp.project_id.clone());

    // Populate cache via first list call
    PostgresBackend::list_roles(
        project_id.clone(),
        CatalogListRolesByIdFilter::builder()
            .role_ids(Some(&role_ids))
            .build(),
        PaginationQuery::new_with_page_size(100),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    // Both roles should be in cache now
    assert!(ROLE_CACHE.get(&role_a.id()).await.is_some());
    assert!(ROLE_CACHE.get(&role_b.id()).await.is_some());

    // List with role_ids + source_id filter — cache should apply the filter
    let src_a = make_source_id("src-filter-a");
    let src_a_ref: &RoleSourceId = &src_a;
    let result = PostgresBackend::list_roles(
        project_id.clone(),
        CatalogListRolesByIdFilter::builder()
            .role_ids(Some(&role_ids))
            .source_ids(Some(&[src_a_ref]))
            .build(),
        PaginationQuery::new_with_page_size(100),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    // Only role_a matches the source_id filter
    assert_eq!(result.roles.len(), 1);
    assert_eq!(result.roles[0].id(), role_a.id());
}
