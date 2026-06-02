use sqlx::PgPool;

use crate::{
    ProjectId,
    api::{
        RequestMetadata,
        iceberg::v1::PaginationQuery,
        management::v1::{
            ApiServer,
            role::{
                CreateRoleRequest, Service as _, UpdateRoleRequest, UpdateRoleSourceSystemRequest,
            },
        },
    },
    implementations::postgres::PostgresBackend,
    service::{
        ArcProjectId, CachePolicy, CatalogCreateRoleRequest, CatalogListRolesByIdFilter,
        CatalogRoleOps, CatalogStore, RoleId, RoleProviderId, RoleSourceId,
        SYSTEM_ROLE_PROVIDER_ID, SystemRoleSeederCap, SystemRoleSpec, Transaction, authn::Actor,
        authz::AllowAllAuthorizer, role_cache::ROLE_CACHE,
    },
    tests::{SetupTestCatalog, memory_io_profile, random_request_metadata},
};

fn request_metadata_with_project(project_id: &ProjectId) -> RequestMetadata {
    RequestMetadata::new_test(
        None,
        None,
        Actor::Anonymous,
        Some(project_id.clone().into()),
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
    assert_eq!(*role.project_id(), *warehouse_resp.project_id);
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
    let project_id = warehouse_resp.project_id.clone();

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

    let project_id: ArcProjectId = warehouse_resp.project_id.clone();

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
    let project_id: ArcProjectId = warehouse_resp.project_id.clone();

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

    let project_id: ArcProjectId = warehouse_resp.project_id.clone();

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
            project_id: Some((*warehouse_resp.project_id).clone()),
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
            project_id: Some((*warehouse_resp.project_id).clone()),
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
    let project_id: ArcProjectId = warehouse_resp.project_id.clone();
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
    let project_id: ArcProjectId = warehouse_resp.project_id.clone();
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
    let project_id: ArcProjectId = warehouse_resp.project_id.clone();

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

// ==================== System role rejection tests ====================

/// `create_role` rejects requests with `provider_id = "system"`.
#[sqlx::test]
async fn test_create_role_rejects_system_provider_id(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let err = ApiServer::create_role(
        CreateRoleRequest {
            name: "my-attempted-system-role".to_string(),
            description: None,
            project_id: Some((*warehouse_resp.project_id).clone()),
            provider_id: Some((*SYSTEM_ROLE_PROVIDER_ID).clone()),
            source_id: Some("custom-admin".parse().unwrap()),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.r#type, "RoleProviderIdReserved");
    assert_eq!(err.error.code, http::StatusCode::BAD_REQUEST.as_u16());
}

/// Create a system role directly via the catalog layer (bypasses the
/// `reject_system_provider` API guard). Used as fixture by tests that need
/// an existing system row to verify the immutability guards.
async fn seed_test_system_role(
    ctx: &crate::api::ApiContext<
        crate::service::State<
            AllowAllAuthorizer,
            PostgresBackend,
            crate::implementations::postgres::SecretsState,
        >,
    >,
    project_id: &ProjectId,
    source_id: &str,
) -> RoleId {
    let source: RoleSourceId = source_id.parse().unwrap();
    let name = format!("Test {source_id}");
    let request = CatalogCreateRoleRequest::builder()
        .role_id(RoleId::new_random())
        .role_name(&name)
        .source_id(&source)
        .provider_id(&SYSTEM_ROLE_PROVIDER_ID)
        .build();
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let created = PostgresBackend::create_roles(project_id, vec![request], tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    created[0].id()
}

/// `delete_role` rejects a system role with `SystemRoleImmutable`.
#[sqlx::test]
async fn test_delete_role_rejects_system_role(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role_id = seed_test_system_role(&ctx, &warehouse_resp.project_id, "test_admin").await;

    let err = ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(&warehouse_resp.project_id),
        role_id,
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.r#type, "SystemRoleImmutable");
    assert_eq!(err.error.code, http::StatusCode::BAD_REQUEST.as_u16());

    // Row is still present.
    let still_there = PostgresBackend::get_role_by_id(
        &warehouse_resp.project_id,
        role_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(still_there.id(), role_id);
}

/// `update_role` rejects a system role with `SystemRoleImmutable`.
#[sqlx::test]
async fn test_update_role_rejects_system_role(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role_id = seed_test_system_role(&ctx, &warehouse_resp.project_id, "test_admin").await;

    let err = ApiServer::update_role(
        ctx.clone(),
        request_metadata_with_project(&warehouse_resp.project_id),
        role_id,
        UpdateRoleRequest {
            name: "Renamed".to_string(),
            description: Some("nope".to_string()),
        },
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.r#type, "SystemRoleImmutable");
    assert_eq!(err.error.code, http::StatusCode::BAD_REQUEST.as_u16());
}

/// `update_role_source_system` rejects when the target role is a system role.
#[sqlx::test]
async fn test_update_role_source_system_rejects_system_target(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let role_id = seed_test_system_role(&ctx, &warehouse_resp.project_id, "test_admin").await;

    let err = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(&warehouse_resp.project_id),
        role_id,
        UpdateRoleSourceSystemRequest {
            provider_id: "oidc".parse().unwrap(),
            source_id: "moved-out".parse().unwrap(),
        },
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.r#type, "SystemRoleImmutable");
}

/// `update_role_source_system` rejects when the *new* `provider_id` is `system`.
#[sqlx::test]
async fn test_update_role_source_system_rejects_system_provider(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Create a customer role that we'll try to rebind into the system namespace.
    let role = db_create_role(
        &ctx,
        &warehouse_resp.project_id,
        "customer-role",
        "src-customer",
    )
    .await;

    let err = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(&warehouse_resp.project_id),
        role.id(),
        UpdateRoleSourceSystemRequest {
            provider_id: (*SYSTEM_ROLE_PROVIDER_ID).clone(),
            source_id: "smuggled".parse().unwrap(),
        },
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.r#type, "RoleProviderIdReserved");
}

/// The `Role` API response surfaces a system role's identity via
/// `provider-id = "system"`. Customer-created roles default to
/// `provider-id = "lakekeeper"`.
#[sqlx::test]
async fn test_role_response_provider_id_distinguishes_system_from_customer(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Customer role via the API: defaults to provider-id = "lakekeeper".
    let customer = ApiServer::create_role(
        CreateRoleRequest {
            name: "my-customer-role".to_string(),
            description: None,
            project_id: Some((*warehouse_resp.project_id).clone()),
            provider_id: None,
            source_id: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(customer.provider_id.as_str(), "lakekeeper");

    // System role seeded via the catalog (bypassing the API guard):
    // provider-id = "system".
    let system_role_id =
        seed_test_system_role(&ctx, &warehouse_resp.project_id, "example_role").await;
    let role = ApiServer::get_role(
        ctx.clone(),
        request_metadata_with_project(&warehouse_resp.project_id),
        system_role_id,
    )
    .await
    .unwrap();
    assert_eq!(role.provider_id.as_str(), "system");
    assert_eq!(role.source_id.as_str(), "example_role");
}

fn system_role_spec(source_id: &'static str, name: &'static str) -> SystemRoleSpec {
    SystemRoleSpec {
        source_id: RoleSourceId::try_new(source_id).unwrap(),
        name,
        description: "test system role",
    }
}

/// `upsert_system_roles` inserts new specs and refreshes only the rows that
/// actually changed. The same call twice in a row returns an empty Vec.
#[sqlx::test]
async fn test_upsert_system_roles_via_trait(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;

    // First call: inserts both rows.
    let specs = vec![
        system_role_spec("svc_admin", "Service Admin"),
        system_role_spec("svc_user", "Service User"),
    ];
    let cap = SystemRoleSeederCap::new();

    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let inserted = PostgresBackend::upsert_system_roles(project_id, &specs, cap, tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(inserted.len(), 2);

    // Second call with identical specs: no-op upsert, empty Vec.
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let nochange = PostgresBackend::upsert_system_roles(project_id, &specs, cap, tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(nochange.len(), 0, "idempotent re-seed must be a no-op");

    // Third call with one changed name: only the changed row is returned.
    let refreshed = vec![
        SystemRoleSpec {
            source_id: RoleSourceId::try_new("svc_admin").unwrap(),
            name: "Renamed Admin",
            description: "test system role",
        },
        system_role_spec("svc_user", "Service User"),
    ];
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let changed =
        PostgresBackend::upsert_system_roles(project_id, &refreshed, cap, tx.transaction())
            .await
            .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].name, "Renamed Admin");
    assert_eq!(changed[0].ident.source_id().as_str(), "svc_admin");
}

/// `delete_system_roles` removes rows by `source_id` and is idempotent: a
/// second call returns an empty Vec.
#[sqlx::test]
async fn test_delete_system_roles_via_trait(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let cap = SystemRoleSeederCap::new();

    // Seed one row.
    let specs = vec![system_role_spec("retired_role", "Retired")];
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::upsert_system_roles(project_id, &specs, cap, tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // First delete: returns one row.
    let source_id = RoleSourceId::try_new("retired_role").unwrap();
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let deleted =
        PostgresBackend::delete_system_roles(project_id, &[&source_id], cap, tx.transaction())
            .await
            .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(deleted.len(), 1);

    // Second delete: idempotent, no error.
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let again =
        PostgresBackend::delete_system_roles(project_id, &[&source_id], cap, tx.transaction())
            .await
            .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(again.len(), 0);
}

/// `upsert_system_roles` rejects duplicate `source_ids` in a single batch
/// with `RoleSourceIdConflict`. Without this check, Postgres would raise
/// a `cardinality_violation` (`ON CONFLICT DO UPDATE` can't touch the
/// same row twice) and surface it as an opaque backend error.
#[sqlx::test]
async fn test_upsert_system_roles_rejects_duplicate_source_ids(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let cap = SystemRoleSeederCap::new();

    let specs = vec![
        system_role_spec("dup", "First"),
        system_role_spec("dup", "Second"),
    ];
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let err = PostgresBackend::upsert_system_roles(project_id, &specs, cap, tx.transaction())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::service::CreateRoleError::RoleSourceIdConflict(_)
        ),
        "expected RoleSourceIdConflict, got: {err:?}"
    );
}

// ==================== Role-assignment API tests ====================

mod role_assignment_api_tests {
    use sqlx::PgPool;

    use super::{db_create_role, make_source_id};
    use crate::{
        ProjectId,
        api::management::v1::{
            ApiServer,
            role_assignment::{
                CreateRoleAssignmentRequest, ListRoleAssignmentsQuery, Service as _,
            },
            user::{UserLastUpdatedWith, UserType},
        },
        implementations::postgres::PostgresBackend,
        service::{
            CatalogCreateRoleRequest, CatalogRoleOps, CatalogStore, RoleId, RoleProviderId,
            Transaction, authn::UserId, authz::AllowAllAuthorizer,
        },
        tests::{SetupTestCatalog, memory_io_profile},
    };

    fn random_user() -> UserId {
        UserId::new_unchecked("oidc", &uuid::Uuid::now_v7().to_string())
    }

    /// Provision a user row so it can be a role-assignment target.
    async fn provision_user(ctx: &TestCtx, user_id: &UserId) {
        let mut tx = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        PostgresBackend::create_or_update_user(
            user_id,
            &format!("User {user_id}"),
            None,
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            tx.transaction(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    type TestCtx = crate::api::ApiContext<
        crate::service::State<
            AllowAllAuthorizer,
            PostgresBackend,
            crate::implementations::postgres::SecretsState,
        >,
    >;

    /// Create a role with a specific provider id in a given project, directly
    /// via the catalog (no API guard, no events).
    async fn db_create_role_with_provider(
        ctx: &TestCtx,
        project_id: &ProjectId,
        role_name: &str,
        source_id: &str,
        provider_id: &RoleProviderId,
    ) -> RoleId {
        let sid = make_source_id(source_id);
        let role_id = RoleId::new_random();
        let mut tx = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        let role = PostgresBackend::create_role(
            project_id,
            CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .source_id(&sid)
                .provider_id(provider_id)
                .build(),
            tx.transaction(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        role.id()
    }

    async fn assignment_count(ctx: &TestCtx, user_id: &UserId, role_id: RoleId) -> i64 {
        sqlx::query_scalar!(
            r#"SELECT COUNT(*) AS "count!" FROM role_assignment
               WHERE user_id = $1 AND role_id = $2"#,
            user_id.to_string(),
            *role_id,
        )
        .fetch_one(&ctx.v1_state.catalog.read_pool())
        .await
        .unwrap()
    }

    /// `AllowAll` + postgres: assign creates a row; revoke removes it.
    ///
    /// HTTP status note: these tests drive the service-layer trait fns directly,
    /// which return typed values rather than an HTTP response. The handler layer
    /// in `api/management/mod.rs` maps `create_role_assignment` →
    /// `(StatusCode::OK, Json(..))` (200) and `delete_role_assignment` →
    /// `(StatusCode::NO_CONTENT, ())` (204); those mappings are total (`.map(..)`
    /// over the `Result`) and are pinned by the `OpenAPI` `responses` annotations
    /// on those handlers. Here we assert the typed values the service fns return.
    #[sqlx::test]
    async fn test_assign_and_revoke_roundtrip(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;
        let role = db_create_role(&ctx, &warehouse.project_id, "ra-role", "ra-src").await;
        let user = random_user();
        provision_user(&ctx, &user).await;

        // POST (handler → 200): returns the assignment with a populated timestamp.
        let resp = ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            CreateRoleAssignmentRequest {
                user_id: user.clone(),
                role_id: role.id(),
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.user_id, user);
        assert_eq!(resp.role_id, role.id());
        assert!(resp.created_at.is_some());
        assert_eq!(assignment_count(&ctx, &user, role.id()).await, 1);

        // DELETE (handler → 204): returns `()`; row removed.
        let deleted: () = ApiServer::delete_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            user.clone(),
            role.id(),
        )
        .await
        .unwrap();
        assert_eq!(deleted, ());
        assert_eq!(assignment_count(&ctx, &user, role.id()).await, 0);

        // DELETE again on the now-absent pair (handler → 204): idempotent no-op.
        let deleted_again: () = ApiServer::delete_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            user.clone(),
            role.id(),
        )
        .await
        .unwrap();
        assert_eq!(deleted_again, ());
        assert_eq!(assignment_count(&ctx, &user, role.id()).await, 0);
    }

    /// Assigning a role to a user that has not been provisioned → 404
    /// `RoleAssignmentUserNotFound`, and no assignment row is written.
    #[sqlx::test]
    async fn test_assign_unknown_user_404(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;
        let role = db_create_role(&ctx, &warehouse.project_id, "nouser-role", "nouser-src").await;
        let user = random_user();

        let err = ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            CreateRoleAssignmentRequest {
                user_id: user.clone(),
                role_id: role.id(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.r#type, "RoleAssignmentUserNotFound");
        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
        assert_eq!(assignment_count(&ctx, &user, role.id()).await, 0);
    }

    /// POST is idempotent (200 twice); DELETE is idempotent (204 twice, even
    /// when absent).
    #[sqlx::test]
    async fn test_assign_revoke_idempotent(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;
        let role = db_create_role(&ctx, &warehouse.project_id, "idem-role", "idem-src").await;
        let user = random_user();
        provision_user(&ctx, &user).await;

        let req = || CreateRoleAssignmentRequest {
            user_id: user.clone(),
            role_id: role.id(),
        };
        // First POST (handler → 200): creates the row, stamps created_at.
        let first = ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            req(),
        )
        .await
        .unwrap();
        // Second POST on the same pair (handler → 200): idempotent upsert.
        let second = ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            req(),
        )
        .await
        .unwrap();
        // Idempotent: the second POST returns the *same* original created_at
        // (not a new timestamp), proving it did not re-insert the row.
        let first_created_at = first.created_at.expect("postgres path stamps created_at");
        let second_created_at = second.created_at.expect("postgres path stamps created_at");
        assert_eq!(first_created_at, second_created_at);
        assert_eq!(second.user_id, user);
        assert_eq!(second.role_id, role.id());
        assert_eq!(assignment_count(&ctx, &user, role.id()).await, 1);

        // First DELETE (handler → 204): removes the row.
        let first_delete: () = ApiServer::delete_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            user.clone(),
            role.id(),
        )
        .await
        .unwrap();
        assert_eq!(first_delete, ());
        assert_eq!(assignment_count(&ctx, &user, role.id()).await, 0);

        // Second DELETE on the now-absent pair (handler → 204): no-op.
        let second_delete: () = ApiServer::delete_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            user.clone(),
            role.id(),
        )
        .await
        .unwrap();
        assert_eq!(second_delete, ());
        assert_eq!(assignment_count(&ctx, &user, role.id()).await, 0);
    }

    /// Assigning a role that lives in a different project is rejected with the
    /// resolved-role not-found error (404 `RoleIdNotFoundInProject`).
    #[sqlx::test]
    async fn test_assign_cross_project_role_rejected(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;

        // Create a second project + a role inside it.
        let other_project = ProjectId::new(uuid::Uuid::now_v7());
        let mut tx = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        PostgresBackend::create_project(&other_project, "other".to_string(), tx.transaction())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let foreign_role =
            db_create_role(&ctx, &other_project, "foreign-role", "foreign-src").await;

        let err = ApiServer::create_role_assignment(
            ctx.clone(),
            // Request scoped to the *warehouse* project, not the role's project.
            random_request_metadata_with_project(&warehouse.project_id),
            CreateRoleAssignmentRequest {
                user_id: random_user(),
                role_id: foreign_role.id(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.r#type, "RoleNotFoundInProject");
        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
    }

    /// list: missing both filters → 400; both filters → 400.
    #[sqlx::test]
    async fn test_list_filter_required(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;

        let none = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: None,
                role_id: None,
                page_token: None,
                page_size: None,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(none.error.r#type, "RoleAssignmentFilterRequired");
        assert_eq!(none.error.code, http::StatusCode::BAD_REQUEST.as_u16());

        let both = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: Some(random_user()),
                role_id: Some(RoleId::new_random()),
                page_token: None,
                page_size: None,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(both.error.r#type, "RoleAssignmentFilterRequired");
        assert_eq!(both.error.code, http::StatusCode::BAD_REQUEST.as_u16());
    }

    /// list by-user and by-role each return the assignment.
    #[sqlx::test]
    async fn test_list_by_user_and_by_role(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;
        let role = db_create_role(&ctx, &warehouse.project_id, "list-role", "list-src").await;
        let user = random_user();
        provision_user(&ctx, &user).await;
        ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            CreateRoleAssignmentRequest {
                user_id: user.clone(),
                role_id: role.id(),
            },
        )
        .await
        .unwrap();

        let by_user = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: Some(user.clone()),
                role_id: None,
                page_token: None,
                page_size: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(by_user.assignments.len(), 1);
        assert_eq!(by_user.assignments[0].user_id, user);
        assert_eq!(by_user.assignments[0].role_id, role.id());

        let by_role = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: None,
                role_id: Some(role.id()),
                page_token: None,
                page_size: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(by_role.assignments.len(), 1);
        assert_eq!(by_role.assignments[0].user_id, user);
        assert_eq!(by_role.assignments[0].role_id, role.id());
    }

    /// A role whose provider is neither `lakekeeper` nor `system` rejects manual
    /// assignment with 409 `RoleNotManuallyAssignable`; a default
    /// (`lakekeeper`-provider) role is allowed.
    #[sqlx::test]
    async fn test_non_catalog_managed_provider_rejected(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;

        let provider = RoleProviderId::try_new("oidc").unwrap();
        let managed_role = db_create_role_with_provider(
            &ctx,
            &warehouse.project_id,
            "managed-role",
            "managed-src",
            &provider,
        )
        .await;

        let user = random_user();
        provision_user(&ctx, &user).await;

        let err = ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            CreateRoleAssignmentRequest {
                user_id: user.clone(),
                role_id: managed_role,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.r#type, "RoleNotManuallyAssignable");
        assert_eq!(err.error.code, http::StatusCode::CONFLICT.as_u16());

        // A lakekeeper-provider (default) role is unaffected.
        let ok_role = db_create_role(&ctx, &warehouse.project_id, "ok-role", "ok-src").await;
        let ok_user = random_user();
        provision_user(&ctx, &ok_user).await;
        ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            CreateRoleAssignmentRequest {
                user_id: ok_user,
                role_id: ok_role.id(),
            },
        )
        .await
        .unwrap();
    }

    /// A `system`-provider role *is* manually assignable: `is_system()` is in
    /// the allowlist. Proves the positive path (the rejection tests only cover
    /// non-catalog providers). Assign succeeds and the row is visible via list.
    #[sqlx::test]
    async fn test_assign_system_role_succeeds(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;

        // Seed a real system-provider role (provider_id = "system").
        let role_id = super::seed_test_system_role(&ctx, &warehouse.project_id, "sys_assign").await;
        let user = random_user();
        provision_user(&ctx, &user).await;

        // Assign (handler → 200): succeeds for a system role and writes a row.
        let resp = ApiServer::create_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            CreateRoleAssignmentRequest {
                user_id: user.clone(),
                role_id,
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.user_id, user);
        assert_eq!(resp.role_id, role_id);
        assert!(resp.created_at.is_some());
        assert_eq!(assignment_count(&ctx, &user, role_id).await, 1);

        // Visible in the by-role listing.
        let by_role = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: None,
                role_id: Some(role_id),
                page_token: None,
                page_size: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(by_role.assignments.len(), 1);
        assert_eq!(by_role.assignments[0].user_id, user);
        assert_eq!(by_role.assignments[0].role_id, role_id);
    }

    /// The allowlist guards DELETE too, not just POST: revoking an assignment on
    /// a role owned by a non-catalog provider (here `oidc`) is rejected with
    /// 409 `RoleNotManuallyAssignable`.
    #[sqlx::test]
    async fn test_delete_non_catalog_managed_provider_rejected(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;

        let provider = RoleProviderId::try_new("oidc").unwrap();
        let managed_role = db_create_role_with_provider(
            &ctx,
            &warehouse.project_id,
            "managed-del-role",
            "managed-del-src",
            &provider,
        )
        .await;
        let user = random_user();
        provision_user(&ctx, &user).await;

        let err = ApiServer::delete_role_assignment(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            user.clone(),
            managed_role,
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.r#type, "RoleNotManuallyAssignable");
        assert_eq!(err.error.code, http::StatusCode::CONFLICT.as_u16());
    }

    /// Pagination round-trip over `ByRole`: three assignments, `page_size` = 2.
    /// First page yields exactly 2 rows + a `next_page_token`. Feeding the token
    /// back yields the remaining 1 row (plus a token — the keyset paginator emits
    /// a token whenever the page is non-empty); the *next* request with that
    /// token returns an empty page with `next_page_token == None`, signalling the
    /// end. Across all pages no row is dropped or repeated and the union equals
    /// exactly the assigned set.
    #[sqlx::test]
    async fn test_list_pagination_round_trip(pool: PgPool) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;
        let role = db_create_role(&ctx, &warehouse.project_id, "page-role", "page-src").await;

        // Three distinct users assigned to the same role.
        let mut expected_users = std::collections::HashSet::new();
        for _ in 0..3 {
            let user = random_user();
            provision_user(&ctx, &user).await;
            ApiServer::create_role_assignment(
                ctx.clone(),
                random_request_metadata_with_project(&warehouse.project_id),
                CreateRoleAssignmentRequest {
                    user_id: user.clone(),
                    role_id: role.id(),
                },
            )
            .await
            .unwrap();
            expected_users.insert(user);
        }
        assert_eq!(expected_users.len(), 3);

        // Page 1: page_size = 2 → exactly 2 rows + a next_page_token.
        let page1 = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: None,
                role_id: Some(role.id()),
                page_token: None,
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(page1.assignments.len(), 2);
        let token1 = page1
            .next_page_token
            .clone()
            .expect("a next_page_token is returned when a full page of rows was read");

        // Page 2: feed the token back → the remaining 1 row. The keyset paginator
        // still emits a token for a non-empty page (it does not look ahead).
        let page2 = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: None,
                role_id: Some(role.id()),
                page_token: Some(token1),
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(page2.assignments.len(), 1);
        let token2 = page2
            .next_page_token
            .clone()
            .expect("a token is emitted for any non-empty page");

        // Page 3: the token from the final non-empty page yields an empty page
        // with no further token — this is the end-of-stream signal.
        let page3 = ApiServer::list_role_assignments(
            ctx.clone(),
            random_request_metadata_with_project(&warehouse.project_id),
            ListRoleAssignmentsQuery {
                user_id: None,
                role_id: Some(role.id()),
                page_token: Some(token2),
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(page3.assignments.len(), 0);
        assert_eq!(page3.next_page_token, None);

        // Every page row is for this role; collect the user set across pages.
        let mut seen_users = std::collections::HashSet::new();
        for a in page1
            .assignments
            .iter()
            .chain(page2.assignments.iter())
            .chain(page3.assignments.iter())
        {
            assert_eq!(a.role_id, role.id());
            // No duplicate across pages.
            assert!(
                seen_users.insert(a.user_id.clone()),
                "user {} appeared on more than one page",
                a.user_id
            );
        }
        // Union of all pages equals exactly the assigned set (none skipped).
        assert_eq!(seen_users, expected_users);
    }

    fn random_request_metadata_with_project(project_id: &ProjectId) -> crate::api::RequestMetadata {
        crate::api::RequestMetadata::new_test(
            None,
            None,
            crate::service::authn::Actor::Anonymous,
            Some(project_id.clone().into()),
            None,
            http::Method::default(),
        )
    }
}
