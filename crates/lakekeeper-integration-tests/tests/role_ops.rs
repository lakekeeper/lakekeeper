use lakekeeper::{
    ProjectId,
    api::{
        RequestMetadata, RequestMetadataTestBuilder,
        iceberg::v1::PaginationQuery,
        management::v1::{
            ApiServer,
            role::{
                CreateRoleRequest, DeleteRoleQuery, Service as _, UpdateRoleRequest,
                UpdateRoleSourceSystemRequest,
            },
        },
    },
    service::{
        ArcProjectId, CachePolicy, CatalogCreateRoleRequest, CatalogGrantOps as _,
        CatalogListRolesByIdFilter, CatalogRoleOps, CatalogStore, RoleId, RoleProviderId,
        RoleSourceId, SYSTEM_ROLE_PROVIDER_ID, SystemRoleSeederCap, SystemRoleSpec, Transaction,
        authz::{AllowAllAuthorizer, GrantResource, GrantSpec, UserOrRoleId},
        events::EventListener,
        role_cache::ROLE_CACHE,
    },
};
use lakekeeper_integration_tests::{
    CapturingAuthzListener, SetupTestCatalog, memory_io_profile, random_request_metadata,
};
use lakekeeper_storage_postgres::PostgresBackend;
use sqlx::PgPool;

fn request_metadata_with_project(project_id: &ProjectId) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .project_id(Some(project_id.clone().into()))
        .build()
}

fn make_provider() -> RoleProviderId {
    RoleProviderId::try_new("lakekeeper").unwrap()
}

fn make_source_id(s: &str) -> RoleSourceId {
    RoleSourceId::try_new(s).unwrap()
}

/// Create a role directly via `PostgresBackend` (no events fired, no cache update).
async fn db_create_role(
    ctx: &lakekeeper::api::ApiContext<
        lakekeeper::service::State<
            AllowAllAuthorizer,
            PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
        >,
    >,
    project_id: &ProjectId,
    role_name: &str,
    source_id: &str,
) -> std::sync::Arc<lakekeeper::service::Role> {
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
        lakekeeper::service::GetRoleInProjectError::RoleIdNotFoundInProject(_)
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
        DeleteRoleQuery::default(),
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
    lakekeeper::service::role_cache::role_cache_invalidate(role.id()).await;

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

/// `create_role` rejects requests with `provider_id = "system"`, recorded as the
/// request's one denial.
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
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
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
    assert_eq!(listener.settled_counts(0, 1).await, (0, 1));
}

/// A caller who may not create roles is refused by the authorizer before the
/// provider guard runs, so a reserved `provider_id` reveals nothing to them.
#[sqlx::test]
async fn test_create_role_authz_denial_precedes_provider_guard(pool: PgPool) {
    use lakekeeper::service::authz::tests::HidingAuthorizer;

    let authorizer = HidingAuthorizer::new();
    authorizer.block_action("project:CreateRole");
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(authorizer)
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let err = ApiServer::create_role(
        create_request(
            &warehouse_resp.project_id,
            "my-attempted-system-role",
            Some(("system", "custom-admin")),
        ),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, http::StatusCode::FORBIDDEN.as_u16());
    assert_ne!(err.error.r#type, "RoleProviderIdReserved");
    assert_eq!(listener.settled_counts(0, 1).await, (0, 1));
}

/// Create a system role directly via the catalog layer (bypasses the
/// `reject_role_provider_target` API guard). Used as fixture by tests that need
/// an existing system row to verify the immutability guards.
async fn seed_test_system_role(
    ctx: &lakekeeper::api::ApiContext<
        lakekeeper::service::State<
            AllowAllAuthorizer,
            PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
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
        DeleteRoleQuery::default(),
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
    let cap = SystemRoleSeederCap::for_storage_backend_seeding();

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
    let cap = SystemRoleSeederCap::for_storage_backend_seeding();

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
    let cap = SystemRoleSeederCap::for_storage_backend_seeding();

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
            lakekeeper::service::CreateRoleError::RoleSourceIdConflict(_)
        ),
        "expected RoleSourceIdConflict, got: {err:?}"
    );
}

// ==================== Audit ordering ====================

/// A write that fails *after* authorization succeeded must still record the
/// authorization outcome as a success — the write failure is not an
/// authorization failure.
///
/// The second request is identical to the first, so `require_project_action`
/// passes again while the catalog rejects the duplicate `provider~source_id` —
/// a write failure reachable only once authorization has already been decided.
#[sqlx::test]
async fn test_create_role_audits_authz_before_failing_write(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Attach after setup so only the two calls below are captured.
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let request = || CreateRoleRequest {
        name: "audit-order-role".to_string(),
        description: None,
        project_id: Some((*warehouse_resp.project_id).clone()),
        provider_id: Some(make_provider()),
        source_id: Some(make_source_id("src-audit-order")),
    };

    ApiServer::create_role(request(), ctx.clone(), random_request_metadata())
        .await
        .expect("first create succeeds");
    assert_eq!(
        listener.settled_counts(1, 0).await,
        (1, 0),
        "the successful call must be audited exactly once"
    );

    let write_error = ApiServer::create_role(request(), ctx.clone(), random_request_metadata())
        .await
        .expect_err("re-creating the same role must fail");

    assert_eq!(
        listener.settled_counts(2, 0).await,
        (2, 0),
        "the second authorization attempt must be audited as a success even though \
         the write that followed it failed: {write_error:?}"
    );
}

/// Same contract on the update path: renaming onto an existing name is rejected
/// by the write, after `require_role_action` has already allowed the update.
#[sqlx::test]
async fn test_update_role_audits_authz_before_failing_write(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;

    db_create_role(&ctx, project_id, "taken-name", "src-taken").await;
    let victim = db_create_role(&ctx, project_id, "renamable", "src-renamable").await;

    // Attach after setup so only the call below is captured.
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let write_error = ApiServer::update_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        victim.id,
        UpdateRoleRequest {
            name: "taken-name".to_string(),
            description: None,
        },
    )
    .await
    .expect_err("renaming onto an existing name must fail");

    assert_eq!(
        listener.settled_counts(1, 0).await,
        (1, 0),
        "the authorization attempt must be audited as a success even though the \
         write that followed it failed: {write_error:?}"
    );
}

/// An identity guard refuses a role the authorizer already allowed the action on.
/// That refusal *is* the authorization outcome, so it must be audited as a denial
/// — an `AuthorizationFailedEvent` and no success event — rather than an
/// "allowed" record for a change that never happened.
///
/// Covers all three lifecycle endpoints: the guard lives in `check_role_action`,
/// which they share, so each must produce the same single verdict. Counts are
/// cumulative over one listener, so a stray success event from any step fails the
/// next assertion too.
///
/// The two assertion kinds pin different things. The counts pin the *shape* — one
/// verdict, no stray success — and catch the guard being evaluated after the emit
/// rather than inside the check. The `failure_reasons` assertion pins the *label*:
/// routed through the `DeleteRoleError`/`UpdateRoleError` wrappers these report
/// `InternalCatalogError` ("no verdict was reached"), which is what a deliberate
/// refusal must not say.
#[sqlx::test]
async fn test_system_role_lifecycle_guards_audit_as_denials(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    // Every call below is refused, so one seeded role serves all three.
    let role_id = seed_test_system_role(&ctx, project_id, "test_admin").await;

    // Attach after setup so only the calls below are captured.
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let delete_err = ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        DeleteRoleQuery::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(delete_err.error.r#type, "SystemRoleImmutable");
    assert_eq!(
        listener.settled_counts(0, 1).await,
        (0, 1),
        "delete_role: the refusal is the authorization outcome — one denial, no success event"
    );

    let update_err = ApiServer::update_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        UpdateRoleRequest {
            name: "renamed".to_string(),
            description: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(update_err.error.r#type, "SystemRoleImmutable");
    assert_eq!(
        listener.settled_counts(0, 2).await,
        (0, 2),
        "update_role: same guard, same single-denial verdict"
    );

    let rebind_err = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        UpdateRoleSourceSystemRequest {
            provider_id: make_provider(),
            source_id: make_source_id("rebound"),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(rebind_err.error.r#type, "SystemRoleImmutable");
    assert_eq!(
        listener.settled_counts(0, 3).await,
        (0, 3),
        "update_role_source_system: same guard, same single-denial verdict"
    );

    assert_eq!(
        listener.failure_reasons(),
        vec![lakekeeper::service::events::AuthorizationFailureReason::ActionForbidden; 3],
        "a deliberate refusal is `ActionForbidden`, not an internal-catalog-error non-verdict"
    );
}

/// The other arm of the same guard: a role whose provider namespace is owned by a
/// configured role provider cannot be renamed or rebound, and that refusal is the
/// authorization outcome — a single denial, labelled `ActionForbidden`. Deleting it
/// is allowed: the provider recreates the role on its next sync if the group still
/// exists.
///
/// Needs a non-`AllowAll` authorizer because the deny-set comes from
/// `Authorizer::managed_role_provider_ids`, and needs store-level seeding because
/// `reject_role_provider_target` refuses a managed provider-id on create, so the API
/// cannot produce this state.
#[sqlx::test]
async fn test_managed_role_refuses_edits_allows_delete(pool: PgPool) {
    use lakekeeper::service::authz::tests::HidingAuthorizer;

    let provider: RoleProviderId = "corporate-ldap".parse().unwrap();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(HidingAuthorizer::new().with_managed_role_providers([provider.clone()]))
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let role_id = seed_role(&ctx, project_id, &provider, "ldap-1", "ldap-role").await;

    // Attach after setup so only the calls below are captured.
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let update_err = ApiServer::update_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        UpdateRoleRequest {
            name: "renamed".to_string(),
            description: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(update_err.error.r#type, "ManagedRoleImmutable");
    assert_eq!(
        listener.settled_counts(0, 1).await,
        (0, 1),
        "a provider-managed role is refused as the authorization outcome — one \
         denial, no success event"
    );

    let rebind_err = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        UpdateRoleSourceSystemRequest {
            provider_id: make_provider(),
            source_id: make_source_id("rebound"),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(rebind_err.error.r#type, "ManagedRoleImmutable");
    assert_eq!(listener.settled_counts(0, 2).await, (0, 2));
    assert_eq!(
        listener.failure_reasons(),
        vec![lakekeeper::service::events::AuthorizationFailureReason::ActionForbidden; 2],
    );

    // A provider-managed role holding a grant needs `force`, like any other role.
    PostgresBackend::apply_grants(
        &[GrantSpec {
            principal: UserOrRoleId::Role(role_id),
            resource: GrantResource::Warehouse(warehouse_resp.warehouse_id),
            privilege: "get_metadata".to_string(),
        }],
        &[],
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let delete_err = ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        DeleteRoleQuery::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(delete_err.error.r#type, "RoleHasGrants");
    assert_eq!(listener.settled_counts(1, 2).await, (1, 2));

    ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        DeleteRoleQuery::builder().force().build(),
    )
    .await
    .expect("a provider-managed role can be deleted");
    assert_eq!(listener.settled_counts(2, 2).await, (2, 2));
    assert!(
        PostgresBackend::get_role_by_id(project_id, role_id, ctx.v1_state.catalog.clone())
            .await
            .is_err(),
        "the role row is gone"
    );
}

/// Deleting a provider-managed role expires its members' sync records for that
/// provider, so the provider re-syncs them on their next request.
#[sqlx::test]
async fn test_delete_provider_role_expires_member_syncs(pool: PgPool) {
    use lakekeeper::{
        api::management::v1::user::UserLastUpdatedWith,
        service::{
            CatalogRoleAssignmentOps as _, CatalogRoleForAssignment, CatalogUserRoleAssignmentUser,
            RoleIdent, UserId, authz::tests::HidingAuthorizer,
        },
    };

    let provider: RoleProviderId = "corporate-ldap".parse().unwrap();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(HidingAuthorizer::new().with_managed_role_providers([provider.clone()]))
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let alice = std::sync::Arc::new(UserId::new_unchecked("oidc", "alice"));
    let ident = std::sync::Arc::new(RoleIdent::new_unchecked("corporate-ldap", "contractors"));
    let synced = PostgresBackend::sync_user_role_assignments(
        CatalogUserRoleAssignmentUser {
            user_id: &alice,
            name: Some("Alice"),
            email: None,
            user_type: None,
            updated_with: UserLastUpdatedWith::RoleProvider,
        },
        project_id,
        &provider,
        &[CatalogRoleForAssignment {
            ident: &ident,
            name: Some("Contractors"),
            description: None,
        }],
        ctx.v1_state.catalog.clone(),
        &ctx.v1_state.events,
    )
    .await
    .unwrap();
    assert_eq!(synced.provider_sync_times.len(), 1);

    ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        synced.roles[0].role_id,
        DeleteRoleQuery::default(),
    )
    .await
    .unwrap();

    let after =
        PostgresBackend::list_role_assignments_for_user(&alice, ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    assert!(after.roles.is_empty());
    assert!(
        after.provider_sync_times.is_empty(),
        "the member's sync record is gone: {:?}",
        after.provider_sync_times
    );
}

/// Under an authorizer with its own grant store, rows in the catalog's grant table
/// confer nothing, so they need no `force`; the delete removes them with the role.
#[sqlx::test]
async fn test_delete_role_ignores_catalog_grants_under_own_grant_store(pool: PgPool) {
    use lakekeeper::service::authz::tests::HidingAuthorizer;

    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(HidingAuthorizer::new().with_own_grant_store())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let role_id = seed_role(&ctx, project_id, &make_provider(), "leftover", "leftover").await;
    PostgresBackend::apply_grants(
        &[GrantSpec {
            principal: UserOrRoleId::Role(role_id),
            resource: GrantResource::Warehouse(warehouse_resp.warehouse_id),
            privilege: "get_metadata".to_string(),
        }],
        &[],
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        role_id,
        DeleteRoleQuery::default(),
    )
    .await
    .expect("catalog grant rows do not hold up the delete");
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM grant_assignment")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

// ==================== Provider namespaces the API may manage ====================

type TestCtx<A> = lakekeeper::api::ApiContext<
    lakekeeper::service::State<A, PostgresBackend, lakekeeper_storage_postgres::SecretsState>,
>;

/// Create a role in any namespace directly via `PostgresBackend`, as a provider
/// sync or an older deployment would have left it.
async fn seed_role<A: lakekeeper::service::authz::Authorizer>(
    ctx: &TestCtx<A>,
    project_id: &ProjectId,
    provider_id: &RoleProviderId,
    source_id: &str,
    name: &str,
) -> RoleId {
    let source_id = make_source_id(source_id);
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let role = PostgresBackend::create_role(
        project_id,
        CatalogCreateRoleRequest::builder()
            .role_id(RoleId::new_random())
            .role_name(name)
            .source_id(&source_id)
            .provider_id(provider_id)
            .build(),
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    role.id()
}

fn create_request(
    project_id: &ProjectId,
    name: &str,
    provider_and_source: Option<(&str, &str)>,
) -> CreateRoleRequest {
    CreateRoleRequest {
        name: name.to_string(),
        description: None,
        project_id: Some(project_id.clone()),
        provider_id: provider_and_source.map(|(p, _)| p.parse().unwrap()),
        source_id: provider_and_source.map(|(_, s)| make_source_id(s)),
    }
}

/// Under `LakekeeperOnly`, create accepts `lakekeeper` roles only, and every
/// refused provider is recorded as the request's one denial.
#[sqlx::test]
async fn test_create_role_lakekeeper_only(pool: PgPool) {
    use lakekeeper::service::authz::{ApiRoleProviders, tests::HidingAuthorizer};

    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(
            HidingAuthorizer::new()
                .with_managed_role_providers(["corporate-ldap".parse().unwrap()])
                .with_api_role_providers(ApiRoleProviders::LakekeeperOnly),
        )
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let defaulted = ApiServer::create_role(
        create_request(project_id, "defaulted", None),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(defaulted.provider_id, RoleProviderId::lakekeeper());
    assert_eq!(defaulted.source_id.as_str(), defaulted.id.to_string());

    let named = ApiServer::create_role(
        create_request(project_id, "named", Some(("lakekeeper", "analysts"))),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(named.source_id.as_str(), "analysts");

    for (provider, expected) in [
        ("entra", "RoleProviderNotApiManaged"),
        ("corporate-ldap", "ManagedRoleImmutable"),
        ("system", "RoleProviderIdReserved"),
    ] {
        let err = ApiServer::create_role(
            create_request(project_id, provider, Some((provider, "admins"))),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.r#type, expected, "provider `{provider}`");
        assert_eq!(err.error.code, http::StatusCode::BAD_REQUEST.as_u16());
    }

    assert_eq!(listener.settled_counts(2, 3).await, (2, 3));
    assert_eq!(
        listener.failure_reasons(),
        vec![lakekeeper::service::events::AuthorizationFailureReason::ActionForbidden; 3],
    );
}

/// The default, `AnyUnmanaged`, keeps namespaces no provider owns writable, so
/// external provisioning can label roles with its own provider id.
#[sqlx::test]
async fn test_create_and_rebind_into_unmanaged_namespace_by_default(pool: PgPool) {
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;

    let created = ApiServer::create_role(
        create_request(project_id, "external", Some(("entra", "group-1"))),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(created.provider_id.as_str(), "entra");

    let native = ApiServer::create_role(
        create_request(project_id, "native", None),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let rebound = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(project_id),
        native.id,
        UpdateRoleSourceSystemRequest {
            provider_id: "entra".parse().unwrap(),
            source_id: make_source_id("group-2"),
        },
    )
    .await
    .unwrap();
    assert_eq!(rebound.provider_id.as_str(), "entra");
}

/// Under `LakekeeperOnly`, a rebind must start and end in `lakekeeper`. A role left
/// in another namespace can still be renamed and deleted, so it can be cleaned up.
#[sqlx::test]
async fn test_rebind_and_cleanup_lakekeeper_only(pool: PgPool) {
    use lakekeeper::service::authz::{ApiRoleProviders, tests::HidingAuthorizer};

    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(
            HidingAuthorizer::new().with_api_role_providers(ApiRoleProviders::LakekeeperOnly),
        )
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let native = seed_role(&ctx, project_id, &make_provider(), "analysts", "analysts").await;
    let err = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(project_id),
        native,
        UpdateRoleSourceSystemRequest {
            provider_id: "entra".parse().unwrap(),
            source_id: make_source_id("admins"),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "RoleProviderNotApiManaged");

    let renamed = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(project_id),
        native,
        UpdateRoleSourceSystemRequest {
            provider_id: make_provider(),
            source_id: make_source_id("data-analysts"),
        },
    )
    .await
    .unwrap();
    assert_eq!(renamed.source_id.as_str(), "data-analysts");

    let orphan_provider: RoleProviderId = "retired-ldap".parse().unwrap();
    let orphan = seed_role(&ctx, project_id, &orphan_provider, "admins", "old-admins").await;
    let err = ApiServer::update_role_source_system(
        ctx.clone(),
        request_metadata_with_project(project_id),
        orphan,
        UpdateRoleSourceSystemRequest {
            provider_id: make_provider(),
            source_id: make_source_id("admins"),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "RoleProviderNotApiManaged");
    assert_eq!(listener.settled_counts(1, 2).await, (1, 2));

    ApiServer::update_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        orphan,
        UpdateRoleRequest {
            name: "retired admins".to_string(),
            description: None,
        },
    )
    .await
    .expect("a role in a namespace nothing syncs can be renamed");
    ApiServer::delete_role(
        ctx.clone(),
        request_metadata_with_project(project_id),
        orphan,
        DeleteRoleQuery::default(),
    )
    .await
    .expect("a role in a namespace nothing syncs can be deleted");
}

/// An error from the authorizer's `create_role` hook reaches the caller with its
/// own status, and the role is rolled back. Authorization had already allowed the
/// request, so it stays the one recorded verdict.
#[sqlx::test]
async fn test_create_role_hook_error_passes_through(pool: PgPool) {
    use lakekeeper::service::authz::tests::HidingAuthorizer;

    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(HidingAuthorizer::new().with_create_role_rejection("TestHookRejected"))
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let project_id = &warehouse_resp.project_id;
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    let err = ApiServer::create_role(
        create_request(project_id, "rejected", None),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "TestHookRejected");
    assert_eq!(err.error.code, http::StatusCode::CONFLICT.as_u16());
    assert_eq!(listener.settled_counts(1, 0).await, (1, 0));

    let roles = PostgresBackend::list_roles(
        project_id.clone(),
        CatalogListRolesByIdFilter::builder().build(),
        PaginationQuery::empty(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert!(
        roles.roles.iter().all(|r| r.name != "rejected"),
        "the role is rolled back"
    );
}
