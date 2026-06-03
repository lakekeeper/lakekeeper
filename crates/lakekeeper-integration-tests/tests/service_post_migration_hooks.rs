use lakekeeper::{
    ProjectId,
    api::iceberg::v1::{PageToken, PaginationQuery},
    service::{
        CatalogListRolesByIdFilter, RoleSourceId, SYSTEM_ROLE_PROVIDER_ID, SystemRoleSpec,
        Transaction as _, run_post_migration_hooks,
    },
};
use lakekeeper_integration_tests::upsert_system_roles_in_all_projects;
use lakekeeper_storage_postgres::{
    CatalogState, PostgresBackend, PostgresTransaction, migrations::migrate_core_only,
    role::list_roles,
};
use sqlx::PgPool;

fn spec(source_id: &str, name: &'static str, description: &'static str) -> SystemRoleSpec {
    SystemRoleSpec {
        source_id: source_id.parse::<RoleSourceId>().unwrap(),
        name,
        description,
    }
}

/// Read every system role for `project_id`. Returns
/// `(source_id, name, description, version)` tuples ordered by `source_id`.
async fn list_system_roles(
    pool: &PgPool,
    project_id: &ProjectId,
) -> Vec<(String, String, Option<String>, i64)> {
    let provider = &*SYSTEM_ROLE_PROVIDER_ID;
    let providers = [provider];
    let filter = CatalogListRolesByIdFilter::builder()
        .provider_ids(Some(&providers))
        .build();
    let mut roles = list_roles(
        Some(project_id),
        filter,
        PaginationQuery {
            page_size: Some(100),
            page_token: PageToken::Empty,
        },
        pool,
    )
    .await
    .unwrap()
    .roles;
    roles.sort_by(|a, b| {
        a.ident
            .source_id()
            .as_str()
            .cmp(b.ident.source_id().as_str())
    });
    roles
        .into_iter()
        .map(|r| {
            (
                r.ident.source_id().as_str().to_string(),
                r.name.clone(),
                r.description.clone(),
                *r.version,
            )
        })
        .collect()
}

#[sqlx::test]
async fn test_upsert_system_roles_in_all_projects_inserts_then_refreshes(pool: PgPool) {
    migrate_core_only(&pool).await.unwrap();
    let state = CatalogState::from_pools(pool.clone(), pool.clone());

    // Three projects, none with system roles yet.
    let p1 = ProjectId::new_random();
    let p2 = ProjectId::new_random();
    let p3 = ProjectId::new_random();
    for pid in &[&p1, &p2, &p3] {
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        lakekeeper_storage_postgres::warehouse::create_project(
            pid,
            format!("Project {pid}"),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
    }
    for pid in &[&p1, &p2, &p3] {
        assert!(list_system_roles(&pool, pid).await.is_empty());
    }

    // First backfill: all three projects get both specs.
    let specs = vec![
        spec("admin_role", "Admin", "Admin description"),
        spec("user_role", "User", "User description"),
    ];
    upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &specs)
        .await
        .unwrap();

    for pid in &[&p1, &p2, &p3] {
        let rows = list_system_roles(&pool, pid).await;
        assert_eq!(rows.len(), 2, "project {pid} should have 2 system roles");
        assert_eq!(rows[0].0, "admin_role");
        assert_eq!(rows[0].1, "Admin");
        assert_eq!(rows[0].2.as_deref(), Some("Admin description"));
        assert_eq!(rows[0].3, 0);
        assert_eq!(rows[1].0, "user_role");
    }

    // Second backfill with the SAME specs is a no-op via IS DISTINCT FROM —
    // no row's version bumps.
    upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &specs)
        .await
        .unwrap();
    for pid in &[&p1, &p2, &p3] {
        let rows = list_system_roles(&pool, pid).await;
        for row in &rows {
            assert_eq!(row.3, 0, "version must not bump on no-op upsert");
        }
    }

    // Third backfill with an updated description for one spec refreshes every
    // project's matching row; the other spec stays unchanged.
    let refreshed_specs = vec![
        spec("admin_role", "Admin", "Updated admin description"),
        spec("user_role", "User", "User description"),
    ];
    upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &refreshed_specs)
        .await
        .unwrap();
    for pid in &[&p1, &p2, &p3] {
        let rows = list_system_roles(&pool, pid).await;
        assert_eq!(rows.len(), 2);
        // admin_role got new description, version bumps
        assert_eq!(rows[0].0, "admin_role");
        assert_eq!(rows[0].2.as_deref(), Some("Updated admin description"));
        assert_eq!(rows[0].3, 1, "admin version bumps after description change");
        // user_role unchanged, version stays at 0
        assert_eq!(rows[1].0, "user_role");
        assert_eq!(rows[1].2.as_deref(), Some("User description"));
        assert_eq!(rows[1].3, 0, "user version unchanged");
    }
}

#[sqlx::test]
async fn test_upsert_system_roles_in_all_projects_with_empty_specs_is_noop(pool: PgPool) {
    migrate_core_only(&pool).await.unwrap();
    let state = CatalogState::from_pools(pool.clone(), pool.clone());

    let p1 = ProjectId::new_random();
    let mut t = PostgresTransaction::begin_write(state.clone())
        .await
        .unwrap();
    lakekeeper_storage_postgres::warehouse::create_project(
        &p1,
        "Empty-Backfill".to_string(),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    // Empty specs — no rows inserted, no error.
    upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &[])
        .await
        .unwrap();
    assert!(list_system_roles(&pool, &p1).await.is_empty());
}

/// Locks in the OSS no-impact contract: `run_post_migration_hooks` with an
/// empty `Vec` returns `Ok` AND seeds no system-role rows in any existing
/// project.
#[sqlx::test]
async fn test_run_post_migration_hooks_oss_no_registry_is_ok(pool: PgPool) {
    migrate_core_only(&pool).await.unwrap();
    let state = CatalogState::from_pools(pool.clone(), pool.clone());

    let p1 = ProjectId::new_random();
    let mut t = PostgresTransaction::begin_write(state.clone())
        .await
        .unwrap();
    lakekeeper_storage_postgres::warehouse::create_project(
        &p1,
        "OSS-NoOp".to_string(),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    run_post_migration_hooks::<PostgresBackend>(state, Vec::new())
        .await
        .unwrap();

    assert!(
        list_system_roles(&pool, &p1).await.is_empty(),
        "OSS path must not create any system-role rows"
    );
}

/// Backfill is now fatal: a failure inside
/// `upsert_system_roles_in_all_projects` propagates via `?`. We inject the
/// failure by closing the pool before the helper starts, which causes the
/// first `begin_write` to return `PoolClosed`.
#[sqlx::test]
async fn test_upsert_system_roles_in_all_projects_propagates_errors(pool: PgPool) {
    migrate_core_only(&pool).await.unwrap();
    let state = CatalogState::from_pools(pool.clone(), pool.clone());
    pool.close().await;

    let specs = vec![spec("svc_admin", "Service Admin", "X")];
    let result = upsert_system_roles_in_all_projects::<PostgresBackend>(state, &specs).await;
    assert!(
        result.is_err(),
        "closed pool must propagate as Err, got: {result:?}"
    );
}
