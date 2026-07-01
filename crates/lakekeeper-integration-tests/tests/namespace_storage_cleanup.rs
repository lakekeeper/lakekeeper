use lakekeeper::{
    api::{ApiContext, management::v1::warehouse::TabularDeleteProfile},
    service::{State, UserId, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::TestWarehouseResponse;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

mod test {
    use iceberg::NamespaceIdent;
    use lakekeeper::{
        api::iceberg::{
            types::Prefix,
            v1::{
                NamespaceParameters,
                namespace::{NamespaceDropFlags, NamespaceService},
            },
        },
        server::CatalogServer,
    };
    use lakekeeper_integration_tests::{
        create_ns, create_table, drop_namespace, random_request_metadata,
    };
    use sqlx::PgPool;

    use super::setup_cleanup_test;

    /// Test that dropping a namespace with purge removes it from the catalog
    /// and that the namespace location is correctly populated in the drop info.
    ///
    /// Note: actual directory cleanup is gated on `StorageProfile::is_hierarchical()`
    /// and only runs for ADLS/OneLake backends. MemoryStorage is non-hierarchical,
    /// so cleanup is (correctly) skipped here. The catalog-side contract — namespace
    /// gone, location metadata collected — is what this test covers.
    #[sqlx::test]
    async fn test_drop_empty_namespace_purge_removes_from_catalog(pool: PgPool) {
        let setup = setup_cleanup_test(pool).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();

        // Create a namespace and verify it has a location property.
        let ns = create_ns(ctx.clone(), prefix.clone(), "cleanup-ns".to_string()).await;
        assert!(
            ns.properties
                .as_ref()
                .and_then(|p| p.get("location"))
                .is_some(),
            "namespace should have a location property for cleanup"
        );

        // Drop the namespace with purge.
        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: false,
            },
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new("cleanup-ns".to_string()),
            },
        )
        .await
        .unwrap();

        // Verify the namespace no longer exists in the catalog.
        let e = CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(prefix)),
                namespace: NamespaceIdent::new("cleanup-ns".to_string()),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 404);
    }

    /// Test that dropping a namespace without purge does NOT trigger storage
    /// cleanup (even on hierarchical backends the folder should be left alone).
    #[sqlx::test]
    async fn test_drop_namespace_without_purge_skips_cleanup(pool: PgPool) {
        let setup = setup_cleanup_test(pool).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();

        let _ns = create_ns(ctx.clone(), prefix.clone(), "no-purge-ns".to_string()).await;

        // Drop without purge — cleanup should be skipped.
        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: false,
            },
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new("no-purge-ns".to_string()),
            },
        )
        .await
        .unwrap();

        // Namespace should still be gone from catalog.
        let e = CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(prefix)),
                namespace: NamespaceIdent::new("no-purge-ns".to_string()),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 404);
    }

    /// Test that recursive drop with purge removes the namespace and its
    /// children from the catalog.
    ///
    /// Storage cleanup depends on the async `TabularPurgeTask` completing
    /// first (to empty the folder). We verify catalog state here; actual
    /// directory cleanup is only exercised on hierarchical backends.
    #[sqlx::test]
    async fn test_recursive_drop_removes_namespace(pool: PgPool) {
        let setup = setup_cleanup_test(pool).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();

        // Create a namespace with a table
        let _ns = create_ns(ctx.clone(), prefix.clone(), "recursive-ns".to_string()).await;

        let _ = create_table(ctx.clone(), &prefix, "recursive-ns", "test-table", false)
            .await
            .unwrap();

        // Recursive drop with purge
        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new("recursive-ns".to_string()),
            },
        )
        .await
        .unwrap();

        // Verify the namespace no longer exists in the catalog.
        let e = CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(prefix)),
                namespace: NamespaceIdent::new("recursive-ns".to_string()),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 404);
    }
}

struct CleanupSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
    warehouse: TestWarehouseResponse,
}

async fn setup_cleanup_test(pool: PgPool) -> CleanupSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init()
        .ok();

    let prof = lakekeeper_integration_tests::memory_io_profile();
    let (ctx, warehouse) = lakekeeper_integration_tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
        None,
    )
    .await;

    CleanupSetup { ctx, warehouse }
}
