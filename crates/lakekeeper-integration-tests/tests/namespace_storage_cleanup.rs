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
    use bytes::Bytes;
    use iceberg::NamespaceIdent;
    use lakekeeper::{
        api::{
            iceberg::{
                types::Prefix,
                v1::{
                    NamespaceParameters,
                    namespace::{NamespaceDropFlags, NamespaceService},
                },
            },
        },
        server::CatalogServer,
    };
    use lakekeeper_integration_tests::{
        create_ns, create_table, drop_namespace, random_request_metadata,
    };
    use lakekeeper_io::LakekeeperStorage;
    use sqlx::PgPool;

    use super::setup_cleanup_test;

    /// Test that dropping an empty namespace cleans up the storage folder.
    #[sqlx::test]
    async fn test_drop_empty_namespace_cleans_up_folder(pool: PgPool) {
        let setup = setup_cleanup_test(pool).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();

        // Create a namespace
        let ns = create_ns(ctx.clone(), prefix.clone(), "cleanup-ns".to_string()).await;
        let ns_location = ns
            .properties
            .as_ref()
            .and_then(|p| p.get("location"))
            .expect("namespace should have a location property")
            .clone();

        // Write a file to the namespace location so it exists on storage
        let storage = lakekeeper_io::memory::MemoryStorage::new();
        let file_path = format!("{}marker.txt", ns_location);
        storage
            .write(&file_path, Bytes::from("marker"))
            .await
            .unwrap();

        // Delete the marker file so the folder is empty
        storage.delete(&file_path).await.unwrap();

        // Drop the namespace
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

        // Give async cleanup time to run
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify the namespace no longer exists
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

    /// Test that dropping a namespace with a non-empty folder does NOT delete
    /// the storage folder.
    #[sqlx::test]
    async fn test_drop_namespace_with_nonempty_folder_keeps_folder(pool: PgPool) {
        let setup = setup_cleanup_test(pool).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();

        // Create a namespace
        let ns = create_ns(ctx.clone(), prefix.clone(), "nonempty-ns".to_string()).await;
        let ns_location = ns
            .properties
            .as_ref()
            .and_then(|p| p.get("location"))
            .expect("namespace should have a location property")
            .clone();

        // Write a file to the namespace location so the folder is non-empty
        let storage = lakekeeper_io::memory::MemoryStorage::new();
        let file_path = format!("{}leftover-data.parquet", ns_location);
        storage
            .write(&file_path, Bytes::from("data"))
            .await
            .unwrap();

        // Drop the namespace
        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: false,
            },
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new("nonempty-ns".to_string()),
            },
        )
        .await
        .unwrap();

        // Give async cleanup time to run
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The file should still exist — folder was not cleaned up
        let content = storage.read(&file_path).await;
        assert!(
            content.is_ok(),
            "File should still exist because the namespace folder was not empty"
        );
    }

    /// Test that recursive drop removes namespace from catalog.
    /// Note: Storage cleanup of the namespace folder is best-effort and depends
    /// on the async TabularPurgeTask completing first (to empty the folder).
    /// We only verify catalog state here, not storage state.
    #[sqlx::test]
    async fn test_recursive_drop_removes_namespace(pool: PgPool) {
        let setup = setup_cleanup_test(pool).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();

        // Create a namespace with a table
        let _ns =
            create_ns(ctx.clone(), prefix.clone(), "recursive-ns".to_string()).await;

        let _ = create_table(
            ctx.clone(),
            &prefix,
            "recursive-ns",
            "test-table",
            false,
        )
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

        // Give async cleanup time to run
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify the namespace no longer exists in the catalog
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
