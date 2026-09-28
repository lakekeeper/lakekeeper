use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use lakekeeper::{
    api::{
        ApiContext,
        iceberg::{
            types::Prefix,
            v1::{
                NamespaceParameters,
                namespace::{NamespaceDropFlags, NamespaceService},
            },
        },
    },
    server::CatalogServer,
    service::{State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{drop_namespace, random_request_metadata};
use lakekeeper_io::Location;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

fn ident(path: &[&str]) -> NamespaceIdent {
    NamespaceIdent::from_strs(path).unwrap()
}

async fn create_ns(ctx: &Ctx, prefix: &str, path: &[&str], location: Option<&str>) -> Location {
    let response = CatalogServer::create_namespace(
        Some(Prefix(prefix.to_string())),
        CreateNamespaceRequest {
            namespace: ident(path),
            properties: location.map(|l| [("location".to_string(), l.to_string())].into()),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    response.properties.unwrap()["location"].parse().unwrap()
}

async fn drop_ns(ctx: &Ctx, prefix: &str, path: &[&str], purge: bool, recursive: bool) {
    drop_namespace(
        ctx.clone(),
        NamespaceDropFlags {
            force: false,
            purge,
            recursive,
        },
        NamespaceParameters {
            prefix: Some(Prefix(prefix.to_string())),
            namespace: ident(path),
        },
    )
    .await
    .unwrap();
}

mod test {
    use lakekeeper::service::storage::{MemoryProfile, storage_layout::StorageLayout};
    use lakekeeper_integration_tests::SetupTestCatalog;
    use sqlx::PgPool;

    use super::*;

    /// Purge drops on storage without directory entities reach the backend and succeed.
    #[sqlx::test]
    async fn test_purge_drop_with_nested_locations_on_memory_storage(pool: PgPool) {
        let mut profile = MemoryProfile::default();
        profile.storage_layout =
            Some(StorageLayout::try_new_full("{name}".to_string(), "{uuid}".to_string()).unwrap());
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool)
            .storage_profile(profile.into())
            .authorizer(AllowAllAuthorizer::default())
            .build()
            .setup()
            .await;
        let prefix = warehouse.warehouse_id.to_string();

        create_ns(&ctx, &prefix, &["parent"], None).await;
        create_ns(&ctx, &prefix, &["parent", "child"], None).await;
        create_ns(&ctx, &prefix, &["leaf"], None).await;

        drop_ns(&ctx, &prefix, &["parent"], true, true).await;
        drop_ns(&ctx, &prefix, &["leaf"], true, false).await;

        for path in [&["parent"][..], &["parent", "child"], &["leaf"]] {
            let err = CatalogServer::namespace_exists(
                NamespaceParameters {
                    prefix: Some(Prefix(prefix.clone())),
                    namespace: ident(path),
                },
                ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 404, "{path:?}");
        }
    }

    mod azure_integration_tests {
        use bytes::Bytes;
        use futures::StreamExt;
        use lakekeeper::{
            api::management::v1::warehouse::TabularDeleteProfile,
            service::storage::{
                AzCredential, GenericAdlsProfile, StorageCredential, StorageProfile,
                storage_layout::StorageLayout,
            },
        };
        use lakekeeper_integration_tests::setup;
        use lakekeeper_io::{LakekeeperStorage, RemoveEmptyDirectoryOutcome, StorageBackend};
        use sqlx::PgPool;

        use super::super::*;

        fn env(key: &str) -> String {
            std::env::var(key).unwrap_or_else(|_| panic!("{key} to be set"))
        }

        fn adls_profile(storage_layout: Option<StorageLayout>) -> StorageProfile {
            GenericAdlsProfile {
                filesystem: env("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM"),
                key_prefix: Some(format!("test-{}", uuid::Uuid::now_v7())),
                account_name: env("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME"),
                authority_host: None,
                host: None,
                sas_token_validity_seconds: None,
                allow_alternative_protocols: false,
                sas_enabled: true,
                storage_layout,
            }
            .into()
        }

        fn adls_credential() -> StorageCredential {
            AzCredential::ClientCredentials {
                client_id: env("LAKEKEEPER_TEST__AZURE_CLIENT_ID"),
                client_secret: env("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET"),
                tenant_id: env("LAKEKEEPER_TEST__AZURE_TENANT_ID"),
            }
            .into()
        }

        async fn setup_adls(
            pool: PgPool,
            storage_layout: Option<StorageLayout>,
        ) -> (Ctx, String, StorageBackend, Location) {
            let profile = adls_profile(storage_layout);
            let credential = adls_credential();
            let (ctx, warehouse) = setup(
                pool,
                profile.clone(),
                Some(credential.clone()),
                AllowAllAuthorizer::default(),
                TabularDeleteProfile::Hard {},
                None,
                1,
                None,
            )
            .await;
            let io = profile.file_io(Some(&credential)).await.unwrap();
            let base = profile.base_location().unwrap();
            (ctx, warehouse.warehouse_id.to_string(), io, base)
        }

        /// Leaves an empty directory at `location`, as a finished table purge does.
        async fn leave_empty_dir(io: &StorageBackend, location: &Location) {
            let marker = location.cloning_push("marker");
            io.write(marker.as_str(), Bytes::from_static(b"marker"))
                .await
                .unwrap();
            io.delete(marker.as_str()).await.unwrap();
        }

        async fn list_all(io: &StorageBackend, location: &Location) -> Vec<String> {
            let mut entries = Vec::new();
            let mut stream = io.list(location.as_str(), None).await.unwrap();
            while let Some(page) = stream.next().await {
                entries.extend(page.unwrap().iter().map(|f| f.location().to_string()));
            }
            entries
        }

        fn dir_exists(entries: &[String], location: &Location) -> bool {
            let mut dir = location.clone();
            dir.with_trailing_slash();
            entries.iter().any(|e| e.starts_with(dir.as_str()))
        }

        #[sqlx::test]
        async fn test_drop_removes_empty_namespace_directories(pool: PgPool) {
            let layout =
                StorageLayout::try_new_full("{name}".to_string(), "{uuid}".to_string()).unwrap();
            let (ctx, prefix, io, base) = setup_adls(pool, Some(layout)).await;

            let parent = create_ns(&ctx, &prefix, &["parent"], None).await;
            let child = create_ns(&ctx, &prefix, &["parent", "child"], None).await;
            let empty = create_ns(&ctx, &prefix, &["empty"], None).await;
            let not_purged = create_ns(&ctx, &prefix, &["not-purged"], None).await;
            let non_empty = create_ns(&ctx, &prefix, &["non-empty"], None).await;
            assert!(child.is_sublocation_of(&parent));

            for location in [&child, &empty, &not_purged] {
                leave_empty_dir(&io, location).await;
            }
            let kept_file = non_empty.cloning_push("kept");
            io.write(kept_file.as_str(), Bytes::from_static(b"kept"))
                .await
                .unwrap();
            let entries = list_all(&io, &base).await;
            for location in [&parent, &child, &empty, &not_purged, &non_empty] {
                assert!(dir_exists(&entries, location), "{location} in {entries:?}");
            }

            // `parent` only holds the empty `child` directory, so it goes once `child` does.
            drop_ns(&ctx, &prefix, &["parent"], true, true).await;
            drop_ns(&ctx, &prefix, &["empty"], true, false).await;
            drop_ns(&ctx, &prefix, &["not-purged"], false, false).await;
            drop_ns(&ctx, &prefix, &["non-empty"], true, false).await;

            let entries = list_all(&io, &base).await;
            for (location, expected) in [
                (&parent, false),
                (&child, false),
                (&empty, false),
                (&not_purged, true),
                (&non_empty, true),
            ] {
                assert_eq!(
                    dir_exists(&entries, location),
                    expected,
                    "{location} in {entries:?}"
                );
            }
            assert_eq!(
                io.read(kept_file.as_str()).await.unwrap(),
                Bytes::from_static(b"kept")
            );

            io.remove_all(base.as_str()).await.unwrap();
        }

        #[sqlx::test]
        async fn test_drop_keeps_empty_warehouse_base(pool: PgPool) {
            let (ctx, prefix, io, base) = setup_adls(pool, None).await;
            // Replace whatever warehouse validation left with an empty base directory.
            io.remove_all(base.as_str()).await.unwrap();
            leave_empty_dir(&io, &base).await;

            // On the default layout a namespace location is the base itself.
            let flat = create_ns(&ctx, &prefix, &["flat"], None).await;
            let mut base_without_slash = base.clone();
            base_without_slash.without_trailing_slash();
            assert_eq!(flat, base_without_slash);
            let doubled_slash = format!("{}//", base_without_slash.as_str());
            create_ns(&ctx, &prefix, &["doubled-slash"], Some(&doubled_slash)).await;

            drop_ns(&ctx, &prefix, &["flat"], true, false).await;
            drop_ns(&ctx, &prefix, &["doubled-slash"], true, false).await;

            // `Removed` proves the base survived the drops, and cleans it up.
            assert_eq!(
                io.remove_empty_directory(base.as_str()).await.unwrap(),
                RemoveEmptyDirectoryOutcome::Removed
            );
        }
    }
}
