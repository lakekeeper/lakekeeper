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
    use iceberg_ext::catalog::rest::CreateNamespaceRequest;
    use lakekeeper::{
        api::{
            RequestMetadata,
            iceberg::{
                types::{PageToken, Prefix},
                v1::{
                    ListTablesQuery, NamespaceParameters,
                    namespace::{NamespaceDropFlags, NamespaceService},
                    tables::TablesService,
                    views::ViewService,
                },
            },
            management::v1::{
                ApiServer, namespace::NamespaceManagementService,
                table::TableManagementService as _, view::ViewManagementService as _,
                warehouse::TabularDeleteProfile,
            },
        },
        server::CatalogServer,
        service::{ListNamespacesQuery, NamespaceId, TableId},
    };
    use lakekeeper_integration_tests::{
        create_ns, create_table, drop_namespace, random_request_metadata,
    };
    use sqlx::PgPool;

    use super::setup_drop_test;

    #[sqlx::test]
    async fn test_recursive_drop_drops(pool: PgPool) {
        let setup = setup_drop_test(pool, 1, 1, 2, TabularDeleteProfile::Hard {}).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let ns_names = setup.namespace_names;
        let ns1_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: NamespaceIdent::new(ns_names[0].clone()),
        };
        let tables = CatalogServer::list_tables(
            ns1_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                return_uuids: false,
                return_protection_status: false,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(tables.identifiers.len(), 1);
        assert_eq!(tables.identifiers[0].name, "tab0");

        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            ns1_params.clone(),
        )
        .await
        .unwrap();

        // Give the async event handler time to run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let e = CatalogServer::list_tables(
            ns1_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                return_uuids: false,
                return_protection_status: false,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 404);

        let e = CatalogServer::namespace_exists(ns1_params, ctx.clone(), random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(e.error.code, 404);

        CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                namespace: NamespaceIdent::new(ns_names[1].clone()),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_recursive_drop_with_nested_ns(pool: PgPool) {
        let setup = setup_drop_test(pool, 0, 0, 0, TabularDeleteProfile::Hard {}).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();
        let ns1 = create_ns(ctx.clone(), prefix.clone(), "ns1".to_string()).await;

        let ns2 = CatalogServer::create_namespace(
            Some(Prefix(prefix.clone())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns2".to_string()])
                    .unwrap(),
                properties: None,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let ns3 = CatalogServer::create_namespace(
            Some(Prefix(prefix.clone())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::from_vec(vec![
                    "ns1".to_string(),
                    "ns2".to_string(),
                    "ns3".to_string(),
                ])
                .unwrap(),
                properties: None,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: ns3.namespace.clone(),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: ns2.namespace.clone(),
            },
        )
        .await
        .unwrap();

        let e = CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: ns3.namespace.clone(),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 404);

        let e = CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: ns2.namespace.clone(),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 404);

        CatalogServer::namespace_exists(
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: ns1.namespace.clone(),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_recursive_drop_with_soft_delete(pool: PgPool) {
        let setup = setup_drop_test(
            pool,
            1,
            1,
            1,
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(10),
            },
        )
        .await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new("ns0".to_string()),
        };

        let e = drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            ns_params.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 400);

        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: true,
                purge: true,
                recursive: true,
            },
            ns_params.clone(),
        )
        .await
        .unwrap();

        let e = CatalogServer::namespace_exists(ns_params, ctx.clone(), random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(e.error.code, 404);
    }

    #[sqlx::test]
    async fn test_cannot_recursive_drop_namespace_with_protected_view(pool: PgPool) {
        let setup = setup_drop_test(pool, 1, 1, 1, TabularDeleteProfile::Hard {}).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new("ns0".to_string()),
        };
        let views = CatalogServer::list_views(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                return_uuids: true,
                return_protection_status: false,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        ApiServer::set_view_protection(
            (*views.table_uuids.clone().unwrap().first().unwrap()).into(),
            warehouse.warehouse_id,
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            ns_params.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 409, "{}", e.error);

        ApiServer::set_view_protection(
            (*views.table_uuids.as_deref().unwrap().first().unwrap()).into(),
            warehouse.warehouse_id,
            false,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            ns_params.clone(),
        )
        .await
        .unwrap();

        let e = CatalogServer::namespace_exists(ns_params, ctx.clone(), random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(e.error.code, 404);
    }

    #[sqlx::test]
    async fn test_cannot_recursive_drop_namespace_with_protected_namespace(pool: PgPool) {
        let setup = setup_drop_test(
            pool,
            1,
            1,
            1,
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(10),
            },
        )
        .await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();
        let root_ns = NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new("ns0".to_string()),
        };

        let _ = CatalogServer::create_namespace(
            Some(Prefix(prefix.clone())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::from_vec(vec!["ns0".to_string(), "ns1".to_string()])
                    .unwrap(),
                properties: None,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let ns_id = NamespaceId::from(
            *CatalogServer::list_namespaces(
                Some(Prefix(warehouse.warehouse_id.to_string())),
                ListNamespacesQuery {
                    page_token: PageToken::NotSpecified,
                    page_size: Some(1),
                    parent: Some(root_ns.namespace.clone()),
                    return_uuids: true,
                    return_protection_status: true,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap()
            .namespace_uuids
            .unwrap()
            .first()
            .unwrap(),
        );

        ApiServer::set_namespace_protection(
            ns_id,
            warehouse.warehouse_id,
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            root_ns.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 400, "{}", e.error);

        ApiServer::set_namespace_protection(
            ns_id,
            warehouse.warehouse_id,
            false,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: true,
                purge: true,
                recursive: true,
            },
            root_ns.clone(),
        )
        .await
        .unwrap();

        let e = CatalogServer::namespace_exists(root_ns, ctx.clone(), random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(e.error.code, 404);
    }

    #[sqlx::test]
    async fn test_cannot_recursive_drop_namespace_with_protected_table(pool: PgPool) {
        let setup = setup_drop_test(pool, 1, 1, 1, TabularDeleteProfile::Hard {}).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new("ns0".to_string()),
        };
        let tables = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                return_uuids: true,
                return_protection_status: false,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        ApiServer::set_table_protection(
            TableId::from(*tables.table_uuids.as_deref().unwrap().first().unwrap()),
            warehouse.warehouse_id,
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            ns_params.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.error.code, 409, "{}", e.error);

        ApiServer::set_table_protection(
            TableId::from(*tables.table_uuids.as_deref().unwrap().first().unwrap()),
            warehouse.warehouse_id,
            false,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: true,
            },
            ns_params.clone(),
        )
        .await
        .unwrap();

        let e = CatalogServer::namespace_exists(ns_params, ctx.clone(), random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(e.error.code, 404);
    }

    #[sqlx::test]
    async fn test_can_drop_namespace_with_staged_tabulars(pool: PgPool) {
        let setup = setup_drop_test(pool, 0, 0, 1, TabularDeleteProfile::Hard {}).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let prefix = warehouse.warehouse_id.to_string();

        let _ = create_table(
            ctx.clone(),
            &prefix,
            setup.namespace_names[0].clone(),
            "tab0",
            true,
        )
        .await
        .unwrap();
        let tables = CatalogServer::list_tables(
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new(setup.namespace_names[0].clone()),
            },
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                return_uuids: true,
                return_protection_status: false,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        // staged tables are not included in the list
        assert_eq!(tables.identifiers.len(), 0);

        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(setup.namespace_names[0].clone()),
        };
        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force: false,
                purge: true,
                recursive: false,
            },
            ns_params.clone(),
        )
        .await
        .unwrap();

        let e = CatalogServer::namespace_exists(ns_params, ctx.clone(), random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(e.error.code, 404);
    }
}

struct DropSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
    warehouse: TestWarehouseResponse,
    namespace_names: Vec<String>,
}

async fn setup_drop_test(
    pool: PgPool,
    n_tabs: usize,
    n_views: usize,
    n_namespaces: usize,
    delete_profile: TabularDeleteProfile,
) -> DropSetup {
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
        delete_profile,
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
        None,
    )
    .await;
    let mut ns_names = Vec::new();
    for ns in 0..n_namespaces {
        let ns_name = format!("ns{ns}");

        let _ = lakekeeper_integration_tests::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            ns_name.clone(),
        )
        .await;
        for i in 0..n_tabs {
            let tab_name = format!("tab{i}");

            let _ = lakekeeper_integration_tests::create_table(
                ctx.clone(),
                &warehouse.warehouse_id.to_string(),
                &ns_name,
                &tab_name,
                false,
            )
            .await
            .unwrap();
        }

        for i in 0..n_views {
            let view_name = format!("view{i}");
            lakekeeper_integration_tests::create_view(
                ctx.clone(),
                &warehouse.warehouse_id.to_string(),
                &ns_name,
                &view_name,
                None,
            )
            .await
            .unwrap();
        }
        ns_names.push(ns_name);
    }

    DropSetup {
        ctx,
        warehouse,
        namespace_names: ns_names,
    }
}

mod recursive_purge {
    use std::{collections::HashMap, time::Duration};

    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::NamespaceIdent;
    use lakekeeper::{
        api::{
            data::v1::generic_tables::{CreateGenericTableRequest, GenericTableService as _},
            iceberg::{
                types::Prefix,
                v1::{NamespaceParameters, namespace::NamespaceDropFlags},
            },
            management::v1::warehouse::TabularDeleteProfile,
        },
        server::CatalogServer,
        service::{GenericTableFormat, tasks::tabular_purge_queue},
    };
    use lakekeeper_integration_tests::{
        create_table, drop_namespace, random_request_metadata, spawn_build_in_queues,
    };
    use lakekeeper_io::{LakekeeperStorage, Location, StorageBackend, memory::MemoryStorage};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::setup_drop_test;

    async fn list_all(io: &StorageBackend, location: &Location) -> Vec<String> {
        let mut entries = Vec::new();
        let mut stream = io.list(location.as_str(), None).await.unwrap();
        while let Some(page) = stream.next().await {
            entries.extend(page.unwrap().iter().map(|f| f.location().to_string()));
        }
        entries
    }

    /// Locations of the purge tasks scheduled for `entity_id`, pending or finished.
    async fn purge_locations(pool: &PgPool, entity_id: Uuid) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT task_data->>'tabular_location' FROM task WHERE queue_name = $1 AND entity_id = $2 \
             UNION ALL \
             SELECT task_data->>'tabular_location' FROM task_log WHERE queue_name = $1 AND entity_id = $2",
        )
        .bind(tabular_purge_queue::QUEUE_NAME.as_str())
        .bind(entity_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    pub(super) async fn tabular_id(pool: &PgPool, name: &str) -> Uuid {
        sqlx::query_scalar("SELECT tabular_id FROM tabular WHERE name = $1")
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A recursive drop with `purge` removes the files of the tables and the generic tables in
    /// the namespace.
    async fn recursive_purge_removes_all_tabular_files(
        pool: PgPool,
        delete_profile: TabularDeleteProfile,
        force: bool,
    ) {
        let setup = setup_drop_test(pool.clone(), 0, 0, 1, delete_profile).await;
        let ctx = setup.ctx;
        let prefix = setup.warehouse.warehouse_id.to_string();
        let ns_name = setup.namespace_names[0].clone();
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        };
        let cancellation_token = lakekeeper::CancellationToken::new();
        let _queues = spawn_build_in_queues(
            &ctx,
            Some(Duration::from_millis(100)),
            cancellation_token.clone(),
        )
        .await;
        let io = StorageBackend::Memory(MemoryStorage::new());

        let table = create_table(ctx.clone(), &prefix, &ns_name, "tab", false)
            .await
            .unwrap();
        let table_location: Location = table.metadata.location().parse().unwrap();
        let generic_table = CatalogServer::create_generic_table(
            ns_params.clone(),
            CreateGenericTableRequest {
                name: "gt".to_string(),
                format: GenericTableFormat::Unknown("lance".to_string()),
                base_location: None,
                doc: None,
                properties: HashMap::default(),
                schema: None,
                statistics: None,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        let generic_table_location: Location = generic_table.table.base_location.parse().unwrap();
        for location in [&table_location, &generic_table_location] {
            io.write(
                location.cloning_push("data/file.bin").as_str(),
                Bytes::from_static(b"data"),
            )
            .await
            .unwrap();
            assert!(!list_all(&io, location).await.is_empty(), "{location}");
        }
        let table_id = tabular_id(&pool, "tab").await;
        let generic_table_id = tabular_id(&pool, "gt").await;

        drop_namespace(
            ctx.clone(),
            NamespaceDropFlags {
                force,
                purge: true,
                recursive: true,
            },
            ns_params,
        )
        .await
        .unwrap();

        assert_eq!(
            purge_locations(&pool, table_id).await,
            vec![table_location.to_string()]
        );
        assert_eq!(
            purge_locations(&pool, generic_table_id).await,
            vec![generic_table_location.to_string()]
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let table_files = list_all(&io, &table_location).await;
            let generic_table_files = list_all(&io, &generic_table_location).await;
            if table_files.is_empty() && generic_table_files.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "files left after the purge: {table_files:?} {generic_table_files:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        cancellation_token.cancel();
    }

    #[sqlx::test]
    async fn test_recursive_purge_hard_deletion(pool: PgPool) {
        recursive_purge_removes_all_tabular_files(pool, TabularDeleteProfile::Hard {}, false).await;
    }

    #[sqlx::test]
    async fn test_recursive_purge_soft_deletion_force(pool: PgPool) {
        recursive_purge_removes_all_tabular_files(
            pool,
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(3600),
            },
            true,
        )
        .await;
    }
}

/// A purge removes the location of a dropped tabular after the dropped row is gone. Until the
/// purge has finished, no other tabular can take that location or one that overlaps it.
mod purge_after_takeover {
    use std::{collections::HashMap, time::Duration};

    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::{LoadTableResult, RegisterTableRequest};
    use lakekeeper::{
        api::{
            ApiContext, Result,
            data::v1::generic_tables::{CreateGenericTableRequest, GenericTableService as _},
            iceberg::{
                types::Prefix,
                v1::{DataAccess, NamespaceParameters, tables::TablesService as _},
            },
            management::v1::warehouse::TabularDeleteProfile,
        },
        server::CatalogServer,
        service::{
            CatalogWarehouseOps, GenericTableFormat, State, WarehouseId, WarehouseStatus,
            authz::AllowAllAuthorizer, tasks::tabular_purge_queue,
        },
    };
    use lakekeeper_integration_tests::{
        create_table_request, drop_table, eventually, memory_files_under, put_memory_file,
        random_request_metadata, spawn_build_in_queues,
    };
    use lakekeeper_io::Location;
    use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::{recursive_purge::tabular_id, setup_drop_test};

    type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

    /// A warehouse with one namespace.
    struct Catalog {
        pool: PgPool,
        ctx: Ctx,
        warehouse_id: WarehouseId,
        ns_name: String,
    }

    /// The table `tab`, created with a data file and dropped with `purge`.
    struct DroppedTable {
        id: Uuid,
        location: Location,
        metadata_location: String,
    }

    impl Catalog {
        async fn new(pool: PgPool) -> Self {
            let setup = setup_drop_test(pool.clone(), 0, 0, 1, TabularDeleteProfile::Hard {}).await;
            Self {
                pool,
                ctx: setup.ctx,
                warehouse_id: setup.warehouse.warehouse_id,
                ns_name: setup.namespace_names[0].clone(),
            }
        }

        fn ns_params(&self) -> NamespaceParameters {
            NamespaceParameters {
                prefix: Some(Prefix(self.warehouse_id.to_string())),
                namespace: NamespaceIdent::new(self.ns_name.clone()),
            }
        }

        /// No tabular can use this location itself, only locations inside it.
        async fn base_location(&self) -> Location {
            PostgresBackend::get_warehouse_by_id(
                self.warehouse_id,
                WarehouseStatus::active(),
                self.ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap()
            .storage_profile
            .base_location()
            .unwrap()
        }

        async fn create_table(
            &self,
            name: &str,
            location: Option<&Location>,
        ) -> Result<LoadTableResult> {
            let mut request = create_table_request(Some(name.to_string()), Some(false));
            request.location = location.map(ToString::to_string);
            CatalogServer::create_table(
                self.ns_params(),
                request,
                DataAccess::not_specified(),
                self.ctx.clone(),
                random_request_metadata(),
            )
            .await
        }

        async fn register_table(&self, name: &str, metadata_location: &str) -> Result<()> {
            CatalogServer::register_table(
                self.ns_params(),
                RegisterTableRequest::builder()
                    .name(name.to_string())
                    .metadata_location(metadata_location.to_string())
                    .build(),
                DataAccess::not_specified(),
                self.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .map(|_| ())
        }

        async fn create_generic_table(&self, name: &str, location: &Location) -> Result<()> {
            CatalogServer::create_generic_table(
                self.ns_params(),
                CreateGenericTableRequest {
                    name: name.to_string(),
                    format: GenericTableFormat::Unknown("lance".to_string()),
                    base_location: Some(location.to_string()),
                    doc: None,
                    properties: HashMap::default(),
                    schema: None,
                    statistics: None,
                },
                self.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .map(|_| ())
        }

        /// Creates the table `tab` with a data file, and drops it with `purge`, which queues
        /// its purge task.
        async fn drop_table_with_purge(&self, location: Option<&Location>) -> DroppedTable {
            let table = self.create_table("tab", location).await.unwrap();
            let location: Location = table.metadata.location().parse().unwrap();
            put_memory_file(location.cloning_push("data/file.bin").as_str()).await;
            let id = tabular_id(&self.pool, "tab").await;
            drop_table(
                self.ctx.clone(),
                &self.warehouse_id.to_string(),
                &self.ns_name,
                "tab",
                Some(true),
                false,
            )
            .await
            .unwrap();
            DroppedTable {
                id,
                location,
                metadata_location: table.metadata_location.unwrap(),
            }
        }

        /// Runs the built-in queues until the purge task of `dropped_id` has succeeded.
        async fn run_purge(&self, dropped_id: Uuid) {
            let cancellation_token = lakekeeper::CancellationToken::new();
            let queues = spawn_build_in_queues(
                &self.ctx,
                Some(Duration::from_millis(100)),
                cancellation_token.clone(),
            )
            .await;
            eventually("the purge task succeeds", || async {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (SELECT 1 FROM task_log WHERE queue_name = $1 \
                     AND entity_id = $2 AND status = 'success')",
                )
                .bind(tabular_purge_queue::QUEUE_NAME.as_str())
                .bind(dropped_id)
                .fetch_one(&self.pool)
                .await
                .unwrap()
            })
            .await;
            cancellation_token.cancel();
            queues.await.unwrap();
        }
    }

    /// Asserts that `result` is the conflict for `location` while a purge is pending.
    fn assert_purge_pending<T: std::fmt::Debug>(result: Result<T>, location: &Location) {
        let error = result
            .expect_err("the location is free while its purge is pending")
            .error;
        assert_eq!(
            (error.code, error.r#type.as_str(), error.message.as_str()),
            (
                409,
                "LocationAlreadyTaken",
                format!(
                    "Location '{location}' overlaps the location of a dropped table, view or \
                     generic table whose files a purge task will delete. The location is free \
                     again when that task has finished."
                )
                .as_str()
            )
        );
    }

    /// Registering the dropped table's own metadata file would adopt its location and id.
    #[sqlx::test]
    async fn test_register_from_the_dropped_metadata_waits_for_the_purge(pool: PgPool) {
        let catalog = Catalog::new(pool).await;
        let dropped = catalog.drop_table_with_purge(None).await;
        assert_purge_pending(
            catalog
                .register_table("tab", &dropped.metadata_location)
                .await,
            &dropped.location,
        );

        catalog.run_purge(dropped.id).await;
        assert_eq!(
            memory_files_under(dropped.location.as_str()).await,
            Vec::<String>::new()
        );
    }

    /// The inner location is empty, so only the pending purge stops the create. After the
    /// purge, the location is free.
    #[sqlx::test]
    async fn test_create_inside_the_location_waits_for_the_purge(pool: PgPool) {
        let catalog = Catalog::new(pool).await;
        let dropped = catalog.drop_table_with_purge(None).await;
        let inner_location = dropped.location.cloning_push("inner");
        assert_purge_pending(
            catalog.create_table("inner", Some(&inner_location)).await,
            &inner_location,
        );

        catalog.run_purge(dropped.id).await;
        let inner = catalog
            .create_table("inner", Some(&inner_location))
            .await
            .unwrap();
        assert_eq!(
            memory_files_under(dropped.location.as_str()).await,
            vec![inner.metadata_location.unwrap()]
        );
    }

    /// The parent folder holds the dropped table's files, so a generic table created there
    /// needs `RegisterTable` on the warehouse.
    #[sqlx::test]
    async fn test_generic_table_at_a_parent_location_waits_for_the_purge(pool: PgPool) {
        let catalog = Catalog::new(pool).await;
        let parent = catalog.base_location().await.cloning_push("p");
        let dropped = catalog
            .drop_table_with_purge(Some(&parent.cloning_push("tab")))
            .await;
        assert_purge_pending(catalog.create_generic_table("gt", &parent).await, &parent);

        catalog.run_purge(dropped.id).await;
        catalog.create_generic_table("gt", &parent).await.unwrap();
    }
}
