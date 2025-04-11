use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{management::v1::warehouse::TabularDeleteProfile, ApiContext},
    implementations::postgres::{PostgresCatalog, SecretsState},
    service::{authz::AllowAllAuthorizer, task_queue::TaskQueueConfig, State, UserId},
    tests::TestWarehouseResponse,
};

mod test {
    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::CreateNamespaceRequest;
    use sqlx::PgPool;

    use crate::{
        api::{
            iceberg::{
                types::{PageToken, Prefix},
                v1::{
                    namespace::{NamespaceDropFlags, NamespaceService},
                    tables::TablesService,
                    views::ViewService,
                    ListTablesQuery, NamespaceParameters,
                },
            },
            management::v1::{
                namespace::NamespaceManagementService, table::TableManagementService as _,
                view::ViewManagementService as _, warehouse::TabularDeleteProfile, ApiServer,
            },
            RequestMetadata,
        },
        catalog::CatalogServer,
        service::{ListNamespacesQuery, NamespaceIdentUuid, TableIdentUuid},
        tests::{create_ns, drop_recursive::setup_drop_test, random_request_metadata},
    };

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

        super::super::drop_namespace(
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

        super::super::drop_namespace(
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

        let e = super::super::drop_namespace(
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

        super::super::drop_namespace(
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

        let e = super::super::drop_namespace(
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

        super::super::drop_namespace(
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

        let ns_id = NamespaceIdentUuid::from(
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

        let e = super::super::drop_namespace(
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

        super::super::drop_namespace(
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
            TableIdentUuid::from(*tables.table_uuids.as_deref().unwrap().first().unwrap()),
            warehouse.warehouse_id,
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = super::super::drop_namespace(
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
            TableIdentUuid::from(*tables.table_uuids.as_deref().unwrap().first().unwrap()),
            warehouse.warehouse_id,
            false,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        super::super::drop_namespace(
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
}

struct DropSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
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

    let prof = crate::tests::test_io_profile();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer,
        delete_profile,
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        Some(TaskQueueConfig {
            max_retries: 1,
            max_age: chrono::Duration::seconds(60),
            poll_interval: std::time::Duration::from_secs(10),
        }),
        1,
    )
    .await;
    let mut ns_names = Vec::new();
    for ns in 0..n_namespaces {
        let ns_name = format!("ns{ns}");

        let _ = crate::tests::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            ns_name.to_string(),
        )
        .await;
        for i in 0..n_tabs {
            let tab_name = format!("tab{i}");

            let _ = crate::tests::create_table(
                ctx.clone(),
                &warehouse.warehouse_id.to_string(),
                &ns_name,
                &tab_name,
            )
            .await
            .unwrap();
        }

        for i in 0..n_views {
            let view_name = format!("view{i}");
            crate::tests::create_view(
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
