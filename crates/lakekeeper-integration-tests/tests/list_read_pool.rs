//! List endpoints with more concurrent requests than read connections, and an authorizer
//! that takes a read connection in every check.

use std::time::Duration;

use futures::{FutureExt, future::join_all};
use iceberg::{NamespaceIdent, TableIdent};
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::{PageToken, Prefix},
            v1::{
                DataAccess, ListTablesQuery, NamespaceParameters, namespace::NamespaceService,
                tables::TablesService, views::ViewService,
            },
        },
        management::v1::{
            ApiServer,
            warehouse::{ListDeletedTabularsQuery, Service as _, TabularDeleteProfile},
        },
    },
    server::{CatalogServer, NAMESPACE_ID_PROPERTY},
    service::{ListNamespacesQuery, State, UserId, authz::tests::HidingAuthorizer},
};
use lakekeeper_integration_tests::{
    TestWarehouseResponse, create_ns, create_table, create_view_request, drop_table,
    memory_io_profile, random_request_metadata, setup_simple,
};
use lakekeeper_storage_postgres::{CatalogState, PostgresBackend, SecretsState};
use sqlx::{PgPool, postgres::PgPoolOptions};

const READ_CONNECTIONS: u32 = 2;
const CONCURRENT_LISTS: usize = 8;

type Ctx = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;

/// A catalog whose read pool is also the one the authorizer reads from during checks.
/// `can_list_everything` is blocked, so every listed object is checked.
async fn setup_with_small_read_pool(
    pool: PgPool,
    delete_profile: TabularDeleteProfile,
) -> (Ctx, HidingAuthorizer, TestWarehouseResponse) {
    let read_pool = PgPoolOptions::new()
        .max_connections(READ_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let hook_pool = read_pool.clone();
    let authz = HidingAuthorizer::new().with_check_hook(move || {
        let pool = hook_pool.clone();
        async move {
            sqlx::query("SELECT pg_sleep(0.01)")
                .execute(&pool)
                .await
                .unwrap();
        }
        .boxed()
    });
    authz.block_can_list_everything();

    let (mut ctx, warehouse) = setup_simple(
        pool.clone(),
        memory_io_profile(),
        None,
        authz.clone(),
        delete_profile,
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    ctx.v1_state.catalog = CatalogState::from_pools(read_pool, pool);
    (ctx, authz, warehouse)
}

fn ns1_params(warehouse: &TestWarehouseResponse) -> NamespaceParameters {
    NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: NamespaceIdent::new("ns1".to_string()),
    }
}

// Five per page against two visible objects: the first page is short with one object
// filtered out, so each request also fetches and authorizes a second, empty page.
fn list_tables_query() -> ListTablesQuery {
    ListTablesQuery {
        page_token: PageToken::NotSpecified,
        page_size: Some(5),
        return_uuids: false,
        return_protection_status: false,
    }
}

#[sqlx::test]
async fn list_namespaces_with_more_requests_than_read_connections(pool: PgPool) {
    let (ctx, authz, warehouse) =
        setup_with_small_read_pool(pool, TabularDeleteProfile::Hard {}).await;
    let prefix = warehouse.warehouse_id.to_string();
    for name in ["0", "1", "2"] {
        let ns = create_ns(ctx.clone(), prefix.clone(), name.to_string()).await;
        if name == "1" {
            let id = &ns.properties.unwrap()[NAMESPACE_ID_PROPERTY];
            authz.hide(&format!("namespace:{id}"));
        }
    }

    let responses = join_all((0..CONCURRENT_LISTS).map(|_| {
        CatalogServer::list_namespaces(
            Some(Prefix(prefix.clone())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            ctx.clone(),
            random_request_metadata(),
        )
    }))
    .await;

    for response in responses {
        let response = response.unwrap();
        assert_eq!(
            *response.namespaces,
            vec![
                NamespaceIdent::new("0".to_string()),
                NamespaceIdent::new("2".to_string())
            ]
        );
        assert_eq!(response.next_page_token, None);
    }
}

#[sqlx::test]
async fn list_tables_with_more_requests_than_read_connections(pool: PgPool) {
    let (ctx, authz, warehouse) =
        setup_with_small_read_pool(pool, TabularDeleteProfile::Hard {}).await;
    let prefix = warehouse.warehouse_id.to_string();
    create_ns(ctx.clone(), prefix.clone(), "ns1".to_string()).await;
    for name in ["0", "1", "2"] {
        let table = create_table(ctx.clone(), &prefix, "ns1", name, false)
            .await
            .unwrap();
        if name == "1" {
            authz.hide(&format!("table:{prefix}/{}", table.metadata.uuid()));
        }
    }

    let responses = join_all((0..CONCURRENT_LISTS).map(|_| {
        CatalogServer::list_tables(
            ns1_params(&warehouse),
            list_tables_query(),
            ctx.clone(),
            random_request_metadata(),
        )
    }))
    .await;

    let ns1 = NamespaceIdent::new("ns1".to_string());
    for response in responses {
        let response = response.unwrap();
        assert_eq!(
            *response.identifiers,
            vec![
                TableIdent::new(ns1.clone(), "0".to_string()),
                TableIdent::new(ns1.clone(), "2".to_string())
            ]
        );
        assert_eq!(response.next_page_token, None);
    }
}

#[sqlx::test]
async fn list_views_with_more_requests_than_read_connections(pool: PgPool) {
    let (ctx, authz, warehouse) =
        setup_with_small_read_pool(pool, TabularDeleteProfile::Hard {}).await;
    let prefix = warehouse.warehouse_id.to_string();
    create_ns(ctx.clone(), prefix.clone(), "ns1".to_string()).await;
    for name in ["0", "1", "2"] {
        let view = CatalogServer::create_view(
            ns1_params(&warehouse),
            create_view_request(Some(name), None),
            ctx.clone(),
            DataAccess::not_specified(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        if name == "1" {
            authz.hide(&format!("view:{prefix}/{}", view.metadata.uuid()));
        }
    }

    let responses = join_all((0..CONCURRENT_LISTS).map(|_| {
        CatalogServer::list_views(
            ns1_params(&warehouse),
            list_tables_query(),
            ctx.clone(),
            random_request_metadata(),
        )
    }))
    .await;

    let ns1 = NamespaceIdent::new("ns1".to_string());
    for response in responses {
        let response = response.unwrap();
        assert_eq!(
            *response.identifiers,
            vec![
                TableIdent::new(ns1.clone(), "0".to_string()),
                TableIdent::new(ns1.clone(), "2".to_string())
            ]
        );
        assert_eq!(response.next_page_token, None);
    }
}

#[sqlx::test]
async fn list_soft_deleted_tabulars_with_more_requests_than_read_connections(pool: PgPool) {
    let (ctx, authz, warehouse) = setup_with_small_read_pool(
        pool,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(10),
        },
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    create_ns(ctx.clone(), prefix.clone(), "ns1".to_string()).await;
    for name in ["0", "1", "2"] {
        let table = create_table(ctx.clone(), &prefix, "ns1", name, false)
            .await
            .unwrap();
        drop_table(ctx.clone(), &prefix, "ns1", name, None, false)
            .await
            .unwrap();
        if name == "1" {
            authz.hide(&format!("table:{prefix}/{}", table.metadata.uuid()));
        }
    }

    let responses = join_all((0..CONCURRENT_LISTS).map(|_| {
        ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_token: None,
                page_size: Some(5),
            },
            ctx.clone(),
            random_request_metadata(),
        )
    }))
    .await;

    for response in responses {
        let response = response.unwrap();
        let names = response
            .tabulars
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["0", "2"]);
        assert_eq!(response.next_page_token, None);
    }
}
