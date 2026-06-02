// Extracted from crates/lakekeeper/src/api/management/v1/warehouse/mod.rs (sqlx test section).
// VAK-437 split.

use std::sync::Arc;

use iceberg::TableIdent;
use itertools::Itertools;
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{DataAccess, DropParams, NamespaceParameters, ViewParameters, views::ViewService},
        },
        management::v1::{
            ApiServer,
            warehouse::{ListDeletedTabularsQuery, Service as _, TabularDeleteProfile},
        },
    },
    server::CatalogServer,
    service::{State, UserId, authz::tests::HidingAuthorizer},
};
use lakekeeper_integration_tests::{create_view_request, impl_pagination_tests};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

async fn setup_pagination_test(
    pool: sqlx::PgPool,
    n_tabulars: usize,
    hidden_ranges: &[(usize, usize)],
) -> (
    ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>,
    WarehouseId,
) {
    let prof = lakekeeper_integration_tests::memory_io_profile();

    let authz = HidingAuthorizer::new();
    authz.block_can_list_everything();

    let (ctx, warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(10),
        },
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = lakekeeper_integration_tests::create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    // create 10 staged tables
    for i in 0..n_tabulars {
        let v = CatalogServer::create_view(
            ns_params.clone(),
            create_view_request(Some(&format!("{i}")), None),
            ctx.clone(),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        CatalogServer::drop_view(
            ViewParameters {
                prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                view: TableIdent {
                    name: format!("{i}"),
                    namespace: ns.namespace.clone(),
                },
            },
            DropParams {
                purge_requested: true,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        if hidden_ranges
            .iter()
            .any(|(start, end)| i >= *start && i < *end)
        {
            authz.hide(&format!(
                "view:{}/{}",
                warehouse.warehouse_id,
                v.metadata.uuid()
            ));
        }
    }

    (ctx, warehouse.warehouse_id)
}

impl_pagination_tests!(
    soft_deleted_tabular,
    setup_pagination_test,
    ApiServer,
    ListDeletedTabularsQuery,
    tabulars,
    |tid| { tid.name }
);

#[sqlx::test]
async fn test_deleted_tabulars_pagination(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();

    let authz = HidingAuthorizer::new();
    authz.block_can_list_everything();

    let (ctx, warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(10),
        },
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = lakekeeper_integration_tests::create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    for i in 0..10 {
        let _ = CatalogServer::create_view(
            ns_params.clone(),
            create_view_request(Some(&format!("view-{i}")), None),
            ctx.clone(),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        CatalogServer::drop_view(
            ViewParameters {
                prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                view: TableIdent {
                    name: format!("view-{i}"),
                    namespace: ns.namespace.clone(),
                },
            },
            DropParams {
                purge_requested: true,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    // list 1 more than existing tables
    let all = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: None,
            page_size: Some(11),
            page_token: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.tabulars.len(), 10);

    // list exactly amount of existing tables
    let all = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: None,
            page_size: Some(10),
            page_token: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.tabulars.len(), 10);

    // next page is empty
    let next = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: None,
            page_size: Some(10),
            page_token: all.next_page_token,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(next.tabulars.len(), 0);
    assert!(next.next_page_token.is_none());

    let first_six = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: None,
            page_size: Some(6),
            page_token: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(first_six.tabulars.len(), 6);
    assert!(first_six.next_page_token.is_some());
    let first_six_items = first_six
        .tabulars
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (i, item) in first_six_items.iter().enumerate().take(6) {
        assert_eq!(item, &format!("view-{i}"));
    }

    let next_four = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: None,
            page_size: Some(6),
            page_token: first_six.next_page_token,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(next_four.tabulars.len(), 4);
    // page-size > number of items left -> no next page
    assert!(next_four.next_page_token.is_none());

    let next_four_items = next_four
        .tabulars
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (idx, i) in (6..10).enumerate() {
        assert_eq!(next_four_items[idx], format!("view-{i}"));
    }

    let mut ids = Arc::unwrap_or_clone(all.tabulars);
    ids.sort_by_key(|e| e.id);
    for t in ids.iter().take(6).skip(4) {
        authz.hide(&format!("view:{}/{}", warehouse.warehouse_id, t.id));
    }

    let page = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: None,
            page_size: Some(5),
            page_token: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(page.tabulars.len(), 5);
    assert!(page.next_page_token.is_some());
    let page_items = page
        .tabulars
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();
    for (i, item) in page_items.iter().enumerate() {
        let tab_id = if i > 3 { i + 2 } else { i };
        assert_eq!(item, &format!("view-{tab_id}"));
    }

    let next_page = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: None,
            page_size: Some(6),
            page_token: page.next_page_token,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(next_page.tabulars.len(), 3);

    let next_page_items = next_page
        .tabulars
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (idx, i) in (7..10).enumerate() {
        assert_eq!(next_page_items[idx], format!("view-{i}"));
    }
}
