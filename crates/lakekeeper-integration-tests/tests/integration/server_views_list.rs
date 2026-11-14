use itertools::Itertools;
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::{PageToken, Prefix},
            v1::{DataAccess, ListTablesQuery, NamespaceParameters, views::ViewService},
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{State, UserId, authz::tests::HidingAuthorizer},
};
use lakekeeper_integration_tests::{
    create_ns, create_view_request, impl_pagination_tests, memory_io_profile, setup_simple,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

async fn pagination_test_setup(
    pool: PgPool,
    n_tables: usize,
    hidden_ranges: &[(usize, usize)],
) -> (
    ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>,
    NamespaceParameters,
) {
    let authz = HidingAuthorizer::new();
    // Prevent hidden views from becoming visible through `can_list_everything`.
    authz.block_can_list_everything();

    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    for i in 0..n_tables {
        let view = CatalogServer::create_view(
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
        for (start, end) in hidden_ranges.iter().copied() {
            if i >= start && i < end {
                authz.hide(&format!(
                    "view:{}/{}",
                    warehouse.warehouse_id,
                    view.metadata.uuid()
                ));
            }
        }
    }

    (ctx, ns_params)
}

impl_pagination_tests!(
    view,
    pagination_test_setup,
    CatalogServer,
    ListTablesQuery,
    identifiers,
    |tid| { tid.name }
);

#[sqlx::test]
async fn test_view_pagination(pool: sqlx::PgPool) {
    let authz: HidingAuthorizer = HidingAuthorizer::new();
    // Prevent hidden views from becoming visible through `can_list_everything`.
    authz.block_can_list_everything();

    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = create_ns(
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
    }

    // list 1 more than existing tables
    let all = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);

    // list exactly amount of existing tables
    let all = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(10),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);

    // next page is empty
    let next = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::Present(all.next_page_token.unwrap()),
            page_size: Some(10),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(next.identifiers.len(), 0);
    assert!(next.next_page_token.is_none());

    // Fetch in two steps - 6 and 4
    let first_six = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(6),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(first_six.identifiers.len(), 6);
    assert!(first_six.next_page_token.is_some());
    let first_six_items = first_six
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (i, item) in first_six_items.iter().enumerate().take(6) {
        assert_eq!(item, &format!("view-{i}"));
    }

    let next_four = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::Present(first_six.next_page_token.unwrap()),
            page_size: Some(6),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(next_four.identifiers.len(), 4);
    assert!(next_four.next_page_token.is_none());

    let next_four_items = next_four
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (idx, i) in (6..10).enumerate() {
        assert_eq!(next_four_items[idx], format!("view-{i}"));
    }

    // Hiding 2 views
    let mut ids = all.table_uuids.unwrap();
    ids.sort();
    for t in ids.iter().take(6).skip(4) {
        authz.hide(&format!("view:{}/{t}", warehouse.warehouse_id));
    }

    let page = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(5),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(page.identifiers.len(), 5);
    assert!(page.next_page_token.is_some());
    let page_items = page
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();
    for (i, item) in page_items.iter().enumerate() {
        let tab_id = if i > 3 { i + 2 } else { i };
        assert_eq!(item, &format!("view-{tab_id}"));
    }

    let next_page = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::Present(page.next_page_token.unwrap()),
            page_size: Some(6),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(next_page.identifiers.len(), 3);

    let next_page_items = next_page
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (idx, i) in (7..10).enumerate() {
        assert_eq!(next_page_items[idx], format!("view-{i}"));
    }
}

#[sqlx::test]
async fn test_list_views(pool: sqlx::PgPool) {
    let authz: HidingAuthorizer = HidingAuthorizer::new();

    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };

    // create 10 staged views
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
    }

    // By default `HidingAuthorizer` allows everything, meaning the quick check path in
    // `list_views` will be hit since `can_list_everything: true`.
    let all = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);

    // Block `can_list_everything` to hit alternative code path.
    ctx.v1_state.authz.block_can_list_everything();
    let all = CatalogServer::list_views(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);
}

/// Regression test: paginated list must not return duplicate views across pages.
#[sqlx::test]
async fn test_view_pagination_no_duplicates(pool: sqlx::PgPool) {
    let authz: HidingAuthorizer = HidingAuthorizer::new();
    authz.block_can_list_everything();

    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };

    let n_views: usize = 300;
    for i in 0..n_views {
        CatalogServer::create_view(
            ns_params.clone(),
            create_view_request(Some(&format!("view-{i:04}")), None),
            ctx.clone(),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    let page_size = 100;
    let mut all_names: Vec<String> = Vec::new();
    let mut page_token = PageToken::NotSpecified;
    let mut pages: usize = 0;

    loop {
        let page = CatalogServer::list_views(
            ns_params.clone(),
            ListTablesQuery {
                page_token,
                page_size: Some(page_size),
                return_uuids: false,
                return_protection_status: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        all_names.extend(page.identifiers.iter().map(|i| i.name.clone()));
        pages += 1;

        match page.next_page_token {
            Some(token) => page_token = PageToken::Present(token),
            None => break,
        }

        assert!(pages <= n_views, "Too many pages, likely infinite loop");
    }

    assert_eq!(
        all_names.len(),
        n_views,
        "Expected {n_views} total views across all pages, got {}",
        all_names.len()
    );

    let unique_names: std::collections::HashSet<&String> = all_names.iter().collect();
    assert_eq!(
        unique_names.len(),
        all_names.len(),
        "Found duplicate view names across pages: {:?}",
        all_names.iter().duplicates().collect::<Vec<_>>()
    );
}
