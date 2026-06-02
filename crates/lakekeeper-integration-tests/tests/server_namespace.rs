use std::{collections::HashSet, hash::RandomState};

use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::{PageToken, Prefix},
            v1::{
                NamespaceParameters,
                namespace::{NamespaceDropFlags, NamespaceService},
            },
        },
        management::v1::{
            ApiServer as ManagementApiServer, namespace::NamespaceManagementService,
            warehouse::TabularDeleteProfile,
        },
    },
    server::{CatalogServer, NAMESPACE_ID_PROPERTY},
    service::{
        ListNamespacesQuery, NamespaceId, State, UserId,
        authz::{AllowAllAuthorizer, tests::HidingAuthorizer},
    },
};
use lakekeeper_integration_tests::{impl_pagination_tests, memory_io_profile, setup};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

async fn ns_paginate_test_setup(
    pool: PgPool,
    number_of_namespaces: usize,
    hide_ranges: &[(usize, usize)],
) -> (
    ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>,
    Option<Prefix>,
) {
    let prof = memory_io_profile();

    let authz = HidingAuthorizer::new();
    // Prevent hidden namespaces from becoming visible through `can_list_everything`.
    authz.block_can_list_everything();

    let (ctx, warehouse) = setup(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
        None,
    )
    .await;

    for n in 0..number_of_namespaces {
        let ns = format!("{n}");
        let ns = CatalogServer::create_namespace(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::new(ns),
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        for (range_start, range_end) in hide_ranges {
            if n >= *range_start && n < *range_end {
                authz.hide(&format!(
                    "namespace:{}",
                    ns.properties
                        .as_ref()
                        .unwrap()
                        .get(NAMESPACE_ID_PROPERTY)
                        .unwrap()
                ));
            }
        }
    }
    (ctx, Some(Prefix(warehouse.warehouse_id.to_string())))
}

impl_pagination_tests!(
    namespace,
    ns_paginate_test_setup,
    CatalogServer,
    ListNamespacesQuery,
    namespaces,
    |ns| ns.inner()[0].clone()
);

#[sqlx::test]
async fn cannot_drop_protected_namespace(pool: sqlx::PgPool) {
    let (ctx, warehouse) = setup(
        pool.clone(),
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
        None,
    )
    .await;
    let ns = CatalogServer::create_namespace(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        CreateNamespaceRequest {
            namespace: NamespaceIdent::new("ns".to_string()),
            properties: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let ns_id = NamespaceId::from(
        *CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(1),
                parent: None,
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
    ManagementApiServer::set_namespace_protection(
        ns_id,
        warehouse.warehouse_id,
        true,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let e = CatalogServer::drop_namespace(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        },
        NamespaceDropFlags {
            recursive: false,
            force: false,
            purge: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(e.error.code, http::StatusCode::CONFLICT);

    ManagementApiServer::set_namespace_protection(
        ns_id,
        warehouse.warehouse_id,
        false,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    CatalogServer::drop_namespace(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        },
        NamespaceDropFlags {
            recursive: false,
            force: false,
            purge: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_list_namespaces(pool: PgPool) {
    let authz = HidingAuthorizer::new();

    let (ctx, warehouse) = setup(
        pool.clone(),
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
        None,
    )
    .await;

    // Create parent namespace.
    let parent_ns_name = "parent-ns".to_string();
    let _ = CatalogServer::create_namespace(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        CreateNamespaceRequest {
            namespace: NamespaceIdent::new(parent_ns_name.clone()),
            properties: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Create child namespaces.
    for n in 0..10 {
        let namespace =
            NamespaceIdent::from_vec(vec![parent_ns_name.clone(), format!("ns-{n}")]).unwrap();
        let _ = CatalogServer::create_namespace(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace,
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    // By default `HidingAuthorizer` allows everything, meaning the quick check path in
    // `list_namespaces` will be hit since `can_list_everything: true`.
    let all = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            parent: Some(NamespaceIdent::new(parent_ns_name.clone())),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.namespaces.len(), 10);

    // Block `can_list_everything` to hit alternative code path.
    ctx.v1_state.authz.block_can_list_everything();
    let all = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            parent: Some(NamespaceIdent::new(parent_ns_name)),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.namespaces.len(), 10);
}

#[sqlx::test]
async fn test_ns_pagination(pool: sqlx::PgPool) {
    let authz = HidingAuthorizer::new();
    // Prevent hidden namespaces from becoming visible through `can_list_everything`.
    authz.block_can_list_everything();

    let (ctx, warehouse) = setup(
        pool.clone(),
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
        None,
    )
    .await;
    for n in 0..10 {
        let ns = format!("ns-{n}");
        let _ = CatalogServer::create_namespace(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::new(ns),
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    let all = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            parent: None,
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.namespaces.len(), 10);

    let _ = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(10),
            parent: None,
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.namespaces.len(), 10);

    let first_six = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(6),
            parent: None,
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(first_six.namespaces.len(), 6);
    let first_six_items: HashSet<String, RandomState> = first_six
        .namespaces
        .iter()
        .map(iceberg::NamespaceIdent::to_url_string)
        .collect();
    for i in 0..6 {
        assert!(first_six_items.contains(&format!("ns-{i}")));
    }

    let next_four = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::Present(first_six.next_page_token.unwrap()),
            page_size: Some(6),
            parent: None,
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let next_four_items: HashSet<String, RandomState> = next_four
        .namespaces
        .iter()
        .map(iceberg::NamespaceIdent::to_url_string)
        .collect();
    for i in 6..10 {
        assert!(next_four_items.contains(&format!("ns-{i}")));
    }

    let mut ids = all.namespace_uuids.unwrap();
    ids.sort();
    for i in ids.iter().take(6).skip(4) {
        authz.hide(&format!("namespace:{i}"));
    }

    let page = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(5),
            parent: None,
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(page.namespaces.len(), 5);
    assert!(page.next_page_token.is_some());

    let page_items: HashSet<String, RandomState> = page
        .namespaces
        .iter()
        .map(iceberg::NamespaceIdent::to_url_string)
        .collect();

    for i in 0..5 {
        let ns_id = if i > 3 { i + 2 } else { i };
        assert!(page_items.contains(&format!("ns-{ns_id}")));
    }
    let next_page = CatalogServer::list_namespaces(
        Some(Prefix(warehouse.warehouse_id.to_string())),
        ListNamespacesQuery {
            page_token: PageToken::Present(page.next_page_token.unwrap()),
            page_size: Some(5),
            parent: None,
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(next_page.namespaces.len(), 3);

    let next_page_items: HashSet<String, RandomState> = next_page
        .namespaces
        .iter()
        .map(iceberg::NamespaceIdent::to_url_string)
        .collect();

    for i in 7..10 {
        assert!(next_page_items.contains(&format!("ns-{i}")));
    }
}
