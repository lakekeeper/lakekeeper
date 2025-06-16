use futures::FutureExt;
use iceberg_ext::catalog::rest::ListTablesResponse;
use itertools::Itertools;

use crate::{
    api::{
        iceberg::v1::{ListTablesQuery, NamespaceParameters, PaginationQuery},
        ApiContext, Result,
    },
    catalog::{
        namespace::authorized_namespace_ident_to_id, require_warehouse_id, tabular::list_entities,
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogNamespaceAction, CatalogViewAction},
        Catalog, SecretStore, State, Transaction,
    },
};

pub(crate) async fn list_views<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    query: ListTablesQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListTablesResponse> {
    let return_uuids = query.return_uuids;
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters { namespace, prefix } = parameters;
    let warehouse_id = require_warehouse_id(prefix)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;

    let mut t: <C as Catalog>::Transaction =
        C::Transaction::begin_read(state.v1_state.catalog).await?;

    let namespace_id = authorized_namespace_ident_to_id::<C, _>(
        authorizer.clone(),
        &request_metadata,
        &warehouse_id,
        &namespace,
        CatalogNamespaceAction::CanListViews,
        t.transaction(),
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------

    let (identifiers, view_uuids, next_page_token) =
        crate::catalog::fetch_until_full_page::<_, _, _, C>(
            query.page_size,
            query.page_token,
            list_entities!(
                View,
                list_views,
                view_action,
                namespace,
                namespace_id,
                authorizer,
                request_metadata,
                warehouse_id
            ),
            &mut t,
        )
        .await?;
    t.commit().await?;

    let mut idents = Vec::with_capacity(identifiers.len());
    let mut protection_status = Vec::with_capacity(identifiers.len());
    for ident in identifiers {
        idents.push(ident.table_ident);
        protection_status.push(ident.protected);
    }

    Ok(ListTablesResponse {
        next_page_token,
        identifiers: idents,
        table_uuids: return_uuids.then_some(view_uuids.into_iter().map(|id| *id).collect()),
        protection_status: query.return_protection_status.then_some(protection_status),
    })
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use sqlx::PgPool;

    use crate::{
        api::{
            iceberg::{
                types::{PageToken, Prefix},
                v1::{views::ViewService, DataAccess, ListTablesQuery, NamespaceParameters},
            },
            management::v1::warehouse::TabularDeleteProfile,
            ApiContext,
        },
        catalog::{test::impl_pagination_tests, CatalogServer},
        implementations::postgres::{PostgresCatalog, SecretsState},
        request_metadata::RequestMetadata,
        service::{authz::tests::HidingAuthorizer, State, UserId},
    };

    async fn pagination_test_setup(
        pool: PgPool,
        n_tables: usize,
        hidden_ranges: &[(usize, usize)],
    ) -> (
        ApiContext<State<HidingAuthorizer, PostgresCatalog, SecretsState>>,
        NamespaceParameters,
    ) {
        let prof = crate::catalog::test::test_io_profile();
        let authz = HidingAuthorizer::new();
        // Prevent hidden views from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
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
                crate::catalog::views::create::test::create_view_request(
                    Some(&format!("{i}")),
                    None,
                ),
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
                    authz.hide(&format!("view:{}", view.metadata.uuid()));
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
        let prof = crate::catalog::test::test_io_profile();

        let authz: HidingAuthorizer = HidingAuthorizer::new();
        // Prevent hidden views from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
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
                crate::catalog::views::create::test::create_view_request(
                    Some(&format!("view-{i}")),
                    None,
                ),
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
        // page-size > number of items left -> no next page
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
            authz.hide(&format!("view:{t}"));
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
        let prof = crate::catalog::test::test_io_profile();

        let authz: HidingAuthorizer = HidingAuthorizer::new();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
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
                crate::catalog::views::create::test::create_view_request(
                    Some(&format!("view-{i}")),
                    None,
                ),
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
}
