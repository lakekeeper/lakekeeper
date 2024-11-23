use crate::api::iceberg::v1::{ListTablesQuery, NamespaceParameters, PaginationQuery};
use crate::api::ApiContext;
use crate::api::Result;
use crate::catalog::namespace::validate_namespace_ident;
use crate::catalog::tabular::list_entities;
use crate::catalog::{require_warehouse_id, PageStatus};
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{
    Authorizer, CatalogNamespaceAction, CatalogViewAction, CatalogWarehouseAction,
};
use crate::service::{Catalog, SecretStore, State, Transaction};
use futures::FutureExt;
use iceberg_ext::catalog::rest::ListTablesResponse;
use itertools::Itertools;

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
    validate_namespace_ident(&namespace)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            &CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t: <C as Catalog>::Transaction =
        C::Transaction::begin_read(state.v1_state.catalog).await?;
    let namespace_id = C::namespace_to_id(warehouse_id, &namespace, t.transaction()).await; // We can't fail before AuthZ.

    authorizer
        .require_namespace_action(
            &request_metadata,
            warehouse_id,
            namespace_id,
            &CatalogNamespaceAction::CanListViews,
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
                authorizer,
                request_metadata,
                warehouse_id
            ),
            &mut t,
        )
        .await?;
    t.commit().await?;

    Ok(ListTablesResponse {
        next_page_token,
        identifiers,
        table_uuids: return_uuids.then_some(view_uuids.into_iter().map(|id| *id).collect()),
    })
}

#[cfg(test)]
mod test {
    use crate::api::iceberg::types::{PageToken, Prefix};
    use crate::api::iceberg::v1::{DataAccess, ListTablesQuery, NamespaceParameters};
    use crate::api::management::v1::warehouse::TabularDeleteProfile;
    use crate::catalog::test::random_request_metadata;
    use crate::catalog::CatalogServer;
    use crate::service::authz::implementations::openfga::tests::ObjectHidingMock;

    use crate::api::iceberg::v1::views::Service;
    use itertools::Itertools;

    #[sqlx::test]
    async fn test_view_pagination(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::test_io_profile();

        let hiding_mock = ObjectHidingMock::new();
        let authz = hiding_mock.to_authorizer();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz,
            TabularDeleteProfile::Hard {},
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
                random_request_metadata(),
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
            },
            ctx.clone(),
            random_request_metadata(),
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
            },
            ctx.clone(),
            random_request_metadata(),
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
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(next.identifiers.len(), 0);
        assert!(next.next_page_token.is_none());

        let first_six = CatalogServer::list_views(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(6),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
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
            },
            ctx.clone(),
            random_request_metadata(),
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

        let mut ids = all.table_uuids.unwrap();
        ids.sort();
        for t in ids.iter().take(6).skip(4) {
            hiding_mock.hide(&format!("view:{t}"));
        }

        let page = CatalogServer::list_views(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
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
            },
            ctx.clone(),
            random_request_metadata(),
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
}
