//! `impl_pagination_tests!` — paste!-macro suite that exercises `list_*`
//! endpoints against a `HidingAuthorizer` setup, verifying that hidden
//! entities do not break or leak from pagination.
//!
//! Generated tests:
//! - `test_<typ>_pagination_with_no_items`
//! - `test_<typ>_pagination_with_all_items_hidden`
//! - `test_pagination_multiple_pages_hidden`
//! - `test_pagination_first_page_is_hidden`
//! - `test_pagination_middle_page_is_hidden`
//! - `test_pagination_last_page_is_hidden`
//!
//! The macro is exported from this crate so per-entity test files
//! (`tests/pagination_tables.rs`, …) can invoke it independently.
//!
//! Callers must bring `RequestMetadata`, `sqlx::PgPool`, and `PgPool` into
//! scope at the invocation site (used by the macro-generated bodies).

/// Generates a fixed suite of pagination tests that exercise hidden-entity
/// behavior. The 6 parameters identify the entity-specific bits:
///
/// 1. `$typ` — singular entity ident, e.g. `table`, `view`
/// 2. `$setup_fn` — fn `(pool, n_entities, &[(hidden_from, hidden_to)]) -> (ctx, ns_params)`
/// 3. `$server_typ` — server type with `list_<typ>s` method, e.g. `CatalogServer`
/// 4. `$query_typ` — query struct, e.g. `ListTablesQuery`
/// 5. `$entity_ident` — field name on the response, e.g. `identifiers`
/// 6. `$map_block` — closure mapping a returned ident to its `String` name
#[macro_export]
macro_rules! impl_pagination_tests {
    ($typ:ident, $setup_fn:ident, $server_typ:ident, $query_typ:ident, $entity_ident:ident, $map_block:expr) => {
        use pastey::paste;
        paste! {
            #[sqlx::test]
            async fn [<test_$typ _pagination_with_no_items>](pool: sqlx::PgPool) {
                let (ctx, ns_params) = $setup_fn(pool, 0, &[]).await;
                let all = $server_typ::[<list_ $typ s>](
                    ns_params.clone(),
                    serde_json::from_value::<$query_typ>(serde_json::json!(
                       {
                        "pageSize": 10,
                        "return_uuids": true,
                        }
                    )).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                assert_eq!(all.$entity_ident.len(), 0);
                assert!(all.next_page_token.is_none());
            }
        }
        paste! {
            #[sqlx::test]
            async fn [<test_$typ _pagination_with_all_items_hidden>](pool: PgPool) {
                let (ctx, ns_params) = $setup_fn(pool, 20, &[(0, 20)]).await;
                let all = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                     serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageSize": 10,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                assert_eq!(all.$entity_ident.len(), 0);
                assert!(all.next_page_token.is_none());
            }

            #[sqlx::test]
            async fn test_pagination_multiple_pages_hidden(pool: sqlx::PgPool) {
                let (ctx, ns_params) = $setup_fn(pool, 200, &[(95, 150), (195, 200)]).await;

                let first_page = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                    serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageSize": 105,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                let mut idents = std::sync::Arc::unwrap_or_clone(first_page.$entity_ident);

                assert_eq!(idents.len(), 105);

                for i in (0..95).chain(150..160).rev() {
                    assert_eq!(idents.pop().map($map_block), Some(format!("{i}")));
                }

                let next_page = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                     serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageToken": first_page.next_page_token.unwrap(),
                        "pageSize": 100,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                let mut idents = std::sync::Arc::unwrap_or_clone(next_page.$entity_ident);

                assert_eq!(idents.len(), 35);
                for i in (160..195).rev() {
                    assert_eq!(idents.pop().map($map_block), Some(format!("{i}")));
                }
                assert_eq!(next_page.next_page_token, None);
            }

            #[sqlx::test]
            async fn test_pagination_first_page_is_hidden(pool: PgPool) {
                let (ctx, ns_params) = $setup_fn(pool, 20, &[(0, 10)]).await;

                let first_page = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                     serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageSize": 10,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                let mut idents = std::sync::Arc::unwrap_or_clone(first_page.$entity_ident);

                assert_eq!(idents.len(), 10);
                assert!(first_page.next_page_token.is_some());
                for i in (10..20).rev() {
                    assert_eq!(idents.pop().map($map_block), Some(format!("{i}")));
                }
            }

            #[sqlx::test]
            async fn test_pagination_middle_page_is_hidden(pool: PgPool) {
                let (ctx, ns_params) = $setup_fn(pool, 20, &[(5, 15)]).await;

                let first_page = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                    serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageSize": 5,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                let mut idents = std::sync::Arc::unwrap_or_clone(first_page.$entity_ident);

                assert_eq!(idents.len(), 5);

                for i in (0..5).rev() {
                    assert_eq!(idents.pop().map($map_block), Some(format!("{i}")));
                }

                let next_page = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                    serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageToken": first_page.next_page_token.unwrap(),
                        "pageSize": 6,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                let mut idents = std::sync::Arc::unwrap_or_clone(next_page.$entity_ident);

                assert_eq!(idents.len(), 5);
                for i in (15..20).rev() {
                    assert_eq!(idents.pop().map($map_block), Some(format!("{i}")));
                }
                assert_eq!(next_page.next_page_token, None);
            }

            #[sqlx::test]
            async fn test_pagination_last_page_is_hidden(pool: PgPool) {
                let (ctx, ns_params) = $setup_fn(pool, 20, &[(10, 20)]).await;

                let first_page = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                    serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageSize": 10,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();
                let mut idents = std::sync::Arc::unwrap_or_clone(first_page.$entity_ident);

                assert_eq!(idents.len(), 10);

                for i in (0..10).rev() {
                    assert_eq!(idents.pop().map($map_block), Some(format!("{i}")));
                }

                let next_page = $server_typ::[<list_$typ s>](
                    ns_params.clone(),
                    serde_json::from_value::<$query_typ>(serde_json::json!({
                        "pageToken": first_page.next_page_token.unwrap(),
                        "pageSize": 11,
                        "returnUuids": true,
                    })).unwrap(),
                    ctx.clone(),
                    RequestMetadata::new_unauthenticated(),
                )
                .await
                .unwrap();

                assert_eq!(next_page.$entity_ident.len(), 0);
                assert_eq!(next_page.next_page_token, None);
            }
        }
    };
}
