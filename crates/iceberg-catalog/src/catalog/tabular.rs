use crate::service::ListFlags;

pub(crate) fn default_view_flags() -> bool {
    false
}

pub(crate) fn default_table_flags() -> ListFlags {
    ListFlags {
        include_active: true,
        include_staged: false,
        include_deleted: false,
    }
}

macro_rules! list_entities {
    ($entity:ident, $list_fn:ident, $action:ident, $namespace:ident, $authorizer:ident, $request_metadata:ident, $warehouse_id:ident) => {
        |ps, page_token, trx| {
            use ::paste::paste;
            paste! {
                use crate::catalog::tabular::[<default_ $entity:snake _flags>] as default_flags;
            }
            use crate::catalog::FetchResult;
            let namespace = $namespace.clone();
            let authorizer = $authorizer.clone();
            let request_metadata = $request_metadata.clone();
            async move {
                let query = PaginationQuery {
                    page_size: Some(ps),
                    page_token: page_token.into(),
                };
                let entities = C::$list_fn(
                    $warehouse_id,
                    &namespace,
                    default_flags(),
                    trx.transaction(),
                    query,
                )
                .await?;
                let (ids, idents, tokens): (Vec<_>, Vec<_>, Vec<_>) =
                    entities.into_iter_with_page_tokens().multiunzip();

                let (next_idents, next_uuids, next_page_tokens, mask): (
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                    Vec<bool>,
                ) = futures::future::try_join_all(ids.iter().map(|n| {
                    paste! {
                        authorizer.[<is_allowed_ $action>](
                            &request_metadata,
                            $warehouse_id,
                            *n,
                            &paste! { [<Catalog $entity Action>]::CanIncludeInList },
                        )
                    }
                }))
                .await?
                .into_iter()
                .zip(idents.into_iter().zip(ids.into_iter()))
                .zip(tokens.into_iter())
                .map(|((allowed, namespace), token)| (namespace.0, namespace.1, token, allowed))
                .multiunzip();

                Ok(FetchResult::new(
                    next_idents,
                    next_uuids,
                    next_page_tokens,
                    mask,
                    ps.clamp(0, i64::MAX).try_into().expect("we clamped it"),
                ))
            }
            .boxed()
        }
    };
}

pub(crate) use list_entities;

#[cfg(test)]
pub(crate) mod test {
    macro_rules! impl_tabular_pagination_tests {
        ($typ:ident, $setup_fn:ident) => {
            use paste::paste;
            paste! {
                #[allow(unused_imports)]
                use crate::api::iceberg::v1::[<$typ s>]::Service as _;

                #[sqlx::test]
                async fn [<test_$typ _pagination_with_no_items>](pool: sqlx::PgPool) {
                    let (ctx, ns_params) = $setup_fn(pool, 0, &[]).await;
                    let all = CatalogServer::[<list_ $typ s>](
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
                    assert_eq!(all.identifiers.len(), 0);
                    assert!(all.next_page_token.is_none());
                }
            }
            paste! {

                    #[sqlx::test]
                    async fn [<test_$typ _pagination_with_all_items_hidden>](pool: PgPool) {
                        let (ctx, ns_params) = $setup_fn(pool, 20, &[(0, 20)]).await;
                        let all = CatalogServer::[<list_$typ s>](
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
                        assert_eq!(all.identifiers.len(), 0);
                        assert!(all.next_page_token.is_none());
                    }

                    #[sqlx::test]
                    async fn test_pagination_multiple_pages_hidden(pool: sqlx::PgPool) {
                        let (ctx, ns_params) = $setup_fn(pool, 20, &[(5, 15)]).await;

                        let mut first_page = CatalogServer::[<list_$typ s>](
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

                        assert_eq!(first_page.identifiers.len(), 5);

                        for i in (0..5).rev() {
                            assert_eq!(
                                first_page.identifiers.pop().map(|tid| tid.name),
                                Some(format!("{i}"))
                            );
                        }

                        let mut next_page = CatalogServer::[<list_$typ s>](
                            ns_params.clone(),
                            ListTablesQuery {
                                page_token: first_page.next_page_token.into(),
                                page_size: Some(6),
                                return_uuids: true,
                            },
                            ctx.clone(),
                            random_request_metadata(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(next_page.identifiers.len(), 5);
                        for i in (15..20).rev() {
                            assert_eq!(
                                next_page.identifiers.pop().map(|tid| tid.name),
                                Some(format!("{i}"))
                            );
                        }
                        assert_eq!(next_page.next_page_token, None);
                    }

                    #[sqlx::test]
                    async fn test_pagination_first_page_is_hidden(pool: PgPool) {
                        let (ctx, ns_params) = $setup_fn(pool, 20, &[(0, 10)]).await;

                        let mut first_page = CatalogServer::[<list_$typ s>](
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

                        assert_eq!(first_page.identifiers.len(), 10);
                        // In this case, next_page_token is None since first page is hidden and we fetch 100 items
                        // if we have an empty page. Since there only 10 items left, the returned page is considered
                        // partial and we get no next_page_token, leaving this comment here in case someone changes
                        // the fetch amount and wonders why this test starts failing.
                        assert_eq!(first_page.next_page_token, None);
                        for i in (10..20).rev() {
                            assert_eq!(
                                first_page.identifiers.pop().map(|tid| tid.name),
                                Some(format!("{i}"))
                            );
                        }
                    }

                    #[sqlx::test]
                    async fn test_pagination_middle_page_is_hidden(pool: PgPool) {
                        let (ctx, ns_params) = $setup_fn(pool, 20, &[(5, 15)]).await;

                        let mut first_page = CatalogServer::[<list_$typ s>](
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

                        assert_eq!(first_page.identifiers.len(), 5);

                        for i in (0..5).rev() {
                            assert_eq!(
                                first_page.identifiers.pop().map(|tid| tid.name),
                                Some(format!("{i}"))
                            );
                        }

                        let mut next_page = CatalogServer::[<list_$typ s>](
                            ns_params.clone(),
                            ListTablesQuery {
                                page_token: first_page.next_page_token.into(),
                                page_size: Some(6),
                                return_uuids: true,
                            },
                            ctx.clone(),
                            random_request_metadata(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(next_page.identifiers.len(), 5);
                        for i in (15..20).rev() {
                            assert_eq!(
                                next_page.identifiers.pop().map(|tid| tid.name),
                                Some(format!("{i}"))
                            );
                        }
                        assert_eq!(next_page.next_page_token, None);
                    }

                    #[sqlx::test]
                    async fn test_pagination_last_page_is_hidden(pool: PgPool) {
                        let (ctx, ns_params) = $setup_fn(pool, 20, &[(10, 20)]).await;

                        let mut first_page = CatalogServer::[<list_$typ s>](
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

                        assert_eq!(first_page.identifiers.len(), 10);

                        for i in (0..10).rev() {
                            assert_eq!(
                                first_page.identifiers.pop().map(|tid| tid.name),
                                Some(format!("{i}"))
                            );
                        }

                        let next_page = CatalogServer::[<list_$typ s>](
                            ns_params.clone(),
                            ListTablesQuery {
                                page_token: first_page.next_page_token.into(),
                                page_size: Some(11),
                                return_uuids: true,
                            },
                            ctx.clone(),
                            random_request_metadata(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(next_page.table_uuids.unwrap().len(), 0);
                        assert_eq!(next_page.next_page_token, None);
                    }
            }
        };
    }
    pub(crate) use impl_tabular_pagination_tests;
}
