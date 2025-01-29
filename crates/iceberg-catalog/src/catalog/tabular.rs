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
            use crate::catalog::UnfilteredPage;
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

                Ok(UnfilteredPage::new(
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
