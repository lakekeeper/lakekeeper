pub(crate) mod commit_tables;
pub(crate) mod compression_codec;
mod config;
pub mod generic_tables;
pub(crate) mod io;
mod metrics;
pub mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
pub mod tables;
pub(crate) mod tabular;
pub mod views;

use std::{collections::HashMap, fmt::Debug, marker::PhantomData, sync::Arc};

use futures::future::BoxFuture;
use iceberg::spec::{TableMetadata, ViewMetadata};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use itertools::{FoldWhile, Itertools};
pub use namespace::{MAX_NAMESPACE_DEPTH, NAMESPACE_ID_PROPERTY, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::{
    CONFIG, WarehouseId,
    api::{
        ErrorModel, Result,
        iceberg::v1::{PageToken, Prefix},
    },
    service::{CatalogStore, authz::Authorizer, secrets::SecretStore, storage::StorageCredential},
};

pub trait MetadataProperties {
    fn properties(&self) -> &HashMap<String, String>;
}

macro_rules! impl_metadata_properties {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl MetadataProperties for $ty {
                fn properties(&self) -> &HashMap<String, String> {
                    self.properties()
                }
            }

            impl MetadataProperties for &$ty {
                fn properties(&self) -> &HashMap<String, String> {
                    (*self).properties()
                }
            }

            impl MetadataProperties for &mut $ty {
                fn properties(&self) -> &HashMap<String, String> {
                    (**self).properties()
                }
            }

            impl MetadataProperties for Arc<$ty> {
                fn properties(&self) -> &HashMap<String, String> {
                    self.as_ref().properties()
                }
            }

            impl MetadataProperties for &Arc<$ty> {
                fn properties(&self) -> &HashMap<String, String> {
                    self.as_ref().properties()
                }
            }

            impl MetadataProperties for Box<$ty> {
                fn properties(&self) -> &HashMap<String, String> {
                    self.as_ref().properties()
                }
            }

            impl MetadataProperties for &Box<$ty> {
                fn properties(&self) -> &HashMap<String, String> {
                    self.as_ref().properties()
                }
            }
        )+
    };
}

impl_metadata_properties!(TableMetadata, ViewMetadata);

#[derive(Clone, Debug)]

pub struct CatalogServer<C: CatalogStore, A: Authorizer, S: SecretStore> {
    auth_handler: PhantomData<A>,
    catalog_backend: PhantomData<C>,
    secret_store: PhantomData<S>,
}

fn require_warehouse_id(prefix: Option<&Prefix>) -> std::result::Result<WarehouseId, ErrorModel> {
    WarehouseId::from_str_or_bad_request(
        prefix
            .ok_or_else(|| {
                tracing::debug!("No prefix specified.");
                ErrorModel::bad_request(
                    "No prefix specified. The warehouse-id must be provided as prefix in the URL."
                        .to_string(),
                    "NoPrefixProvided",
                    None,
                )
            })?
            .as_ref(),
    )
}

pub(crate) async fn maybe_get_secret<S: SecretStore>(
    secret: Option<crate::SecretId>,
    state: &S,
) -> Result<Option<Arc<StorageCredential>>, IcebergErrorResponse> {
    if let Some(secret_id) = secret {
        Ok(Some(
            state.require_storage_secret_by_id(secret_id).await?.secret,
        ))
    } else {
        Ok(None)
    }
}

pub struct UnfilteredPage<Entity, EntityId> {
    pub entities: Vec<Entity>,
    pub entity_ids: Vec<EntityId>,
    pub page_tokens: Vec<String>,
    pub authz_approved: Vec<bool>,
    pub n_filtered: usize,
    pub page_size: usize,
}

impl<T, Z> Debug for UnfilteredPage<T, Z> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchResult")
            .field("page_tokens", &self.page_tokens)
            .field("authz_mask", &self.authz_approved)
            .field("n_filtered", &self.n_filtered)
            .field("page_size", &self.page_size)
            .finish()
    }
}

impl<Entity, EntityId> UnfilteredPage<Entity, EntityId> {
    #[must_use]
    pub(crate) fn new(
        entities: Vec<Entity>,
        entity_ids: Vec<EntityId>,
        page_tokens: Vec<String>,
        authz_approved_items: Vec<bool>,
        page_size: usize,
    ) -> Self {
        let n_filtered = authz_approved_items
            .iter()
            .map(|allowed| usize::from(!*allowed))
            .sum();
        Self {
            entities,
            entity_ids,
            page_tokens,
            authz_approved: authz_approved_items,
            n_filtered,
            page_size,
        }
    }

    #[must_use]
    pub(crate) fn take_n_authz_approved(
        self,
        n: usize,
    ) -> (Vec<Entity>, Vec<EntityId>, Option<String>) {
        #[derive(Debug)]
        enum State {
            Open,
            LoopingForLastNextPage,
        }
        let (entities, ids, token, _) = self
            .authz_approved
            .into_iter()
            .zip(self.entities)
            .zip(self.entity_ids)
            .zip(self.page_tokens)
            .fold_while(
                (vec![], vec![], None, State::Open),
                |(mut entities, mut entity_ids, mut page_token, mut state),
                 (((authz, entity), id), token)| {
                    if authz {
                        if matches!(state, State::Open) {
                            entities.push(entity);
                            entity_ids.push(id);
                        } else if matches!(state, State::LoopingForLastNextPage) {
                            return FoldWhile::Done((entities, entity_ids, page_token, state));
                        }
                    }
                    page_token = Some(token);
                    state = if entities.len() == n {
                        State::LoopingForLastNextPage
                    } else {
                        State::Open
                    };
                    FoldWhile::Continue((entities, entity_ids, page_token, state))
                },
            )
            .into_inner();

        (entities, ids, token)
    }

    #[must_use]
    fn is_partial(&self) -> bool {
        self.entities.len() < self.page_size
    }

    #[must_use]
    fn has_authz_denied_items(&self) -> bool {
        self.n_filtered > 0
    }
}

pub(crate) async fn fetch_until_full_page<'b, 'd: 'b, Entity, EntityId, FetchFun, C: CatalogStore>(
    page_size: Option<i64>,
    page_token: PageToken,
    mut fetch_fn: FetchFun,
    transaction: &'d mut C::Transaction,
) -> Result<(Vec<Entity>, Vec<EntityId>, Option<String>)>
where
    FetchFun: for<'c> FnMut(
        i64,
        Option<String>,
        &'c mut C::Transaction,
    ) -> BoxFuture<'c, Result<UnfilteredPage<Entity, EntityId>>>,
    // you may feel tempted to change the Vec<String> of page-tokens to Option<String>
    // a word of advice: don't, we need to take the nth page-token of the next page when
    // we're filling a auth-filtered page. Without a vec, that won't fly.
{
    let page_size = page_size
        .unwrap_or(if matches!(page_token, PageToken::NotSpecified) {
            CONFIG.pagination_size_max.into()
        } else {
            CONFIG.pagination_size_default.into()
        })
        .clamp(1, CONFIG.pagination_size_max.into());
    let page_as_usize: usize = page_size
        .try_into()
        .expect("should be running on at least 32 bit architecture");

    let page_token = page_token.as_option().map(ToString::to_string);
    let unfiltered_page = fetch_fn(page_size, page_token, transaction).await?;

    if unfiltered_page.is_partial() && !unfiltered_page.has_authz_denied_items() {
        return Ok((unfiltered_page.entities, unfiltered_page.entity_ids, None));
    }

    let (mut entities, mut entity_ids, mut next_page_token) =
        unfiltered_page.take_n_authz_approved(page_as_usize);

    while entities.len() < page_as_usize {
        let new_unfiltered_page = fetch_fn(
            CONFIG.pagination_size_default.into(),
            next_page_token.clone(),
            transaction,
        )
        .await?;

        let number_of_requested_items = page_as_usize - entities.len();
        let page_was_authz_reduced = new_unfiltered_page.has_authz_denied_items();

        let (more_entities, more_ids, n_page) =
            new_unfiltered_page.take_n_authz_approved(number_of_requested_items);
        let number_of_new_items = more_entities.len();
        entities.extend(more_entities);
        entity_ids.extend(more_ids);

        if (number_of_new_items < number_of_requested_items) && !page_was_authz_reduced {
            next_page_token = None;
            break;
        }
        next_page_token = n_page;
    }

    Ok((entities, entity_ids, next_page_token))
}
