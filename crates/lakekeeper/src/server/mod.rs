pub(crate) mod commit_tables;
pub(crate) mod compression_codec;
mod config;
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
    api::{
        iceberg::v1::{PageToken, Prefix},
        ErrorModel, Result,
    },
    service::{authz::Authorizer, secrets::SecretStore, storage::StorageCredential, CatalogStore},
    WarehouseId, CONFIG,
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

pub struct CatalogServer<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> {
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

#[cfg(test)]
pub(crate) mod test {
    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::CreateNamespaceRequest;
    use sqlx::PgPool;
    use uuid::Uuid;

    pub(crate) use crate::tests::memory_io_profile;
    use crate::{
        api::{
            iceberg::{
                types::Prefix,
                v1::{namespace::NamespaceService, NamespaceParameters},
            },
            management::v1::warehouse::TabularDeleteProfile,
            ApiContext,
        },
        implementations::{
            postgres::{PostgresBackend, SecretsState},
            CatalogState,
        },
        request_metadata::RequestMetadata,
        server::CatalogServer,
        service::{
            authz::{AllowAllAuthorizer, Authorizer},
            storage::{
                s3::S3AccessKeyCredential, S3Credential, S3Flavor, S3Profile, StorageCredential,
                StorageProfile,
            },
            CatalogNamespaceOps, CreateNamespaceResponse, NamespaceWithParent, State, UserId,
        },
        WarehouseId,
    };

    #[allow(dead_code)]
    pub(crate) fn s3_compatible_profile() -> (StorageProfile, StorageCredential) {
        let key_prefix = format!("test_prefix-{}", Uuid::now_v7());
        let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap();
        let region = std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into());
        let aws_access_key_id = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap();
        let aws_secret_access_key = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap();
        let endpoint: url::Url = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT")
            .unwrap()
            .parse()
            .unwrap();

        let cred: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
            aws_access_key_id,
            aws_secret_access_key,
            external_id: None,
        })
        .into();

        let mut profile: StorageProfile = S3Profile::builder()
            .bucket(bucket)
            .key_prefix(key_prefix)
            .region(region)
            .endpoint(endpoint.clone())
            .path_style_access(true)
            .sts_enabled(true)
            .flavor(S3Flavor::S3Compat)
            .allow_alternative_protocols(false)
            .build()
            .into();

        profile.normalize(Some(&cred)).unwrap();
        (profile, cred)
    }

    pub(crate) async fn create_ns<T: Authorizer>(
        api_context: ApiContext<State<T, PostgresBackend, SecretsState>>,
        prefix: String,
        ns_name: String,
    ) -> CreateNamespaceResponse {
        CatalogServer::create_namespace(
            Some(Prefix(prefix)),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::new(ns_name),
                properties: None,
            },
            api_context.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
    }

    pub(crate) async fn setup<T: Authorizer>(
        pool: PgPool,
        storage_profile: StorageProfile,
        storage_credential: Option<StorageCredential>,
        authorizer: T,
        delete_profile: TabularDeleteProfile,
        user_id: Option<UserId>,
    ) -> (
        ApiContext<State<T, PostgresBackend, SecretsState>>,
        TestWarehouseResponse,
    ) {
        crate::tests::setup(
            pool,
            storage_profile,
            storage_credential,
            authorizer,
            delete_profile,
            user_id,
            1,
        )
        .await
    }

    #[sqlx::test]
    async fn test_setup(pool: PgPool) {
        let prof = memory_io_profile();
        setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;
    }

    /// Setups up `num_warehouses` in the same project and creates one namespace in each warehouse.
    pub(crate) async fn tabular_test_multi_warehouse_setup(
        pool: PgPool,
        num_warehouses: usize,
        delete_profile: TabularDeleteProfile,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
        Vec<(WarehouseId, NamespaceWithParent, NamespaceParameters)>,
        String,
    ) {
        let prof = crate::server::test::memory_io_profile();
        let base_loc = prof.base_location().unwrap().to_string();
        let (ctx, res) = crate::tests::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            delete_profile,
            None,
            num_warehouses,
        )
        .await;

        let mut wh_ids = Vec::with_capacity(num_warehouses);
        wh_ids.push(res.warehouse_id);
        for (wh_id, _) in &res.additional_warehouses {
            wh_ids.push(*wh_id);
        }
        assert_eq!(wh_ids.len(), num_warehouses);

        let mut wh_ns_data = Vec::with_capacity(num_warehouses);
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        for wh_id in wh_ids {
            crate::server::test::create_ns(ctx.clone(), wh_id.to_string(), "myns".to_string())
                .await;
            let namespace_hierarchy = PostgresBackend::get_namespace(
                wh_id,
                NamespaceIdent::new("myns".to_string()),
                state.clone(),
            )
            .await
            .unwrap()
            .unwrap();
            let ns_params = NamespaceParameters {
                prefix: Some(Prefix(wh_id.to_string())),
                namespace: namespace_hierarchy.namespace_ident().clone(),
            };
            wh_ns_data.push((wh_id, namespace_hierarchy.namespace.clone(), ns_params));
        }

        (ctx, wh_ns_data, base_loc)
    }

    macro_rules! impl_pagination_tests {
        ($typ:ident, $setup_fn:ident, $server_typ:ident, $query_typ:ident, $entity_ident:ident, $map_block:expr) => {
            use paste::paste;
            // we're constructing queries via json here to sidestep different query types, going
            // from json to rust doesn't blow up with extra params so we can pass return uuids to
            // list fns that dont support it without having to care about it.
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
                        let (ctx, ns_params) = $setup_fn(pool, 200, &[(95, 150),(195,200)]).await;

                        let mut first_page = $server_typ::[<list_$typ s>](
                            ns_params.clone(),
                             serde_json::from_value::<$query_typ>(serde_json::json!(
                           {
                            "pageSize": 105,
                            "returnUuids": true,
                            }
                            )).unwrap(),
                            ctx.clone(),
                            RequestMetadata::new_unauthenticated(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(first_page.$entity_ident.len(), 105);

                        for i in (0..95).chain(150..160).rev() {
                            assert_eq!(
                                first_page.$entity_ident.pop().map($map_block),
                                Some(format!("{i}"))
                            );
                        }

                        let mut next_page = $server_typ::[<list_$typ s>](
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

                        assert_eq!(next_page.$entity_ident.len(), 35);
                        for i in (160..195).rev() {
                            assert_eq!(
                                next_page.$entity_ident.pop().map($map_block),
                                Some(format!("{i}"))
                            );
                        }
                        assert_eq!(next_page.next_page_token, None);
                    }

                    #[sqlx::test]
                    async fn test_pagination_first_page_is_hidden(pool: PgPool) {
                             let (ctx, ns_params) = $setup_fn(pool, 20, &[(0, 10)]).await;

                        let mut first_page = $server_typ::[<list_$typ s>](
                            ns_params.clone(),
                             serde_json::from_value::<$query_typ>(serde_json::json!(
                           {
                            "pageSize": 10,
                            "returnUuids": true,
                            }
                            )).unwrap(),
                            ctx.clone(),
                            RequestMetadata::new_unauthenticated(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(first_page.$entity_ident.len(), 10);
                        assert!(first_page.next_page_token.is_some());
                        for i in (10..20).rev() {
                            assert_eq!(
                                first_page.$entity_ident.pop().map($map_block),
                                Some(format!("{i}"))
                            );
                        }
                    }

                    #[sqlx::test]
                    async fn test_pagination_middle_page_is_hidden(pool: PgPool) {
                        let (ctx, ns_params) = $setup_fn(pool, 20, &[(5, 15)]).await;

                        let mut first_page = $server_typ::[<list_$typ s>](
                            ns_params.clone(),
                            serde_json::from_value::<$query_typ>(serde_json::json!(
                           {
                            "pageSize": 5,
                            "returnUuids": true,
                            }
                            )).unwrap(),
                            ctx.clone(),
                            RequestMetadata::new_unauthenticated(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(first_page.$entity_ident.len(), 5);

                        for i in (0..5).rev() {
                            assert_eq!(
                                first_page.$entity_ident.pop().map($map_block),
                                Some(format!("{i}"))
                            );
                        }

                        let mut next_page = $server_typ::[<list_$typ s>](
                            ns_params.clone(),
                            serde_json::from_value::<$query_typ>(serde_json::json!(
                           {
                                "pageToken": first_page.next_page_token.unwrap(),
                                "pageSize": 6,
                                "returnUuids": true,
                            }
                            )).unwrap(),
                            ctx.clone(),
                            RequestMetadata::new_unauthenticated(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(next_page.$entity_ident.len(), 5);
                        for i in (15..20).rev() {
                            assert_eq!(
                                next_page.$entity_ident.pop().map($map_block),
                                Some(format!("{i}"))
                            );
                        }
                        assert_eq!(next_page.next_page_token, None);
                    }

                    #[sqlx::test]
                    async fn test_pagination_last_page_is_hidden(pool: PgPool) {
                        let (ctx, ns_params) = $setup_fn(pool, 20, &[(10, 20)]).await;

                        let mut first_page = $server_typ::[<list_$typ s>](
                            ns_params.clone(),
                            serde_json::from_value::<$query_typ>(serde_json::json!(
                           {
                                "pageSize": 10,
                                "returnUuids": true,
                            }
                            )).unwrap(),
                            ctx.clone(),
                            RequestMetadata::new_unauthenticated(),
                        )
                        .await
                        .unwrap();

                        assert_eq!(first_page.$entity_ident.len(), 10);

                        for i in (0..10).rev() {
                            assert_eq!(
                                first_page.$entity_ident.pop().map($map_block),
                                Some(format!("{i}"))
                            );
                        }

                        let next_page = $server_typ::[<list_$typ s>](
                            ns_params.clone(),
                            serde_json::from_value::<$query_typ>(serde_json::json!(
                           {
                                "pageToken": first_page.next_page_token.unwrap(),
                                "pageSize": 11,
                                "returnUuids": true,
                            }
                            )).unwrap(),
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
    pub(crate) use impl_pagination_tests;

    use crate::tests::TestWarehouseResponse;
}
