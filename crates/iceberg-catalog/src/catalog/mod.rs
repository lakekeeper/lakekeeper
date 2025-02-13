pub(crate) mod commit_tables;
pub(crate) mod compression_codec;
mod config;
pub(crate) mod io;
mod metrics;
pub(crate) mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
pub(crate) mod tables;
pub(crate) mod tabular;
pub(crate) mod views;

use std::{collections::HashMap, fmt::Debug, marker::PhantomData};

use futures::future::BoxFuture;
use iceberg::spec::{TableMetadata, ViewMetadata};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use itertools::{FoldWhile, Itertools};
pub use namespace::{MAX_NAMESPACE_DEPTH, NAMESPACE_ID_PROPERTY, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::{
    api::{
        iceberg::v1::{PageToken, Prefix, MAX_PAGE_SIZE},
        ErrorModel, Result,
    },
    service::{authz::Authorizer, secrets::SecretStore, storage::StorageCredential, Catalog},
    WarehouseIdent,
};

pub trait CommonMetadata {
    fn properties(&self) -> &HashMap<String, String>;
}

impl CommonMetadata for TableMetadata {
    fn properties(&self) -> &HashMap<String, String> {
        TableMetadata::properties(self)
    }
}

impl CommonMetadata for ViewMetadata {
    fn properties(&self) -> &HashMap<String, String> {
        ViewMetadata::properties(self)
    }
}

#[derive(Clone, Debug)]

pub struct CatalogServer<C: Catalog, A: Authorizer + Clone, S: SecretStore> {
    auth_handler: PhantomData<A>,
    catalog_backend: PhantomData<C>,
    secret_store: PhantomData<S>,
}

fn require_warehouse_id(prefix: Option<Prefix>) -> Result<WarehouseIdent> {
    prefix
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message(
                    "No prefix specified. The warehouse-id must be provided as prefix in the URL."
                        .to_string(),
                )
                .r#type("NoPrefixProvided".to_string())
                .build(),
        )?
        .try_into()
}

pub(crate) async fn maybe_get_secret<S: SecretStore>(
    secret: Option<crate::SecretIdent>,
    state: &S,
) -> Result<Option<StorageCredential>, IcebergErrorResponse> {
    if let Some(secret_id) = &secret {
        Ok(Some(state.get_secret_by_id(secret_id).await?.secret))
    } else {
        Ok(None)
    }
}

pub const DEFAULT_PAGE_SIZE: i64 = 100;

lazy_static::lazy_static! {
    pub static ref DEFAULT_PAGE_SIZE_USIZE: usize = DEFAULT_PAGE_SIZE.try_into().expect("1, 1000 is a valid usize");
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

pub(crate) async fn fetch_until_full_page<'b, 'd: 'b, Entity, EntityId, FetchFun, C: Catalog>(
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
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page_as_usize: usize = page_size.try_into().expect("1, 1000 is a valid usize");

    let page_token = page_token.as_option().map(ToString::to_string);
    let unfiltered_page = fetch_fn(page_size, page_token, transaction).await?;

    if unfiltered_page.is_partial() && !unfiltered_page.has_authz_denied_items() {
        return Ok((unfiltered_page.entities, unfiltered_page.entity_ids, None));
    }

    let (mut entities, mut entity_ids, mut next_page_token) =
        unfiltered_page.take_n_authz_approved(page_as_usize);

    while entities.len() < page_as_usize {
        let new_unfiltered_page =
            fetch_fn(DEFAULT_PAGE_SIZE, next_page_token.clone(), transaction).await?;

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
#[allow(dead_code)]
pub(crate) mod test {
    use std::sync::Arc;

    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::{CreateNamespaceRequest, CreateNamespaceResponse};
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        api::{
            iceberg::{types::Prefix, v1::namespace::Service},
            management::v1::{
                bootstrap::{BootstrapRequest, Service as _},
                warehouse::{
                    CreateWarehouseRequest, CreateWarehouseResponse, Service as _,
                    TabularDeleteProfile,
                },
                ApiServer,
            },
            ApiContext,
        },
        catalog::CatalogServer,
        implementations::postgres::{CatalogState, PostgresCatalog, ReadWrite, SecretsState},
        request_metadata::RequestMetadata,
        service::{
            authz::Authorizer,
            contract_verification::ContractVerifiers,
            event_publisher::CloudEventsPublisher,
            storage::{
                S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile, TestProfile,
            },
            task_queue::TaskQueues,
            State, UserId,
        },
        CONFIG,
    };

    pub(crate) fn test_io_profile() -> StorageProfile {
        TestProfile::default().into()
    }

    pub(crate) fn minio_profile() -> (StorageProfile, StorageCredential) {
        let key_prefix = Some(format!("test_prefix-{}", Uuid::now_v7()));
        let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap();
        let region = std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into());
        let aws_access_key_id = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap();
        let aws_secret_access_key = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap();
        let endpoint = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT")
            .unwrap()
            .parse()
            .unwrap();

        let cred: StorageCredential = S3Credential::AccessKey {
            aws_access_key_id,
            aws_secret_access_key,
        }
        .into();

        let mut profile: StorageProfile = S3Profile {
            bucket,
            key_prefix,
            assume_role_arn: None,
            endpoint: Some(endpoint),
            region,
            path_style_access: Some(true),
            sts_role_arn: None,
            flavor: S3Flavor::S3Compat,
            sts_enabled: true,
        }
        .into();

        profile.normalize().unwrap();
        (profile, cred)
    }

    pub(crate) async fn create_ns<T: Authorizer>(
        api_context: ApiContext<State<T, PostgresCatalog, SecretsState>>,
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
        ApiContext<State<T, PostgresCatalog, SecretsState>>,
        CreateWarehouseResponse,
    ) {
        let api_context = get_api_context(pool, authorizer);
        let _state = api_context.v1_state.catalog.clone();
        let metadata = if let Some(user_id) = user_id {
            RequestMetadata::random_human(user_id)
        } else {
            RequestMetadata::random_human(UserId::new_unchecked(
                "oidc",
                &uuid::Uuid::now_v7().to_string(),
            ))
        };
        ApiServer::bootstrap(
            api_context.clone(),
            metadata.clone(),
            BootstrapRequest {
                accept_terms_of_use: true,
                is_operator: true,
                user_name: None,
                user_email: None,
                user_type: None,
            },
        )
        .await
        .unwrap();
        let warehouse = ApiServer::create_warehouse(
            CreateWarehouseRequest {
                warehouse_name: format!("test-warehouse-{}", Uuid::now_v7()),
                project_id: None,
                storage_profile,
                storage_credential,
                delete_profile,
            },
            api_context.clone(),
            metadata,
        )
        .await
        .unwrap();

        (api_context, warehouse)
    }

    pub(crate) fn get_api_context<T: Authorizer>(
        pool: PgPool,
        auth: T,
    ) -> ApiContext<State<T, PostgresCatalog, SecretsState>> {
        let (tx, _) = tokio::sync::mpsc::channel(1000);

        ApiContext {
            v1_state: State {
                authz: auth,
                catalog: CatalogState::from_pools(pool.clone(), pool.clone()),
                secrets: SecretsState::from_pools(pool.clone(), pool.clone()),
                publisher: CloudEventsPublisher::new(tx.clone()),
                contract_verifiers: ContractVerifiers::new(vec![]),
                queues: TaskQueues::new(
                    Arc::new(
                        crate::implementations::postgres::task_queues::TabularExpirationQueue::from_config(ReadWrite::from_pools(pool.clone(), pool.clone()), CONFIG.queue_config.clone()).unwrap(),
                    ),
                    Arc::new(
                        crate::implementations::postgres::task_queues::TabularPurgeQueue::from_config(ReadWrite::from_pools(pool.clone(), pool), CONFIG.queue_config.clone()).unwrap()
                    ),
                ),
            },
        }
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
}
