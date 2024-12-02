pub(crate) mod commit_tables;
pub(crate) mod compression_codec;
mod config;
pub(crate) mod io;
mod metrics;
pub(crate) mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
pub(crate) mod tables;
mod tabular;
pub(crate) mod views;

use iceberg::spec::{TableMetadata, ViewMetadata};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
pub use namespace::{MAX_NAMESPACE_DEPTH, NAMESPACE_ID_PROPERTY, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::api::iceberg::v1::{PageToken, MAX_PAGE_SIZE};
use crate::api::{iceberg::v1::Prefix, ErrorModel, Result};
use crate::service::storage::StorageCredential;
use crate::{
    service::{authz::Authorizer, secrets::SecretStore, Catalog},
    WarehouseIdent,
};
use futures::future::BoxFuture;
use itertools::{FoldWhile, Itertools};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

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

#[derive(Debug)]
pub struct FetchedStuff<Entity, EntityId> {
    pub entities: Vec<Entity>,
    pub entity_ids: Vec<EntityId>,
    pub page_tokens: Vec<String>,
    pub authz_mask: Vec<bool>,
    pub n_filtered: usize,
    pub page_size: usize,
}

impl<Entity, EntityId> FetchedStuff<Entity, EntityId> {
    #[must_use]
    pub(crate) fn new(
        entities: Vec<Entity>,
        entity_ids: Vec<EntityId>,
        page_tokens: Vec<String>,
        authz_mask: Vec<bool>,
        page_size: usize,
    ) -> Self {
        let n_filtered = authz_mask.iter().map(|b| usize::from(*b)).sum();
        Self {
            entities,
            entity_ids,
            page_tokens,
            authz_mask,
            n_filtered,
            page_size,
        }
    }

    #[must_use]
    pub(crate) fn take_n(self, n: usize) -> (Vec<Entity>, Vec<EntityId>, Option<String>) {
        #[derive(Debug)]
        enum State {
            Open,
            LoopingForLastNextPage,
        }
        let (entities, ids, token, _) = self
            .authz_mask
            .into_iter()
            .zip(self.entities)
            .zip(self.entity_ids)
            .zip(self.page_tokens)
            .fold_while(
                (vec![], vec![], None, State::Open),
                |(mut entities, mut entity_ids, mut page_token, mut state),
                 (((authz, entity), id), token)| {
                    eprintln!("state: {state:?}");
                    if authz {
                        if matches!(state, State::Open) {
                            entities.push(entity);
                            entity_ids.push(id);
                            page_token = Some(token);
                        } else if matches!(state, State::LoopingForLastNextPage) {
                            return FoldWhile::Done((entities, entity_ids, page_token, state));
                        }
                    } else if !authz {
                        page_token = Some(token);
                    }
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
    pub(crate) fn is_partial(&self) -> bool {
        self.entities.len() < self.page_size
    }

    #[must_use]
    pub(crate) fn is_filtered(&self) -> bool {
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
    ) -> BoxFuture<'c, Result<FetchedStuff<Entity, EntityId>>>,
    // you may feel tempted to change the Vec<String> of page-tokens to Option<String>
    // a word of advice: don't, we need to take the nth page-token of the next page when
    // we're filling a auth-filtered page. Without a vec, that won't fly.
{
    let page_size = page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page_as_usize: usize = page_size.try_into().expect("1, 1000 is a valid usize");

    let page_token = page_token.as_option().map(ToString::to_string);
    let fetched_stuff = fetch_fn(page_size, page_token, transaction).await?;

    if fetched_stuff.is_partial() && !fetched_stuff.is_filtered() {
        return Ok((fetched_stuff.entities, fetched_stuff.entity_ids, None));
    }

    let (mut entities, mut entity_ids, mut next_page) = fetched_stuff.take_n(page_as_usize);

    while entities.len() < page_as_usize {
        let new_page = fetch_fn(DEFAULT_PAGE_SIZE, next_page.clone(), transaction).await?;

        if new_page.is_partial() && !new_page.is_filtered() {
            entities.extend(new_page.entities);
            entity_ids.extend(new_page.entity_ids);
            next_page = None;
            break;
        }

        let (more_entities, more_ids, n_page) = new_page.take_n(page_as_usize - entities.len());
        next_page = n_page;
        entities.extend(more_entities);
        entity_ids.extend(more_ids);
    }

    Ok((entities, entity_ids, next_page))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod test {
    use crate::api::iceberg::types::Prefix;
    use crate::api::iceberg::v1::namespace::Service;
    use crate::api::management::v1::project::{CreateProjectRequest, Service as _};
    use crate::api::management::v1::warehouse::{
        CreateWarehouseRequest, CreateWarehouseResponse, Service as _, TabularDeleteProfile,
    };
    use crate::api::management::v1::ApiServer;
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;
    use crate::implementations::postgres::{
        CatalogState, PostgresCatalog, ReadWrite, SecretsState,
    };
    use crate::request_metadata::RequestMetadata;
    use crate::service::authz::Authorizer;
    use crate::service::contract_verification::ContractVerifiers;
    use crate::service::event_publisher::CloudEventsPublisher;
    use crate::service::storage::{
        S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile, TestProfile,
    };
    use crate::service::task_queue::TaskQueues;
    use crate::service::{AuthDetails, State};
    use crate::CONFIG;
    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::{CreateNamespaceRequest, CreateNamespaceResponse};
    use sqlx::PgPool;
    use std::sync::Arc;
    use uuid::Uuid;

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
            flavor: S3Flavor::Minio,
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
            random_request_metadata(),
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
    ) -> (
        ApiContext<State<T, PostgresCatalog, SecretsState>>,
        CreateWarehouseResponse,
    ) {
        let api_context = get_api_context(pool, authorizer);
        let _state = api_context.v1_state.catalog.clone();
        let proj = ApiServer::create_project(
            CreateProjectRequest {
                project_name: format!("test-project-{}", Uuid::now_v7()),
                project_id: Some(Uuid::now_v7()),
            },
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        let warehouse = ApiServer::create_warehouse(
            CreateWarehouseRequest {
                warehouse_name: format!("test-warehouse-{}", Uuid::now_v7()),
                project_id: Some(proj.project_id),
                storage_profile,
                storage_credential,
                delete_profile,
            },
            api_context.clone(),
            random_request_metadata(),
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

    pub(crate) fn random_request_metadata() -> RequestMetadata {
        RequestMetadata {
            request_id: Uuid::new_v4(),
            auth_details: AuthDetails::Unauthenticated,
        }
    }
}
