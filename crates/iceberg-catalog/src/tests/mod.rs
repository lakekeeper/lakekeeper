use crate::api::iceberg::types::Prefix;
use crate::api::iceberg::v1::namespace::Service as _;
use crate::api::iceberg::v1::tables::TablesService;
use crate::api::iceberg::v1::{DataAccess, NamespaceParameters};
use crate::api::management::v1::bootstrap::{BootstrapRequest, Service as _};
use crate::api::management::v1::warehouse::{
    CreateWarehouseRequest, CreateWarehouseResponse, Service as _, TabularDeleteProfile,
};
use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::catalog::CatalogServer;
use crate::implementations::postgres::task_queues::{TabularExpirationQueue, TabularPurgeQueue};
use crate::implementations::postgres::{CatalogState, PostgresCatalog, ReadWrite, SecretsState};
use crate::request_metadata::RequestMetadata;
use crate::service::authz::Authorizer;
use crate::service::contract_verification::ContractVerifiers;
use crate::service::event_publisher::CloudEventsPublisher;
use crate::service::storage::{
    S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile, TestProfile,
};
use crate::service::task_queue::{TaskQueueConfig, TaskQueues};
use crate::service::{AuthDetails, State, UserId};
use crate::CONFIG;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{
    CreateNamespaceRequest, CreateNamespaceResponse, LoadTableResult,
};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

mod soft_deletions;

pub(crate) fn test_io_profile() -> StorageProfile {
    TestProfile::default().into()
}

#[allow(dead_code)]
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
        random_request_metadata(),
    )
    .await
    .unwrap()
}

pub(crate) async fn create_table<T: Authorizer>(
    api_context: ApiContext<State<T, PostgresCatalog, SecretsState>>,
    prefix: &str,
    ns_name: &str,
    name: &str,
) -> crate::api::Result<LoadTableResult> {
    CatalogServer::create_table(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.to_string())),
            namespace: NamespaceIdent::new(ns_name.to_string()),
        },
        crate::catalog::tables::test::create_request(Some(name.to_string())),
        DataAccess::none(),
        api_context,
        random_request_metadata(),
    )
    .await
}

pub(crate) async fn setup<T: Authorizer>(
    pool: PgPool,
    storage_profile: StorageProfile,
    storage_credential: Option<StorageCredential>,
    authorizer: T,
    delete_profile: TabularDeleteProfile,
    user_id: Option<UserId>,
    q_config: Option<TaskQueueConfig>,
) -> (
    ApiContext<State<T, PostgresCatalog, SecretsState>>,
    CreateWarehouseResponse,
) {
    let api_context = get_api_context(pool, authorizer, q_config);

    let metadata = if let Some(user_id) = user_id {
        RequestMetadata::random_human(user_id)
    } else {
        random_request_metadata()
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
    queue_config: Option<TaskQueueConfig>,
) -> ApiContext<State<T, PostgresCatalog, SecretsState>> {
    let (tx, _) = tokio::sync::mpsc::channel(1000);
    let q_config = queue_config.unwrap_or_else(|| CONFIG.queue_config.clone());
    ApiContext {
        v1_state: State {
            authz: auth,
            catalog: CatalogState::from_pools(pool.clone(), pool.clone()),
            secrets: SecretsState::from_pools(pool.clone(), pool.clone()),
            publisher: CloudEventsPublisher::new(tx.clone()),
            contract_verifiers: ContractVerifiers::new(vec![]),
            queues: TaskQueues::new(
                Arc::new(
                    TabularExpirationQueue::from_config(
                        ReadWrite::from_pools(pool.clone(), pool.clone()),
                        q_config.clone(),
                    )
                    .unwrap(),
                ),
                Arc::new(
                    TabularPurgeQueue::from_config(
                        ReadWrite::from_pools(pool.clone(), pool),
                        q_config,
                    )
                    .unwrap(),
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

pub(crate) fn spawn_drop_queues<T: Authorizer>(
    ctx: &ApiContext<State<T, PostgresCatalog, SecretsState>>,
) {
    let ctx = ctx.clone();
    tokio::task::spawn(async move {
        ctx.clone()
            .v1_state
            .queues
            .spawn_queues::<PostgresCatalog, SecretsState, T>(
                ctx.v1_state.catalog,
                ctx.v1_state.secrets,
                ctx.v1_state.authz,
            )
            .await
            .unwrap();
    });
}
