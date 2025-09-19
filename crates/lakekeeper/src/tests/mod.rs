mod drop_recursive;
mod drop_warehouse;
mod endpoint_stats;
mod stats;
mod tasks;

use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::{
    CreateNamespaceRequest, CreateNamespaceResponse, LoadTableResult, LoadViewResult,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::{
        iceberg::{
            types::Prefix,
            v1::{
                namespace::{NamespaceDropFlags, NamespaceService as _},
                tables::TablesService,
                views::ViewService,
                DataAccess, DropParams, NamespaceParameters, TableParameters,
            },
        },
        management::v1::{
            bootstrap::{BootstrapRequest, Service as _},
            warehouse::{CreateWarehouseRequest, Service as _, TabularDeleteProfile},
            ApiServer,
        },
        ApiContext,
    },
    catalog::CatalogServer,
    implementations::postgres::{CatalogState, PostgresCatalog, SecretsState},
    request_metadata::RequestMetadata,
    service::{
        authz::Authorizer,
        contract_verification::ContractVerifiers,
        endpoint_hooks::EndpointHookCollection,
        storage::{
            s3::S3AccessKeyCredential, MemoryProfile, S3Credential, S3Flavor, S3Profile,
            StorageCredential, StorageProfile,
        },
        task_queue::TaskQueueRegistry,
        Catalog, SecretStore, State, UserId,
    },
    WarehouseId, CONFIG,
};

pub(crate) fn memory_io_profile() -> StorageProfile {
    MemoryProfile::default().into()
}

#[allow(dead_code)]
pub(crate) fn minio_profile() -> (StorageProfile, StorageCredential) {
    let key_prefix = format!("test_prefix-{}", Uuid::now_v7());
    let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap();
    let region = std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into());
    let aws_access_key_id = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap();
    let aws_secret_access_key = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap();
    let endpoint = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT")
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
        .endpoint(endpoint)
        .path_style_access(true)
        .flavor(S3Flavor::S3Compat)
        .region(region)
        .sts_enabled(true)
        .build()
        .into();

    profile.normalize(Some(&cred)).unwrap();
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
    prefix: impl Into<String>,
    ns_name: impl Into<String>,
    name: impl Into<String>,
    stage: bool,
) -> crate::api::Result<LoadTableResult> {
    CatalogServer::create_table(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.into())),
            namespace: NamespaceIdent::new(ns_name.into()),
        },
        crate::catalog::tables::test::create_request(Some(name.into()), Some(stage)),
        DataAccess::not_specified(),
        api_context,
        random_request_metadata(),
    )
    .await
}

pub(crate) async fn drop_table<T: Authorizer>(
    api_context: ApiContext<State<T, PostgresCatalog, SecretsState>>,
    prefix: &str,
    ns_name: &str,
    name: &str,
    purge_requested: Option<bool>,
    force: bool,
) -> crate::api::Result<()> {
    CatalogServer::drop_table(
        TableParameters {
            prefix: Some(Prefix(prefix.to_string())),
            table: TableIdent::new(NamespaceIdent::new(ns_name.to_string()), name.to_string()),
        },
        DropParams {
            purge_requested: purge_requested.unwrap_or_default(),
            force,
        },
        api_context,
        random_request_metadata(),
    )
    .await
}

pub(crate) async fn create_view<T: Authorizer>(
    api_context: ApiContext<State<T, PostgresCatalog, SecretsState>>,
    prefix: &str,
    ns_name: &str,
    name: &str,
    location: Option<&str>,
) -> crate::api::Result<LoadViewResult> {
    CatalogServer::create_view(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.to_string())),
            namespace: NamespaceIdent::new(ns_name.to_string()),
        },
        crate::catalog::views::create::test::create_view_request(Some(name), location),
        api_context,
        DataAccess::not_specified(),
        random_request_metadata(),
    )
    .await
}

pub(crate) async fn drop_namespace<A: Authorizer, C: Catalog, S: SecretStore>(
    api_context: ApiContext<State<A, C, S>>,
    flags: NamespaceDropFlags,
    namespace_parameters: NamespaceParameters,
) -> crate::api::Result<()> {
    CatalogServer::drop_namespace(
        namespace_parameters,
        flags,
        api_context,
        random_request_metadata(),
    )
    .await
}

#[derive(Debug)]
pub struct TestWarehouseResponse {
    pub warehouse_id: WarehouseId,
    pub warehouse_name: String,
    pub additional_warehouses: Vec<(WarehouseId, String)>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn setup<T: Authorizer>(
    pool: PgPool,
    storage_profile: StorageProfile,
    storage_credential: Option<StorageCredential>,
    authorizer: T,
    delete_profile: TabularDeleteProfile,
    user_id: Option<UserId>,
    number_of_warehouses: usize,
) -> (
    ApiContext<State<T, PostgresCatalog, SecretsState>>,
    TestWarehouseResponse,
) {
    assert!(
        number_of_warehouses > 0,
        "Number of warehouses must be greater than 0",
    );

    let api_context = get_api_context(&pool, authorizer).await;

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
    let warehouse_name = format!("test-warehouse-{}", Uuid::now_v7());
    let warehouse = ApiServer::create_warehouse(
        CreateWarehouseRequest {
            warehouse_name: warehouse_name.clone(),
            project_id: None,
            storage_profile,
            storage_credential,
            delete_profile,
        },
        api_context.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();
    let mut additional_warehouses = vec![];
    for i in 1..number_of_warehouses {
        let warehouse_name = format!("test-warehouse-{}-{}", i, Uuid::now_v7());
        let warehouse = ApiServer::create_warehouse(
            CreateWarehouseRequest {
                warehouse_name: warehouse_name.clone(),
                project_id: None,
                storage_profile: memory_io_profile(),
                storage_credential: None,
                delete_profile,
            },
            api_context.clone(),
            metadata.clone(),
        )
        .await
        .unwrap();
        additional_warehouses.push((warehouse.warehouse_id, warehouse_name.clone()));
    }
    (
        api_context,
        TestWarehouseResponse {
            warehouse_id: warehouse.warehouse_id,
            warehouse_name,
            additional_warehouses,
        },
    )
}

pub(crate) async fn get_api_context<T: Authorizer>(
    pool: &PgPool,
    auth: T,
) -> ApiContext<State<T, PostgresCatalog, SecretsState>> {
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    let secret_store = SecretsState::from_pools(pool.clone(), pool.clone());

    let task_queues = TaskQueueRegistry::new();
    task_queues
        .register_built_in_queues::<PostgresCatalog, _, _>(
            catalog_state.clone(),
            secret_store.clone(),
            auth.clone(),
            CONFIG.task_poll_interval,
        )
        .await;
    let registered_task_queues = task_queues.registered_task_queues();
    ApiContext {
        v1_state: State {
            authz: auth,
            catalog: catalog_state,
            secrets: secret_store,
            contract_verifiers: ContractVerifiers::new(vec![]),
            hooks: EndpointHookCollection::new(vec![]),
            registered_task_queues,
        },
    }
}

pub(crate) fn random_request_metadata() -> RequestMetadata {
    RequestMetadata::new_unauthenticated()
}

pub(crate) async fn spawn_build_in_queues<T: Authorizer>(
    ctx: &ApiContext<State<T, PostgresCatalog, SecretsState>>,
    poll_interval: Option<std::time::Duration>,
    cancellation_token: crate::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let task_queues = TaskQueueRegistry::new();
    task_queues
        .register_built_in_queues::<PostgresCatalog, _, _>(
            ctx.v1_state.catalog.clone(),
            ctx.v1_state.secrets.clone(),
            ctx.v1_state.authz.clone(),
            poll_interval.unwrap_or(CONFIG.task_poll_interval),
        )
        .await;
    let task_runner = task_queues.task_queues_runner(cancellation_token).await;

    tokio::task::spawn(task_runner.run_queue_workers(true))
}
