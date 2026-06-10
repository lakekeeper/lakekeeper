//! Postgres-pinned test helpers for exercising the lakekeeper service layer.
//!
//! Lives here (rather than in `lakekeeper-integration-tests`) so that
//! `lakekeeper-storage-postgres`'s own inline tests can use them without
//! creating a dev-dep cycle. The integration-tests crate re-exports from
//! here for downstream test files.

use std::sync::Arc;

use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use lakekeeper::{
    CONFIG, ProjectId, WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{NamespaceParameters, namespace::NamespaceService},
        },
        management::v1::{
            ApiServer,
            server::{APACHE_LICENSE_STATUS, BootstrapRequest, DEFAULT_BUILD_INFO, Service as _},
            warehouse::{CreateWarehouseRequest, Service as _, TabularDeleteProfile},
        },
    },
    server::CatalogServer,
    service::{
        ArcProjectId, CatalogNamespaceOps, CreateNamespaceResponse, NamespaceWithParent, State,
        UserId,
        authz::{AllowAllAuthorizer, Authorizer},
        contract_verification::ContractVerifiers,
        events::EventDispatcher,
        namespace_cache::NamespaceCacheEventListener,
        role_cache::RoleCacheEventListener,
        storage::{
            S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile,
            s3::S3AccessKeyCredential,
        },
        tasks::TaskQueueRegistry,
        warehouse_cache::WarehouseCacheEventListener,
    },
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{CatalogState, PostgresBackend, SecretsState, migrations::migrate_core_only};

#[must_use]
pub fn memory_io_profile() -> StorageProfile {
    lakekeeper::service::storage::MemoryProfile::default().into()
}

#[derive(Debug)]
pub struct TestWarehouseResponse {
    pub warehouse_id: WarehouseId,
    pub project_id: ArcProjectId,
    pub warehouse_name: String,
    pub additional_warehouses: Vec<(ArcProjectId, WarehouseId, String)>,
}

pub async fn spawn_build_in_queues<T: Authorizer>(
    ctx: &ApiContext<State<T, PostgresBackend, SecretsState>>,
    poll_interval: Option<std::time::Duration>,
    cancellation_token: lakekeeper::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let task_queues = TaskQueueRegistry::new();
    task_queues
        .register_built_in_queues::<PostgresBackend, _, _>(
            ctx.v1_state.catalog.clone(),
            ctx.v1_state.secrets.clone(),
            ctx.v1_state.authz.clone(),
            poll_interval.unwrap_or(CONFIG.task_poll_interval),
        )
        .await;
    let task_runner = task_queues.task_queues_runner(cancellation_token).await;

    tokio::task::spawn(task_runner.run_queue_workers(true))
}

#[derive(typed_builder::TypedBuilder, Debug)]
pub struct SetupTestCatalog<T: Authorizer> {
    pool: PgPool,
    #[builder(default = StorageProfile::Memory(Default::default()))]
    storage_profile: StorageProfile,
    authorizer: T,
    #[builder(default = TabularDeleteProfile::Hard {})]
    delete_profile: TabularDeleteProfile,
    #[builder(default)]
    user_id: Option<UserId>,
    #[builder(default = 1)]
    number_of_warehouses: usize,
    #[builder(default)]
    project_id: Option<ArcProjectId>,
}

impl<T: Authorizer> SetupTestCatalog<T> {
    pub async fn setup(
        self,
    ) -> (
        ApiContext<State<T, PostgresBackend, SecretsState>>,
        TestWarehouseResponse,
    ) {
        setup(
            self.pool,
            self.storage_profile,
            None,
            self.authorizer,
            self.delete_profile,
            self.user_id,
            self.number_of_warehouses,
            self.project_id.map(Arc::unwrap_or_clone),
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn setup<T: Authorizer>(
    pool: PgPool,
    storage_profile: StorageProfile,
    storage_credential: Option<StorageCredential>,
    authorizer: T,
    delete_profile: TabularDeleteProfile,
    user_id: Option<UserId>,
    number_of_warehouses: usize,
    project_id: Option<ProjectId>,
) -> (
    ApiContext<State<T, PostgresBackend, SecretsState>>,
    TestWarehouseResponse,
) {
    let (ctx, warehouse, _registry) = setup_with_registry(
        pool,
        storage_profile,
        storage_credential,
        authorizer,
        delete_profile,
        user_id,
        number_of_warehouses,
        project_id,
    )
    .await;
    (ctx, warehouse)
}

/// Like [`setup`] but also returns the `TaskQueueRegistry` so tests can
/// register additional queues (e.g. a `user_schedulable=true` fixture for
/// scheduling-endpoint lifecycle tests). The registry shares interior-mutable
/// state with the `RegisteredTaskQueues` inside the returned `ApiContext`,
/// so a later `register_queue` call is visible to subsequent endpoint
/// invocations on `ctx` — no rebuild needed.
#[allow(clippy::too_many_arguments)]
pub async fn setup_with_registry<T: Authorizer>(
    pool: PgPool,
    storage_profile: StorageProfile,
    storage_credential: Option<StorageCredential>,
    authorizer: T,
    delete_profile: TabularDeleteProfile,
    user_id: Option<UserId>,
    number_of_warehouses: usize,
    project_id: Option<ProjectId>,
) -> (
    ApiContext<State<T, PostgresBackend, SecretsState>>,
    TestWarehouseResponse,
    TaskQueueRegistry,
) {
    assert!(
        number_of_warehouses > 0,
        "Number of warehouses must be greater than 0",
    );
    migrate_core_only(&pool).await.unwrap();
    let (api_context, registry) = get_api_context_with_registry(&pool, authorizer).await;

    let metadata = if let Some(user_id) = user_id {
        RequestMetadata::test_user(user_id)
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
    let warehouse_name = format!("test-warehouse-{}", uuid::Uuid::now_v7());
    let warehouse = ApiServer::create_warehouse(
        CreateWarehouseRequest {
            warehouse_name: warehouse_name.clone(),
            project_id,
            storage_profile,
            storage_credential,
            delete_profile,
            allowed_format_versions: None,
            default_format_version: None,
            managed_by: Default::default(),
        },
        api_context.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();
    let mut additional_warehouses = vec![];
    for i in 1..number_of_warehouses {
        let warehouse_name = format!("test-warehouse-{}-{}", i, uuid::Uuid::now_v7());
        let create_wh_response = ApiServer::create_warehouse(
            CreateWarehouseRequest {
                warehouse_name: warehouse_name.clone(),
                project_id: Some(Arc::unwrap_or_clone(warehouse.project_id())),
                storage_profile: memory_io_profile(),
                storage_credential: None,
                delete_profile,
                allowed_format_versions: None,
                default_format_version: None,
                managed_by: Default::default(),
            },
            api_context.clone(),
            metadata.clone(),
        )
        .await
        .unwrap();
        additional_warehouses.push((
            create_wh_response.project_id(),
            create_wh_response.warehouse_id(),
            warehouse_name.clone(),
        ));
    }
    (
        api_context,
        TestWarehouseResponse {
            project_id: warehouse.project_id(),
            warehouse_id: warehouse.warehouse_id(),
            warehouse_name,
            additional_warehouses,
        },
        registry,
    )
}

pub async fn get_api_context<T: Authorizer>(
    pool: &PgPool,
    auth: T,
) -> ApiContext<State<T, PostgresBackend, SecretsState>> {
    get_api_context_with_registry(pool, auth).await.0
}

pub async fn get_api_context_with_registry<T: Authorizer>(
    pool: &PgPool,
    auth: T,
) -> (
    ApiContext<State<T, PostgresBackend, SecretsState>>,
    TaskQueueRegistry,
) {
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    let secret_store = SecretsState::from_pools(pool.clone(), pool.clone());

    let task_queues = TaskQueueRegistry::new();
    task_queues
        .register_built_in_queues::<PostgresBackend, _, _>(
            catalog_state.clone(),
            secret_store.clone(),
            auth.clone(),
            CONFIG.task_poll_interval,
        )
        .await;
    let registered_task_queues = task_queues.registered_task_queues();
    let ctx = ApiContext {
        v1_state: State {
            authz: auth,
            catalog: catalog_state,
            secrets: secret_store,
            contract_verifiers: ContractVerifiers::new(vec![]),
            events: EventDispatcher::new(vec![
                Arc::new(WarehouseCacheEventListener {}),
                Arc::new(NamespaceCacheEventListener {}),
                Arc::new(RoleCacheEventListener {}),
            ]),
            registered_task_queues,
            license_status: &APACHE_LICENSE_STATUS,
            build_info: &DEFAULT_BUILD_INFO,
        },
    };
    (ctx, task_queues)
}

#[must_use]
pub fn random_request_metadata() -> RequestMetadata {
    RequestMetadata::new_unauthenticated()
}

#[allow(dead_code)] // Only used by tests that opt into S3 integration.
pub fn s3_compatible_profile() -> (StorageProfile, StorageCredential) {
    let key_prefix = format!("test_prefix-{}", Uuid::now_v7());
    let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap();
    let region = std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into());
    let access_key_id = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap();
    let secret_access_key = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap();
    let endpoint: url::Url = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT")
        .unwrap()
        .parse()
        .unwrap();

    let cred: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
        access_key_id,
        secret_access_key,
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

pub async fn create_ns<T: Authorizer>(
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

/// Sets up `num_warehouses` in the same project and creates one namespace
/// in each. The shared base location of the storage profile is returned as
/// the third tuple element so tests can build expected URLs.
pub async fn tabular_test_multi_warehouse_setup(
    pool: PgPool,
    num_warehouses: usize,
    delete_profile: TabularDeleteProfile,
) -> (
    ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
    Vec<(WarehouseId, NamespaceWithParent, NamespaceParameters)>,
    String,
) {
    let prof = memory_io_profile();
    let base_loc = prof.base_location().unwrap().to_string();
    let (ctx, res) = setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer::default(),
        delete_profile,
        None,
        num_warehouses,
        None,
    )
    .await;

    let mut wh_ids = Vec::with_capacity(num_warehouses);
    wh_ids.push(res.warehouse_id);
    for (_, wh_id, _) in &res.additional_warehouses {
        wh_ids.push(*wh_id);
    }
    assert_eq!(wh_ids.len(), num_warehouses);

    let mut wh_ns_data = Vec::with_capacity(num_warehouses);
    let state = CatalogState::from_pools(pool.clone(), pool.clone());
    for wh_id in wh_ids {
        create_ns(ctx.clone(), wh_id.to_string(), "myns".to_string()).await;
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
