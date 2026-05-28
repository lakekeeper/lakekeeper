use std::sync::Arc;

use crate::{
    ProjectId,
    api::{
        RequestMetadata,
        management::v1::{
            ApiServer,
            server::{APACHE_LICENSE_STATUS, BootstrapRequest, DEFAULT_BUILD_INFO, Service as _},
            warehouse::{CreateWarehouseRequest, Service as _, TabularDeleteProfile},
        },
    },
    implementations::{
        CatalogState,
        postgres::{PostgresBackend, SecretsState, migrations::migrate_core_only},
    },
    service::{
        ArcProjectId, UserId,
        contract_verification::ContractVerifiers,
        events::EventDispatcher,
        namespace_cache::NamespaceCacheEventListener,
        role_cache::RoleCacheEventListener,
        storage::{StorageCredential, StorageProfile},
        warehouse_cache::WarehouseCacheEventListener,
    },
};

#[cfg(test)]
mod drop_recursive;
#[cfg(test)]
mod drop_warehouse;
#[cfg(test)]
mod endpoint_stats;
#[cfg(test)]
mod namespace_ops;
#[cfg(test)]
mod referenced_by;
#[cfg(test)]
mod role_ops;
#[cfg(test)]
mod soft_deletion;
#[cfg(test)]
mod stats;
#[cfg(test)]
mod tasks;
#[cfg(test)]
mod warehouse_ops;
use crate::{
    CONFIG, WarehouseId,
    api::ApiContext,
    service::{State, authz::Authorizer, tasks::TaskQueueRegistry},
};

#[cfg(test)]
mod internal_helper;
#[cfg(test)]
pub(crate) use internal_helper::*;
use sqlx::PgPool;

#[cfg(feature = "test-utils")]
#[must_use]
pub fn memory_io_profile() -> StorageProfile {
    crate::service::storage::MemoryProfile::default().into()
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
    cancellation_token: crate::CancellationToken,
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
pub(crate) async fn setup<T: Authorizer>(
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
pub(crate) async fn setup_with_registry<T: Authorizer>(
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

/// Backwards-compatible wrapper for callers that don't need the registry.
/// Prefer [`get_api_context_with_registry`] for new tests that need to
/// register additional queues post-bootstrap.
#[allow(dead_code)] // Some call sites are only enabled under specific feature combos.
pub(crate) async fn get_api_context<T: Authorizer>(
    pool: &PgPool,
    auth: T,
) -> ApiContext<State<T, PostgresBackend, SecretsState>> {
    get_api_context_with_registry(pool, auth).await.0
}

/// Like [`get_api_context`] but also returns the `TaskQueueRegistry`.
/// Lets tests register additional queues into the same shared state the
/// returned `ApiContext` reads from.
pub(crate) async fn get_api_context_with_registry<T: Authorizer>(
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
                std::sync::Arc::new(WarehouseCacheEventListener {}),
                std::sync::Arc::new(NamespaceCacheEventListener {}),
                std::sync::Arc::new(RoleCacheEventListener {}),
            ]),
            registered_task_queues,
            license_status: &APACHE_LICENSE_STATUS,
            build_info: &DEFAULT_BUILD_INFO,
        },
    };
    (ctx, task_queues)
}

pub(crate) fn random_request_metadata() -> RequestMetadata {
    RequestMetadata::new_unauthenticated()
}
