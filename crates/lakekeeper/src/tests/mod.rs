use std::sync::Arc;

use crate::{
    api::{
        management::v1::{
            server::{BootstrapRequest, Service as _, APACHE_LICENSE_STATUS},
            warehouse::{CreateWarehouseRequest, Service as _, TabularDeleteProfile},
            ApiServer,
        },
        RequestMetadata,
    },
    implementations::{
        postgres::{migrations::migrate, PostgresBackend, SecretsState},
        CatalogState,
    },
    service::{
        contract_verification::ContractVerifiers,
        endpoint_hooks::EndpointHookCollection,
        storage::{StorageCredential, StorageProfile},
        warehouse_cache::WarehouseCacheEndpointHook,
        UserId,
    },
};

#[cfg(test)]
mod drop_recursive;
#[cfg(test)]
mod drop_warehouse;
#[cfg(test)]
mod endpoint_stats;
#[cfg(test)]
mod soft_deletion;
#[cfg(test)]
mod stats;
#[cfg(test)]
mod tasks;
#[cfg(test)]
mod warehouse_ops;
use crate::{
    api::ApiContext,
    service::{authz::Authorizer, tasks::TaskQueueRegistry, State},
    WarehouseId, CONFIG,
};

#[cfg(test)]
mod internal_helper;
#[cfg(test)]
pub(crate) use internal_helper::*;
use sqlx::PgPool;
use uuid::Uuid;

#[cfg(feature = "test-utils")]
#[must_use]
pub fn memory_io_profile() -> StorageProfile {
    crate::service::storage::MemoryProfile::default().into()
}

#[derive(Debug)]
pub struct TestWarehouseResponse {
    pub warehouse_id: WarehouseId,
    pub warehouse_name: String,
    pub additional_warehouses: Vec<(WarehouseId, String)>,
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
) -> (
    ApiContext<State<T, PostgresBackend, SecretsState>>,
    TestWarehouseResponse,
) {
    assert!(
        number_of_warehouses > 0,
        "Number of warehouses must be greater than 0",
    );
    migrate(&pool).await.unwrap();
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
        let create_wh_response = ApiServer::create_warehouse(
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
        additional_warehouses.push((create_wh_response.warehouse_id(), warehouse_name.clone()));
    }
    (
        api_context,
        TestWarehouseResponse {
            warehouse_id: warehouse.warehouse_id(),
            warehouse_name,
            additional_warehouses,
        },
    )
}

pub(crate) async fn get_api_context<T: Authorizer>(
    pool: &PgPool,
    auth: T,
) -> ApiContext<State<T, PostgresBackend, SecretsState>> {
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
    ApiContext {
        v1_state: State {
            authz: auth,
            catalog: catalog_state,
            secrets: secret_store,
            contract_verifiers: ContractVerifiers::new(vec![]),
            hooks: EndpointHookCollection::new(vec![Arc::new(WarehouseCacheEndpointHook {})]),
            registered_task_queues,
            license_status: &APACHE_LICENSE_STATUS,
        },
    }
}

pub(crate) fn random_request_metadata() -> RequestMetadata {
    RequestMetadata::new_unauthenticated()
}
