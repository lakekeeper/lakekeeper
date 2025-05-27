use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{Debug, Formatter},
    ops::Deref,
    sync::{Arc, LazyLock},
    time::Duration,
};

use chrono::Utc;
use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Serialize};
use strum::EnumIter;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{authz::Authorizer, Transaction, WarehouseId};
use crate::service::{
    task_queue::{
        tabular_expiration_queue::ExpirationQueueConfig, tabular_purge_queue::PurgeQueueConfig,
    },
    Catalog, SecretStore,
};

pub mod tabular_expiration_queue;
pub mod tabular_purge_queue;

pub const DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT: chrono::Duration =
    valid_max_time_since_last_heartbeat(3600);
pub const DEFAULT_MAX_RETRIES: i32 = 5;
pub const DEFAULT_NUM_WORKERS: usize = 2;
#[allow(clippy::declare_interior_mutable_const)]
pub static BUILT_IN_API_CONFIGS: LazyLock<Vec<QueueApiConfig>> = LazyLock::new(|| {
    vec![
        tabular_expiration_queue::API_CONFIG.clone(),
        tabular_purge_queue::API_CONFIG.clone(),
    ]
});

/// Infinitely running task worker loop function that polls tasks from a queue and
/// processes.
pub type TaskQueueWorker = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>;
type ValidatorFn = Arc<dyn Fn(serde_json::Value) -> serde_json::Result<()> + Send + Sync>;

/// Warehouse specific configuration for a task queue.
pub trait QueueConfig: ToSchema + Serialize + DeserializeOwned {
    fn max_time_since_last_heartbeat(&self) -> chrono::Duration {
        DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT
    }
    fn max_retries(&self) -> i32 {
        DEFAULT_MAX_RETRIES
    }
    fn num_workers(&self) -> usize {
        DEFAULT_NUM_WORKERS
    }
}

/// A container for registered task queues that can be used for validation and API configuration.
/// This can be included in the Axum application state.
#[derive(Clone, Default, Debug)]
pub struct RegisteredTaskQueues {
    // Mapping of queue names to their configurations
    queues: Arc<HashMap<&'static str, RegisteredQueue>>,
}

impl RegisteredTaskQueues {
    /// Get the validator function for a queue by name
    ///
    /// # Returns
    /// Some(ValidatorFn) if the queue exists, None otherwise
    #[must_use]
    pub fn validate_config_fn(&self, queue_name: &str) -> Option<ValidatorFn> {
        self.queues
            .get(queue_name)
            .map(|q| Arc::clone(&q.schema_validator_fn))
    }

    /// Get the API configuration for all registered queues
    #[must_use]
    pub fn api_config(&self) -> Vec<&QueueApiConfig> {
        self.queues.values().map(|q| &q.api_config).collect()
    }

    /// Get the names of all registered queues
    #[must_use]
    pub fn queue_names(&self) -> Vec<&'static str> {
        self.queues.keys().copied().collect()
    }
}

#[derive(Clone)]
struct RegisteredQueue {
    /// API configuration for this queue
    api_config: QueueApiConfig,
    /// Schema validator function for the queue configuration
    /// This function is called to validate the configuration payload
    schema_validator_fn: ValidatorFn,
}

impl Debug for RegisteredQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredQueue")
            .field("api_config", &self.api_config)
            .field("schema_validator_fn", &"Fn(...)")
            .finish()
    }
}

#[derive(Clone)]
struct RegisteredTaskQueueWorker {
    worker_fn: TaskQueueWorker,
    /// Number of workers that run locally for this queue
    num_workers: usize,
}

impl Debug for RegisteredTaskQueueWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredTaskQueueWorker")
            .field("worker_fn", &"Fn(...)")
            .field("num_workers", &self.num_workers)
            .finish()
    }
}

/// Task queue registry used for registering and starting task queues
#[derive(Debug)]
pub struct TaskQueueRegistry {
    // Mapping of queue names to their configurations
    registered_queues: HashMap<&'static str, RegisteredQueue>,
    // Mapping of queue names to their worker configuration
    task_workers: HashMap<&'static str, RegisteredTaskQueueWorker>,
}

impl Default for TaskQueueRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueueRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            registered_queues: HashMap::new(),
            task_workers: HashMap::new(),
        }
    }

    pub fn register_queue<T: QueueConfig>(
        &mut self,
        queue_name: &'static str,
        worker_fn: TaskQueueWorker,
        num_workers: usize,
    ) -> &mut Self {
        let schema_validator_fn = |v| serde_json::from_value::<T>(v).map(|_| ());
        let schema_validator_fn = Arc::new(schema_validator_fn) as ValidatorFn;
        let api_config = QueueApiConfig {
            queue_name,
            utoipa_type_name: T::name().to_string().into(),
            utoipa_schema: utoipa::openapi::RefOr::Ref(utoipa::openapi::Ref::from_schema_name(
                T::name(),
            )),
        };

        self.registered_queues.insert(
            queue_name,
            RegisteredQueue {
                api_config,
                schema_validator_fn,
            },
        );

        self.task_workers.insert(
            queue_name,
            RegisteredTaskQueueWorker {
                worker_fn,
                num_workers,
            },
        );
        self
    }

    pub fn register_built_in_queues<C: Catalog, S: SecretStore, A: Authorizer>(
        &mut self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
        poll_interval: Duration,
    ) -> &mut Self {
        let catalog_state_clone = catalog_state.clone();
        self.register_queue::<ExpirationQueueConfig>(
            tabular_expiration_queue::QUEUE_NAME,
            Arc::new(move || {
                let authorizer = authorizer.clone();
                let catalog_state_clone = catalog_state_clone.clone();
                Box::pin({
                    async move {
                        tabular_expiration_queue::tabular_expiration_worker::<C, A>(
                            catalog_state_clone.clone(),
                            authorizer.clone(),
                            poll_interval,
                        )
                        .await;
                    }
                })
            }),
            2,
        );

        self.register_queue::<PurgeQueueConfig>(
            tabular_purge_queue::QUEUE_NAME,
            Arc::new(move || {
                let catalog_state_clone = catalog_state.clone();
                let secret_store = secret_store.clone();
                Box::pin(async move {
                    tabular_purge_queue::tabular_purge_worker::<C, S>(
                        catalog_state_clone.clone(),
                        secret_store.clone(),
                        poll_interval,
                    )
                    .await;
                })
            }),
            2,
        );

        self
    }

    /// Creates [`RegisteredTaskQueues`] for use in application state
    #[must_use]
    pub fn registered_task_queues(&self) -> RegisteredTaskQueues {
        RegisteredTaskQueues {
            queues: Arc::new(self.registered_queues.clone()),
        }
    }

    /// Creates a [`TaskQueuesRunner`] that can be used to start the task queue workers
    #[must_use]
    pub fn task_queues_runner(&self) -> TaskQueuesRunner {
        let mut registered_task_queues = HashMap::new();

        for name in self.registered_queues.keys() {
            if let Some(worker) = self.task_workers.get(name) {
                registered_task_queues.insert(
                    *name,
                    QueueWorkerConfig {
                        worker_fn: Arc::clone(&worker.worker_fn),
                        num_workers: worker.num_workers,
                    },
                );
            }
        }

        TaskQueuesRunner {
            registered_queues: Arc::new(registered_task_queues),
        }
    }
}

/// Runner for task queues that manages the worker processes
#[derive(Debug, Clone)]
pub struct TaskQueuesRunner {
    registered_queues: Arc<HashMap<&'static str, QueueWorkerConfig>>,
}

impl TaskQueuesRunner {
    /// Runs all registered task queue workers and monitors them, restarting any that exit.
    pub async fn run_queue_workers(self, restart_workers: bool) {
        // Create a structure to track worker information and hold task handles
        struct WorkerInfo {
            queue_name: &'static str,
            worker_id: usize,
            handle: tokio::task::JoinHandle<()>,
        }

        let mut workers = Vec::new();
        let registered_queues = Arc::clone(&self.registered_queues);

        // Initialize all workers
        for (queue_name, queue) in registered_queues.iter() {
            tracing::info!(
                "Starting task queue {queue_name} with {} workers",
                queue.num_workers
            );

            for worker_id in 0..queue.num_workers {
                let task_fn = Arc::clone(&queue.worker_fn);
                tracing::debug!("Starting task queue {queue_name} worker {worker_id}");
                workers.push(WorkerInfo {
                    queue_name,
                    worker_id,
                    handle: tokio::task::spawn(task_fn()),
                });
            }
        }

        // Main worker monitoring loop
        loop {
            if workers.is_empty() {
                return;
            }

            // Wait for any worker to complete
            let mut_handles: Vec<_> = workers.iter_mut().map(|w| &mut w.handle).collect();
            let (result, index, _) = futures::future::select_all(mut_handles).await;

            // Get the completed worker's info
            let worker = workers.swap_remove(index);

            let log_msg_suffix = if restart_workers {
                "Restarting worker"
            } else {
                "Restarting worker disabled"
            };

            // Log the result
            match result {
                Ok(()) => tracing::error!(
                    "Task queue {} worker {} finished. {log_msg_suffix}",
                    worker.queue_name,
                    worker.worker_id
                ),
                Err(e) => tracing::error!(
                    ?e,
                    "Task queue {} worker {} panicked: {e}. {log_msg_suffix}",
                    worker.queue_name,
                    worker.worker_id
                ),
            }

            // Restart the worker
            if restart_workers {
                if let Some(queue) = registered_queues.get(worker.queue_name) {
                    let task_fn = Arc::clone(&queue.worker_fn);
                    tracing::debug!(
                        "Restarting task queue {} worker {}",
                        worker.queue_name,
                        worker.worker_id
                    );
                    workers.push(WorkerInfo {
                        queue_name: worker.queue_name,
                        worker_id: worker.worker_id,
                        handle: tokio::task::spawn(task_fn()),
                    });
                }
            }
        }
    }
}

#[derive(Clone)]
/// Contains all required information to dynamically generate API documentation
/// for the warehouse-specific configuration of a task queue.
pub struct QueueApiConfig {
    /// Name of the task queue
    pub queue_name: &'static str,
    /// Name of the configuration type used in the API documentation
    pub utoipa_type_name: Cow<'static, str>,
    /// Schema for the configuration type used in the API documentation
    pub utoipa_schema: utoipa::openapi::RefOr<utoipa::openapi::Schema>,
}

impl Debug for QueueApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueApiConfig")
            .field("queue_name", &self.queue_name)
            .field("utoipa_type_name", &self.utoipa_type_name)
            .field("utoipa_schema", &"<schema>")
            .finish()
    }
}

#[derive(Clone)]
struct QueueWorkerConfig {
    worker_fn: TaskQueueWorker,
    /// Number of workers that run locally for this queue
    num_workers: usize,
}

impl Debug for QueueWorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueWorkerConfig")
            .field("worker_fn", &"Fn(...)")
            .field("num_workers", &self.num_workers)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub struct TaskId(Uuid);

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for TaskId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<TaskId> for Uuid {
    fn from(id: TaskId) -> Self {
        id.0
    }
}

impl Deref for TaskId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskFilter {
    WarehouseId(WarehouseId),
    TaskIds(Vec<TaskId>),
}

#[derive(Debug, Clone)]
pub struct TaskInput {
    /// Metadata for this task instance.
    /// Metadata type is shared between different task types.
    pub task_metadata: TaskMetadata,
    /// Specific payload for this task type
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskMetadata {
    pub warehouse_id: WarehouseId,
    pub parent_task_id: Option<TaskId>,
    pub entity_id: EntityId,
    pub schedule_for: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum EntityId {
    Tabular(Uuid),
}

impl EntityId {
    #[must_use]
    pub fn to_uuid(&self) -> Uuid {
        match self {
            EntityId::Tabular(id) => *id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub task_metadata: TaskMetadata,
    pub queue_name: String,
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub attempt: i32,
    pub(crate) config: Option<serde_json::Value>,
    pub(crate) state: serde_json::Value,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskCheckState {
    Stop,
    Continue,
}

impl Task {
    /// Extracts the task state from the task.
    ///
    /// # Errors
    /// Returns an error if the task state cannot be deserialized into the specified type.
    pub fn task_state<T: DeserializeOwned>(&self) -> crate::api::Result<T> {
        Ok(serde_json::from_value(self.state.clone()).map_err(|e| {
            crate::api::ErrorModel::internal(
                format!("Failed to deserialize task state: {e}"),
                "TaskStateDeserializationError",
                Some(Box::new(e)),
            )
        })?)
    }

    /// Extracts the task configuration from the task.
    ///
    /// # Errors
    /// Returns an error if the task configuration cannot be deserialized into the specified type.
    pub fn task_config<T: DeserializeOwned>(&self) -> crate::api::Result<Option<T>> {
        Ok(self
            .config
            .as_ref()
            .map(|cfg| {
                serde_json::from_value(cfg.clone()).map_err(|e| {
                    crate::api::ErrorModel::internal(
                        format!("Failed to deserialize task config: {e}"),
                        "TaskConfigDeserializationError",
                        Some(Box::new(e)),
                    )
                })
            })
            .transpose()?)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, EnumIter)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "task_intermediate_status", rename_all = "kebab-case")
)]
pub enum TaskStatus {
    Scheduled,
    Running,
    ShouldStop,
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "task_final_status", rename_all = "kebab-case")
)]
pub enum TaskOutcome {
    Failed,
    Cancelled,
    Success,
}

#[derive(Debug)]
pub enum Status<'a> {
    Success(Option<&'a str>),
    Failure(&'a str, i32),
}

impl std::fmt::Display for Status<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Success(details) => write!(f, "success ({})", details.unwrap_or("")),
            Status::Failure(details, _) => write!(f, "failure ({details})"),
        }
    }
}

pub(crate) async fn record_error_with_catalog<C: Catalog>(
    catalog_state: C::State,
    error: &str,
    max_retries: i32,
    task_id: TaskId,
) {
    let mut trx: C::Transaction = match Transaction::begin_write(catalog_state).await.map_err(|e| {
        tracing::error!("Failed to start transaction: {:?}", e);
        e
    }) {
        Ok(trx) => trx,
        Err(e) => {
            tracing::error!("Failed to start transaction: {:?}", e);
            return;
        }
    };
    C::retrying_record_task_failure(task_id, error, max_retries, trx.transaction()).await;
    let _ = trx.commit().await.inspect_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
    });
}

const fn valid_max_time_since_last_heartbeat(num: i64) -> chrono::Duration {
    assert!(
        num > 0,
        "max_seconds_since_last_heartbeat must be greater than 0"
    );
    let dur = chrono::Duration::seconds(num);
    assert!(dur.num_microseconds().is_some());
    dur
}

#[cfg(test)]
mod test {

    use sqlx::PgPool;
    use tracing_test::traced_test;

    use crate::{
        api::{
            iceberg::v1::PaginationQuery,
            management::v1::{DeleteKind, TabularType},
        },
        implementations::postgres::{
            tabular::table::tests::initialize_table, warehouse::test::initialize_warehouse,
            CatalogState, PostgresCatalog, PostgresTransaction, SecretsState,
        },
        service::{
            authz::AllowAllAuthorizer,
            storage::TestProfile,
            task_queue::{
                tabular_expiration_queue::TabularExpirationPayload, EntityId, TaskMetadata,
            },
            Catalog, ListFlags, Transaction,
        },
    };

    #[sqlx::test]
    #[traced_test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let mut queues = crate::service::task_queue::TaskQueueRegistry::new();

        let secrets =
            crate::implementations::postgres::SecretsState::from_pools(pool.clone(), pool);
        let cat = catalog_state.clone();
        let sec = secrets.clone();
        let auth = AllowAllAuthorizer;
        queues.register_built_in_queues::<PostgresCatalog, SecretsState, AllowAllAuthorizer>(
            cat,
            sec,
            auth,
            std::time::Duration::from_millis(100),
        );
        let _queue_task = tokio::task::spawn(queues.task_queues_runner().run_queue_workers(true));

        let warehouse = initialize_warehouse(
            catalog_state.clone(),
            Some(TestProfile::default().into()),
            None,
            None,
            true,
        )
        .await;

        let tab = initialize_table(
            warehouse,
            catalog_state.clone(),
            false,
            None,
            Some("tab".to_string()),
        )
        .await;
        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();
        let _ = <PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: true,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .unwrap();
        trx.commit().await.unwrap();
        let mut trx = <PostgresCatalog as Catalog>::Transaction::begin_write(catalog_state.clone())
            .await
            .unwrap();
        let _ = PostgresCatalog::queue_tabular_expiration(
            TaskMetadata {
                warehouse_id: warehouse,
                entity_id: EntityId::Tabular(tab.table_id.0),
                parent_task_id: None,
                schedule_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
            },
            TabularExpirationPayload {
                tabular_type: TabularType::Table,
                deletion_kind: DeleteKind::Purge,
            },
            trx.transaction(),
        )
        .await
        .unwrap();

        <PostgresCatalog as Catalog>::mark_tabular_as_deleted(
            tab.table_id.into(),
            false,
            trx.transaction(),
        )
        .await
        .unwrap();

        trx.commit().await.unwrap();

        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();

        let del = <PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .unwrap()
        .deletion_details;
        del.unwrap();
        trx.commit().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1250)).await;

        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();

        assert!(<PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .is_none());
        trx.commit().await.unwrap();
    }
}
