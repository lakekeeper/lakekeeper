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
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use serde::{de::DeserializeOwned, Serialize};
use strum::EnumIter;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
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

pub(crate) const DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT: chrono::Duration =
    valid_max_time_since_last_heartbeat(3600);
const DEFAULT_MAX_RETRIES: i32 = 5;

#[allow(clippy::declare_interior_mutable_const)]
pub static BUILT_IN_API_CONFIGS: LazyLock<Vec<QueueApiConfig>> = LazyLock::new(|| {
    vec![
        tabular_expiration_queue::API_CONFIG.clone(),
        tabular_purge_queue::API_CONFIG.clone(),
    ]
});

/// Infinitely running task worker loop function that polls tasks from a queue and
/// processes. Accepts a cancellation token for graceful shutdown.
pub type TaskQueueWorker = Arc<
    dyn Fn(tokio_util::sync::CancellationToken) -> BoxFuture<'static, ()> + Send + Sync + 'static,
>;
type ValidatorFn = Arc<dyn Fn(serde_json::Value) -> serde_json::Result<()> + Send + Sync>;

/// Warehouse specific configuration for a task queue.
pub trait QueueConfig: ToSchema + Serialize + DeserializeOwned {
    #[must_use]
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT
    }

    #[must_use]
    fn max_retries() -> i32 {
        DEFAULT_MAX_RETRIES
    }

    fn queue_name() -> &'static str;
}

/// Task Payload
pub trait TaskData: Clone + Serialize + DeserializeOwned {}

/// A container for registered task queues that can be used for validation and API configuration.
/// This can be included in the Axum application state.
#[derive(Clone, Default, Debug)]
pub struct RegisteredTaskQueues {
    // Mapping of queue names to their configurations
    queues: Arc<RwLock<HashMap<&'static str, RegisteredQueue>>>,
}

impl RegisteredTaskQueues {
    /// Get the validator function for a queue by name
    ///
    /// # Returns
    /// Some(ValidatorFn) if the queue exists, None otherwise
    #[must_use]
    pub async fn validate_config_fn(&self, queue_name: &str) -> Option<ValidatorFn> {
        self.queues
            .read()
            .await
            .get(queue_name)
            .map(|q| Arc::clone(&q.schema_validator_fn))
    }

    /// Get the API configuration for all registered queues
    #[must_use]
    pub async fn api_config(&self) -> Vec<QueueApiConfig> {
        self.queues
            .read()
            .await
            .values()
            .map(|q| q.api_config.clone())
            .collect()
    }

    /// Get the names of all registered queues
    #[must_use]
    pub async fn queue_names(&self) -> Vec<&'static str> {
        self.queues.read().await.keys().copied().collect()
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
    registered_queues: Arc<RwLock<HashMap<&'static str, RegisteredQueue>>>,
    // Mapping of queue names to their worker configuration
    task_workers: Arc<RwLock<HashMap<&'static str, RegisteredTaskQueueWorker>>>,
}

impl Default for TaskQueueRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct QueueRegistration {
    /// Name of the queue
    pub queue_name: &'static str,
    /// Worker function for the queue
    pub worker_fn: TaskQueueWorker,
    /// Number of workers that run locally for this queue
    pub num_workers: usize,
}

impl Debug for QueueRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueRegistration")
            .field("queue_name", &self.queue_name)
            .field("worker_fn", &"Fn(...)")
            .field("num_workers", &self.num_workers)
            .finish()
    }
}

impl TaskQueueRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            registered_queues: Arc::new(RwLock::new(HashMap::new())),
            task_workers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_queue<T: QueueConfig>(&self, task_queue: QueueRegistration) -> &Self {
        let QueueRegistration {
            queue_name,
            worker_fn,
            num_workers,
        } = task_queue;
        let schema_validator_fn = |v| serde_json::from_value::<T>(v).map(|_| ());
        let schema_validator_fn = Arc::new(schema_validator_fn) as ValidatorFn;
        let api_config = QueueApiConfig {
            queue_name,
            utoipa_type_name: T::name().to_string().into(),
            utoipa_schema: utoipa::openapi::RefOr::Ref(utoipa::openapi::Ref::from_schema_name(
                T::name(),
            )),
        };

        self.registered_queues.write().await.insert(
            queue_name,
            RegisteredQueue {
                api_config,
                schema_validator_fn,
            },
        );

        self.task_workers.write().await.insert(
            queue_name,
            RegisteredTaskQueueWorker {
                worker_fn,
                num_workers,
            },
        );
        self
    }

    pub async fn register_built_in_queues<C: Catalog, S: SecretStore, A: Authorizer>(
        &self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
        poll_interval: Duration,
    ) -> &Self {
        let catalog_state_clone = catalog_state.clone();
        self.register_queue::<ExpirationQueueConfig>(QueueRegistration {
            queue_name: tabular_expiration_queue::QUEUE_NAME,
            worker_fn: Arc::new(move |cancellation_token| {
                let authorizer = authorizer.clone();
                let catalog_state_clone = catalog_state_clone.clone();
                Box::pin({
                    async move {
                        tabular_expiration_queue::tabular_expiration_worker::<C, A>(
                            catalog_state_clone.clone(),
                            authorizer.clone(),
                            &poll_interval,
                            cancellation_token,
                        )
                        .await;
                    }
                })
            }),
            num_workers: 2,
        })
        .await;

        self.register_queue::<PurgeQueueConfig>(QueueRegistration {
            queue_name: tabular_purge_queue::QUEUE_NAME,
            worker_fn: Arc::new(move |cancellation_token| {
                let catalog_state_clone = catalog_state.clone();
                let secret_store = secret_store.clone();
                Box::pin(async move {
                    tabular_purge_queue::tabular_purge_worker::<C, S>(
                        catalog_state_clone.clone(),
                        secret_store.clone(),
                        &poll_interval,
                        cancellation_token,
                    )
                    .await;
                })
            }),
            num_workers: 2,
        })
        .await;

        self
    }

    /// Creates [`RegisteredTaskQueues`] for use in application state
    #[must_use]
    pub fn registered_task_queues(&self) -> RegisteredTaskQueues {
        RegisteredTaskQueues {
            // It is important to share the interior mutable state,
            // so that tasks that register later are reflected to the state
            // that previously registered tasks have a reference to.
            queues: self.registered_queues.clone(),
        }
    }

    #[must_use]
    pub async fn len(&self) -> usize {
        self.registered_queues.read().await.len()
    }

    #[must_use]
    pub async fn is_empty(&self) -> bool {
        self.registered_queues.read().await.is_empty()
    }

    /// Creates a [`TaskQueuesRunner`] that can be used to start the task queue workers
    #[must_use]
    pub async fn task_queues_runner(
        &self,
        cancellation_token: CancellationToken,
    ) -> TaskQueuesRunner {
        let mut registered_task_queues = HashMap::new();

        let queues = self.registered_queues.read().await;
        let workers = self.task_workers.read().await;

        for name in queues.keys() {
            if let Some(worker) = workers.get(name) {
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
            cancellation_token,
        }
    }
}

/// Runner for task queues that manages the worker processes
#[derive(Debug, Clone)]
pub struct TaskQueuesRunner {
    registered_queues: Arc<HashMap<&'static str, QueueWorkerConfig>>,
    cancellation_token: CancellationToken,
}

impl TaskQueuesRunner {
    /// Runs all registered task queue workers and monitors them, restarting any that exit.
    /// Accepts a cancellation token for graceful shutdown.
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
                "Starting {} workers for task queue `{queue_name}`.",
                queue.num_workers
            );

            for worker_id in 0..queue.num_workers {
                let task_fn = Arc::clone(&queue.worker_fn);
                let cancellation_token_clone = self.cancellation_token.clone();
                tracing::debug!(
                    "Starting `{queue_name}` worker {worker_id}/{}",
                    queue.num_workers
                );
                workers.push(WorkerInfo {
                    queue_name,
                    worker_id,
                    handle: tokio::task::spawn(task_fn(cancellation_token_clone)),
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
                Ok(()) if !self.cancellation_token.is_cancelled() => tracing::warn!(
                    "Task queue {} worker {} finished. {log_msg_suffix}",
                    worker.queue_name,
                    worker.worker_id
                ),
                Ok(()) => tracing::info!(
                    "Task queue {} worker {} finished gracefully after cancellation.",
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

            // Restart the worker only if cancellation hasn't been requested
            if restart_workers && !self.cancellation_token.is_cancelled() {
                if let Some(queue) = registered_queues.get(worker.queue_name) {
                    let task_fn = Arc::clone(&queue.worker_fn);
                    let cancellation_token_clone = self.cancellation_token.clone();
                    tracing::debug!(
                        "Restarting task queue {} worker {}",
                        worker.queue_name,
                        worker.worker_id
                    );
                    workers.push(WorkerInfo {
                        queue_name: worker.queue_name,
                        worker_id: worker.worker_id,
                        handle: tokio::task::spawn(task_fn(cancellation_token_clone)),
                    });
                }
            } else if self.cancellation_token.is_cancelled() {
                tracing::info!(
                    "Cancellation requested, not restarting task queue {} worker {}",
                    worker.queue_name,
                    worker.worker_id
                );
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
    pub(crate) data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpecializedTask<C: QueueConfig, P: TaskData> {
    pub task_metadata: TaskMetadata,
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub attempt: i32,
    pub config: Option<C>,
    pub data: P,
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
    pub fn task_data<T: TaskData>(&self) -> crate::api::Result<T> {
        Ok(serde_json::from_value(self.data.clone()).map_err(|e| {
            crate::api::ErrorModel::internal(
                format!(
                    "Failed to deserialize task data for task {} in queue `{}`: {e}",
                    self.task_id, self.queue_name
                ),
                "TaskStateDeserializationError",
                Some(Box::new(e)),
            )
        })?)
    }

    /// Extracts the task configuration from the task.
    ///
    /// # Errors
    /// Returns an error if the task configuration cannot be deserialized into the specified type.
    pub fn queue_config<T: QueueConfig>(&self) -> crate::api::Result<Option<T>> {
        Ok(self
            .config
            .as_ref()
            .map(|cfg| {
                serde_json::from_value(cfg.clone()).map_err(|e| {
                    crate::api::ErrorModel::internal(
                        format!(
                            "Failed to deserialize configuration for task queue `{}`: {e}",
                            self.queue_name
                        ),
                        "TaskConfigDeserializationError",
                        Some(Box::new(e)),
                    )
                })
            })
            .transpose()?)
    }
}

impl<Q: QueueConfig, D: TaskData> SpecializedTask<Q, D> {
    /// Pick a new task from the queue. If no task is available, returns None.
    ///
    /// # Errors
    /// Returns an error if the task cannot be picked from the queue or if
    /// deserialization of the queue configuration or task data fails.
    pub async fn pick_new_task<C: Catalog>(
        catalog_state: C::State,
    ) -> crate::api::Result<Option<Self>> {
        let task = C::pick_new_task(
            Q::queue_name(),
            Q::max_time_since_last_heartbeat(),
            catalog_state.clone(),
        )
        .await
        .map_err(|e| e.append_detail(format!("Failed to pick new `{}` task.", Q::queue_name())))?;

        if let Some(task) = task {
            let state = match task.task_data::<D>() {
                Ok(state) => state,
                Err(err) => {
                    Self::report_deserialization_failure::<C>(
                        catalog_state,
                        task.task_id,
                        &err.to_string(),
                    )
                    .await;
                    return Ok(None);
                }
            };
            let config = match task.queue_config::<Q>() {
                Ok(config) => config,
                Err(err) => {
                    Self::report_deserialization_failure::<C>(
                        catalog_state,
                        task.task_id,
                        &err.to_string(),
                    )
                    .await;
                    return Ok(None);
                }
            };
            Ok(Some(Self {
                task_metadata: task.task_metadata,
                task_id: task.task_id,
                status: task.status,
                picked_up_at: task.picked_up_at,
                attempt: task.attempt,
                config,
                data: state,
            }))
        } else {
            Ok(None)
        }
    }

    /// Continuously poll for a new task in the queue until a task is found.
    /// Returns None if cancellation is requested.
    pub async fn poll_for_new_task<C: Catalog>(
        catalog_state: C::State,
        poll_interval: &Duration,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Option<Self> {
        loop {
            tokio::select! {
                () = cancellation_token.cancelled() => {
                    tracing::info!("Graceful shutdown requested for queue `{}`", Q::queue_name());
                    return None;
                }
                task_result = Self::pick_new_task::<C>(catalog_state.clone()) => {
                    let task = match task_result {
                        Ok(task) => task,
                        Err(e) => {
                            tracing::error!(
                                "Failed to pick new task from queue `{}`. Retrying in 5s. Error: {e}",
                                Q::queue_name()
                            );
                            tokio::select! {
                                () = cancellation_token.cancelled() => {
                                    tracing::info!("Graceful shutdown requested for queue `{}`", Q::queue_name());
                                    return None;
                                }
                                () = tokio::time::sleep(Duration::from_secs(5)) => continue,
                            }
                        }
                    };

                    let Some(task) = task else {
                        let jitter = { fastrand::u64(0..500) };
                        tokio::select! {
                            () = cancellation_token.cancelled() => {
                                tracing::info!("Graceful shutdown requested for queue `{}`", Q::queue_name());
                                return None;
                            }
                            () = tokio::time::sleep(*poll_interval + Duration::from_millis(jitter)) => continue,
                        }
                    };

                    tracing::debug!("Picked up `{}` task {}.", task.task_id, Q::queue_name());
                    return Some(task);
                }
            }
        }
    }

    async fn report_deserialization_failure<C: Catalog>(
        catalog_state: C::State,
        task_id: TaskId,
        error: &str,
    ) {
        tracing::error!("{error}. TaskID: {task_id}");

        let mut trx = match C::Transaction::begin_write(catalog_state).await {
            Ok(trx) => trx,
            Err(e) => {
                tracing::error!(
                    "Failed to start DB transaction to record deserialization failure for `{}` task {task_id}: {e}. Original Error: {error}",
                    Q::queue_name()
                );
                return;
            }
        };

        let r = C::record_task_failure(
            task_id,
            format!("Failed to deserialize task data: {error}").as_str(),
            Q::max_retries(),
            &mut trx.transaction(),
        )
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to record deserialization failure for `{task_id}` task {}.",
                Q::queue_name()
            ))
            .append_detail(format!("Original Error: {error}"))
        });

        if let Err(e) = r {
            tracing::error!(
                "Failed to record deserialization failure for `{task_id}` task {}: {e}. Original Error: {error}",
                Q::queue_name()
            );
            return;
        }

        if let Err(e) = trx.commit().await {
            tracing::error!(
                "Failed to commit transaction for recording deserialization failure for `{task_id}` task {}: {e}. Original Error: {error}",
                Q::queue_name()
            );
        };
    }

    pub fn queue_name(&self) -> &'static str {
        Q::queue_name()
    }

    /// Records an failure for a task in the catalog, updating its status and retry count.
    ///
    /// Does not return an error, but logs it.
    pub async fn record_failure<C: Catalog>(&self, catalog_state: C::State, error: &str) {
        let max_retries = Q::max_retries();

        let status = Status::Failure(error, max_retries);

        for attempt in 1..=5 {
            match self
                .record_status_for_state::<C>(catalog_state.clone(), status.clone())
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        "Successfully recorded error for task {} in queue '{}' on attempt {attempt}",
                        self.task_id,
                        self.queue_name(),
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to record error for task {} in queue '{}' on attempt {attempt}/5: {e}",
                        self.task_id,
                        self.queue_name(),
                    );

                    if attempt < 5 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    } else {
                        tracing::error!(
                            "Failed to record error for task {} in queue '{}' after 5 attempts. {e}. Original Error: {error}",
                            self.task_id,
                            self.queue_name()
                        );
                    }
                }
            }
        }
    }

    /// Record success.
    ///
    /// Records the success of a task in the catalog, updating its status.
    /// Does not return an error, but logs it.
    pub async fn record_success<C: Catalog>(&self, catalog_state: C::State, details: Option<&str>) {
        let status = Status::Success(details);

        for attempt in 1..=5 {
            match self
                .record_status_for_state::<C>(catalog_state.clone(), status.clone())
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        "Successfully recorded success for task {} in queue '{}' on attempt {attempt}",
                        self.task_id,
                        self.queue_name(),
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to record success for task {} in queue '{}' on attempt {attempt}/5: {e}",
                        self.task_id,
                        self.queue_name(),
                    );

                    if attempt < 5 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    } else {
                        tracing::error!(
                            "Failed to record success for task {} in queue '{}' after 5 attempts. {e}. Original Success Details: {}",
                            self.task_id,
                            self.queue_name(),
                            details.unwrap_or("No details provided")
                        );
                    }
                }
            }
        }
    }

    /// Record success in an existing transaction.
    ///
    /// Records the success of a task in the catalog, updating its status.
    /// Does not return an error, but logs it.
    pub async fn record_success_in_transaction<C: Catalog>(
        &self,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
        details: Option<&str>,
    ) {
        let status = Status::Success(details);

        match self
            .record_status_for_transaction::<C>(status, transaction)
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    "Successfully recorded success for task {} in queue '{}'",
                    self.task_id,
                    self.queue_name(),
                );
            }
            Err(e) => {
                tracing::error!(
                    "Failed to record success for task {} in queue '{}': {e}. Original Success Details: {}",
                    self.task_id,
                    self.queue_name(),
                    details.unwrap_or("No details provided")
                );
            }
        }
    }

    async fn record_status_for_state<C: Catalog>(
        &self,
        catalog_state: C::State,
        result: Status<'_>,
    ) -> Result<(), IcebergErrorResponse> {
        let mut transaction: C::Transaction = match Transaction::begin_write(catalog_state).await {
            Ok(trx) => trx,
            Err(e) => {
                return Err(e
                    .append_detail(format!(
                    "Failed to start DB transaction to record status for task {} in queue `{}`.",
                    self.task_id, self.queue_name()
                ))
                    .append_detail(format!("Task Status that failed to record: `{result}`")));
            }
        };

        self.record_status_for_transaction::<C>(result.clone(), transaction.transaction())
            .await?;

        transaction.commit().await.map_err(|e| {
            e.append_detail(format!(
                "Failed to commit DB transaction to record status for task {} in queue `{}`.",
                self.task_id,
                self.queue_name()
            ))
            .append_detail(format!("Task Status that failed to commit: `{result}`"))
        })?;

        Ok(())
    }

    async fn record_status_for_transaction<C: Catalog>(
        &self,
        result: Status<'_>,
        mut transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<(), IcebergErrorResponse> {
        match result {
            Status::Success(details) => {
                C::record_task_success(self.task_id, details, &mut transaction)
                    .await
                    .map_err(|e| {
                        e.append_detail(format!(
                            "Failed to record success for `{}` task {}.",
                            self.queue_name(),
                            self.task_id,
                        ))
                        .append_detail(format!(
                            "Original Success Details: `{}`",
                            details.unwrap_or("No details provided")
                        ))
                    })
            }
            Status::Failure(details, max_retries) => {
                C::record_task_failure(self.task_id, details, max_retries, &mut transaction)
                    .await
                    .map_err(|e| {
                        e.append_detail(format!(
                            "Failed to record failure for `{}` task {}.",
                            self.queue_name(),
                            self.task_id
                        ))
                        .append_detail(format!("Original Error Details: `{details}`"))
                    })
            }
        }
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

#[derive(Debug, Clone)]
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

#[must_use]
pub const fn valid_max_time_since_last_heartbeat(num: i64) -> chrono::Duration {
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

    use std::time::Duration;

    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use tracing_test::traced_test;
    use utoipa::ToSchema;

    use super::*;
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
            storage::MemoryProfile,
            task_queue::{
                tabular_expiration_queue::TabularExpirationPayload, EntityId, TaskMetadata,
            },
            Catalog, ListFlags, Transaction,
        },
    };

    #[tokio::test]
    async fn test_shared_interior_mutable_state() {
        // This test verifies that RegisteredTaskQueues instances share the same
        // interior mutable state, so that tasks registered later are reflected
        // in previously created RegisteredTaskQueues instances.

        #[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
        struct TestQueueConfig {
            test_field: String,
        }

        impl QueueConfig for TestQueueConfig {
            fn queue_name() -> &'static str {
                "test-queue"
            }
        }

        // Register another queue and verify both instances see it
        #[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
        struct SecondTestQueueConfig {
            other_field: i32,
        }

        impl QueueConfig for SecondTestQueueConfig {
            fn queue_name() -> &'static str {
                "second-test-queue"
            }
        }

        let registry = crate::service::task_queue::TaskQueueRegistry::new();

        // Create an initial RegisteredTaskQueues instance before registering any queues
        let initial_queues = registry.registered_task_queues();

        // Verify registry starts empty and initial_queues reflects this
        assert_eq!(registry.len().await, 0);
        assert!(registry.is_empty().await);
        assert!(initial_queues.api_config().await.is_empty());

        registry
            .register_queue::<TestQueueConfig>(super::QueueRegistration {
                queue_name: "test-queue",
                worker_fn: std::sync::Arc::new(move |_cancellation_token| {
                    Box::pin(async {
                        // Empty worker for testing
                    })
                }),
                num_workers: 1,
            })
            .await;

        // Create another RegisteredTaskQueues instance after registration
        let later_queues = registry.registered_task_queues();

        // Registry should now show the registered queue
        assert_eq!(registry.len().await, 1);
        assert!(!registry.is_empty().await);

        // Both RegisteredTaskQueues instances should now see the registered queue due to shared state
        let initial_api_config = initial_queues.api_config().await;
        let later_api_config = later_queues.api_config().await;
        assert_eq!(initial_api_config.len(), 1);
        assert_eq!(later_api_config.len(), 1);
        assert_eq!(initial_api_config[0].queue_name, "test-queue");
        assert_eq!(later_api_config[0].queue_name, "test-queue");

        // Both should have access to the validator function
        assert!(initial_queues
            .validate_config_fn("test-queue")
            .await
            .is_some());
        assert!(later_queues
            .validate_config_fn("test-queue")
            .await
            .is_some());
        assert!(initial_queues
            .validate_config_fn("non-existent")
            .await
            .is_none());
        assert!(later_queues
            .validate_config_fn("non-existent")
            .await
            .is_none());

        registry
            .register_queue::<SecondTestQueueConfig>(super::QueueRegistration {
                queue_name: "second-test-queue",
                worker_fn: std::sync::Arc::new(move |_cancellation_token| {
                    Box::pin(async {
                        // Empty worker for testing
                    })
                }),
                num_workers: 2,
            })
            .await;

        // Registry should now show both queues
        assert_eq!(registry.len().await, 2);

        // Both RegisteredTaskQueues instances should now see both queues due to shared interior mutable state
        let initial_api_config = initial_queues.api_config().await;
        let later_api_config = later_queues.api_config().await;
        assert_eq!(initial_api_config.len(), 2);
        assert_eq!(later_api_config.len(), 2);

        // Check that both queues are accessible from both instances
        assert!(initial_queues
            .validate_config_fn("test-queue")
            .await
            .is_some());
        assert!(initial_queues
            .validate_config_fn("second-test-queue")
            .await
            .is_some());
        assert!(later_queues
            .validate_config_fn("test-queue")
            .await
            .is_some());
        assert!(later_queues
            .validate_config_fn("second-test-queue")
            .await
            .is_some());

        // Verify that the queue names are correctly registered in both instances
        let mut initial_queue_names: Vec<_> =
            initial_api_config.iter().map(|q| q.queue_name).collect();
        let mut later_queue_names: Vec<_> = later_api_config.iter().map(|q| q.queue_name).collect();
        initial_queue_names.sort_unstable();
        later_queue_names.sort_unstable();

        assert_eq!(initial_queue_names, vec!["second-test-queue", "test-queue"]);
        assert_eq!(later_queue_names, vec!["second-test-queue", "test-queue"]);
    }

    #[sqlx::test]
    #[traced_test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let queues = crate::service::task_queue::TaskQueueRegistry::new();

        let secrets =
            crate::implementations::postgres::SecretsState::from_pools(pool.clone(), pool);
        let cat = catalog_state.clone();
        let sec = secrets.clone();
        let auth = AllowAllAuthorizer;
        queues
            .register_built_in_queues::<PostgresCatalog, SecretsState, AllowAllAuthorizer>(
                cat,
                sec,
                auth,
                Duration::from_millis(100),
            )
            .await;
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let runner = queues.task_queues_runner(cancellation_token.clone()).await;
        let _queue_task = tokio::task::spawn(runner.run_queue_workers(true));

        let warehouse = initialize_warehouse(
            catalog_state.clone(),
            Some(MemoryProfile::default().into()),
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

        cancellation_token.cancel();
    }
}
