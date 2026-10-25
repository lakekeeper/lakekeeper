use std::{collections::HashMap, fmt::Formatter, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::{
    CONFIG, CancellationToken,
    service::{
        CatalogStore, SecretStore,
        authz::Authorizer,
        tasks::{
            OnWarehouseDelete, TaskConfig, TaskDemandClass, TaskQueueName, TaskQueueWorkerFn,
            TaskQueuesRunner, queue_demand::is_valid_demand_class_name,
            task_queues_runner::QueueWorkerConfig,
        },
    },
};

pub type ValidatorFn = Arc<dyn Fn(serde_json::Value) -> serde_json::Result<()> + Send + Sync>;

/// Whether a queue is exposed via the schedule endpoint, and (under
/// `open-api`) what payload schema its request body uses.
///
/// Folds two coupled fields into one decision so a reader can't end up
/// with `Disabled` + a meaningless payload schema, or `Enabled` while
/// silently emitting an untyped body.
#[cfg(feature = "open-api")]
#[derive(Clone)]
pub enum UserScheduling {
    /// Not exposed via the schedule endpoint. The `OpenAPI` patcher omits
    /// the queue's path; the endpoint rejects manual scheduling for this
    /// queue with `400 QueueNotUserSchedulable`. Destructive or
    /// lifecycle-managed queues (e.g. `tabular_purge`, `tabular_expiration`)
    /// must use this so they can't be enqueued out-of-band.
    Disabled,
    /// Exposed via the schedule endpoint. `payload_schema` shapes the
    /// per-queue request body: `None` strips the `payload` field;
    /// `Some(schema)` references the typed payload.
    Enabled {
        payload_schema: Option<utoipa::openapi::RefOr<utoipa::openapi::Schema>>,
    },
}

#[cfg(not(feature = "open-api"))]
#[derive(Clone, Debug)]
pub enum UserScheduling {
    Disabled,
    Enabled,
}

impl UserScheduling {
    /// Whether the queue is exposed via the schedule endpoint.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

#[cfg(feature = "open-api")]
impl std::fmt::Debug for UserScheduling {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Enabled { payload_schema } => f
                .debug_struct("Enabled")
                .field(
                    "payload_schema",
                    &payload_schema.as_ref().map(|_| "<schema>"),
                )
                .finish(),
        }
    }
}

/// Eligibility pre-check invoked by the `task-queue/{name}/schedule`
/// endpoint after authz. Receives the queue's current raw config JSON and
/// the entity's table properties; returns `Err(ErrorModel)` to reject the
/// schedule call with a clear status (typically 400) instead of creating a
/// task the worker would immediately skip at pickup. See
/// `TaskConfig::check_schedule_eligibility` for the typed hook each queue
/// implements.
pub type ScheduleEligibilityFn = Arc<
    dyn Fn(
            serde_json::Value,
            std::collections::HashMap<String, String>,
            crate::service::tasks::WarehouseTaskEntityId,
        ) -> Result<(), iceberg_ext::catalog::rest::ErrorModel>
        + Send
        + Sync,
>;

/// Check run by the `task-queue/{name}/config` endpoint after the config is parsed. Receives
/// the raw config JSON and returns `Err(ErrorModel)` to refuse the save. Wraps
/// [`TaskConfig::check_config`].
pub type ConfigCheckFn = Arc<
    dyn Fn(serde_json::Value) -> Result<(), iceberg_ext::catalog::rest::ErrorModel> + Send + Sync,
>;

#[derive(Clone)]
struct RegisteredQueue {
    /// API configuration for this queue
    api_config: QueueApiConfig,
    /// Schema validator function for the queue configuration
    /// This function is called to validate the configuration payload
    schema_validator_fn: ValidatorFn,
    /// Check called by the config endpoint. Wraps `T::check_config` into a type-erased dispatch.
    config_check_fn: ConfigCheckFn,
    /// Pre-check called by the schedule endpoint. Wraps
    /// `T::check_schedule_eligibility` into a type-erased dispatch.
    schedule_eligibility_fn: ScheduleEligibilityFn,
    /// Structural validator for the queue's task payload — deserialises the
    /// caller-supplied JSON against the queue's `TaskData` type. The schedule
    /// endpoint runs this before enqueueing so a malformed payload fails
    /// fast with `400` instead of producing a task the worker can't decode
    /// at pickup. Built from the `D` generic at `register_queue` time.
    payload_validator_fn: ValidatorFn,
    /// Names this queue was known by before a rename. The config endpoints
    /// resolve these to the queue's current name so requests against an old
    /// `task-queue/{name}/config` path keep working. From
    /// `TaskConfig::legacy_queue_names` at registration time.
    legacy_names: Vec<&'static TaskQueueName>,
    /// From `TaskConfig::on_warehouse_delete` at registration time.
    on_warehouse_delete: OnWarehouseDelete,
    /// From `TaskConfig::max_time_since_last_heartbeat` at registration time.
    max_time_since_last_heartbeat: chrono::Duration,
    /// Named resource demand ranges autoscalers ask for.
    demand_classes: Vec<TaskDemandClass>,
}

impl std::fmt::Debug for RegisteredQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredQueue")
            .field("api_config", &self.api_config)
            .field("schema_validator_fn", &"Fn(...)")
            .field("config_check_fn", &"Fn(...)")
            .field("schedule_eligibility_fn", &"Fn(...)")
            .field("payload_validator_fn", &"Fn(...)")
            .field("legacy_names", &self.legacy_names)
            .field("on_warehouse_delete", &self.on_warehouse_delete)
            .field(
                "max_time_since_last_heartbeat",
                &self.max_time_since_last_heartbeat,
            )
            .field("demand_classes", &self.demand_classes)
            .finish()
    }
}

/// A container for registered task queues that can be used for validation and API configuration.
/// This can be included in the Axum application state.
#[derive(Clone, Default, Debug)]
pub struct RegisteredTaskQueues {
    // Mapping of queue names to their configurations
    queues: Arc<RwLock<HashMap<&'static TaskQueueName, RegisteredQueue>>>,
}

impl RegisteredTaskQueues {
    /// Get the validator function for a queue by name
    ///
    /// # Returns
    /// Some(ValidatorFn) if the queue exists, None otherwise
    #[must_use]
    pub async fn validate_config_fn(&self, queue_name: &TaskQueueName) -> Option<ValidatorFn> {
        self.queues
            .read()
            .await
            .get(queue_name)
            .map(|q| Arc::clone(&q.schema_validator_fn))
    }

    /// Config check for the config endpoint, run after [`Self::validate_config_fn`] accepted
    /// the config. Returns `None` if the queue is not registered.
    #[must_use]
    pub async fn config_check_fn(&self, queue_name: &TaskQueueName) -> Option<ConfigCheckFn> {
        self.queues
            .read()
            .await
            .get(queue_name)
            .map(|q| Arc::clone(&q.config_check_fn))
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

    /// Get the names of all registered queues.
    /// Results are sorted by name for consistency.
    #[must_use]
    pub async fn queue_names(&self) -> Vec<&'static TaskQueueName> {
        let mut v: Vec<_> = self.queues.read().await.keys().copied().collect();
        v.sort_unstable();
        v
    }

    /// Resolve a user-provided `TaskQueueName` to the `&'static` reference
    /// the registry holds. Required by `C::enqueue_task`, which expects a
    /// `&'static` queue name.
    #[must_use]
    pub async fn static_queue_name(
        &self,
        queue_name: &TaskQueueName,
    ) -> Option<&'static TaskQueueName> {
        self.queues
            .read()
            .await
            .get_key_value(queue_name)
            .map(|(k, _)| *k)
    }

    /// Resolve a possibly-legacy queue name to the current name of the queue
    /// that answers to it. Returns the canonical `&'static TaskQueueName` when
    /// `queue_name` is a registered queue or one of its pre-rename aliases;
    /// `None` when no registered queue claims it. Lets the config endpoints
    /// accept requests against an old `task-queue/{name}/config` path.
    #[must_use]
    pub async fn resolve_queue_name(
        &self,
        queue_name: &TaskQueueName,
    ) -> Option<&'static TaskQueueName> {
        let queues = self.queues.read().await;
        if let Some((name, _)) = queues.get_key_value(queue_name) {
            return Some(*name);
        }
        queues
            .iter()
            .find(|(_, q)| q.legacy_names.iter().any(|n| **n == *queue_name))
            .map(|(name, _)| *name)
    }

    /// Structural payload validator for the schedule endpoint. Returns
    /// `None` if the queue is not registered. Deserialises the
    /// caller-supplied JSON against the queue's `TaskData` type; fails the
    /// schedule request with `400` before enqueue if the shape is wrong.
    #[must_use]
    pub async fn payload_validator_fn(&self, queue_name: &TaskQueueName) -> Option<ValidatorFn> {
        self.queues
            .read()
            .await
            .get(queue_name)
            .map(|q| Arc::clone(&q.payload_validator_fn))
    }

    /// Eligibility pre-check for the schedule endpoint. Returns `None` if
    /// the queue is not registered.
    #[must_use]
    pub async fn schedule_eligibility_fn(
        &self,
        queue_name: &TaskQueueName,
    ) -> Option<ScheduleEligibilityFn> {
        self.queues
            .read()
            .await
            .get(queue_name)
            .map(|q| Arc::clone(&q.schedule_eligibility_fn))
    }

    /// Whether a registered queue accepts manual scheduling via the
    /// `task-queue/{name}/schedule` endpoint. Returns `None` if the queue is
    /// not registered.
    #[must_use]
    pub async fn is_user_schedulable(&self, queue_name: &TaskQueueName) -> Option<bool> {
        self.queues
            .read()
            .await
            .get(queue_name)
            .map(|q| q.api_config.user_scheduling.is_enabled())
    }

    /// Names of all registered queues that opted in to manual scheduling.
    /// Sorted for stable output.
    #[must_use]
    pub async fn user_schedulable_queue_names(&self) -> Vec<&'static TaskQueueName> {
        let mut v: Vec<_> = self
            .queues
            .read()
            .await
            .iter()
            .filter_map(|(name, q)| q.api_config.user_scheduling.is_enabled().then_some(*name))
            .collect();
        v.sort_unstable();
        v
    }

    /// What the demand of `class` of the queue `queue_name` or of one of its earlier names
    /// counts. Without a class, the whole queue.
    ///
    /// # Errors
    /// If no registered queue answers to `queue_name`, or the queue has no such class.
    #[cfg(feature = "router")]
    pub(crate) async fn demand_target(
        &self,
        queue_name: &TaskQueueName,
        class: Option<&str>,
    ) -> Result<DemandTarget, DemandLookupError> {
        let queues = self.queues.read().await;
        let (name, queue) = queues
            .get_key_value(queue_name)
            .or_else(|| {
                queues
                    .iter()
                    .find(|(_, q)| q.legacy_names.iter().any(|n| **n == *queue_name))
            })
            .ok_or(DemandLookupError::UnknownQueue)?;
        let resource_demand = match class {
            None => super::TaskResourceDemandRange::default(),
            Some(class) => {
                queue
                    .demand_classes
                    .iter()
                    .find(|c| c.name == class)
                    .ok_or(DemandLookupError::UnknownClass)?
                    .resource_demand
            }
        };
        Ok(DemandTarget {
            queue_name: name,
            legacy_queue_names: queue.legacy_names.clone(),
            max_time_since_last_heartbeat: queue.max_time_since_last_heartbeat,
            resource_demand,
        })
    }

    /// Queues whose tasks are cancelled when their warehouse is deleted, each with its
    /// legacy names. Sorted by name, so concurrent deletes cancel the queues in the same order.
    #[must_use]
    pub(crate) async fn queues_cancelled_on_warehouse_delete(
        &self,
    ) -> Vec<(&'static TaskQueueName, Vec<&'static TaskQueueName>)> {
        let mut v: Vec<_> = self
            .queues
            .read()
            .await
            .iter()
            .filter(|(_, q)| match q.on_warehouse_delete {
                OnWarehouseDelete::Cancel => true,
                OnWarehouseDelete::Block => false,
            })
            .map(|(name, q)| (*name, q.legacy_names.clone()))
            .collect();
        v.sort_unstable();
        v
    }
}

/// The tasks a demand count covers.
#[cfg(feature = "router")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DemandTarget {
    pub(crate) queue_name: &'static TaskQueueName,
    pub(crate) legacy_queue_names: Vec<&'static TaskQueueName>,
    /// Used where no task configuration of the queue sets a heartbeat timeout.
    pub(crate) max_time_since_last_heartbeat: chrono::Duration,
    pub(crate) resource_demand: super::TaskResourceDemandRange,
}

#[cfg(feature = "router")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DemandLookupError {
    #[error("Unknown task queue")]
    UnknownQueue,
    #[error("Unknown demand class of the task queue")]
    UnknownClass,
}

#[derive(Clone)]
struct RegisteredTaskQueueWorker {
    worker_fn: TaskQueueWorkerFn,
    /// Number of workers that run locally for this queue
    num_workers: usize,
}

impl std::fmt::Debug for RegisteredTaskQueueWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredTaskQueueWorker")
            .field("worker_fn", &"Fn(...)")
            .field("num_workers", &self.num_workers)
            .finish()
    }
}

/// Task queue registry used for registering and starting task queues
#[derive(Debug, Clone)]
pub struct TaskQueueRegistry {
    // Mapping of queue names to their configurations
    registered_queues: Arc<RwLock<HashMap<&'static TaskQueueName, RegisteredQueue>>>,

    // Mapping of queue names to their worker configuration
    task_workers: Arc<RwLock<HashMap<&'static TaskQueueName, RegisteredTaskQueueWorker>>>,
}

impl Default for TaskQueueRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct QueueRegistration {
    /// Name of the queue
    pub queue_name: &'static TaskQueueName,
    /// Worker function for the queue
    pub worker_fn: TaskQueueWorkerFn,
    /// Number of workers that run locally for this queue
    pub num_workers: usize,
    /// Scope of the queue configuration
    pub scope: QueueScope,
    /// Whether this queue is exposed via the schedule endpoint and (under
    /// `open-api`) the payload schema of its request body. See [`UserScheduling`].
    pub user_scheduling: UserScheduling,
}

impl std::fmt::Debug for QueueRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueRegistration")
            .field("queue_name", &self.queue_name)
            .field("worker_fn", &"Fn(...)")
            .field("num_workers", &self.num_workers)
            .field("scope", &self.scope)
            .field("user_scheduling", &self.user_scheduling)
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

    pub async fn register_queue<T: TaskConfig, D: super::TaskData>(
        &self,
        task_queue: QueueRegistration,
    ) -> &Self {
        let QueueRegistration {
            queue_name,
            worker_fn,
            num_workers,
            scope,
            user_scheduling,
        } = task_queue;
        let schema_validator_fn = |v| serde_json::from_value::<T>(v).map(|_| ());
        let schema_validator_fn = Arc::new(schema_validator_fn) as ValidatorFn;
        // Structural payload validator: deserialises caller-supplied JSON
        // against the queue's `TaskData` type. Fails fast at the schedule
        // endpoint instead of letting a malformed payload land on the
        // worker and fail at pickup.
        let payload_validator_fn = |v| serde_json::from_value::<D>(v).map(|_| ());
        let payload_validator_fn = Arc::new(payload_validator_fn) as ValidatorFn;
        let config_check_fn: ConfigCheckFn =
            Arc::new(|raw_config| T::check_config(&parse_queue_config::<T>(raw_config)?));
        let schedule_eligibility_fn: ScheduleEligibilityFn =
            Arc::new(|raw_config, entity_props, entity| {
                let config = parse_queue_config::<T>(raw_config)?;
                T::check_schedule_eligibility(&config, &entity_props, entity)
            });
        let api_config = QueueApiConfig {
            queue_name,
            #[cfg(feature = "open-api")]
            utoipa_type_name: T::name().to_string().into(),
            #[cfg(feature = "open-api")]
            utoipa_schema: utoipa::openapi::RefOr::Ref(utoipa::openapi::Ref::from_schema_name(
                T::name(),
            )),
            #[cfg(not(feature = "open-api"))]
            utoipa_type_name: (),
            #[cfg(not(feature = "open-api"))]
            utoipa_schema: (),
            scope,
            user_scheduling,
        };

        if let Some(_prev) = self.registered_queues.write().await.insert(
            queue_name,
            RegisteredQueue {
                api_config,
                schema_validator_fn,
                config_check_fn,
                schedule_eligibility_fn,
                payload_validator_fn,
                legacy_names: T::legacy_queue_names(),
                on_warehouse_delete: T::on_warehouse_delete(),
                max_time_since_last_heartbeat: T::max_time_since_last_heartbeat(),
                demand_classes: Vec::new(),
            },
        ) {
            tracing::warn!("Overwriting registration for queue `{queue_name}`");
        }

        self.task_workers.write().await.insert(
            queue_name,
            RegisteredTaskQueueWorker {
                worker_fn,
                num_workers,
            },
        );
        self
    }

    /// Names ranges of resource demands of a registered queue, so that autoscalers can ask for
    /// the demand of each by its name. Replaces the queue's earlier classes.
    ///
    /// # Errors
    /// If the queue is not registered, or a class name is not lower case letters, digits and
    /// `-`, or two classes share a name.
    pub async fn register_demand_classes(
        &self,
        queue_name: &TaskQueueName,
        classes: Vec<TaskDemandClass>,
    ) -> anyhow::Result<()> {
        for (i, class) in classes.iter().enumerate() {
            anyhow::ensure!(
                is_valid_demand_class_name(&class.name),
                "Demand class `{}` of queue `{queue_name}`: a name consists of lower case letters, \
                 digits and `-`",
                class.name
            );
            anyhow::ensure!(
                classes[..i].iter().all(|c| c.name != class.name),
                "Demand class `{}` of queue `{queue_name}` is named twice",
                class.name
            );
        }
        let mut queues = self.registered_queues.write().await;
        let queue = queues
            .get_mut(queue_name)
            .ok_or_else(|| anyhow::anyhow!("Queue `{queue_name}` is not registered"))?;
        queue.demand_classes = classes;
        Ok(())
    }

    pub async fn register_built_in_queues<C: CatalogStore, S: SecretStore, A: Authorizer>(
        &self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
        poll_interval: Duration,
    ) -> &Self {
        use super::{tabular_expiration_queue, tabular_purge_queue, task_log_cleanup_queue};

        let catalog_state_clone_for_tabular_expiration = catalog_state.clone();
        self.register_queue::<
            tabular_expiration_queue::SoftDeletionQueueConfig,
            tabular_expiration_queue::TabularExpirationPayload,
        >(
            QueueRegistration {
                queue_name: &tabular_expiration_queue::QUEUE_NAME,
                worker_fn: Arc::new(move |cancellation_token| {
                    let authorizer = authorizer.clone();
                    let catalog_state_clone = catalog_state_clone_for_tabular_expiration.clone();
                    Box::pin({
                        async move {
                            tabular_expiration_queue::tabular_expiration_worker::<C, A>(
                                catalog_state_clone,
                                authorizer.clone(),
                                poll_interval,
                                cancellation_token,
                            )
                            .await;
                        }
                    })
                }),
                num_workers: CONFIG.task_soft_deletion_workers,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            },
        )
        .await;

        let catalog_state_clone_for_tabular_purge = catalog_state.clone();
        self.register_queue::<
            tabular_purge_queue::PurgeQueueConfig,
            tabular_purge_queue::TabularPurgePayload,
        >(QueueRegistration {
            queue_name: &tabular_purge_queue::QUEUE_NAME,
            worker_fn: Arc::new(move |cancellation_token| {
                let catalog_state_clone = catalog_state_clone_for_tabular_purge.clone();
                let secret_store = secret_store.clone();
                Box::pin(async move {
                    tabular_purge_queue::tabular_purge_worker::<C, S>(
                        catalog_state_clone,
                        secret_store,
                        poll_interval,
                        cancellation_token,
                    )
                    .await;
                })
            }),
            num_workers: CONFIG.task_tabular_purge_workers,
            scope: QueueScope::Warehouse,
            user_scheduling: UserScheduling::Disabled,
        })
        .await;

        let catalog_state_for_task_log_cleanup = catalog_state.clone();
        self.register_queue::<
            task_log_cleanup_queue::TaskLogCleanupConfig,
            task_log_cleanup_queue::TaskLogCleanupPayload,
        >(QueueRegistration {
            queue_name: &task_log_cleanup_queue::QUEUE_NAME,
            worker_fn: Arc::new(move |cancellation_token| {
                let catalog_state_clone = catalog_state_for_task_log_cleanup.clone();
                Box::pin(async move {
                    task_log_cleanup_queue::log_cleanup_worker::<C>(
                        catalog_state_clone,
                        poll_interval,
                        cancellation_token,
                    )
                    .await;
                })
            }),
            num_workers: CONFIG.task_log_cleanup_workers,
            scope: QueueScope::Project,
            user_scheduling: UserScheduling::Disabled,
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

    /// Total number of workers across all registered queues. A queue registers
    /// even with `num_workers = 0`, so this differs from `is_empty`: the registry
    /// can be non-empty yet schedule no workers (e.g. every `TASK_*_WORKERS=0`).
    #[must_use]
    pub async fn total_workers(&self) -> usize {
        self.task_workers
            .read()
            .await
            .values()
            .map(|w| w.num_workers)
            .sum()
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
            control: Arc::default(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum QueueScope {
    /// Warehouse-specific configuration
    Warehouse,
    /// Project-specific configuration
    Project,
}

#[derive(Clone)]
/// Contains all required information to dynamically generate API documentation
/// for the warehouse-specific configuration of a task queue.
pub struct QueueApiConfig {
    /// Name of the task queue
    pub queue_name: &'static TaskQueueName,
    /// Name of the configuration type used in the API documentation
    #[cfg(feature = "open-api")]
    pub utoipa_type_name: std::borrow::Cow<'static, str>,
    #[cfg(not(feature = "open-api"))]
    pub utoipa_type_name: (),
    /// Schema for the configuration type used in the API documentation
    #[cfg(feature = "open-api")]
    pub utoipa_schema: utoipa::openapi::RefOr<utoipa::openapi::Schema>,
    #[cfg(not(feature = "open-api"))]
    pub utoipa_schema: (),
    pub scope: QueueScope,
    /// Whether this queue is exposed via the schedule endpoint and (under
    /// `open-api`) the schema of its request payload. See [`UserScheduling`].
    pub user_scheduling: UserScheduling,
}

impl std::fmt::Debug for QueueApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueApiConfig")
            .field("queue_name", &self.queue_name)
            .field("utoipa_type_name", &self.utoipa_type_name)
            .field("utoipa_schema", &"<schema>")
            .field("scope", &self.scope)
            .field("user_scheduling", &self.user_scheduling)
            .finish()
    }
}

/// Parse a stored or just-validated queue config. A failure is an internal error.
fn parse_queue_config<T: TaskConfig>(
    raw_config: serde_json::Value,
) -> Result<T, iceberg_ext::catalog::rest::ErrorModel> {
    serde_json::from_value(raw_config).map_err(|e| {
        iceberg_ext::catalog::rest::ErrorModel::internal(
            format!(
                "Failed to deserialize queue config for queue '{}': {e}",
                T::queue_name()
            ),
            "TaskConfigDeserializeError",
            Some(Box::new(e)),
        )
    })
}

#[cfg(test)]
mod test {

    use std::sync::LazyLock;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::service::tasks::{TaskData, TaskQueueName, TaskResourceDemandRange};

    /// Placeholder payload used by the test queues below. Real queues bind
    /// their actual `TaskData` via the `D` generic of `register_queue`;
    /// these tests don't run a worker so the type just needs to be empty
    /// and `TaskData`-conformant.
    #[derive(Clone, Debug, Serialize, Deserialize, Default)]
    struct TestPayload {}
    impl TaskData for TestPayload {}

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_shared_interior_mutable_state() {
        // This test verifies that RegisteredTaskQueues instances share the same
        // interior mutable state, so that tasks registered later are reflected
        // in previously created RegisteredTaskQueues instances.

        static FIRST_QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| "test-queue".into());
        static SECOND_QUEUE_NAME: LazyLock<TaskQueueName> =
            LazyLock::new(|| "second-test-queue".into());

        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct TestQueueConfig {
            test_field: String,
        }

        impl TaskConfig for TestQueueConfig {
            fn queue_name() -> &'static TaskQueueName {
                &FIRST_QUEUE_NAME
            }

            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(300)
            }
        }

        // Register another queue and verify both instances see it
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct SecondTestQueueConfig {
            other_field: i32,
        }

        impl TaskConfig for SecondTestQueueConfig {
            fn queue_name() -> &'static TaskQueueName {
                &SECOND_QUEUE_NAME
            }

            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(300)
            }
        }

        let registry = TaskQueueRegistry::new();

        // Create an initial RegisteredTaskQueues instance before registering any queues
        let initial_queues = registry.registered_task_queues();

        // Verify registry starts empty and initial_queues reflects this
        assert_eq!(registry.len().await, 0);
        assert!(registry.is_empty().await);
        assert!(initial_queues.api_config().await.is_empty());

        registry
            .register_queue::<TestQueueConfig, TestPayload>(super::QueueRegistration {
                queue_name: &FIRST_QUEUE_NAME,
                worker_fn: std::sync::Arc::new(move |_cancellation_token| {
                    Box::pin(async {
                        // Empty worker for testing
                    })
                }),
                num_workers: 1,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
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
        assert_eq!(initial_api_config[0].queue_name, &*FIRST_QUEUE_NAME);
        assert_eq!(later_api_config[0].queue_name, &*FIRST_QUEUE_NAME);

        // Both should have access to the validator function
        assert!(
            initial_queues
                .validate_config_fn(&FIRST_QUEUE_NAME)
                .await
                .is_some()
        );
        assert!(
            later_queues
                .validate_config_fn(&FIRST_QUEUE_NAME)
                .await
                .is_some()
        );
        let non_existent_queue = TaskQueueName::from("non-existent");
        assert!(
            initial_queues
                .validate_config_fn(&non_existent_queue)
                .await
                .is_none()
        );
        assert!(
            later_queues
                .validate_config_fn(&non_existent_queue)
                .await
                .is_none()
        );

        registry
            .register_queue::<SecondTestQueueConfig, TestPayload>(super::QueueRegistration {
                queue_name: &SECOND_QUEUE_NAME,
                worker_fn: std::sync::Arc::new(move |_cancellation_token| {
                    Box::pin(async {
                        // Empty worker for testing
                    })
                }),
                num_workers: 2,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
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
        assert!(
            initial_queues
                .validate_config_fn(&FIRST_QUEUE_NAME)
                .await
                .is_some()
        );
        assert!(
            initial_queues
                .validate_config_fn(&SECOND_QUEUE_NAME)
                .await
                .is_some()
        );
        assert!(
            later_queues
                .validate_config_fn(&FIRST_QUEUE_NAME)
                .await
                .is_some()
        );
        assert!(
            later_queues
                .validate_config_fn(&SECOND_QUEUE_NAME)
                .await
                .is_some()
        );

        // Verify that the queue names are correctly registered in both instances
        let mut initial_queue_names = initial_api_config
            .iter()
            .map(|q| q.queue_name)
            .collect::<Vec<_>>();
        let mut later_queue_names = later_api_config
            .iter()
            .map(|q| q.queue_name)
            .collect::<Vec<_>>();
        initial_queue_names.sort_unstable();
        later_queue_names.sort_unstable();

        assert_eq!(
            initial_queue_names,
            vec![&*SECOND_QUEUE_NAME, &*FIRST_QUEUE_NAME]
        );
        assert_eq!(
            later_queue_names,
            vec![&*SECOND_QUEUE_NAME, &*FIRST_QUEUE_NAME]
        );
    }

    #[tokio::test]
    async fn test_total_workers_counts_configured_workers_not_registrations() {
        static ZERO_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "zero-worker-queue".into());
        static BUSY_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "busy-queue".into());

        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Cfg {}
        impl TaskConfig for Cfg {
            fn queue_name() -> &'static TaskQueueName {
                &ZERO_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
        }

        let registry = TaskQueueRegistry::new();
        assert_eq!(registry.total_workers().await, 0);

        // A queue registered with zero workers: the registry is non-empty, yet no
        // workers will run. This is the state that `TASK_*_WORKERS=0` produces and
        // that the worker-monitor gate must detect.
        registry
            .register_queue::<Cfg, TestPayload>(QueueRegistration {
                queue_name: &ZERO_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
        assert!(!registry.is_empty().await);
        assert_eq!(registry.total_workers().await, 0);

        registry
            .register_queue::<Cfg, TestPayload>(QueueRegistration {
                queue_name: &BUSY_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 3,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
        assert_eq!(registry.total_workers().await, 3);
    }

    #[tokio::test]
    async fn test_user_schedulable_propagates_through_register_queue() {
        static DEFAULT_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "default-schedulable".into());
        static OPTED_IN_QN: LazyLock<TaskQueueName> =
            LazyLock::new(|| "opted-in-schedulable".into());

        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Cfg {}
        impl TaskConfig for Cfg {
            fn queue_name() -> &'static TaskQueueName {
                &DEFAULT_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
        }

        let registry = TaskQueueRegistry::new();
        let queues = registry.registered_task_queues();
        registry
            .register_queue::<Cfg, TestPayload>(QueueRegistration {
                queue_name: &DEFAULT_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
        registry
            .register_queue::<Cfg, TestPayload>(QueueRegistration {
                queue_name: &OPTED_IN_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                #[cfg(feature = "open-api")]
                user_scheduling: UserScheduling::Enabled {
                    payload_schema: None,
                },
                #[cfg(not(feature = "open-api"))]
                user_scheduling: UserScheduling::Enabled,
            })
            .await;

        assert_eq!(queues.is_user_schedulable(&DEFAULT_QN).await, Some(false));
        assert_eq!(queues.is_user_schedulable(&OPTED_IN_QN).await, Some(true));
        assert_eq!(
            queues
                .is_user_schedulable(&TaskQueueName::from("never-registered"))
                .await,
            None
        );

        assert_eq!(
            queues.user_schedulable_queue_names().await,
            vec![&*OPTED_IN_QN]
        );

        let api_configs = queues.api_config().await;
        let default_cfg = api_configs
            .iter()
            .find(|c| c.queue_name == &*DEFAULT_QN)
            .expect("default queue registered");
        let opted_in_cfg = api_configs
            .iter()
            .find(|c| c.queue_name == &*OPTED_IN_QN)
            .expect("opted-in queue registered");
        assert!(!default_cfg.user_scheduling.is_enabled());
        assert!(opted_in_cfg.user_scheduling.is_enabled());
    }

    #[tokio::test]
    async fn test_check_schedule_eligibility_dispatches_through_registry() {
        use std::collections::HashMap;

        use crate::service::tasks::WarehouseTaskEntityId;

        static EAGER_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "eager-eligibility".into());
        static PICKY_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "picky-eligibility".into());

        // Eager queue: always eligible. Picky queue: rejects when
        // `disabled-by-table-prop` is present, so we can verify the
        // dispatcher passes entity_properties through faithfully.

        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct EagerCfg {}
        impl TaskConfig for EagerCfg {
            fn queue_name() -> &'static TaskQueueName {
                &EAGER_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
        }

        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct PickyCfg {}
        impl TaskConfig for PickyCfg {
            fn queue_name() -> &'static TaskQueueName {
                &PICKY_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
            fn check_schedule_eligibility(
                _config: &Self,
                entity_properties: &HashMap<String, String>,
                _entity: WarehouseTaskEntityId,
            ) -> Result<(), iceberg_ext::catalog::rest::ErrorModel> {
                if entity_properties
                    .get("disabled-by-table-prop")
                    .map(String::as_str)
                    == Some("true")
                {
                    return Err(iceberg_ext::catalog::rest::ErrorModel::bad_request(
                        "rejected",
                        "PickyRejected",
                        None,
                    ));
                }
                Ok(())
            }
        }

        let registry = TaskQueueRegistry::new();
        let queues = registry.registered_task_queues();
        registry
            .register_queue::<EagerCfg, TestPayload>(QueueRegistration {
                queue_name: &EAGER_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
        registry
            .register_queue::<PickyCfg, TestPayload>(QueueRegistration {
                queue_name: &PICKY_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;

        let entity = WarehouseTaskEntityId::Table {
            table_id: crate::service::TableId::new_random(),
        };

        // Eager queue: any input passes.
        let eager_fn = queues
            .schedule_eligibility_fn(&EAGER_QN)
            .await
            .expect("eager queue registered");
        assert!(
            eager_fn(serde_json::json!({}), HashMap::new(), entity).is_ok(),
            "eager queue should accept empty inputs"
        );

        // Picky queue: rejects when the marker property is set, accepts otherwise.
        let picky_fn = queues
            .schedule_eligibility_fn(&PICKY_QN)
            .await
            .expect("picky queue registered");
        let mut bad_props = HashMap::new();
        bad_props.insert("disabled-by-table-prop".to_string(), "true".to_string());
        let err = picky_fn(serde_json::json!({}), bad_props.clone(), entity)
            .expect_err("picky queue should reject when marker prop is set");
        assert_eq!(err.r#type, "PickyRejected");

        // Without the marker, the same picky queue accepts.
        assert!(
            picky_fn(serde_json::json!({}), HashMap::new(), entity).is_ok(),
            "picky queue should accept when the marker prop is absent"
        );
    }

    #[tokio::test]
    async fn test_check_config_dispatches_through_registry() {
        static STRICT_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "strict-config-check".into());

        // Refuses `level: 0`.
        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct StrictCheckCfg {
            #[serde(default)]
            level: u32,
        }
        impl TaskConfig for StrictCheckCfg {
            fn queue_name() -> &'static TaskQueueName {
                &STRICT_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
            fn check_config(config: &Self) -> Result<(), iceberg_ext::catalog::rest::ErrorModel> {
                if config.level == 0 {
                    return Err(iceberg_ext::catalog::rest::ErrorModel::bad_request(
                        "level must be at least 1",
                        "StrictLevelTooLow",
                        None,
                    ));
                }
                Ok(())
            }
        }

        let registry = TaskQueueRegistry::new();
        let queues = registry.registered_task_queues();
        registry
            .register_queue::<StrictCheckCfg, TestPayload>(QueueRegistration {
                queue_name: &STRICT_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;

        let strict_fn = queues
            .config_check_fn(&STRICT_QN)
            .await
            .expect("strict queue registered");
        let err = strict_fn(serde_json::json!({"level": 0}))
            .expect_err("strict queue must refuse level 0");
        assert_eq!(err.code, 400);
        assert_eq!(err.r#type, "StrictLevelTooLow");
        assert_eq!(err.message, "level must be at least 1");
        assert!(strict_fn(serde_json::json!({"level": 1})).is_ok());
    }

    #[tokio::test]
    async fn test_payload_validator_dispatches_through_registry() {
        static EMPTY_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "empty-payload".into());
        static REQUIRED_FIELD_QN: LazyLock<TaskQueueName> =
            LazyLock::new(|| "required-field-payload".into());

        // Empty-payload queue: accepts `{}` and ignores extras (default serde behavior).
        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct EmptyCfg {}
        impl TaskConfig for EmptyCfg {
            fn queue_name() -> &'static TaskQueueName {
                &EMPTY_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
        }
        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        struct EmptyPayload {}
        impl TaskData for EmptyPayload {}

        // Required-field-payload queue: rejects anything missing `must_have`.
        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct RequiredFieldCfg {}
        impl TaskConfig for RequiredFieldCfg {
            fn queue_name() -> &'static TaskQueueName {
                &REQUIRED_FIELD_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct RequiredFieldPayload {
            #[allow(dead_code)]
            must_have: String,
        }
        impl TaskData for RequiredFieldPayload {}

        let registry = TaskQueueRegistry::new();
        let queues = registry.registered_task_queues();
        registry
            .register_queue::<EmptyCfg, EmptyPayload>(QueueRegistration {
                queue_name: &EMPTY_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
        registry
            .register_queue::<RequiredFieldCfg, RequiredFieldPayload>(QueueRegistration {
                queue_name: &REQUIRED_FIELD_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;

        let empty_validator = queues
            .payload_validator_fn(&EMPTY_QN)
            .await
            .expect("empty queue registered");
        // Empty queue accepts the canonical `{}` the endpoint defaults to.
        assert!(empty_validator(serde_json::json!({})).is_ok());

        let required_validator = queues
            .payload_validator_fn(&REQUIRED_FIELD_QN)
            .await
            .expect("required-field queue registered");
        // Required-field queue rejects `{}` (the endpoint's default) when the
        // payload type has a non-optional field — proving an empty-default
        // payload doesn't silently slip past validation for queues that
        // actually need shape.
        assert!(
            required_validator(serde_json::json!({})).is_err(),
            "required-field payload must reject empty `{{}}`"
        );
        // Correct shape passes.
        assert!(
            required_validator(serde_json::json!({"must_have": "yes"})).is_ok(),
            "valid shape must pass"
        );
    }

    #[tokio::test]
    async fn test_queues_cancelled_on_warehouse_delete() {
        use crate::service::tasks::{
            tabular_expiration_queue::SoftDeletionQueueConfig,
            tabular_purge_queue::PurgeQueueConfig, task_log_cleanup_queue::TaskLogCleanupConfig,
        };

        static BLOCKING_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "blocking-queue".into());
        static CANCELLED_QN: LazyLock<TaskQueueName> = LazyLock::new(|| "cancelled-queue".into());
        static CANCELLED_LEGACY_QN: LazyLock<TaskQueueName> =
            LazyLock::new(|| "cancelled-queue-legacy".into());

        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct BlockingCfg {}
        impl TaskConfig for BlockingCfg {
            fn queue_name() -> &'static TaskQueueName {
                &BLOCKING_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
        }

        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct CancelledCfg {}
        impl TaskConfig for CancelledCfg {
            fn queue_name() -> &'static TaskQueueName {
                &CANCELLED_QN
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
            fn legacy_queue_names() -> Vec<&'static TaskQueueName> {
                vec![&CANCELLED_LEGACY_QN]
            }
            fn on_warehouse_delete() -> OnWarehouseDelete {
                OnWarehouseDelete::Cancel
            }
        }

        // Built-in queues keep the default, so pending purges and soft deletions keep their
        // warehouse.
        assert_eq!(
            SoftDeletionQueueConfig::on_warehouse_delete(),
            OnWarehouseDelete::Block
        );
        assert_eq!(
            PurgeQueueConfig::on_warehouse_delete(),
            OnWarehouseDelete::Block
        );
        assert_eq!(
            TaskLogCleanupConfig::on_warehouse_delete(),
            OnWarehouseDelete::Block
        );

        let registry = TaskQueueRegistry::new();
        let queues = registry.registered_task_queues();
        assert_eq!(queues.queues_cancelled_on_warehouse_delete().await, vec![]);
        registry
            .register_queue::<BlockingCfg, TestPayload>(QueueRegistration {
                queue_name: &BLOCKING_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
        registry
            .register_queue::<CancelledCfg, TestPayload>(QueueRegistration {
                queue_name: &CANCELLED_QN,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;

        assert_eq!(
            queues.queues_cancelled_on_warehouse_delete().await,
            vec![(&*CANCELLED_QN, vec![&*CANCELLED_LEGACY_QN])]
        );
    }

    static DEMAND_QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| "demand-queue".into());
    static DEMAND_LEGACY_NAME: LazyLock<TaskQueueName> =
        LazyLock::new(|| "old-demand-queue".into());

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    struct DemandQueueConfig {}

    impl TaskConfig for DemandQueueConfig {
        fn queue_name() -> &'static TaskQueueName {
            &DEMAND_QUEUE_NAME
        }

        fn legacy_queue_names() -> Vec<&'static TaskQueueName> {
            vec![&DEMAND_LEGACY_NAME]
        }

        fn max_time_since_last_heartbeat() -> chrono::Duration {
            chrono::Duration::seconds(60)
        }
    }

    async fn register_demand_queue(registry: &TaskQueueRegistry) {
        registry
            .register_queue::<DemandQueueConfig, TestPayload>(super::QueueRegistration {
                queue_name: &DEMAND_QUEUE_NAME,
                worker_fn: std::sync::Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
    }

    fn demand_class(name: &str, resource_demand: TaskResourceDemandRange) -> TaskDemandClass {
        TaskDemandClass::builder()
            .name(name)
            .resource_demand(resource_demand)
            .build()
    }

    #[cfg(feature = "router")]
    #[tokio::test]
    async fn demand_targets_resolve_legacy_names_and_classes() {
        let registry = TaskQueueRegistry::new();
        let small = TaskResourceDemandRange::builder().at_most(10).build();
        let large = TaskResourceDemandRange::builder().above(10).build();
        let classes = vec![
            demand_class("small", small),
            demand_class("x-large-2", large),
        ];
        register_demand_queue(&registry).await;
        registry
            .register_demand_classes(&DEMAND_QUEUE_NAME, classes)
            .await
            .unwrap();
        let queues = registry.registered_task_queues();

        let expected = |resource_demand| DemandTarget {
            queue_name: &DEMAND_QUEUE_NAME,
            legacy_queue_names: vec![&DEMAND_LEGACY_NAME],
            max_time_since_last_heartbeat: chrono::Duration::seconds(60),
            resource_demand,
        };
        let whole = TaskResourceDemandRange::default();
        for (queue_name, class, target) in [
            (&*DEMAND_QUEUE_NAME, None, Ok(expected(whole))),
            (&*DEMAND_LEGACY_NAME, None, Ok(expected(whole))),
            (&*DEMAND_QUEUE_NAME, Some("small"), Ok(expected(small))),
            (&*DEMAND_LEGACY_NAME, Some("x-large-2"), Ok(expected(large))),
            (
                &*DEMAND_QUEUE_NAME,
                Some("medium"),
                Err(DemandLookupError::UnknownClass),
            ),
            (
                &TaskQueueName::from("other"),
                None,
                Err(DemandLookupError::UnknownQueue),
            ),
        ] {
            assert_eq!(
                queues.demand_target(queue_name, class).await,
                target,
                "{queue_name} {class:?}"
            );
        }
    }

    #[tokio::test]
    async fn demand_classes_need_a_registered_queue_and_unique_valid_names() {
        let registry = TaskQueueRegistry::new();
        let small = TaskResourceDemandRange::builder().at_most(10).build();
        let not_registered = registry
            .register_demand_classes(&DEMAND_QUEUE_NAME, vec![demand_class("small", small)])
            .await
            .unwrap_err();
        assert_eq!(
            not_registered.to_string(),
            "Queue `demand-queue` is not registered"
        );

        register_demand_queue(&registry).await;
        for (names, error) in [
            (
                vec!["Small"],
                "Demand class `Small` of queue `demand-queue`: a name consists of lower case \
                 letters, digits and `-`",
            ),
            (
                vec![""],
                "Demand class `` of queue `demand-queue`: a name consists of lower case letters, \
                 digits and `-`",
            ),
            (
                vec!["small", "small"],
                "Demand class `small` of queue `demand-queue` is named twice",
            ),
        ] {
            let classes = names
                .into_iter()
                .map(|name| demand_class(name, small))
                .collect();
            let result = registry
                .register_demand_classes(&DEMAND_QUEUE_NAME, classes)
                .await;
            assert_eq!(result.unwrap_err().to_string(), error);
        }
    }
}
