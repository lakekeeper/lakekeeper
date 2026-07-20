use std::{collections::HashMap, fmt::Formatter, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::{
    CONFIG, CancellationToken,
    service::{
        CatalogStore, SecretStore,
        authz::Authorizer,
        tasks::{
            TaskConfig, TaskQueueName, TaskQueueWorkerFn, TaskQueuesRunner,
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

#[derive(Clone)]
struct RegisteredQueue {
    /// API configuration for this queue
    api_config: QueueApiConfig,
    /// Schema validator function for the queue configuration
    /// This function is called to validate the configuration payload
    schema_validator_fn: ValidatorFn,
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
}

impl std::fmt::Debug for RegisteredQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredQueue")
            .field("api_config", &self.api_config)
            .field("schema_validator_fn", &"Fn(...)")
            .field("schedule_eligibility_fn", &"Fn(...)")
            .field("payload_validator_fn", &"Fn(...)")
            .field("legacy_names", &self.legacy_names)
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
        let schedule_eligibility_fn: ScheduleEligibilityFn =
            Arc::new(|raw_config, entity_props, entity| {
                let config: T = serde_json::from_value(raw_config).map_err(|e| {
                    iceberg_ext::catalog::rest::ErrorModel::internal(
                        format!(
                            "Failed to deserialize queue config for queue '{}': {e}",
                            T::queue_name()
                        ),
                        "TaskConfigDeserializeError",
                        Some(Box::new(e)),
                    )
                })?;
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
                schedule_eligibility_fn,
                payload_validator_fn,
                legacy_names: T::legacy_queue_names(),
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

#[cfg(test)]
mod test {

    use std::sync::LazyLock;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::service::tasks::{TaskData, TaskQueueName};

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
}
