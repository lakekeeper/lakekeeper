use std::{borrow::Cow, collections::HashMap, fmt::Formatter, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::{
    service::{
        authz::Authorizer,
        task_queue::{
            task_queues_runner::QueueWorkerConfig, TaskConfig, TaskQueueName, TaskQueueWorkerFn,
            TaskQueuesRunner,
        },
        Catalog, SecretStore,
    },
    CancellationToken,
};

pub type ValidatorFn = Arc<dyn Fn(serde_json::Value) -> serde_json::Result<()> + Send + Sync>;

#[derive(Clone)]
struct RegisteredQueue {
    /// API configuration for this queue
    api_config: QueueApiConfig,
    /// Schema validator function for the queue configuration
    /// This function is called to validate the configuration payload
    schema_validator_fn: ValidatorFn,
}

impl std::fmt::Debug for RegisteredQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredQueue")
            .field("api_config", &self.api_config)
            .field("schema_validator_fn", &"Fn(...)")
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
#[derive(Debug)]
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
}

impl std::fmt::Debug for QueueRegistration {
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

    pub async fn register_queue<T: TaskConfig>(&self, task_queue: QueueRegistration) -> &Self {
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

        if let Some(_prev) = self.registered_queues.write().await.insert(
            queue_name,
            RegisteredQueue {
                api_config,
                schema_validator_fn,
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

    pub async fn register_built_in_queues<C: Catalog, S: SecretStore, A: Authorizer>(
        &self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
        poll_interval: Duration,
    ) -> &Self {
        use super::{tabular_expiration_queue, tabular_purge_queue};

        let catalog_state_clone = catalog_state.clone();
        self.register_queue::<tabular_expiration_queue::ExpirationQueueConfig>(QueueRegistration {
            queue_name: &tabular_expiration_queue::QUEUE_NAME,
            worker_fn: Arc::new(move |cancellation_token| {
                let authorizer = authorizer.clone();
                let catalog_state_clone = catalog_state_clone.clone();
                Box::pin({
                    async move {
                        tabular_expiration_queue::tabular_expiration_worker::<C, A>(
                            catalog_state_clone.clone(),
                            authorizer.clone(),
                            poll_interval,
                            cancellation_token,
                        )
                        .await;
                    }
                })
            }),
            num_workers: 2,
        })
        .await;

        self.register_queue::<tabular_purge_queue::PurgeQueueConfig>(QueueRegistration {
            queue_name: &tabular_purge_queue::QUEUE_NAME,
            worker_fn: Arc::new(move |cancellation_token| {
                let catalog_state_clone = catalog_state.clone();
                let secret_store = secret_store.clone();
                Box::pin(async move {
                    tabular_purge_queue::tabular_purge_worker::<C, S>(
                        catalog_state_clone.clone(),
                        secret_store.clone(),
                        poll_interval,
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

#[derive(Clone)]
/// Contains all required information to dynamically generate API documentation
/// for the warehouse-specific configuration of a task queue.
pub struct QueueApiConfig {
    /// Name of the task queue
    pub queue_name: &'static TaskQueueName,
    /// Name of the configuration type used in the API documentation
    pub utoipa_type_name: Cow<'static, str>,
    /// Schema for the configuration type used in the API documentation
    pub utoipa_schema: utoipa::openapi::RefOr<utoipa::openapi::Schema>,
}

impl std::fmt::Debug for QueueApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueApiConfig")
            .field("queue_name", &self.queue_name)
            .field("utoipa_type_name", &self.utoipa_type_name)
            .field("utoipa_schema", &"<schema>")
            .finish()
    }
}

#[cfg(test)]
mod test {

    use std::sync::LazyLock;

    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    use super::*;
    use crate::service::task_queue::TaskQueueName;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_shared_interior_mutable_state() {
        // This test verifies that RegisteredTaskQueues instances share the same
        // interior mutable state, so that tasks registered later are reflected
        // in previously created RegisteredTaskQueues instances.

        static FIRST_QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| "test-queue".into());
        static SECOND_QUEUE_NAME: LazyLock<TaskQueueName> =
            LazyLock::new(|| "second-test-queue".into());

        #[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
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
        #[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
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
            .register_queue::<TestQueueConfig>(super::QueueRegistration {
                queue_name: &FIRST_QUEUE_NAME,
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
        assert_eq!(initial_api_config[0].queue_name, &*FIRST_QUEUE_NAME);
        assert_eq!(later_api_config[0].queue_name, &*FIRST_QUEUE_NAME);

        // Both should have access to the validator function
        assert!(initial_queues
            .validate_config_fn(&FIRST_QUEUE_NAME)
            .await
            .is_some());
        assert!(later_queues
            .validate_config_fn(&FIRST_QUEUE_NAME)
            .await
            .is_some());
        let non_existent_queue = TaskQueueName::from("non-existent");
        assert!(initial_queues
            .validate_config_fn(&non_existent_queue)
            .await
            .is_none());
        assert!(later_queues
            .validate_config_fn(&non_existent_queue)
            .await
            .is_none());

        registry
            .register_queue::<SecondTestQueueConfig>(super::QueueRegistration {
                queue_name: &SECOND_QUEUE_NAME,
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
            .validate_config_fn(&FIRST_QUEUE_NAME)
            .await
            .is_some());
        assert!(initial_queues
            .validate_config_fn(&SECOND_QUEUE_NAME)
            .await
            .is_some());
        assert!(later_queues
            .validate_config_fn(&FIRST_QUEUE_NAME)
            .await
            .is_some());
        assert!(later_queues
            .validate_config_fn(&SECOND_QUEUE_NAME)
            .await
            .is_some());

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
}
