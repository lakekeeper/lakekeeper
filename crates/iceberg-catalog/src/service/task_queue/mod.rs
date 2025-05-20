use std::{collections::HashMap, fmt::Debug, ops::Deref, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use futures::{future::BoxFuture, FutureExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use strum::EnumIter;
use uuid::Uuid;

use super::{authz::Authorizer, WarehouseId};
use crate::{
    service::{Catalog, SecretStore},
    CONFIG,
};

pub mod tabular_expiration_queue;
pub mod tabular_purge_queue;

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub config: TaskQueueConfig,
    pub channel_size: usize,
}

pub type TaskQueueProducer = Arc<
    dyn Fn(async_channel::Receiver<Task>) -> BoxFuture<'static, crate::api::Result<()>>
        + Send
        + Sync
        + 'static,
>;

pub const DEFAULT_CHANNEL_SIZE: usize = 1000;

#[derive(Clone)]
struct RegisteredQueue {
    pub config: QueueConfig,
    pub queue_task: TaskQueueProducer,
}

impl Debug for RegisteredQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredQueue")
            .field("config", &self.config)
            .field("queue_task", &"Fn(...)")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct TaskQueues {
    queues: HashMap<&'static str, async_channel::Sender<Task>>,
    queue: Arc<dyn TaskQueue + Send + Sync + 'static>,
    registered_queues: HashMap<&'static str, RegisteredQueue>,
}

impl TaskQueues {
    #[must_use]
    pub fn new(queue: Arc<dyn TaskQueue + Send + Sync + 'static>) -> Self {
        Self {
            queues: HashMap::new(),
            queue,
            registered_queues: HashMap::new(),
        }
    }

    pub fn register_queue(
        &mut self,
        queue_name: &'static str,
        config: QueueConfig,
        queue_task: TaskQueueProducer,
    ) {
        self.registered_queues
            .insert(queue_name, RegisteredQueue { config, queue_task });
    }

    async fn run(self, poll_interval: Duration) {
        loop {
            for (queue_name, queue_tx) in &self.queues {
                match self.queue.pick_new_task(queue_name).await {
                    Ok(Some(task)) => {
                        if let Err(e) = tryhard::retry_fn(|| queue_tx.send(task.clone()))
                            .retries(5)
                            .fixed_backoff(Duration::from_millis(100))
                            .await
                        {
                            // TODO: Do we even want to record_failure for this?
                            let _ = self
                                .queue
                                .retrying_record_failure(
                                    &task,
                                    &format!("Failed to forward task to task queue handler {e}"),
                                )
                                .await;
                        };
                    }
                    Ok(None) => {
                        tracing::trace!("No task available for queue '{queue_name}'");
                        // No task available
                    }
                    Err(e) => {
                        tracing::error!("Failed to pick new task: '{}'", e.error);
                    }
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn outer_run(mut self, poll_interval: Duration) -> anyhow::Result<()> {
        let mut queue_tasks = vec![];
        let mut qs = HashMap::with_capacity(0);
        std::mem::swap(&mut self.registered_queues, &mut qs);
        for (
            name,
            RegisteredQueue {
                config,
                queue_task: task_fn,
            },
        ) in qs
        {
            tracing::info!(
                "Starting task queue {name} with {} workers",
                config.config.num_workers
            );
            let (tx, rx) = async_channel::bounded(config.channel_size);
            for n in 0..config.config.num_workers {
                tracing::debug!("Starting task queue {name} worker {n}");
                let task_fut = task_fn(rx.clone());
                queue_tasks.push(tokio::task::spawn(task_fut));
            }
            self.queues.insert(name, tx);
        }
        let feeder = tokio::task::spawn(self.run(poll_interval));
        tokio::select! {
            res = futures::future::select_all(queue_tasks) => {
                let (res, index, _) = res;
                if let Err(e) = res {
                    tracing::error!("Task queue {index} panicked: {e}");
                    return Err(anyhow::anyhow!("Task queue {index} panicked: {e}"));
                }
                tracing::error!("Task queue {index} exited unexpectedly");
                return Err(anyhow::anyhow!("Task queue {index} exited unexpectedly"))
            }
            res = feeder => {
                if let Err(e) = res {
                    tracing::error!("Feeder task panicked: {e}");
                    return Err(anyhow::anyhow!("Feeder task panicked: {e}"))
                }
            }
        };
        Ok(())
    }

    /// Spawns the built-in queues, currently tabular_expiration and tabular_purge alongside any
    /// registered custom queues.
    ///
    /// # Errors
    /// Fails if any of the queue handlers exit unexpectedly.
    pub async fn spawn_queues<C, S, A>(
        mut self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
        poll_interval: Duration,
    ) -> Result<(), anyhow::Error>
    where
        C: Catalog,
        S: SecretStore,
        A: Authorizer,
    {
        let queue = self.queue.clone();
        let catalog_state_clone = catalog_state.clone();
        self.register_queue(
            tabular_expiration_queue::QUEUE_NAME,
            QueueConfig {
                config: CONFIG.queue_config.clone(),
                channel_size: DEFAULT_CHANNEL_SIZE,
            },
            Arc::new(move |rx| {
                let value = queue.clone();
                let catalog_state = catalog_state_clone.clone();
                let authorizer = authorizer.clone();
                async move {
                    tabular_expiration_queue::tabular_expiration_task::<C, A>(
                        rx,
                        value,
                        catalog_state,
                        authorizer,
                    )
                    .await;
                    Ok(())
                }
                .boxed()
            }),
        );
        let queue = self.queue.clone();
        let catalog_state = catalog_state.clone();
        self.register_queue(
            tabular_purge_queue::QUEUE_NAME,
            QueueConfig {
                config: CONFIG.queue_config.clone(),
                channel_size: DEFAULT_CHANNEL_SIZE,
            },
            Arc::new(move |rx| {
                let value = queue.clone();
                let catalog_state = catalog_state.clone();
                let secret_store = secret_store.clone();
                async move {
                    tabular_purge_queue::purge_task::<C, S>(rx, value, catalog_state, secret_store)
                        .await;
                    Ok(())
                }
                .boxed()
            }),
        );
        self.outer_run(poll_interval).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskId(Uuid);

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

/// A filter to select tasks
#[derive(Debug, Clone, PartialEq)]
pub enum TaskFilter {
    WarehouseId(WarehouseId),
    TaskIds(Vec<TaskId>),
}

#[derive(Debug, Clone)]
pub struct TaskInput {
    pub task_metadata: TaskMetadata,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskMetadata {
    pub warehouse_id: WarehouseId,
    pub parent_task_id: Option<Uuid>,
    pub entity_id: EntityId,
    pub suspend_until: Option<chrono::DateTime<Utc>>,
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
    pub task_id: Uuid,
    pub status: TaskStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub attempt: i32,
    pub(crate) config: Option<serde_json::Value>,
    pub(crate) state: serde_json::Value,
}
/// `TaskQueue` is a trait which defines the interface available to a task worker.
///
/// It allows the worker to pick up a new task, record success or failure of a task. It also offers
/// a few convenience methods for recording success or failure with retries.
#[async_trait]
pub trait TaskQueue: Debug {
    async fn pick_new_task(&self, queue_name: &str) -> crate::api::Result<Option<Task>>;
    async fn record_success(&self, id: Uuid, message: Option<&str>) -> crate::api::Result<()>;
    async fn record_failure(&self, id: Uuid, error_details: &str) -> crate::api::Result<()>;

    async fn retrying_record_success(&self, task: &Task, details: Option<&str>) {
        self.retrying_record_success_or_failure(task, Status::Success(details))
            .await;
    }

    async fn retrying_record_failure(&self, task: &Task, details: &str) {
        self.retrying_record_success_or_failure(task, Status::Failure(details))
            .await;
    }

    async fn retrying_record_success_or_failure(&self, task: &Task, result: Status<'_>) {
        let mut retry = 0;
        while let Err(e) = match result {
            Status::Success(details) => self.record_success(task.task_id, details).await,
            Status::Failure(details) => self.record_failure(task.task_id, details).await,
        } {
            tracing::error!("Failed to record {}: {:?}", result, e);
            tokio::time::sleep(Duration::from_secs(1 + retry)).await;
            retry += 1;
            if retry > 5 {
                tracing::error!("Giving up trying to record {}.", result);
                break;
            }
        }
    }
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
    Pending,
    Running,
}

#[derive(Debug)]
pub enum Status<'a> {
    Success(Option<&'a str>),
    Failure(&'a str),
}

impl std::fmt::Display for Status<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Success(details) => write!(f, "success ({})", details.unwrap_or("")),
            Status::Failure(details) => write!(f, "failure ({details})"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskQueueConfig {
    pub max_retries: i32,
    #[serde(
        deserialize_with = "crate::config::seconds_to_duration",
        serialize_with = "crate::config::duration_to_seconds"
    )]
    pub max_age: chrono::Duration,
    #[serde(
        deserialize_with = "crate::config::seconds_to_std_duration",
        serialize_with = "crate::config::serialize_std_duration_as_ms"
    )]
    pub poll_interval: Duration,
    pub num_workers: usize,
}

impl Default for TaskQueueConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            max_age: valid_max_age(3600),
            poll_interval: Duration::from_secs(10),
            num_workers: 2,
        }
    }
}

const fn valid_max_age(num: i64) -> chrono::Duration {
    assert!(num > 0, "max_age must be greater than 0");
    let dur = chrono::Duration::seconds(num);
    assert!(dur.num_microseconds().is_some());
    dur
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use sqlx::PgPool;
    use tracing_test::traced_test;

    use crate::{
        api::{
            iceberg::v1::PaginationQuery,
            management::v1::{DeleteKind, TabularType},
        },
        implementations::postgres::{
            tabular::table::tests::initialize_table, task_queues::PgQueue,
            warehouse::test::initialize_warehouse, CatalogState, PostgresCatalog,
            PostgresTransaction,
        },
        service::{
            authz::AllowAllAuthorizer,
            storage::TestProfile,
            task_queue::{
                tabular_expiration_queue::TabularExpiration, EntityId, TaskMetadata,
                TaskQueueConfig,
            },
            Catalog, ListFlags, Transaction,
        },
    };

    // #[cfg(feature = "sqlx-postgres")]
    #[sqlx::test]
    #[traced_test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let config = TaskQueueConfig {
            max_retries: 5,
            max_age: chrono::Duration::seconds(3600),
            poll_interval: std::time::Duration::from_millis(100),
            num_workers: 1,
        };

        let rw =
            crate::implementations::postgres::ReadWrite::from_pools(pool.clone(), pool.clone());
        let queue = Arc::new(PgQueue::from_config(rw, config).unwrap());

        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let queues = crate::service::task_queue::TaskQueues::new(queue.clone());

        let secrets =
            crate::implementations::postgres::SecretsState::from_pools(pool.clone(), pool);
        let cat = catalog_state.clone();
        let sec = secrets.clone();
        let auth = AllowAllAuthorizer;
        let _queue_task = tokio::task::spawn(
            queues.spawn_queues::<PostgresCatalog, _, AllowAllAuthorizer>(
                cat,
                sec,
                auth,
                std::time::Duration::from_millis(100),
            ),
        );

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
                suspend_until: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
            },
            TabularExpiration {
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
