use std::{fmt::Debug, ops::Deref, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use rand::{rng, Rng as _};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use strum::EnumIter;
use uuid::Uuid;

use super::{authz::Authorizer, WarehouseId};
use crate::{
    service::{
        task_queue::{
            tabular_expiration_queue::TabularExpirationInput,
            tabular_purge_queue::TabularPurgeInput,
        },
        Catalog, SecretStore,
    },
    CONFIG,
};

pub mod tabular_expiration_queue;
pub mod tabular_purge_queue;

#[derive(Debug, Clone)]
pub struct TaskQueues {
    tabular_expiration: tabular_expiration_queue::ExpirationQueue,
    tabular_purge: tabular_purge_queue::TabularPurgeQueue,
}

impl TaskQueues {
    #[must_use]
    pub fn new(
        expiration: tabular_expiration_queue::ExpirationQueue,
        purge: tabular_purge_queue::TabularPurgeQueue,
    ) -> Self {
        Self {
            tabular_expiration: expiration,
            tabular_purge: purge,
        }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn queue_tabular_expiration(
        &self,
        task: TabularExpirationInput,
    ) -> crate::api::Result<()> {
        self.tabular_expiration.enqueue(task).await
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn cancel_tabular_expiration(
        &self,
        filter: TaskFilter,
    ) -> crate::api::Result<()> {
        self.tabular_expiration.cancel_pending_tasks(filter).await
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn queue_tabular_purge(
        &self,
        task: TabularPurgeInput,
    ) -> crate::api::Result<()> {
        self.tabular_purge.enqueue(task).await
    }

    /// Spawns the expiration and purge queues.
    ///
    /// # Errors
    /// Fails if any of the queue handlers exit unexpectedly.
    pub async fn spawn_queues<C, S, A>(
        &self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
    ) -> Result<(), anyhow::Error>
    where
        C: Catalog,
        S: SecretStore,
        A: Authorizer,
    {
        let num_workers_per_queue = CONFIG.queue_config.num_workers;

        let mut expiration_handlers = Vec::with_capacity(num_workers_per_queue);
        let mut purge_handlers = Vec::with_capacity(num_workers_per_queue);

        // Spawn the specified number of workers for each queue
        for _ in 0..num_workers_per_queue {
            let expiration_handler =
                tokio::task::spawn(tabular_expiration_queue::tabular_expiration_task::<C, A>(
                    self.tabular_expiration.clone(),
                    self.tabular_purge.clone(),
                    catalog_state.clone(),
                    authorizer.clone(),
                ));
            expiration_handlers.push(expiration_handler);

            let purge_handler = tokio::task::spawn(tabular_purge_queue::purge_task::<C, S>(
                self.tabular_purge.clone(),
                catalog_state.clone(),
                secret_store.clone(),
            ));
            purge_handlers.push(purge_handler);
        }

        // Wait for any task to exit and report the error
        tokio::select! {
            res = futures::future::select_all(expiration_handlers) => {
                let (res, index, _) = res;
                if let Err(e) = res {
                    tracing::error!("Tabular expiration worker {index} panicked: {e}");
                    return Err(anyhow::anyhow!("Tabular expiration worker {index} panicked: {e}"));
                }
                tracing::error!("Tabular expiration worker {index} exited unexpectedly");
                Err(anyhow::anyhow!("Tabular expiration worker {index} exited unexpectedly"))

            }
            res = futures::future::select_all(purge_handlers) => {
                let (res, index, _) = res;
                if let Err(e) = res {
                    tracing::error!("Tabular purge worker {index} panicked: {e}");
                    return Err(anyhow::anyhow!("Tabular purge worker {index} panicked: {e}"));
                }
                tracing::error!("Tabular purge worker {index} exited unexpectedly");
                Err(anyhow::anyhow!("Tabular purge worker {index} exited unexpectedly"))

            }
        }
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

#[async_trait]
pub trait TaskQueue: Debug {
    type Task: Send + Sync + 'static;
    type Input: Debug + Send + Sync + 'static;

    fn config(&self) -> &TaskQueueConfig;
    fn queue_name(&self) -> &'static str;

    async fn enqueue_batch(&self, task: Vec<Self::Input>) -> crate::api::Result<()>;
    async fn enqueue(&self, task: Self::Input) -> crate::api::Result<()> {
        self.enqueue_batch(vec![task]).await
    }
    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>>;
    async fn record_success(&self, id: Uuid) -> crate::api::Result<()>;
    async fn record_failure(&self, id: Uuid, error_details: &str) -> crate::api::Result<()>;
    async fn cancel_pending_tasks(&self, filter: TaskFilter) -> crate::api::Result<()>;

    async fn retrying_record_success(&self, task: &Task) {
        self.retrying_record_success_or_failure(task, Status::Success)
            .await;
    }

    async fn retrying_record_failure(&self, task: &Task, details: &str) {
        self.retrying_record_success_or_failure(task, Status::Failure(details))
            .await;
    }

    async fn retrying_record_success_or_failure(&self, task: &Task, result: Status<'_>) {
        let mut retry = 0;
        while let Err(e) = match result {
            Status::Success => self.record_success(task.task_id).await,
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

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "sqlx-postgres", derive(FromRow))]
pub struct Task {
    pub task_id: Uuid,
    pub queue_name: String,
    pub status: TaskStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub parent_task_id: Option<Uuid>,
    pub attempt: i32,
}

#[derive(Debug, Copy, Clone, PartialEq, EnumIter)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "task_status", rename_all = "kebab-case")
)]
pub enum TaskStatus {
    Pending,
    Finished,
    Running,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub(crate) fn non_terminal_states() -> &'static [TaskStatus] {
        &[TaskStatus::Pending, TaskStatus::Running]
    }
}

#[derive(Debug)]
pub enum Status<'a> {
    Success,
    Failure(&'a str),
}

impl std::fmt::Display for Status<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Success => write!(f, "success"),
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

/// Generate random duration between 1 and 30ms
pub(crate) fn random_ms_duration() -> std::time::Duration {
    let random_duration = rng().random_range(1..=30);
    std::time::Duration::from_millis(random_duration)
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

    use crate::{
        api::{iceberg::v1::PaginationQuery, management::v1::TabularType},
        implementations::postgres::{
            tabular::table::tests::initialize_table, warehouse::test::initialize_warehouse,
            CatalogState, PostgresCatalog, PostgresTransaction,
        },
        service::{
            authz::AllowAllAuthorizer,
            storage::TestProfile,
            task_queue::{
                tabular_expiration_queue::TabularExpirationInput, TaskQueue, TaskQueueConfig,
            },
            Catalog, ListFlags, Transaction,
        },
    };

    #[cfg(feature = "sqlx-postgres")]
    #[sqlx::test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let config = TaskQueueConfig {
            max_retries: 5,
            max_age: chrono::Duration::seconds(3600),
            poll_interval: std::time::Duration::from_millis(100),
            num_workers: 1,
        };

        let rw =
            crate::implementations::postgres::ReadWrite::from_pools(pool.clone(), pool.clone());
        let expiration_queue = Arc::new(
            crate::implementations::postgres::task_queues::TabularExpirationQueue::from_config(
                rw.clone(),
                config.clone(),
            )
            .unwrap(),
        );
        let purge_queue = Arc::new(
            crate::implementations::postgres::task_queues::TabularPurgeQueue::from_config(
                rw.clone(),
                config,
            )
            .unwrap(),
        );

        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let queues =
            crate::service::task_queue::TaskQueues::new(expiration_queue.clone(), purge_queue);
        let secrets =
            crate::implementations::postgres::SecretsState::from_pools(pool.clone(), pool);
        let cloned = queues.clone();
        let cat = catalog_state.clone();
        let sec = secrets.clone();
        let auth = AllowAllAuthorizer;
        let _queues_task = tokio::task::spawn(async move {
            cloned
                .spawn_queues::<PostgresCatalog, _, _>(cat, sec, auth)
                .await
        });

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
        <PostgresCatalog as Catalog>::mark_tabular_as_deleted(
            tab.table_id.into(),
            false,
            trx.transaction(),
        )
        .await
        .unwrap();
        trx.commit().await.unwrap();

        expiration_queue
            .enqueue(TabularExpirationInput {
                tabular_id: tab.table_id.0,
                warehouse_ident: warehouse,
                tabular_type: TabularType::Table,
                purge: true,
                expire_at: chrono::Utc::now() + chrono::Duration::seconds(1),
            })
            .await
            .unwrap();
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
