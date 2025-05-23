use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    ops::Deref,
    sync::{Arc, OnceLock},
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

pub const DEFAULT_MAX_AGE: chrono::Duration = valid_max_age(3600);
pub const DEFAULT_MAX_RETRIES: i32 = 5;
pub const DEFAULT_NUM_WORKERS: usize = 2;

pub type TaskQueueProducer = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>;
pub type ValidatorFn = Arc<dyn Fn(serde_json::Value) -> serde_json::Result<()> + Send + Sync>;
pub(crate) static RUNNING_QUEUES: OnceLock<HashMap<&'static str, Queue>> = OnceLock::new();

pub trait QueueConfig: ToSchema + Serialize + DeserializeOwned {
    fn max_age(&self) -> chrono::Duration {
        DEFAULT_MAX_AGE
    }
    fn max_retries(&self) -> i32 {
        DEFAULT_MAX_RETRIES
    }
    fn num_workers(&self) -> usize {
        DEFAULT_NUM_WORKERS
    }
}

#[derive(Clone, Serialize, ToSchema)]
pub struct Queue {
    #[serde(skip)]
    validator_fn: ValidatorFn,
    name: String,
    num_workers: usize,
}

impl Queue {
    /// Validates the incoming config payload against the `validator_fn`.
    ///
    /// # Errors
    /// Returns an error if the `payload` is rejected by `validator_fn`.
    pub fn validate_config(&self, payload: serde_json::Value) -> Result<(), serde_json::Error> {
        (self.validator_fn)(payload)
    }
}

impl Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("validator_fn", &"Fn(...)")
            .field("queue_name", &self.name)
            .field("num_workers", &self.num_workers)
            .finish()
    }
}

#[derive(Default)]
pub struct TaskQueues {
    registered_queues: HashMap<&'static str, RegisteredQueue>,
    schemas: Vec<(
        &'static str,
        String,
        utoipa::openapi::RefOr<utoipa::openapi::Schema>,
    )>,
    schema_validators: HashMap<&'static str, ValidatorFn>,
}

impl Debug for TaskQueues {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueues")
            .field("registered_queues", &self.registered_queues.len())
            .field("schemas", &self.schemas.len())
            .field("schema_validators", &self.schema_validators.len())
            .finish()
    }
}

impl TaskQueues {
    #[must_use]
    pub fn new() -> Self {
        Self {
            registered_queues: HashMap::new(),
            schemas: vec![],
            schema_validators: HashMap::default(),
        }
    }

    pub fn register_queue<T: QueueConfig>(
        &mut self,
        queue_name: &'static str,
        queue_task: TaskQueueProducer,
        num_workers: usize,
    ) {
        let des = |v| serde_json::from_value::<T>(v).map(|_| ());
        self.schema_validators.insert(queue_name, Arc::new(des));
        self.schemas.push((
            queue_name,
            T::name().to_string(),
            utoipa::openapi::RefOr::Ref(utoipa::openapi::Ref::from_schema_name(T::name())),
        ));
        self.registered_queues.insert(
            queue_name,
            RegisteredQueue {
                queue_task,
                num_workers,
            },
        );
    }

    #[must_use]
    pub fn schemas(
        &self,
    ) -> Vec<(
        &'static str,
        String,
        utoipa::openapi::RefOr<utoipa::openapi::Schema>,
    )> {
        self.schemas.clone()
    }

    pub fn register_built_in_queues<C: Catalog, S: SecretStore, A: Authorizer>(
        &mut self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
        poll_interval: Duration,
    ) {
        let catalog_state_clone = catalog_state.clone();
        self.register_queue::<ExpirationQueueConfig>(
            tabular_expiration_queue::QUEUE_NAME,
            Arc::new(move || {
                let authorizer = authorizer.clone();
                let catalog_state_clone = catalog_state_clone.clone();
                Box::pin({
                    async move {
                        tabular_expiration_queue::tabular_expiration_task::<C, A>(
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
                    tabular_purge_queue::purge_task::<C, S>(
                        catalog_state_clone.clone(),
                        secret_store.clone(),
                        poll_interval,
                    )
                    .await;
                })
            }),
            2,
        );
    }

    /// Spawns the built-in queues, currently `tabular_expiration_queue` and `tabular_purge_queue` alongside any
    /// registered custom queues.
    ///
    /// # Errors
    /// Fails if any of the queue handlers exit unexpectedly.
    pub async fn spawn_queues(mut self) -> Result<(), anyhow::Error> {
        let mut queue_tasks = vec![];
        let mut qs = HashMap::with_capacity(0);
        let mut queue_info = HashMap::with_capacity(self.registered_queues.len());

        std::mem::swap(&mut self.registered_queues, &mut qs);
        for (
            name,
            RegisteredQueue {
                num_workers,
                queue_task: task_fn,
            },
        ) in qs
        {
            tracing::info!("Starting task queue {name} with {} workers", num_workers);
            for n in 0..num_workers {
                tracing::debug!("Starting task queue {name} worker {n}");
                let task_fut = task_fn();
                queue_tasks.push(tokio::task::spawn(task_fut));
            }
            queue_info.insert(
                name,
                Queue {
                    validator_fn: self
                        .schema_validators
                        .remove(name)
                        .ok_or(anyhow::anyhow!("Validator function not found"))?,
                    name: name.to_string(),
                    num_workers,
                },
            );
        }

        RUNNING_QUEUES.set(queue_info).map_err(|_| {
            tracing::error!("Failed to set schema validators");
            anyhow::anyhow!("Failed to set schema validators")
        })?;
        let (res, index, _) = futures::future::select_all(queue_tasks).await;
        if let Err(e) = res {
            tracing::error!(?e, "Task queue {index} panicked: {e}");
            return Err(anyhow::anyhow!("Task queue {index} panicked: {e}"));
        }
        tracing::error!("Task queue {index} exited unexpectedly");
        Err(anyhow::anyhow!("Task queue {index} exited unexpectedly"))
    }
}

#[derive(Clone)]
struct RegisteredQueue {
    pub queue_task: TaskQueueProducer,
    num_workers: usize,
}

impl Debug for RegisteredQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredQueue")
            .field("queue_task", &"Fn(...)")
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

const fn valid_max_age(num: i64) -> chrono::Duration {
    assert!(num > 0, "max_age must be greater than 0");
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
            task_queue::{tabular_expiration_queue::TabularExpiration, EntityId, TaskMetadata},
            Catalog, ListFlags, Transaction,
        },
    };

    #[sqlx::test]
    #[traced_test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let mut queues = crate::service::task_queue::TaskQueues::new();

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
        let _queue_task = tokio::task::spawn(queues.spawn_queues());

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
