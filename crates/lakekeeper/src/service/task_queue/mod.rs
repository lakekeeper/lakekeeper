use std::{fmt::Debug, ops::Deref, sync::LazyLock, time::Duration};

use chrono::Utc;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use serde::{de::DeserializeOwned, Serialize};
use strum::EnumIter;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{Transaction, WarehouseId};
use crate::service::Catalog;

mod task_queues_runner;
mod task_registry;
pub use task_queues_runner::{TaskQueueWorkerFn, TaskQueuesRunner};
pub use task_registry::{
    QueueApiConfig, QueueRegistration, RegisteredTaskQueues, TaskQueueRegistry, ValidatorFn,
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
/// Metadata stored for each task in the database backend.
/// This is separate from the task payload, which is specific to each task type.
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
    fn task_data<T: TaskData>(&self) -> crate::api::Result<T> {
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
    fn queue_config<T: QueueConfig>(&self) -> crate::api::Result<Option<T>> {
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
    #[must_use]
    pub fn queue_name() -> &'static str {
        Q::queue_name()
    }

    /// Schedule a single task.
    ///
    /// There can only be a single active task for a (`entity_id`, `queue_name`) tuple.
    /// Resubmitting a pending/running task returns a `None` instead of a new `TaskId`.
    ///
    /// # Errors
    /// Returns an error if the task cannot be enqueued / scheduled.
    pub async fn schedule_task<C: Catalog>(
        task_metadata: TaskMetadata,
        payload: D,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<Option<TaskId>, ErrorModel> {
        C::enqueue_task(
            Self::queue_name(),
            TaskInput {
                task_metadata,
                payload: serde_json::to_value(&payload).map_err(|e| {
                    ErrorModel::internal(
                        format!(
                            "Failed to serialize payload for `{}` task: {e}",
                            Self::queue_name()
                        ),
                        "TaskPayloadSerializationError",
                        Some(Box::new(e)),
                    )
                })?,
            },
            transaction,
        )
        .await
        .map_err(Into::into)
    }

    /// Schedule multiple tasks in a single transaction.
    ///
    /// There can only be a single active task for a (`entity_id`, `queue_name`) tuple.
    /// Resubmitting a pending/running task returns a `None` instead of a new `TaskId`.
    ///
    /// CAUTION: `tasks` may be longer than the returned `Vec<TaskId>`
    ///
    /// # Errors
    /// Returns an error if the tasks cannot be enqueued / scheduled.
    pub async fn schedule_tasks<C: Catalog>(
        tasks: Vec<(TaskMetadata, D)>,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>, ErrorModel> {
        let task_inputs = tasks
            .into_iter()
            .map(|(meta, payload)| {
                Ok(TaskInput {
                    task_metadata: meta,
                    payload: serde_json::to_value(&payload).map_err(|e| {
                        ErrorModel::internal(
                            format!(
                                "Failed to serialize payload for `{}` task: {e}",
                                Self::queue_name()
                            ),
                            "TaskPayloadSerializationError",
                            Some(Box::new(e)),
                        )
                    })?,
                })
            })
            .collect::<Result<Vec<_>, ErrorModel>>()?;

        C::enqueue_tasks(Self::queue_name(), task_inputs, transaction)
            .await
            .map_err(Into::into)
    }

    /// Cancel scheduled tasks matching the filter.
    ///
    /// If `cancel_running_and_should_stop` is true, also cancel tasks in the `running` and `should-stop` states.
    #[tracing::instrument(level = "info", skip(transaction), fields(queue_name = %Self::queue_name(), filter = ?filter, cancel_running_and_should_stop))]
    pub async fn cancel_scheduled_tasks<C: Catalog>(
        filter: TaskFilter,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
        cancel_running_and_should_stop: bool,
    ) -> Result<(), IcebergErrorResponse> {
        C::cancel_scheduled_tasks(
            Some(Self::queue_name()),
            filter,
            cancel_running_and_should_stop,
            transaction,
        )
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to cancel scheduled tasks for `{}` queue.",
                Self::queue_name()
            ))
        })
    }

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
                        Self::queue_name(),
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to record error for task {} in queue '{}' on attempt {attempt}/5: {e}",
                        self.task_id,
                        Self::queue_name(),
                    );

                    if attempt < 5 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    } else {
                        tracing::error!(
                            "Failed to record error for task {} in queue '{}' after 5 attempts. {e}. Original Error: {error}",
                            self.task_id,
                            Self::queue_name()
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
                        Self::queue_name(),
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to record success for task {} in queue '{}' on attempt {attempt}/5: {e}",
                        self.task_id,
                        Self::queue_name(),
                    );

                    if attempt < 5 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    } else {
                        tracing::error!(
                            "Failed to record success for task {} in queue '{}' after 5 attempts. {e}. Original Success Details: {}",
                            self.task_id,
                            Self::queue_name(),
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
                    Self::queue_name(),
                );
            }
            Err(e) => {
                tracing::error!(
                    "Failed to record success for task {} in queue '{}': {e}. Original Success Details: {}",
                    self.task_id,
                    Self::queue_name(),
                    details.unwrap_or("No details provided")
                );
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
                    self.task_id, Self::queue_name()
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
                Self::queue_name()
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
                            Self::queue_name(),
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
                            Self::queue_name(),
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

    use sqlx::PgPool;
    use tracing_test::traced_test;

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
        tabular_expiration_queue::TabularExpirationTask::schedule_task::<PostgresCatalog>(
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
