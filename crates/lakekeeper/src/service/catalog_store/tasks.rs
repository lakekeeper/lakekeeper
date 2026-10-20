use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use super::{CatalogStore, Transaction};
use crate::{
    WarehouseId,
    api::management::v1::{
        task_queue::{GetTaskQueueConfigResponse, SetTaskQueueConfigRequest},
        tasks::{ListTasksRequest, TaskAttempt},
    },
    service::{
        ArcProjectId, CatalogBackendError, DatabaseIntegrityError, Result,
        define_transparent_error,
        events::{AuthorizationFailureReason, AuthorizationFailureSource},
        impl_error_stack_methods, impl_from_with_detail,
        task_configs::TaskQueueConfigFilter,
        tasks::{
            CancelTasksFilter, EnqueueOutcome, MaterializedTick, ProjectTaskScheduleState,
            ResolvedTaskEntity, RetryPolicy, Task, TaskAttemptId, TaskCheckState, TaskDetailsScope,
            TaskFilter, TaskId, TaskInfo, TaskInput, TaskQueueName, TaskResolveScope, TaskSchedule,
            TaskScheduleFilter, TaskScheduleId, TaskScheduleStoreError, TaskScheduleUpsert,
            TriggerTaskScheduleError, UpsertTaskScheduleError,
            task_schedule::validate_task_schedule_interval,
        },
    },
};

struct TasksCacheExpiry;
const TASKS_CACHE_TTL: Duration = Duration::from_hours(1);
impl<K, V> moka::Expiry<K, V> for TasksCacheExpiry {
    fn expire_after_create(&self, _key: &K, _value: &V, _created_at: Instant) -> Option<Duration> {
        Some(TASKS_CACHE_TTL)
    }
}
static TASKS_CACHE: LazyLock<moka::future::Cache<TaskId, Arc<ResolvedTask>>> =
    LazyLock::new(|| {
        moka::future::Cache::builder()
            .max_capacity(10000)
            .expire_after(TasksCacheExpiry)
            .build()
    });

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTask {
    pub task_id: TaskId,
    pub project_id: ArcProjectId,
    pub entity: ResolvedTaskEntity,
    pub queue_name: TaskQueueName,
}

impl ResolvedTask {
    #[must_use]
    pub fn warehouse_id(&self) -> Option<WarehouseId> {
        self.entity.warehouse_id()
    }
}

#[derive(Debug)]
pub struct TaskList {
    pub tasks: Vec<TaskInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug)]
pub struct TaskDetails {
    pub task: TaskInfo,
    pub execution_details: Option<serde_json::Value>,
    pub data: serde_json::Value,
    pub attempts: Vec<TaskAttempt>,
    /// Message of the most-recent attempt: success result details if it
    /// succeeded, or the failure reason if it failed. `None` while the
    /// current attempt is still running.
    pub message: Option<String>,
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Task with id `{task_id}` not found")]
pub struct TaskNotFoundError {
    pub task_id: TaskId,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(TaskNotFoundError);
impl From<TaskNotFoundError> for ErrorModel {
    fn from(value: TaskNotFoundError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(value.to_string())
            .r#type("TaskNotFoundError")
            .stack(value.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum ResolveTasksError,
    stack_message: "Error resolving tasks in catalog",
    variants: [
        TaskNotFoundError,
        DatabaseIntegrityError,
        CatalogBackendError
    ]
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Expected Warehouse task but received project task")]
pub struct NoWarehouseTaskError {
    pub stack: Vec<String>,
}

impl AuthorizationFailureSource for NoWarehouseTaskError {
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InvalidRequestData
    }

    fn into_error_model(self) -> ErrorModel {
        self.into()
    }
}

impl_error_stack_methods!(NoWarehouseTaskError);
impl From<NoWarehouseTaskError> for ErrorModel {
    fn from(value: NoWarehouseTaskError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::UNPROCESSABLE_ENTITY.as_u16())
            .message(value.to_string())
            .r#type("NoWarehouseTaskError")
            .stack(value.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum GetTaskDetailsError,
    stack_message: "Error getting task details in catalog",
    variants: [
        TaskNotFoundError,
        DatabaseIntegrityError,
        CatalogBackendError
    ]
}

#[async_trait::async_trait]
pub trait CatalogTaskOps
where
    Self: CatalogStore,
{
    /// `default_max_time_since_last_heartbeat` is only used if no task configuration is found
    /// in the DB for the given `queue_name`, typically before a user has configured the value explicitly.
    ///
    /// An attempt whose heartbeat timed out counts as failed: it is retried after `retry_policy`'s
    /// backoff while retries remain, and otherwise moves to the log as failed. This includes an
    /// attempt that was asked to stop.
    #[tracing::instrument(
        name = "catalog_pick_new_task",
        skip(state, default_max_time_since_last_heartbeat, retry_policy)
    )]
    async fn pick_new_task(
        queue_name: &TaskQueueName,
        legacy_queue_names: &[&TaskQueueName],
        default_max_time_since_last_heartbeat: chrono::Duration,
        retry_policy: RetryPolicy,
        state: Self::State,
    ) -> Result<Option<Task>> {
        Self::pick_new_task_impl(
            queue_name,
            legacy_queue_names,
            default_max_time_since_last_heartbeat,
            retry_policy,
            state,
        )
        .await
    }

    async fn record_task_success(
        id: TaskAttemptId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::record_task_success_impl(id, message, transaction).await
    }

    /// Record a failed attempt. If `id.attempt` is below `max_retries`, the task is retried no
    /// earlier than `retry_backoff` from now; otherwise it moves to the log as failed.
    async fn record_task_failure(
        id: TaskAttemptId,
        error_details: &str,
        max_retries: i32,
        retry_backoff: chrono::Duration,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::record_task_failure_impl(id, error_details, max_retries, retry_backoff, transaction)
            .await
    }

    /// Cancel scheduled tasks matching the filter.
    ///
    /// If `cancel_running_and_should_stop` is true, also cancel tasks in the `running` and `should-stop` states.
    /// If `queue_name` is `None`, cancel tasks in all queues.
    async fn cancel_scheduled_tasks(
        queue_name: Option<&TaskQueueName>,
        legacy_queue_names: &[&TaskQueueName],
        filter: CancelTasksFilter,
        cancel_running_and_should_stop: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::cancel_scheduled_tasks_impl(
            queue_name,
            legacy_queue_names,
            filter,
            cancel_running_and_should_stop,
            transaction,
        )
        .await
    }

    /// Report progress and heartbeat the task. Also checks whether the task should continue to run.
    async fn check_and_heartbeat_task(
        id: TaskAttemptId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        progress: f32,
        execution_details: Option<serde_json::Value>,
    ) -> Result<TaskCheckState> {
        Self::check_and_heartbeat_task_impl(id, transaction, progress, execution_details).await
    }

    /// Sends stop signals to the tasks.
    /// Only affects tasks in the `running` state.
    ///
    /// It is up to the task handler to decide if it can stop.
    async fn stop_tasks(
        task_ids: &[TaskId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::stop_tasks_impl(task_ids, transaction).await
    }

    /// Reschedule tasks to run at a specific time by setting `scheduled_for` to the provided timestamp.
    /// If no `scheduled_for` is `None`, the tasks will be scheduled to run immediately.
    /// Only affects tasks in the `Scheduled` or `Stopping` state.
    async fn run_tasks_at(
        task_ids: &[TaskId],
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::run_tasks_at_impl(task_ids, scheduled_for, transaction).await
    }

    /// Get task details by task id.
    /// Return Ok(None) if the task does not exist.
    async fn get_task_details(
        task_id: TaskId,
        scope: TaskDetailsScope,
        num_attempts: u16,
        state: Self::State,
    ) -> Result<Option<TaskDetails>, GetTaskDetailsError> {
        Self::get_task_details_impl(task_id, scope, num_attempts, state).await
    }

    /// Enqueue a single task to a task queue.
    ///
    /// There can only be a single active task for a (`entity_id`, `queue_name`) tuple.
    /// Resubmitting while one is pending or running returns [`EnqueueOutcome::Existing`]
    /// with that task's id and status.
    async fn enqueue_task(
        queue_name: &'static TaskQueueName,
        task: TaskInput,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<EnqueueOutcome> {
        Self::enqueue_task_impl(queue_name, task, transaction).await
    }

    /// Enqueue a batch of tasks to a task queue.
    ///
    /// There can only be a single task running or pending for a (`entity_id`, `queue_name`) tuple.
    /// Any resubmitted pending/running task will be omitted from the returned task ids.
    ///
    /// CAUTION: `tasks` may be longer than the returned `Vec<TaskId>`.
    async fn enqueue_tasks(
        queue_name: &'static TaskQueueName,
        tasks: Vec<TaskInput>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>> {
        Self::enqueue_tasks_impl(queue_name, tasks, transaction).await
    }

    /// List tasks
    async fn list_tasks(
        filter: &TaskFilter,
        query: &ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<TaskList> {
        Self::list_tasks_impl(filter, query, transaction).await
    }

    /// Resolve tasks among all known active and historical tasks.
    /// Returns a map of `task_id` to `(TaskEntity, queue_name)`.
    /// If a task does not exist, it is not included in the map.
    async fn resolve_tasks(
        scope: TaskResolveScope,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, Arc<ResolvedTask>>, ResolveTasksError> {
        if task_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut cached_results = HashMap::new();
        for id in task_ids {
            if let Some(cached_value) = TASKS_CACHE.get(id).await
                && task_matches_scope(&cached_value, &scope)
            {
                cached_results.insert(*id, cached_value);
            }
        }
        let not_cached_ids: Vec<TaskId> = task_ids
            .iter()
            .copied()
            .filter(|id| !cached_results.contains_key(id))
            .collect();
        if not_cached_ids.is_empty() {
            return Ok(cached_results);
        }
        let resolve_uncached_result =
            Self::resolve_tasks_impl(scope, &not_cached_ids, state).await?;
        for value in resolve_uncached_result {
            let value = Arc::new(value);
            cached_results.insert(value.task_id, value.clone());
            TASKS_CACHE.insert(value.task_id, value).await;
        }
        Ok(cached_results)
    }

    async fn resolve_required_tasks(
        scope: TaskResolveScope,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, Arc<ResolvedTask>>, ResolveTasksError> {
        let tasks = Self::resolve_tasks(scope, task_ids, state).await?;

        for task_id in task_ids {
            if !tasks.contains_key(task_id) {
                return Err(TaskNotFoundError {
                    task_id: *task_id,
                    stack: Vec::new(),
                }
                .into());
            }
        }

        Ok(tasks)
    }

    /// Move a pending task of the queue, under its name or a legacy one, to `new_parent` and
    /// replace its `task_data`, in one write. Returns `false` and changes nothing if the task is
    /// not a pending task of the queue.
    async fn reparent_pending_task(
        queue_name: &TaskQueueName,
        legacy_queue_names: &[&TaskQueueName],
        task_id: TaskId,
        new_parent: TaskId,
        task_data: serde_json::Value,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<bool> {
        Self::reparent_pending_task_impl(
            queue_name,
            legacy_queue_names,
            task_id,
            new_parent,
            task_data,
            transaction,
        )
        .await
    }

    /// Create or update a task schedule. A changed interval or anchor, or enabling a disabled
    /// schedule, moves `next_run_at` to the first tick at or after now; ticks passed over this
    /// way are not counted in `missed_runs`, which is always kept. Fails for a schedule written
    /// by a newer server version.
    async fn upsert_task_schedule(
        schedule: TaskScheduleUpsert,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<TaskSchedule, UpsertTaskScheduleError> {
        validate_task_schedule_interval(schedule.interval)?;
        Self::upsert_task_schedule_impl(schedule, transaction).await
    }

    /// List task schedules, ordered by `schedule_id`.
    async fn list_task_schedules(
        filter: &TaskScheduleFilter,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskSchedule>, TaskScheduleStoreError> {
        Self::list_task_schedules_impl(filter, transaction).await
    }

    /// Delete a task schedule. Tasks it already produced are left alone.
    /// Returns whether it existed.
    async fn delete_task_schedule(
        schedule_id: TaskScheduleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<bool, TaskScheduleStoreError> {
        Self::delete_task_schedule_impl(schedule_id, transaction).await
    }

    /// Every project, with the interval of its project-level schedule of `queue_name` and
    /// `schedule_key` and its project-level configuration of the queue, ordered by project id.
    /// The schedules stay locked until the transaction ends, so a concurrent config change applies
    /// to a schedule only after this transaction writes it.
    async fn list_project_task_schedule_states(
        queue_name: &TaskQueueName,
        schedule_key: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<ProjectTaskScheduleState>, TaskScheduleStoreError> {
        Self::list_project_task_schedule_states_impl(queue_name, schedule_key, transaction).await
    }

    /// Delete every schedule of a warehouse, of any spec version. Tasks they already produced
    /// are left alone.
    async fn delete_warehouse_task_schedules(
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<(), TaskScheduleStoreError> {
        Self::delete_warehouse_task_schedules_impl(warehouse_id, transaction).await
    }

    /// Start an extra run of a schedule now. `next_run_at` is not changed. If an instance is
    /// already active, returns [`EnqueueOutcome::Existing`] with it. A disabled schedule can be
    /// triggered.
    async fn trigger_task_schedule(
        schedule_id: TaskScheduleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<EnqueueOutcome, TriggerTaskScheduleError> {
        Self::trigger_task_schedule_impl(schedule_id, transaction).await
    }

    /// Insert one task for each of at most `limit` enabled, due schedules whose previous instance
    /// has finished, and move each one's `next_run_at` to its first tick after now. Earlier due
    /// ticks the task covers are added to `missed_runs`. A schedule whose previous instance is
    /// still active stays due. Schedules locked by another transaction are left for the next
    /// pass.
    async fn materialize_due_task_schedules(
        limit: i64,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<MaterializedTick>, TaskScheduleStoreError> {
        Self::materialize_due_task_schedules_impl(limit, transaction).await
    }

    async fn set_task_queue_config(
        project_id: ArcProjectId,
        warehouse_id: Option<WarehouseId>,
        queue_name: &TaskQueueName,
        config: &SetTaskQueueConfigRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::set_task_queue_config_impl(project_id, warehouse_id, queue_name, config, transaction)
            .await
    }

    async fn get_task_queue_config(
        filter: &TaskQueueConfigFilter,
        queue_name: &TaskQueueName,
        state: Self::State,
    ) -> Result<Option<GetTaskQueueConfigResponse>> {
        Self::get_task_queue_config_impl(filter, queue_name, state).await
    }
}

impl<T> CatalogTaskOps for T where T: CatalogStore {}

fn task_matches_scope(task: &ResolvedTask, scope: &TaskResolveScope) -> bool {
    match scope {
        TaskResolveScope::Warehouse {
            warehouse_id,
            project_id,
        } => {
            if task.project_id != *project_id {
                return false;
            }
            match (warehouse_id, task.warehouse_id()) {
                (None, Some(_)) => true,               // Alle Warehouses im Projekt
                (Some(wid), Some(tid)) => wid == &tid, // Spezifisches Warehouse
                _ => false,
            }
        }
        TaskResolveScope::Project { project_id } => {
            task.project_id == *project_id && task.warehouse_id().is_none()
        }
    }
}
