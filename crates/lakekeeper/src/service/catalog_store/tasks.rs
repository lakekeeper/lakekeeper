use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use iceberg_ext::catalog::rest::ErrorModel;

use super::{CatalogStore, Transaction};
use crate::{
    ProjectId, WarehouseId,
    api::management::v1::{
        task_queue::{GetTaskQueueConfigResponse, SetTaskQueueConfigRequest},
        tasks::{ListTasksRequest, TaskAttempt},
    },
    service::{
        Result,
        task_configs::TaskQueueConfigFilter,
        tasks::{
            ListTask, Task, TaskAttemptId, TaskCheckState, TaskEntityNamed, TaskFilter, TaskId,
            TaskInput, TaskQueueName,
        },
    },
};

struct TasksCacheExpiry;
const TASKS_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
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
    pub project_id: ProjectId,
    pub entity: TaskEntityNamed,
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
    pub tasks: Vec<ListTask>,
    pub next_page_token: Option<String>,
}

#[derive(Debug)]
pub struct TaskDetails {
    pub task: ListTask,
    pub execution_details: Option<serde_json::Value>,
    pub data: serde_json::Value,
    pub attempts: Vec<TaskAttempt>,
}

#[async_trait::async_trait]
pub trait CatalogTaskOps
where
    Self: CatalogStore,
{
    /// `default_max_time_since_last_heartbeat` is only used if no task configuration is found
    /// in the DB for the given `queue_name`, typically before a user has configured the value explicitly.
    #[tracing::instrument(
        name = "catalog_pick_new_task",
        skip(state, default_max_time_since_last_heartbeat)
    )]
    async fn pick_new_task(
        queue_name: &TaskQueueName,
        default_max_time_since_last_heartbeat: chrono::Duration,
        state: Self::State,
    ) -> Result<Option<Task>> {
        Self::pick_new_task_impl(queue_name, default_max_time_since_last_heartbeat, state).await
    }

    async fn record_task_success(
        id: TaskAttemptId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::record_task_success_impl(id, message, transaction).await
    }

    async fn record_task_failure(
        id: TaskAttemptId,
        error_details: &str,
        max_retries: i32,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::record_task_failure_impl(id, error_details, max_retries, transaction).await
    }

    /// Cancel scheduled tasks matching the filter.
    ///
    /// If `cancel_running_and_should_stop` is true, also cancel tasks in the `running` and `should-stop` states.
    /// If `queue_name` is `None`, cancel tasks in all queues.
    async fn cancel_scheduled_tasks(
        queue_name: Option<&TaskQueueName>,
        filter: TaskFilter,
        cancel_running_and_should_stop: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::cancel_scheduled_tasks_impl(
            queue_name,
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
        project_id: Option<ProjectId>,
        warehouse_id: Option<WarehouseId>,
        task_id: TaskId,
        num_attempts: u16,
        state: Self::State,
    ) -> Result<Option<TaskDetails>> {
        Self::get_task_details_impl(project_id, warehouse_id, task_id, num_attempts, state).await
    }

    /// Enqueue a single task to a task queue.
    ///
    /// There can only be a single active task for a (`entity_id`, `queue_name`) tuple.
    /// Resubmitting a pending/running task will return a `None` instead of a new `TaskId`
    async fn enqueue_task(
        queue_name: &'static TaskQueueName,
        task: TaskInput,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<TaskId>> {
        Ok(Self::enqueue_tasks(queue_name, vec![task], transaction)
            .await
            .map(|v| v.into_iter().next())?)
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
        query: ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<TaskList> {
        Self::list_tasks_impl(filter, query, transaction).await
    }

    /// Resolve tasks among all known active and historical tasks.
    /// Returns a map of `task_id` to `(TaskEntity, queue_name)`.
    /// If a task does not exist, it is not included in the map.
    async fn resolve_tasks(
        project_id: Option<ProjectId>,
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, Arc<ResolvedTask>>> {
        if task_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut cached_results = HashMap::new();
        for id in task_ids {
            if let Some(cached_value) = TASKS_CACHE.get(id).await {
                if cached_value.warehouse_id() != warehouse_id
                    || (project_id.is_some() && Some(cached_value.project_id.clone()) != project_id)
                {
                    continue;
                }
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
            Self::resolve_tasks_impl(project_id, warehouse_id, &not_cached_ids, state).await?;
        for value in resolve_uncached_result {
            let value = Arc::new(value);
            cached_results.insert(value.task_id, value.clone());
            TASKS_CACHE.insert(value.task_id, value).await;
        }
        Ok(cached_results)
    }

    async fn resolve_required_tasks(
        project_id: Option<ProjectId>,
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, Arc<ResolvedTask>>> {
        let tasks = Self::resolve_tasks(project_id, warehouse_id, task_ids, state).await?;

        for task_id in task_ids {
            if !tasks.contains_key(task_id) {
                return Err(ErrorModel::not_found(
                    format!("Task with id `{task_id}` not found"),
                    "TaskNotFound",
                    None,
                )
                .into());
            }
        }

        Ok(tasks)
    }

    async fn set_task_queue_config(
        project_id: ProjectId,
        warehouse_id: Option<WarehouseId>,
        queue_name: &TaskQueueName,
        config: SetTaskQueueConfigRequest,
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
