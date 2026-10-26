#[cfg(feature = "open-api")]
use std::collections::HashMap;
use std::{fmt::Debug, marker::PhantomData, ops::Deref, time::Duration};

use chrono::Utc;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use strum::EnumIter;
use uuid::Uuid;

use super::{Transaction, WarehouseId};
use crate::{
    ProjectId,
    api::management::v1::tasks::TaskStatus,
    service::{
        ArcProjectId, CatalogStore, CatalogTaskOps, GenericTableId, GenericTableNamed, TableId,
        TableNamed, TabularId, ViewId, ViewNamed,
        maintenance_mode::{MAINTENANCE, WaitOutcome, wait_until_writable},
        task_configs::TaskQueueConfigFilter,
    },
};

pub(crate) mod task_drain;
#[cfg(feature = "test-utils")]
pub use task_drain::TestTaskRunner;
mod task_queues_runner;
mod task_registry;
pub use task_queues_runner::{TaskQueueWorkerFn, TaskQueuesRunner};
pub use task_registry::{
    ConfigCheckFn, QueueApiConfig, QueueRegistration, QueueScope, RegisteredTaskQueues,
    ScheduleEligibilityFn, TaskQueueRegistry, UserScheduling, ValidatorFn,
};
pub mod tabular_expiration_queue;
pub mod tabular_purge_queue;
pub mod task_log_cleanup_queue;
pub mod task_schedule;
pub use task_schedule::{
    DueTick, MAX_TASK_SCHEDULE_INTERVAL, MAX_TASK_SCHEDULE_KEY_LENGTH, MIN_TASK_SCHEDULE_INTERVAL,
    MaterializedTick, ProjectTaskScheduleState, TaskSchedule, TaskScheduleCondition,
    TaskScheduleConditionFailedError, TaskScheduleFilter, TaskScheduleId,
    TaskScheduleIntervalOutOfRangeError, TaskScheduleKeyLengthError, TaskScheduleNotFoundError,
    TaskScheduleScopeNotFoundError, TaskScheduleSpecVersionUnsupportedError,
    TaskScheduleStoreError, TaskScheduleTickOutOfRangeError, TaskScheduleUpsert,
    TriggerTaskScheduleError, UpsertTaskScheduleError, due_tick, first_tick_at_or_after,
};

#[cfg(any(test, feature = "test-utils"))]
pub const DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT: chrono::Duration = chrono::Duration::seconds(300);
const DEFAULT_MAX_RETRIES: i32 = 10;
const DEFAULT_RETRY_BACKOFF_BASE: chrono::Duration = chrono::Duration::seconds(30);
const DEFAULT_RETRY_BACKOFF_MAX: chrono::Duration = chrono::Duration::hours(1);

/// Why an attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FailureKind {
    /// The task reported an error.
    Error,
    /// The attempt's heartbeat timed out.
    TimedOut,
}

/// A failed attempt, as a queue's retry policy sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailedAttempt<'a> {
    pub task_id: TaskId,
    /// 1-based.
    pub attempt: i32,
    /// The schedule that produced the task, if any.
    pub schedule_id: Option<TaskScheduleId>,
    pub kind: FailureKind,
    /// The details the failure was recorded with, as free text. Use [`Self::kind`] to tell
    /// failures apart.
    pub error: &'a str,
    /// The resource demand the attempt ran with.
    pub resource_demand: Option<i64>,
    /// For a timed-out attempt, the execution details of its last heartbeat; a heartbeat without
    /// details clears them. `None` for a failure the task reported.
    pub execution_details: Option<&'a serde_json::Value>,
}

/// What follows a failed attempt. Built with [`Self::retry`] and [`Self::give_up`]. After
/// `max_retries` attempts a [`Self::Retry`] is ignored and the task fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry no earlier than `backoff` from now, which counts as at least 0 and at most 100 years.
    /// Zero keeps the task's place in the queue.
    #[non_exhaustive]
    Retry {
        backoff: chrono::Duration,
        /// Resource demand of the retry; a negative one counts as 0. `None` keeps the attempt's.
        resource_demand: Option<i64>,
    },
    /// End the task as failed; `reason` is appended to the attempt's message, also after the
    /// last attempt.
    #[non_exhaustive]
    GiveUp { reason: String },
}

impl RetryDecision {
    /// Retry after `backoff`, keeping the attempt's resource demand.
    #[must_use]
    pub fn retry(backoff: chrono::Duration) -> Self {
        Self::Retry {
            backoff,
            resource_demand: None,
        }
    }

    /// Set the resource demand of a retry, `None` keeping the attempt's; leaves [`Self::GiveUp`]
    /// unchanged.
    #[must_use]
    pub fn with_resource_demand(self, resource_demand: impl Into<Option<i64>>) -> Self {
        match self {
            Self::Retry { backoff, .. } => Self::Retry {
                backoff,
                resource_demand: resource_demand.into(),
            },
            give_up @ Self::GiveUp { .. } => give_up,
        }
    }

    /// End the task as failed.
    #[must_use]
    pub fn give_up(reason: impl Into<String>) -> Self {
        Self::GiveUp {
            reason: reason.into(),
        }
    }
}

/// The message of an attempt whose [`RetryDecision`] gave up.
#[must_use]
pub fn given_up_message(details: &str, reason: &str) -> String {
    format!("{details}\nNot retried: {reason}")
}

/// How a queue retries failed attempts, including attempts whose heartbeat timed out.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Attempts after which a failed task is not retried, whatever `retry_decision` returns.
    pub max_retries: i32,
    /// What follows a failed attempt.
    pub retry_decision: fn(&FailedAttempt<'_>) -> RetryDecision,
}

impl RetryPolicy {
    /// The policy of queue `Q`.
    #[must_use]
    pub fn of<Q: TaskConfig>() -> Self {
        Self {
            max_retries: Q::max_retries(),
            retry_decision: Q::retry_decision,
        }
    }

    /// [`Self::retry_decision`] for `failed`. A hook that panics retries after
    /// [`default_retry_backoff`]: the attempt stays first in line, so a panic would repeat on
    /// every pick of the queue.
    #[must_use]
    pub fn decide(&self, failed: &FailedAttempt<'_>) -> RetryDecision {
        std::panic::catch_unwind(|| (self.retry_decision)(failed)).unwrap_or_else(|_| {
            tracing::error!(
                task_id = %failed.task_id,
                attempt = failed.attempt,
                "Retry decision of a failed attempt panicked; retrying after the default backoff"
            );
            RetryDecision::retry(default_retry_backoff(failed))
        })
    }
}

/// Which tasks a worker picks.
#[derive(Debug, Clone, typed_builder::TypedBuilder)]
pub struct PickTaskRequest<'a> {
    /// The queue to pick from.
    pub queue_name: &'a TaskQueueName,
    /// Earlier names of the queue; their tasks are picked too.
    #[builder(default)]
    pub legacy_queue_names: &'a [&'a TaskQueueName],
    /// Used only if no task configuration of the queue is stored.
    pub default_max_time_since_last_heartbeat: chrono::Duration,
    /// How attempts of the queue whose heartbeat timed out are retried.
    pub retry_policy: RetryPolicy,
    /// Pick only tasks whose resource demand lies in this range. Default: any task. An attempt
    /// whose heartbeat timed out fails whatever its resource demand.
    #[builder(default)]
    pub resource_demand: TaskResourceDemandRange,
}

/// Resource demands `d` with `above < d <= at_most`; a task without a resource demand counts as
/// 0. An unset bound is unbounded, so the default range includes every task.
///
/// A worker picks the oldest due task in its range. A task above every worker's range waits until
/// one covers it, and smaller tasks can keep a larger one waiting; give large tasks a range of
/// their own. The range reserves nothing: loops sharing one capacity must account for each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, typed_builder::TypedBuilder)]
pub struct TaskResourceDemandRange {
    /// Exclusive lower bound.
    #[builder(default, setter(strip_option))]
    pub above: Option<i64>,
    /// Inclusive upper bound.
    #[builder(default, setter(strip_option))]
    pub at_most: Option<i64>,
}

impl TaskResourceDemandRange {
    /// Whether no demand lies in the range.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!((self.above, self.at_most), (Some(above), Some(at_most)) if above >= at_most)
    }
}

/// Delay before a failed attempt is retried: 30 seconds after the first failed attempt,
/// doubling with each further one, at most one hour. Shortened by up to 10%, depending on the
/// task id and attempt, so that tasks failing together do not retry together.
#[must_use]
pub fn default_retry_backoff(failed: &FailedAttempt<'_>) -> chrono::Duration {
    let doublings = u32::try_from(failed.attempt.saturating_sub(1))
        .unwrap_or(0)
        .min(16);
    let backoff = DEFAULT_RETRY_BACKOFF_BASE
        .checked_mul(1 << doublings)
        .map_or(DEFAULT_RETRY_BACKOFF_MAX, |d| {
            d.min(DEFAULT_RETRY_BACKOFF_MAX)
        });
    let seed = failed.task_id.as_u64_pair().1
        ^ u64::from(failed.attempt.unsigned_abs()).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let permille = i32::try_from(seed % 101).unwrap_or(0);
    backoff - backoff * permille / 1000
}

/// Result of enqueueing a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// A new task was inserted.
    Inserted(TaskId),
    /// A task with the same key was already active, so nothing was inserted.
    Existing {
        task_id: TaskId,
        status: TaskIntermediateStatus,
    },
}

impl EnqueueOutcome {
    /// The id of the inserted task, `None` if a task was already active.
    #[must_use]
    pub fn inserted(&self) -> Option<TaskId> {
        match self {
            EnqueueOutcome::Inserted(task_id) => Some(*task_id),
            EnqueueOutcome::Existing { .. } => None,
        }
    }
}

#[cfg(feature = "open-api")]
#[allow(clippy::declare_interior_mutable_const)]
pub static BUILT_IN_API_CONFIGS: std::sync::LazyLock<Vec<QueueApiConfig>> =
    std::sync::LazyLock::new(|| {
        vec![
            tabular_expiration_queue::API_CONFIG.clone(),
            tabular_purge_queue::API_CONFIG.clone(),
        ]
    });

#[cfg(feature = "open-api")]
#[allow(clippy::declare_interior_mutable_const)]
pub static BUILT_IN_PROJECT_API_CONFIGS: std::sync::LazyLock<Vec<QueueApiConfig>> =
    std::sync::LazyLock::new(|| vec![task_log_cleanup_queue::API_CONFIG.clone()]);

#[cfg(feature = "open-api")]
pub static BUILT_IN_DEPENDENT_SCHEMAS: std::sync::LazyLock<
    HashMap<String, utoipa::openapi::RefOr<utoipa::openapi::Schema>>,
> = std::sync::LazyLock::new(HashMap::new);

#[cfg(all(test, feature = "open-api"))]
mod built_in_schedulable_pin_test {
    use super::{BUILT_IN_API_CONFIGS, BUILT_IN_PROJECT_API_CONFIGS};
    use crate::service::tasks::{tabular_expiration_queue, tabular_purge_queue};

    /// Pin the set of OSS queues that opt in to `task-queue/{name}/schedule`.
    ///
    /// **OSS has zero schedulable queues.** Destructive (`tabular_purge`) and
    /// lifecycle-managed (`soft_deletion`) queues intentionally stay
    /// opted out so they can't be enqueued out-of-band; `task_log_cleanup` is
    /// project-scoped and not meaningful to trigger manually.
    ///
    /// Enterprise has its own pin test for `expire_snapshots` and
    /// `remove_orphan_files`. If a new OSS queue legitimately needs to be
    /// manually schedulable, update both this list and the operator docs in
    /// the same PR so the decision is reviewed.
    #[test]
    fn oss_schedulable_queues_pin() {
        let mut names: Vec<&str> = BUILT_IN_API_CONFIGS
            .iter()
            .chain(BUILT_IN_PROJECT_API_CONFIGS.iter())
            .filter(|c| c.user_scheduling.is_enabled())
            .map(|c| c.queue_name.as_str())
            .collect();
        names.sort_unstable();
        let expected: Vec<&str> = vec![];
        assert_eq!(
            names, expected,
            "OSS schedulable-queue set changed; review the security \
             implications and update the operator docs."
        );
    }

    /// Belt-and-braces: explicitly assert the two queues we must never expose
    /// stay `Disabled`. If a future refactor reshuffles `BUILT_IN_API_CONFIGS`
    /// and the aggregate above goes stale, this catches the regression by name.
    #[test]
    fn tabular_purge_and_expiration_are_never_schedulable() {
        assert!(
            !tabular_purge_queue::API_CONFIG.user_scheduling.is_enabled(),
            "tabular_purge is destructive and must never be user-schedulable"
        );
        assert!(
            !tabular_expiration_queue::API_CONFIG
                .user_scheduling
                .is_enabled(),
            "soft_deletion is lifecycle-managed and must never be user-schedulable"
        );
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct TaskQueueName(String);

impl Deref for TaskQueueName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: AsRef<str>> From<T> for TaskQueueName {
    fn from(name: T) -> Self {
        Self(name.as_ref().to_string())
    }
}

impl std::fmt::Display for TaskQueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TaskQueueName {
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Hash, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum WarehouseTaskEntityId {
    #[serde(rename_all = "kebab-case")]
    Table {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        table_id: TableId,
    },
    #[serde(rename_all = "kebab-case")]
    View {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        view_id: ViewId,
    },
    #[serde(rename_all = "kebab-case")]
    GenericTable {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        generic_table_id: GenericTableId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::From)]
pub enum ResolvedTaskEntity {
    Table(TableNamed),
    View(ViewNamed),
    GenericTable(GenericTableNamed),
    Warehouse(WarehouseId),
    Project,
}

impl ResolvedTaskEntity {
    #[must_use]
    pub fn warehouse_id(&self) -> Option<WarehouseId> {
        match self {
            ResolvedTaskEntity::Table(t) => Some(t.warehouse_id),
            ResolvedTaskEntity::View(v) => Some(v.warehouse_id),
            ResolvedTaskEntity::GenericTable(g) => Some(g.warehouse_id),
            ResolvedTaskEntity::Warehouse(w) => Some(*w),
            ResolvedTaskEntity::Project => None,
        }
    }
}

/// How deleting a warehouse treats a queue's tasks in it, see [`TaskConfig::on_warehouse_delete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OnWarehouseDelete {
    /// Default. Deleting the warehouse fails while a task of this queue exists. The built-in
    /// queues use it.
    Block,
    /// Deleting the warehouse cancels the queue's tasks, running ones included. A warehouse with
    /// namespaces cannot be deleted, so these tasks belong to deleted entities or to the warehouse
    /// itself. If the delete fails, the tasks are kept. A running worker
    /// sees [`TaskCheckState::NotActive`] at its next heartbeat.
    ///
    /// A transaction that writes a row referencing the warehouse or locks one of its tasks, and
    /// then waits on another task of the warehouse (update, delete or conflicting enqueue), can
    /// deadlock with the delete; one side then fails with a retryable conflict. So a worker records
    /// its outcome or heartbeats first, then writes, and keeps the transaction short.
    Cancel,
}

#[cfg(feature = "open-api")]
pub trait TaskConfig:
    utoipa::ToSchema + Serialize + DeserializeOwned + Clone + Send + Sync
{
    #[must_use]
    fn max_time_since_last_heartbeat() -> chrono::Duration;

    #[must_use]
    fn max_retries() -> i32 {
        DEFAULT_MAX_RETRIES
    }

    /// What follows a failed attempt: a retry after a delay, for example with a larger resource
    /// demand after the attempt ran out of memory, or giving up. After [`Self::max_retries`]
    /// attempts a retry is ignored and the task fails. Default: a retry after
    /// [`default_retry_backoff`] with the attempt's resource demand. A worker that reports a
    /// failure can decide itself with [`SpecializedTask::record_failure_with_decision`].
    #[must_use]
    fn retry_decision(failed: &FailedAttempt<'_>) -> RetryDecision {
        RetryDecision::retry(default_retry_backoff(failed))
    }

    fn queue_name() -> &'static TaskQueueName;

    /// Names this queue was known by in earlier releases. The worker
    /// dual-reads these alongside [`Self::queue_name`] so tasks enqueued
    /// before a rename are still picked up, and cancellation on drop covers
    /// them too. Default: none.
    #[must_use]
    fn legacy_queue_names() -> Vec<&'static TaskQueueName> {
        Vec::new()
    }

    /// How deleting a warehouse treats this queue's tasks in it, entity and warehouse tasks alike.
    /// Project tasks are never affected. Default: [`OnWarehouseDelete::Block`].
    ///
    /// Only processes that register the queue apply it; elsewhere its tasks block the delete. So
    /// register the queue (zero workers is fine) wherever the management API runs.
    #[must_use]
    fn on_warehouse_delete() -> OnWarehouseDelete {
        OnWarehouseDelete::Block
    }

    /// Decide whether a manual schedule call is acceptable right now.
    ///
    /// Called by the `task-queue/{name}/schedule` endpoint after authz, with
    /// the queue's current config and the target entity's properties already
    /// fetched. Sync + pure: given inputs, decide.
    ///
    /// `entity_properties` carries the properties of whichever entity the
    /// caller targeted — table OR view. Implementors that only support one
    /// entity kind must match on `entity` and reject the unsupported variant
    /// explicitly; the framework no longer rejects views globally.
    ///
    /// Return `Err(ErrorModel)` (typically `400 Bad Request`) when the
    /// configuration is one the worker would skip at pickup — e.g.
    /// `gc.enabled=false` on the table, per-table opt-out property set, or
    /// the queue's master switch is off at the warehouse. Failing here
    /// surfaces the misconfiguration to the operator instead of creating a
    /// no-op task they have to discover via `task/list`.
    ///
    /// Default: always eligible. Queues whose workers have skip-at-pickup
    /// conditions should override.
    #[allow(unused_variables)]
    fn check_schedule_eligibility(
        config: &Self,
        entity_properties: &std::collections::HashMap<String, String>,
        entity: WarehouseTaskEntityId,
    ) -> Result<(), ErrorModel> {
        Ok(())
    }

    /// Decide whether a queue config is acceptable when it is saved through the
    /// `task-queue/{name}/config` endpoint, at warehouse and project scope alike. Called after
    /// authz and after the config is parsed. Sync + pure.
    ///
    /// Return `Err(ErrorModel)` (typically `400 Bad Request`) for a config the worker would refuse
    /// at pickup, so the operator sees the problem when saving. The error reaches the caller
    /// unchanged, so name the setting and what to change in the message. Stored configs are not
    /// re-checked when read, so the worker must still handle a config this check refuses.
    ///
    /// Default: every config that parses is accepted.
    fn check_config(_config: &Self) -> Result<(), ErrorModel> {
        Ok(())
    }
}

#[cfg(not(feature = "open-api"))]
pub trait TaskConfig: Serialize + DeserializeOwned + Clone + Send + Sync {
    #[must_use]
    fn max_time_since_last_heartbeat() -> chrono::Duration;

    #[must_use]
    fn max_retries() -> i32 {
        DEFAULT_MAX_RETRIES
    }

    /// See the `open-api`-enabled trait for full documentation.
    #[must_use]
    fn retry_decision(failed: &FailedAttempt<'_>) -> RetryDecision {
        RetryDecision::retry(default_retry_backoff(failed))
    }

    fn queue_name() -> &'static TaskQueueName;

    /// See the `open-api`-enabled trait for full documentation.
    #[must_use]
    fn legacy_queue_names() -> Vec<&'static TaskQueueName> {
        Vec::new()
    }

    /// See the `open-api`-enabled trait for full documentation.
    #[must_use]
    fn on_warehouse_delete() -> OnWarehouseDelete {
        OnWarehouseDelete::Block
    }

    /// See the `open-api`-enabled trait for full documentation.
    #[allow(unused_variables)]
    fn check_schedule_eligibility(
        config: &Self,
        entity_properties: &std::collections::HashMap<String, String>,
        entity: WarehouseTaskEntityId,
    ) -> Result<(), ErrorModel> {
        Ok(())
    }

    /// See the `open-api`-enabled trait for full documentation.
    fn check_config(_config: &Self) -> Result<(), ErrorModel> {
        Ok(())
    }
}

/// Task Payload.
///
/// Queues whose worker depends on exact payload shape should annotate the
/// payload type with `#[serde(deny_unknown_fields)]`. The schedule
/// endpoint's payload validator is `serde_json::from_value::<D>`, which by
/// default silently ignores unknown fields — fine for queues that take an
/// empty or open-ended payload, but a silent footgun for queues with a
/// strict contract.
pub trait TaskData: Clone + Serialize + DeserializeOwned + Send + Sync {}

pub trait TaskExecutionDetails: Clone + Serialize + DeserializeOwned + Send + Sync {}

#[derive(Hash, Debug, Clone, PartialEq, Serialize, Deserialize, Copy, Eq)]
#[serde(transparent)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttemptId {
    pub task_id: TaskId,
    pub attempt: i32,
}

impl std::fmt::Display for TaskAttemptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (attempt {})", self.task_id, self.attempt)
    }
}

impl AsRef<TaskAttemptId> for TaskAttemptId {
    fn as_ref(&self) -> &TaskAttemptId {
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskFilter {
    WarehouseId {
        warehouse_id: WarehouseId,
        project_id: ArcProjectId,
    },
    TaskIds(Vec<TaskId>),
    ProjectId {
        project_id: ArcProjectId,
        include_sub_tasks: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CancelTasksFilter {
    WarehouseId {
        warehouse_id: WarehouseId,
    },
    TaskIds(Vec<TaskId>),
    ProjectId {
        project_id: ProjectId,
        include_sub_tasks: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskResolveScope {
    Warehouse {
        project_id: ArcProjectId,
        warehouse_id: Option<WarehouseId>,
    },
    Project {
        project_id: ArcProjectId,
    },
}

impl TaskResolveScope {
    #[must_use]
    pub fn project_id(&self) -> ArcProjectId {
        match self {
            TaskResolveScope::Warehouse { project_id, .. }
            | TaskResolveScope::Project { project_id } => project_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskDetailsScope {
    Warehouse {
        project_id: ArcProjectId,
        warehouse_id: WarehouseId,
    },
    Project {
        project_id: ArcProjectId,
    },
}

impl TaskDetailsScope {
    #[must_use]
    pub fn project_id(&self) -> ArcProjectId {
        match self {
            TaskDetailsScope::Warehouse { project_id, .. }
            | TaskDetailsScope::Project { project_id } => project_id.clone(),
        }
    }

    #[must_use]
    pub fn warehouse_id(&self) -> Option<WarehouseId> {
        match self {
            TaskDetailsScope::Warehouse { warehouse_id, .. } => Some(*warehouse_id),
            TaskDetailsScope::Project { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskInput {
    /// Metadata for this task instance.
    /// Metadata type is shared between different task types.
    pub task_metadata: ScheduleTaskMetadata,
    /// Specific payload for this task type
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskEntity {
    Project,
    Warehouse {
        warehouse_id: WarehouseId,
    },
    EntityInWarehouse {
        warehouse_id: WarehouseId,
        entity_id: WarehouseTaskEntityId,
        entity_name: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskMetadata {
    pub project_id: ArcProjectId,
    pub parent_task_id: Option<TaskId>,
    pub scheduled_for: chrono::DateTime<Utc>,
    pub entity: TaskEntity,
    /// The schedule that produced this task, if any.
    pub schedule_id: Option<TaskScheduleId>,
    /// How much worker capacity the task needs, in a unit its queue chooses; a negative one counts
    /// as 0. A worker may pick only tasks in a range of it, where a task without one counts as 0.
    /// Tasks created by a schedule or the schedule endpoint have none.
    pub resource_demand: Option<i64>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleTaskMetadata {
    pub project_id: ArcProjectId,
    pub parent_task_id: Option<TaskId>,
    pub scheduled_for: Option<chrono::DateTime<Utc>>,
    pub entity: TaskEntity,
    /// See [`TaskMetadata::resource_demand`].
    pub resource_demand: Option<i64>,
}

impl TaskMetadata {
    #[must_use]
    pub fn project_id(&self) -> &ArcProjectId {
        &self.project_id
    }

    #[must_use]
    pub fn warehouse_id(&self) -> Option<WarehouseId> {
        match &self.entity {
            TaskEntity::Warehouse { warehouse_id }
            | TaskEntity::EntityInWarehouse { warehouse_id, .. } => Some(*warehouse_id),
            TaskEntity::Project => None,
        }
    }

    #[must_use]
    pub fn parent_task_id(&self) -> Option<TaskId> {
        self.parent_task_id
    }

    #[must_use]
    pub fn schedule_for(&self) -> chrono::DateTime<Utc> {
        self.scheduled_for
    }

    #[must_use]
    pub fn schedule_id(&self) -> Option<TaskScheduleId> {
        self.schedule_id
    }

    #[must_use]
    pub fn resource_demand(&self) -> Option<i64> {
        self.resource_demand
    }

    #[must_use]
    pub fn warehouse_task_sub_entity(
        &self,
    ) -> Option<(WarehouseId, &WarehouseTaskEntityId, &Vec<String>)> {
        match &self.entity {
            TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id,
                entity_name,
            } => Some((*warehouse_id, entity_id, entity_name)),
            TaskEntity::Warehouse { .. } | TaskEntity::Project => None,
        }
    }

    #[must_use]
    pub fn entity_name(&self) -> Option<&Vec<String>> {
        match &self.entity {
            TaskEntity::EntityInWarehouse { entity_name, .. } => Some(entity_name),
            TaskEntity::Warehouse { .. } | TaskEntity::Project => None,
        }
    }

    #[must_use]
    pub fn entity_id(&self) -> Option<WarehouseTaskEntityId> {
        match &self.entity {
            TaskEntity::EntityInWarehouse { entity_id, .. } => Some(*entity_id),
            TaskEntity::Warehouse { .. } | TaskEntity::Project => None,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, strum_macros::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum WarehouseEntityType {
    Table,
    View,
    GenericTable,
}

impl std::fmt::Display for WarehouseTaskEntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarehouseTaskEntityId::Table { table_id } => write!(f, "Table({table_id})"),
            WarehouseTaskEntityId::View { view_id } => write!(f, "View({view_id})"),
            WarehouseTaskEntityId::GenericTable { generic_table_id } => {
                write!(f, "GenericTable({generic_table_id})")
            }
        }
    }
}

impl WarehouseTaskEntityId {
    #[must_use]
    pub fn entity_type(&self) -> WarehouseEntityType {
        match self {
            WarehouseTaskEntityId::Table { .. } => WarehouseEntityType::Table,
            WarehouseTaskEntityId::View { .. } => WarehouseEntityType::View,
            WarehouseTaskEntityId::GenericTable { .. } => WarehouseEntityType::GenericTable,
        }
    }

    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        match self {
            WarehouseTaskEntityId::Table { table_id } => **table_id,
            WarehouseTaskEntityId::View { view_id } => **view_id,
            WarehouseTaskEntityId::GenericTable { generic_table_id } => **generic_table_id,
        }
    }
}

impl From<TabularId> for WarehouseTaskEntityId {
    fn from(id: TabularId) -> Self {
        match id {
            TabularId::Table(table_id) => WarehouseTaskEntityId::Table { table_id },
            TabularId::View(view_id) => WarehouseTaskEntityId::View { view_id },
            TabularId::GenericTable(generic_table_id) => {
                WarehouseTaskEntityId::GenericTable { generic_table_id }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub task_metadata: TaskMetadata,
    pub queue_name: TaskQueueName,
    pub id: TaskAttemptId,
    pub status: TaskIntermediateStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub config: Option<serde_json::Value>,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskInfo {
    pub task_metadata: TaskMetadata,
    pub queue_name: TaskQueueName,
    pub id: TaskAttemptId,
    pub status: TaskStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    pub progress: f32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl TaskInfo {
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.id.task_id
    }

    #[must_use]
    pub fn project_id(&self) -> &ProjectId {
        self.task_metadata.project_id()
    }

    #[must_use]
    pub fn queue_name(&self) -> &TaskQueueName {
        &self.queue_name
    }

    #[must_use]
    pub fn attempt(&self) -> i32 {
        self.id.attempt
    }

    #[must_use]
    pub fn status(&self) -> TaskStatus {
        self.status
    }

    #[must_use]
    pub fn parent_task_id(&self) -> Option<TaskId> {
        self.task_metadata.parent_task_id()
    }

    #[must_use]
    pub fn progress(&self) -> f32 {
        self.progress
    }

    #[must_use]
    pub fn picked_up_at(&self) -> Option<chrono::DateTime<Utc>> {
        self.picked_up_at
    }

    #[must_use]
    pub fn scheduled_for(&self) -> chrono::DateTime<Utc> {
        self.task_metadata.schedule_for()
    }

    #[must_use]
    pub fn last_heartbeat_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.last_heartbeat_at
    }

    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.created_at
    }

    #[must_use]
    pub fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.updated_at
    }
}

impl AsRef<TaskAttemptId> for Task {
    fn as_ref(&self) -> &TaskAttemptId {
        &self.id
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpecializedTask<C: TaskConfig, P: TaskData, E: TaskExecutionDetails> {
    pub task_metadata: TaskMetadata,
    pub id: TaskAttemptId,
    pub status: TaskIntermediateStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub config: Option<C>,
    pub data: P,
    execution_details: PhantomData<E>,
}

impl<C: TaskConfig, P: TaskData, E: TaskExecutionDetails> AsRef<TaskAttemptId>
    for SpecializedTask<C, P, E>
{
    fn as_ref(&self) -> &TaskAttemptId {
        &self.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub enum TaskCheckState {
    Stop,
    Continue,
    NotActive,
}

impl TaskCheckState {
    #[must_use]
    pub fn should_terminate(&self) -> bool {
        matches!(self, TaskCheckState::Stop | TaskCheckState::NotActive)
    }

    #[must_use]
    pub fn should_report_termination(&self) -> bool {
        matches!(self, TaskCheckState::Stop)
    }
}

impl Task {
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.id.task_id
    }

    #[must_use]
    pub fn attempt(&self) -> i32 {
        self.id.attempt
    }

    #[must_use]
    pub fn id(&self) -> TaskAttemptId {
        self.id
    }

    /// Extracts the task state from the task.
    ///
    /// # Errors
    /// Returns an error if the task state cannot be deserialized into the specified type.
    fn task_data<T: TaskData>(&self) -> crate::api::Result<T> {
        Ok(serde_json::from_value(self.data.clone()).map_err(|e| {
            crate::api::ErrorModel::internal(
                format!(
                    "Failed to deserialize task data for task {} in queue `{}`: {e}",
                    self.id, self.queue_name
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
    fn queue_config<T: TaskConfig>(&self) -> crate::api::Result<Option<T>> {
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

impl<Q: TaskConfig, D: TaskData, E: TaskExecutionDetails> SpecializedTask<Q, D, E> {
    #[must_use]
    pub fn queue_name() -> &'static TaskQueueName {
        Q::queue_name()
    }

    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.id.task_id
    }

    #[must_use]
    pub fn attempt(&self) -> i32 {
        self.id.attempt
    }

    #[must_use]
    pub fn id(&self) -> TaskAttemptId {
        self.id
    }

    /// Fetch the configuration for this task queue for the given warehouse.
    ///
    /// # Errors
    /// Returns an error if the configuration cannot be fetched or deserialized.
    pub async fn get_queue_config<C: CatalogStore>(
        warehouse_id: WarehouseId,
        catalog_state: C::State,
    ) -> crate::api::Result<Option<Q>> {
        let config = C::get_task_queue_config(
            &TaskQueueConfigFilter::WarehouseId { warehouse_id },
            Self::queue_name(),
            catalog_state,
        )
        .await?;

        config
            .map(|cfg| {
                serde_json::from_value(cfg.queue_config.config).map_err(|e| {
                    ErrorModel::internal(
                        format!(
                            "Failed to deserialize configuration for task queue `{}`: {e}",
                            Self::queue_name()
                        ),
                        "TaskConfigDeserializationError",
                        Some(Box::new(e)),
                    )
                    .into()
                })
            })
            .transpose()
    }

    /// Schedule a single task.
    ///
    /// There can only be a single active task for a (`entity_id`, `queue_name`) tuple.
    /// Resubmitting while one is pending or running returns [`EnqueueOutcome::Existing`]
    /// with that task's id and status.
    ///
    /// # Errors
    /// Returns an error if the task cannot be enqueued / scheduled.
    pub async fn schedule_task<C: CatalogStore>(
        task_metadata: ScheduleTaskMetadata,
        payload: D,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<EnqueueOutcome, ErrorModel> {
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
    /// Any resubmitted pending/running task is omitted from the returned task ids.
    ///
    /// CAUTION: `tasks` may be longer than the returned `Vec<TaskId>`
    ///
    /// # Errors
    /// Returns an error if the tasks cannot be enqueued / scheduled.
    pub async fn schedule_tasks<C: CatalogStore>(
        tasks: impl Iterator<Item = (ScheduleTaskMetadata, D)>,
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
    ///
    /// # Errors
    /// Returns an error on DB errors
    #[tracing::instrument(level = "info", skip(transaction), fields(queue_name = %Self::queue_name(), filter = ?filter, cancel_running_and_should_stop))]
    pub async fn cancel_scheduled_tasks<C: CatalogStore>(
        filter: CancelTasksFilter,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
        cancel_running_and_should_stop: bool,
    ) -> Result<(), IcebergErrorResponse> {
        C::cancel_scheduled_tasks(
            Some(Self::queue_name()),
            &Q::legacy_queue_names(),
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
    /// Inside a worker of a [`TaskQueuesRunner`], a returned task counts as running until the
    /// worker picks or polls again, or its future ends. A worker runs one task at a time: finish
    /// the task before picking or polling again, as the next pick ends it. While the process
    /// drains its workers, this picks nothing. Outside a runner, picks are neither gated nor
    /// counted.
    ///
    /// # Errors
    /// Returns an error if the task cannot be picked from the queue or if
    /// deserialization of the queue configuration or task data fails.
    pub async fn pick_new_task<C: CatalogStore>(
        catalog_state: C::State,
    ) -> crate::api::Result<Option<Self>> {
        Self::pick_new_task_with_resource_demand::<C>(
            catalog_state,
            TaskResourceDemandRange::default(),
        )
        .await
    }

    /// [`Self::pick_new_task`] limited to tasks whose resource demand lies in `resource_demand`.
    ///
    /// # Errors
    /// Returns an error if the task cannot be picked from the queue or if
    /// deserialization of the queue configuration or task data fails.
    pub async fn pick_new_task_with_resource_demand<C: CatalogStore>(
        catalog_state: C::State,
        resource_demand: TaskResourceDemandRange,
    ) -> crate::api::Result<Option<Self>> {
        task_drain::end_task();
        let Some(permit) = task_drain::admit_pick() else {
            return Ok(None);
        };
        let legacy_queue_names = Q::legacy_queue_names();
        let request = PickTaskRequest::builder()
            .queue_name(Q::queue_name())
            .legacy_queue_names(&legacy_queue_names)
            .default_max_time_since_last_heartbeat(Q::max_time_since_last_heartbeat())
            .retry_policy(RetryPolicy::of::<Q>())
            .resource_demand(resource_demand)
            .build();
        let task = C::pick_new_task(request, catalog_state.clone())
            .await
            .map_err(|e| {
                e.append_detail(format!("Failed to pick new `{}` task.", Q::queue_name()))
            })?;

        if let Some(task) = task {
            let state = match task.task_data::<D>() {
                Ok(state) => state,
                Err(err) => {
                    Self::report_deserialization_failure::<C>(
                        catalog_state,
                        task.id,
                        &task.task_metadata,
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
                        task.id,
                        &task.task_metadata,
                        &err.to_string(),
                    )
                    .await;
                    return Ok(None);
                }
            };
            permit.task_picked();
            Ok(Some(Self {
                task_metadata: task.task_metadata,
                id: task.id,
                status: task.status,
                picked_up_at: task.picked_up_at,
                config,
                data: state,
                execution_details: PhantomData,
            }))
        } else {
            Ok(None)
        }
    }

    /// Continuously poll for a new task in the queue until a task is found.
    /// Picks nothing while this process is in read-only maintenance mode or drains its workers.
    /// Returns None if cancellation is requested.
    ///
    /// A returned task counts as running as described in [`Self::pick_new_task`]. Call this
    /// inside the future of the [`TaskQueueWorkerFn`], not in a task spawned from it: a spawned
    /// task is outside the worker, so its picks are neither gated nor counted.
    pub async fn poll_for_new_task<C: CatalogStore>(
        catalog_state: C::State,
        poll_interval: &Duration,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Option<Self> {
        Self::poll_for_new_task_with_resource_demand::<C>(
            catalog_state,
            TaskResourceDemandRange::default,
            poll_interval,
            cancellation_token,
        )
        .await
    }

    /// [`Self::poll_for_new_task`] limited to tasks whose resource demand lies in the range
    /// `resource_demand` returns. It is called before each pick, so the range can follow the
    /// worker's free capacity.
    ///
    /// Finish the previously returned task before polling again: the poll ends it, and a drain
    /// then no longer waits for it. Call this inside the future of the [`TaskQueueWorkerFn`].
    pub async fn poll_for_new_task_with_resource_demand<C: CatalogStore>(
        catalog_state: C::State,
        mut resource_demand: impl FnMut() -> TaskResourceDemandRange,
        poll_interval: &Duration,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Option<Self> {
        // Ends the previous task before a wait in read-only maintenance mode.
        task_drain::end_task();
        loop {
            if wait_until_writable(&MAINTENANCE, *poll_interval, &cancellation_token).await
                == WaitOutcome::Cancelled
            {
                tracing::info!(
                    "Graceful shutdown requested for queue `{}`",
                    Q::queue_name()
                );
                return None;
            }
            // Not raced against cancellation: a pick dropped mid-flight can leave a task it claimed
            // running until its heartbeat times out, or, while its `BEGIN` is in flight, return its
            // connection to the pool inside an open transaction.
            let task = match Self::pick_new_task_with_resource_demand::<C>(
                catalog_state.clone(),
                resource_demand(),
            )
            .await
            {
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

            tracing::debug!("Picked up `{}` task {}.", task.id, Q::queue_name());
            return Some(task);
        }
    }

    /// Heartbeat this task, while logging progress and checking for should-stop signal.
    ///
    /// # Errors
    /// Returns an error if the heartbeat fails.
    pub async fn heartbeat_in_transaction<C: CatalogStore>(
        &self,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
        progress: f32,
        execution_details: Option<E>,
    ) -> Result<TaskCheckState, ErrorModel> {
        let execution_details = execution_details
            .map(|details| serde_json::to_value(details))
            .transpose()
            .map_err(|e| {
                ErrorModel::internal(
                    format!(
                        "Failed to serialize execution details for `{}` task {}: {e}",
                        Self::queue_name(),
                        self.id
                    ),
                    "TaskExecutionDetailsSerializationError",
                    Some(Box::new(e)),
                )
            })?;

        C::check_and_heartbeat_task(self.id, transaction, progress, execution_details)
            .await
            .map_err(|e| {
                e.append_detail(format!(
                    "Failed to heartbeat `{}` task {}.",
                    Self::queue_name(),
                    self.id
                ))
                .into()
            })
    }

    /// Identical to `heartbeat_in_transaction`, but accepts a catalog state and creates a transaction internally.
    ///
    /// # Errors
    /// * If the transaction cannot be started or committed.
    /// * If the heartbeat fails.
    pub async fn heartbeat<C: CatalogStore>(
        &self,
        catalog_state: C::State,
        progress: f32,
        execution_details: Option<E>,
    ) -> Result<TaskCheckState, ErrorModel> {
        let mut transaction: C::Transaction =
            Transaction::begin_write(catalog_state).await.map_err(|e| {
                e.append_detail(format!(
                    "Failed to start DB transaction to heartbeat `{}` task {}.",
                    Self::queue_name(),
                    self.id
                ))
            })?;
        let state = self
            .heartbeat_in_transaction::<C>(transaction.transaction(), progress, execution_details)
            .await?;

        transaction.commit().await.map_err(|e| {
            e.append_detail(format!(
                "Failed to commit DB transaction to heartbeat `{}` task {}.",
                Self::queue_name(),
                self.id
            ))
        })?;

        Ok(state)
    }

    /// Records a failed attempt of the task. [`TaskConfig::retry_decision`] decides whether and how
    /// it is retried; after [`TaskConfig::max_retries`] attempts a retry is ignored and the task
    /// fails.
    ///
    /// Does not return an error, but logs it.
    pub async fn record_failure<C: CatalogStore>(&self, catalog_state: C::State, error: &str) {
        let failed = FailedAttempt {
            task_id: self.task_id(),
            attempt: self.attempt(),
            schedule_id: self.task_metadata.schedule_id(),
            kind: FailureKind::Error,
            error,
            resource_demand: self.task_metadata.resource_demand(),
            execution_details: None,
        };
        let decision = RetryPolicy::of::<Q>().decide(&failed);
        self.record_failure_with_decision::<C>(catalog_state, error, decision)
            .await;
    }

    /// Records a failure like [`Self::record_failure`], but `decision` says whether and how it is
    /// retried, and [`TaskConfig::retry_decision`] is not asked. After [`TaskConfig::max_retries`]
    /// attempts a retry is ignored and the task fails.
    ///
    /// Does not return an error, but logs it.
    pub async fn record_failure_with_decision<C: CatalogStore>(
        &self,
        catalog_state: C::State,
        error: &str,
        decision: RetryDecision,
    ) {
        let status = Status::Failure {
            details: error,
            max_retries: Q::max_retries(),
            retry_decision: decision,
        };

        for attempt in 1..=5 {
            match self
                .record_status_for_state::<C>(catalog_state.clone(), status.clone())
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        "Successfully recorded error for task {} in queue '{}' on attempt {attempt}",
                        self.id,
                        Self::queue_name(),
                    );
                    return;
                }
                Err(e) if is_task_attempt_closed(&e) => {
                    tracing::info!(
                        "Not recording error for task {} in queue '{}': the task no longer exists or this attempt already has an outcome. {e}. Original Error: {error}",
                        self.id,
                        Self::queue_name(),
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to record error for task {} in queue '{}' on attempt {attempt}/5: {e}",
                        self.id,
                        Self::queue_name(),
                    );

                    if attempt < 5 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    } else {
                        tracing::error!(
                            "Failed to record error for task {} in queue '{}' after 5 attempts. {e}. Original Error: {error}",
                            self.id,
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
    pub async fn record_success<C: CatalogStore>(
        &self,
        catalog_state: C::State,
        details: Option<&str>,
    ) {
        let status = Status::Success(details);

        for attempt in 1..=5 {
            match self
                .record_status_for_state::<C>(catalog_state.clone(), status.clone())
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        "Successfully recorded success for task {} in queue '{}' on attempt {attempt}",
                        self.id,
                        Self::queue_name(),
                    );
                    return;
                }
                Err(e) if is_task_attempt_closed(&e) => {
                    tracing::info!(
                        "Not recording success for task {} in queue '{}': the task no longer exists or this attempt already has an outcome. {e}",
                        self.id,
                        Self::queue_name(),
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to record success for task {} in queue '{}' on attempt {attempt}/5: {e}",
                        self.id,
                        Self::queue_name(),
                    );

                    if attempt < 5 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    } else {
                        tracing::error!(
                            "Failed to record success for task {} in queue '{}' after 5 attempts. {e}. Original Success Details: {}",
                            self.id,
                            Self::queue_name(),
                            details.unwrap_or("No details provided")
                        );
                    }
                }
            }
        }
    }

    /// Move a pending task of this queue to `new_parent` and replace its payload, in one write.
    ///
    /// Returns `false` and changes nothing if the task is not a pending task of this queue
    /// (running, finished, of another queue or unknown).
    ///
    /// # Errors
    /// Returns an error if the payload cannot be serialized or the store fails.
    pub async fn reparent_pending_task<C: CatalogStore>(
        task_id: TaskId,
        new_parent: TaskId,
        payload: &D,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<bool, ErrorModel> {
        let payload = serde_json::to_value(payload).map_err(|e| {
            ErrorModel::internal(
                format!(
                    "Failed to serialize payload for `{}` task: {e}",
                    Self::queue_name()
                ),
                "TaskPayloadSerializationError",
                Some(Box::new(e)),
            )
        })?;
        C::reparent_pending_task(
            Self::queue_name(),
            &Q::legacy_queue_names(),
            task_id,
            new_parent,
            payload,
            transaction,
        )
        .await
        .map_err(Into::into)
    }

    /// Record success in an existing transaction.
    ///
    /// Records the success of a task in the catalog, updating its status.
    /// Does not return an error, but logs it.
    pub async fn record_success_in_transaction<C: CatalogStore>(
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
                    self.id,
                    Self::queue_name(),
                );
            }
            Err(e) if is_task_attempt_closed(&e) => {
                tracing::info!(
                    "Not recording success for task {} in queue '{}': the task no longer exists or this attempt already has an outcome. {e}",
                    self.id,
                    Self::queue_name(),
                );
            }
            Err(e) => {
                tracing::error!(
                    "Failed to record success for task {} in queue '{}': {e}. Original Success Details: {}",
                    self.id,
                    Self::queue_name(),
                    details.unwrap_or("No details provided")
                );
            }
        }
    }

    async fn report_deserialization_failure<C: CatalogStore>(
        catalog_state: C::State,
        id: TaskAttemptId,
        task_metadata: &TaskMetadata,
        error: &str,
    ) {
        tracing::error!("{error}. TaskID: {id}");

        let mut trx = match C::Transaction::begin_write(catalog_state).await {
            Ok(trx) => trx,
            Err(e) => {
                tracing::error!(
                    "Failed to start DB transaction to record deserialization failure for `{}` task {id}: {e}. Original Error: {error}",
                    Q::queue_name()
                );
                return;
            }
        };

        let details = format!("Failed to deserialize task data: {error}");
        let failed = FailedAttempt {
            task_id: id.task_id,
            attempt: id.attempt,
            schedule_id: task_metadata.schedule_id(),
            kind: FailureKind::Error,
            error: &details,
            resource_demand: task_metadata.resource_demand(),
            execution_details: None,
        };
        let r = C::record_task_failure(
            id,
            &details,
            Q::max_retries(),
            &RetryPolicy::of::<Q>().decide(&failed),
            &mut trx.transaction(),
        )
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to record deserialization failure for `{id}` task {}.",
                Q::queue_name()
            ))
            .append_detail(format!("Original Error: {error}"))
        });

        match r {
            Ok(()) => {}
            Err(e) if is_task_attempt_closed(&e) => {
                tracing::info!(
                    "Not recording deserialization failure for `{id}` task {}: the task no longer exists or this attempt already has an outcome. {e}",
                    Q::queue_name()
                );
                return;
            }
            Err(e) => {
                tracing::error!(
                    "Failed to record deserialization failure for `{id}` task {}: {e}. Original Error: {error}",
                    Q::queue_name()
                );
                return;
            }
        }

        if let Err(e) = trx.commit().await {
            tracing::error!(
                "Failed to commit transaction for recording deserialization failure for `{id}` task {}: {e}. Original Error: {error}",
                Q::queue_name()
            );
        }
    }

    async fn record_status_for_state<C: CatalogStore>(
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
                    self.id, Self::queue_name()
                ))
                    .append_detail(format!("Task Status that failed to record: `{result}`")));
            }
        };

        self.record_status_for_transaction::<C>(result.clone(), transaction.transaction())
            .await?;

        transaction.commit().await.map_err(|e| {
            e.append_detail(format!(
                "Failed to commit DB transaction to record status for task {} in queue `{}`.",
                self.id,
                Self::queue_name()
            ))
            .append_detail(format!("Task Status that failed to commit: `{result}`"))
        })?;

        Ok(())
    }

    async fn record_status_for_transaction<C: CatalogStore>(
        &self,
        result: Status<'_>,
        mut transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<(), IcebergErrorResponse> {
        match result {
            Status::Success(details) => C::record_task_success(self.id, details, &mut transaction)
                .await
                .map_err(|e| {
                    e.append_detail(format!(
                        "Failed to record success for `{}` task {}.",
                        Self::queue_name(),
                        self.id,
                    ))
                    .append_detail(format!(
                        "Original Success Details: `{}`",
                        details.unwrap_or("No details provided")
                    ))
                }),
            Status::Failure {
                details,
                max_retries,
                retry_decision,
            } => C::record_task_failure(
                self.id,
                details,
                max_retries,
                &retry_decision,
                &mut transaction,
            )
            .await
            .map_err(|e| {
                e.append_detail(format!(
                    "Failed to record failure for `{}` task {}.",
                    Self::queue_name(),
                    self.id
                ))
                .append_detail(format!("Original Error Details: `{details}`"))
            }),
        }
    }
}

/// Error type of the 409 for recording a success of an attempt that already has an outcome.
pub const TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE: &str = "TaskAttemptAlreadyCompleted";
/// Error type of the 409 for recording a failure of an attempt that already has an outcome.
pub const TASK_ATTEMPT_ALREADY_FAILED_ERROR_TYPE: &str = "TaskAttemptAlreadyFailed";

/// Whether a `CatalogTaskOps::record_task_*` error means the attempt is closed: the task is gone
/// (404, e.g. its warehouse was deleted) or the attempt already has an outcome (409). Retrying
/// cannot succeed then. Other errors, like a failed transaction, may succeed in a new transaction.
#[must_use]
pub fn is_task_attempt_closed(e: &IcebergErrorResponse) -> bool {
    let error = &e.error;
    error.code == http::StatusCode::NOT_FOUND.as_u16()
        || (error.code == http::StatusCode::CONFLICT.as_u16()
            && [
                TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE,
                TASK_ATTEMPT_ALREADY_FAILED_ERROR_TYPE,
            ]
            .contains(&error.r#type.as_str()))
}

#[derive(Debug, Copy, Clone, PartialEq, EnumIter, Hash, Eq)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "task_intermediate_status", rename_all = "kebab-case")
)]
pub enum TaskIntermediateStatus {
    Scheduled,
    Running,
    ShouldStop,
}

#[derive(Debug, Copy, Clone, PartialEq, Hash, Eq)]
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
    Failure {
        details: &'a str,
        max_retries: i32,
        retry_decision: RetryDecision,
    },
}

impl std::fmt::Display for Status<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Success(details) => write!(f, "success ({})", details.unwrap_or("")),
            Status::Failure { details, .. } => write!(f, "failure ({details})"),
        }
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_is_task_attempt_closed() {
        let concurrent_modification = ErrorModel::from(crate::service::CatalogBackendError::new(
            std::io::Error::other("changed"),
            crate::service::CatalogBackendErrorType::ConcurrentModification,
        ));
        assert_eq!(concurrent_modification.code, 409);
        let cases: [(ErrorModel, bool); 6] = [
            (ErrorModel::not_found("gone", "TaskNotFound", None), true),
            (
                ErrorModel::conflict("recorded", "TaskAttemptAlreadyCompleted", None),
                true,
            ),
            (
                ErrorModel::conflict("recorded", "TaskAttemptAlreadyFailed", None),
                true,
            ),
            (
                ErrorModel::conflict("deadlock", "TransactionFailed", None),
                false,
            ),
            (concurrent_modification, false),
            (ErrorModel::internal("failed", "DatabaseError", None), false),
        ];
        for (error, closed) in cases {
            let r#type = error.r#type.clone();
            assert_eq!(is_task_attempt_closed(&error.into()), closed, "{type}");
        }
    }

    #[test]
    fn default_retry_backoff_doubles_from_thirty_seconds_up_to_one_hour_less_jitter() {
        let task_id =
            TaskId::from(Uuid::parse_str("01890a5d-ac96-774b-bcce-b302099a8057").unwrap());
        let delays = [1, 2, 3, 4, 5, 8, 9, 100, 0, -1].map(|attempt| {
            default_retry_backoff(&FailedAttempt {
                task_id,
                attempt,
                schedule_id: None,
                kind: FailureKind::Error,
                error: "",
                resource_demand: None,
                execution_details: None,
            })
        });
        // Before shortening: 30, 60, 120, 240, 480 s, then 1 h; shortened by 4.5%, 5.4%, 5.6%, 2.8%,
        // 7.2%, 4.2%, 6.2%, 5.3%, 9.4% and 4.5%.
        assert_eq!(
            delays,
            [
                28_650, 56_760, 113_280, 233_280, 445_440, 3_448_800, 3_376_800, 3_409_200, 27_180,
                28_650
            ]
            .map(chrono::Duration::milliseconds)
        );
    }

    #[test]
    fn a_panicking_retry_decision_retries_after_the_default_backoff() {
        let policy = RetryPolicy {
            max_retries: 5,
            retry_decision: |_| panic!("hook failed"),
        };
        let failed = FailedAttempt {
            task_id: TaskId::from(Uuid::now_v7()),
            attempt: 3,
            schedule_id: None,
            kind: FailureKind::TimedOut,
            error: "",
            resource_demand: Some(8),
            execution_details: None,
        };
        assert_eq!(
            policy.decide(&failed),
            RetryDecision::retry(default_retry_backoff(&failed))
        );
    }

    #[test]
    fn a_queue_tries_a_task_ten_times_by_default() {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
        struct Config {}
        impl TaskConfig for Config {
            fn queue_name() -> &'static TaskQueueName {
                static QUEUE_NAME: std::sync::LazyLock<TaskQueueName> =
                    std::sync::LazyLock::new(|| "test-default".into());
                &QUEUE_NAME
            }
            fn max_time_since_last_heartbeat() -> chrono::Duration {
                chrono::Duration::seconds(60)
            }
        }
        assert_eq!(RetryPolicy::of::<Config>().max_retries, 10);
    }

    #[test]
    fn test_task_entity_serde_table() {
        let json = serde_json::json!({
            "type": "table",
            "table-id": "550e8400-e29b-41d4-a716-446655440000"
        });
        let deserialized: super::WarehouseTaskEntityId =
            serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            deserialized,
            WarehouseTaskEntityId::Table {
                table_id: TableId::from(
                    Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
                )
            }
        );

        let serialized = serde_json::to_value(deserialized).unwrap();
        assert_eq!(serialized, json);
    }

    #[test]
    fn test_task_entity_serde_generic_table() {
        let json = serde_json::json!({
            "type": "generic-table",
            "generic-table-id": "550e8400-e29b-41d4-a716-446655440111"
        });
        let deserialized: super::WarehouseTaskEntityId =
            serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            deserialized,
            WarehouseTaskEntityId::GenericTable {
                generic_table_id: GenericTableId::from(
                    Uuid::parse_str("550e8400-e29b-41d4-a716-446655440111").unwrap()
                )
            }
        );

        let serialized = serde_json::to_value(deserialized).unwrap();
        assert_eq!(serialized, json);
    }

    #[test]
    fn test_tabular_id_into_warehouse_task_entity_id_generic_table() {
        let gt_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440222").unwrap();
        let tabular = TabularId::GenericTable(GenericTableId::from(gt_uuid));
        assert_eq!(
            WarehouseTaskEntityId::from(tabular),
            WarehouseTaskEntityId::GenericTable {
                generic_table_id: GenericTableId::from(gt_uuid)
            }
        );
    }
}
