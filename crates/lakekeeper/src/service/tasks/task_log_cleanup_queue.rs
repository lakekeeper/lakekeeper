use std::sync::LazyLock;

use chrono::Duration;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

#[cfg(feature = "open-api")]
use super::QueueApiConfig;
use super::TaskQueueName;
use crate::{
    CancellationToken,
    api::Result,
    service::{
        ArcProjectId, CatalogStore, CatalogTaskOps,
        catalog_store::Transaction,
        tasks::{
            ProjectTaskScheduleState, SpecializedTask, TaskConfig, TaskData, TaskExecutionDetails,
            TaskScheduleCondition, TaskScheduleUpsert, UpsertTaskScheduleError,
        },
    },
};

const QN_STR: &str = "task_log_cleanup";
pub static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());

#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<QueueApiConfig> = LazyLock::new(|| QueueApiConfig {
    queue_name: &QUEUE_NAME,
    utoipa_type_name: TaskLogCleanupConfig::name(),
    utoipa_schema: TaskLogCleanupConfig::schema(),
    scope: super::QueueScope::Project,
    user_scheduling: super::UserScheduling::Disabled,
});

const DEFAULT_CLEANUP_PERIOD_DAYS: Duration = Duration::days(1);
const DEFAULT_RETENTION_PERIOD_DAYS: Duration = Duration::days(90);
const MAX_CLEANUP_PERIOD: Duration = super::MAX_TASK_SCHEDULE_INTERVAL;

pub type TaskLogCleanupTask =
    SpecializedTask<TaskLogCleanupConfig, TaskLogCleanupPayload, TaskLogCleanupExecutionDetails>;

impl TaskLogCleanupTask {
    fn cleanup_period(&self) -> Duration {
        self.config.as_ref().map_or(
            DEFAULT_CLEANUP_PERIOD_DAYS,
            TaskLogCleanupConfig::cleanup_period,
        )
    }

    fn retention_period(&self) -> Duration {
        self.config.as_ref().map_or(
            DEFAULT_RETENTION_PERIOD_DAYS,
            TaskLogCleanupConfig::retention_period,
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskLogCleanupPayload {}
impl TaskData for TaskLogCleanupPayload {}

impl Default for TaskLogCleanupPayload {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskLogCleanupPayload {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TaskLogCleanupConfig {
    /// How often to run the cleanup task in ISO8601 duration format. Defaults to once a day (P1D).
    /// Values below 1 day are raised to 1 day; values above 100 years are lowered to 100 years.
    #[cfg_attr(feature = "open-api", schema(example = "PT1H30M45.5S"))]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    cleanup_period: Option<Duration>,
    /// How long to retain task logs before deletion in ISO8601 duration format. Defaults to 90 days.
    #[cfg_attr(feature = "open-api", schema(example = "PT1H30M45.5S"))]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    retention_period: Option<Duration>,
}
impl TaskLogCleanupConfig {
    #[must_use]
    pub fn cleanup_period(&self) -> Duration {
        match self.cleanup_period {
            Some(period) if period < DEFAULT_CLEANUP_PERIOD_DAYS => {
                tracing::warn!(
                    "Specified cleanup_period {period} is below minimum of {DEFAULT_CLEANUP_PERIOD_DAYS}, using the minimum instead",
                );
                DEFAULT_CLEANUP_PERIOD_DAYS
            }
            Some(period) if period > MAX_CLEANUP_PERIOD => {
                tracing::warn!(
                    "Specified cleanup_period {period} is above maximum of {MAX_CLEANUP_PERIOD}, using the maximum instead",
                );
                MAX_CLEANUP_PERIOD
            }
            Some(period) => period,
            None => DEFAULT_CLEANUP_PERIOD_DAYS,
        }
    }

    #[must_use]
    pub fn retention_period(&self) -> Duration {
        self.retention_period
            .unwrap_or(DEFAULT_RETENTION_PERIOD_DAYS)
    }
}
impl TaskConfig for TaskLogCleanupConfig {
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(3600)
    }

    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskLogCleanupExecutionDetails {}
impl TaskExecutionDetails for TaskLogCleanupExecutionDetails {}

pub(crate) async fn log_cleanup_worker<C: CatalogStore>(
    catalog_state: C::State,
    poll_interval: core::time::Duration,
    cancellation_token: CancellationToken,
) {
    loop {
        let task = TaskLogCleanupTask::poll_for_new_task::<C>(
            catalog_state.clone(),
            &poll_interval,
            cancellation_token.clone(),
        )
        .await;
        let Some(task) = task else {
            tracing::info!("Graceful shutdown: exiting `{QN_STR}` worker");
            return;
        };
        let span = tracing::debug_span!(
            QN_STR,
            project_id = %task.task_metadata.project_id(),
            attempt = %task.attempt(),
            task_id = %task.task_id(),
        );

        instrumented_cleanup::<C>(catalog_state.clone(), &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_cleanup<C: CatalogStore>(catalog_state: C::State, task: &TaskLogCleanupTask) {
    match cleanup_tasks::<C>(catalog_state.clone(), task).await {
        Ok(()) => {
            tracing::info!("Task cleanup completed successfully");
        }
        Err(e) => {
            tracing::error!("Task cleanup failed: {:?}", e);
            task.record_failure::<C>(catalog_state, "Task cleanup failed.")
                .await;
        }
    }
}

/// Key of the schedule that runs task log cleanup for a project.
pub const SCHEDULE_KEY: &str = "default";

/// Create the task log cleanup schedule of a new project, with the default period and a
/// [`spread_anchor`].
///
/// # Errors
/// If the store fails.
pub async fn create_project_schedule<C: CatalogStore>(
    project_id: ArcProjectId,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<()> {
    upsert_project_schedule::<C>(
        project_id,
        DEFAULT_CLEANUP_PERIOD_DAYS,
        Some(spread_anchor()),
        None,
        transaction,
    )
    .await?;
    Ok(())
}

/// Up to one default period before now, so the cleanups of projects whose schedules are created
/// together fall at different times. The first run is due within one interval.
fn spread_anchor() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
        - Duration::seconds(fastrand::i64(0..DEFAULT_CLEANUP_PERIOD_DAYS.num_seconds()))
}

/// Set the interval of the project's cleanup schedule from a new queue config, creating the
/// schedule with a [`spread_anchor`] if it is missing.
///
/// # Errors
/// If the config cannot be read or the store fails.
pub(crate) async fn apply_queue_config<C: CatalogStore>(
    project_id: ArcProjectId,
    config: &serde_json::Value,
    transaction: &mut C::Transaction,
) -> Result<()> {
    let config: TaskLogCleanupConfig = serde_json::from_value(config.clone()).map_err(|e| {
        ErrorModel::bad_request(
            format!("Invalid `{QN_STR}` queue config: {e}"),
            "InvalidQueueConfig",
            Some(Box::new(e)),
        )
    })?;
    let interval = config.cleanup_period();
    let written = upsert_project_schedule::<C>(
        project_id.clone(),
        interval,
        Some(spread_anchor()),
        Some(TaskScheduleCondition::Absent),
        transaction.transaction(),
    )
    .await?;
    if !written {
        upsert_project_schedule::<C>(project_id, interval, None, None, transaction.transaction())
            .await?;
    }
    Ok(())
}

/// Give every project without a cleanup schedule one with a [`spread_anchor`], and set the
/// interval of every schedule to its project's configured cleanup period. A schedule changed
/// concurrently is left alone.
///
/// # Errors
/// If the store fails.
pub(crate) async fn sync_project_schedules<C: CatalogStore>(catalog_state: C::State) -> Result<()> {
    let mut t = C::Transaction::begin_write(catalog_state).await?;
    let states =
        C::list_project_task_schedule_states(&QUEUE_NAME, SCHEDULE_KEY, t.transaction()).await?;
    let (mut created, mut updated) = (0_usize, 0_usize);
    for state in states {
        let interval = configured_cleanup_period(&state);
        match state.interval {
            None => {
                if upsert_project_schedule::<C>(
                    state.project_id,
                    interval,
                    Some(spread_anchor()),
                    Some(TaskScheduleCondition::Absent),
                    t.transaction(),
                )
                .await?
                {
                    created += 1;
                }
            }
            Some(stored) if stored.num_seconds() != interval.num_seconds() => {
                if upsert_project_schedule::<C>(
                    state.project_id,
                    interval,
                    None,
                    Some(TaskScheduleCondition::Interval(stored)),
                    t.transaction(),
                )
                .await?
                {
                    updated += 1;
                }
            }
            Some(_) => {}
        }
    }
    t.commit().await?;
    if created > 0 || updated > 0 {
        tracing::info!(
            "Created `{QN_STR}` schedules for {created} projects and set the interval of {updated}"
        );
    }
    Ok(())
}

/// The project's configured cleanup period; the default if it has no valid config.
fn configured_cleanup_period(state: &ProjectTaskScheduleState) -> Duration {
    let Some(config) = &state.queue_config else {
        return DEFAULT_CLEANUP_PERIOD_DAYS;
    };
    match serde_json::from_value::<TaskLogCleanupConfig>(config.clone()) {
        Ok(config) => config.cleanup_period(),
        Err(e) => {
            tracing::warn!(
                "Invalid `{QN_STR}` queue config of project `{}`, using the default period: {e}",
                state.project_id
            );
            DEFAULT_CLEANUP_PERIOD_DAYS
        }
    }
}

/// Upsert the project's cleanup schedule; `None` keeps a stored anchor. A schedule written by a
/// newer server version, or one that does not meet `condition`, is left alone; returns whether
/// the schedule was written.
async fn upsert_project_schedule<C: CatalogStore>(
    project_id: ArcProjectId,
    interval: Duration,
    anchor: Option<chrono::DateTime<chrono::Utc>>,
    condition: Option<TaskScheduleCondition>,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<bool> {
    let schedule = TaskScheduleUpsert::builder()
        .project_id(project_id.clone())
        .queue_name(QUEUE_NAME.clone())
        .schedule_key(SCHEDULE_KEY)
        .interval(interval)
        .anchor(anchor)
        .condition(condition)
        .build();
    match C::upsert_task_schedule(schedule, transaction).await {
        Ok(_) => Ok(true),
        Err(UpsertTaskScheduleError::TaskScheduleSpecVersionUnsupportedError(e)) => {
            tracing::debug!("Leaving `{QN_STR}` schedule of project `{project_id}` alone: {e}");
            Ok(false)
        }
        Err(UpsertTaskScheduleError::TaskScheduleConditionFailedError(_)) => Ok(false),
        Err(e) => Err(e
            .append_detail(format!(
                "Failed to upsert `{QN_STR}` schedule for project `{project_id}`."
            ))
            .into()),
    }
}

async fn cleanup_tasks<C: CatalogStore>(
    catalog_state: C::State,
    task: &TaskLogCleanupTask,
) -> Result<()> {
    let project_id = task.task_metadata.project_id();

    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!("Failed to start transaction for `{QN_STR}` Queue."))
        })?;

    C::cleanup_task_logs_older_than(trx.transaction(), task.retention_period(), project_id)
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to cleanup old tasks for `{QN_STR}` task. Original Task id was `{}`.",
                task.task_id()
            ))
        })?;

    // A task without a schedule was enqueued by an older server: clean up, enqueue no successor.
    let message = task
        .task_metadata
        .schedule_id()
        .is_none()
        .then_some("Superseded by the project's task log cleanup schedule.");

    // A project created by an older server may not have a schedule yet. An existing one is left
    // alone: config changes set its interval.
    upsert_project_schedule::<C>(
        project_id.clone(),
        task.cleanup_period(),
        Some(spread_anchor()),
        Some(TaskScheduleCondition::Absent),
        trx.transaction(),
    )
    .await
    .map_err(|e| {
        e.append_detail(format!(
            "Failed to update `{QN_STR}` schedule. Original Task id was `{}`.",
            task.task_id()
        ))
    })?;

    task.record_success_in_transaction::<C>(trx.transaction(), message)
        .await;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction for `{QN_STR}` task. {e}");
        e
    })?;

    Ok(())
}

#[cfg(test)]
mod test {
    use serde_json::from_str;

    use super::*;

    #[test]
    fn test_parsing_task_cleanup_config_from_json() {
        let config_json = r#"
        {"cleanup-period":"P1W","retention-period":"P90D"}
        "#;
        let config: TaskLogCleanupConfig = from_str(config_json).unwrap();
        assert_eq!(config.cleanup_period(), Duration::days(7));
        assert_eq!(config.retention_period(), Duration::days(90));
    }

    #[test]
    fn test_parsing_task_cleanup_config_sets_period_to_minimum_value_when_period_to_small() {
        let config_json = r#"
        {"cleanup-period":"PT23H59M59S","retention-period":"P90D"}
        "#;
        let config: TaskLogCleanupConfig = from_str(config_json).unwrap();
        assert_eq!(config.cleanup_period(), Duration::days(1));
    }

    #[test]
    fn test_parsing_task_cleanup_config_caps_period_at_100_years() {
        let config_json = r#"
        {"cleanup-period":"P1000000D","retention-period":"P90D"}
        "#;
        let config: TaskLogCleanupConfig = from_str(config_json).unwrap();
        assert_eq!(config.cleanup_period(), Duration::seconds(3_153_600_000));
    }
}
