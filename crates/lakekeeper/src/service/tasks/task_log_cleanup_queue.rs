use std::{sync::LazyLock, time::Duration};

use chrono::{DateTime, Utc};
use derive_more::Debug;
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use super::TaskQueueName;
use crate::{
    CancellationToken,
    api::Result,
    service::{
        CatalogStore,
        catalog_store::Transaction,
        tasks::{
            ScheduleTaskMetadata, SpecializedTask, TaskConfig, TaskData, TaskEntity,
            TaskExecutionDetails,
        },
    },
    utils::period::Period,
};

const QN_STR: &str = "task_log_cleanup";
pub(crate) static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());

pub type TaskLogCleanupTask =
    SpecializedTask<TaskLogCleanupConfig, TaskLogCleanupPayload, TaskLogCleanupExecutionDetails>;

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

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct RetentionPeriod(Period);
impl RetentionPeriod {
    #[must_use]
    pub fn new(period: Period) -> Self {
        Self(period)
    }

    #[must_use]
    pub fn days(days: u16) -> Self {
        Self(Period::Days(days))
    }

    #[must_use]
    pub fn period(&self) -> Period {
        self.0
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct CleanupPeriod(Period);
impl CleanupPeriod {
    #[must_use]
    pub fn new(period: Period) -> Self {
        Self(period)
    }

    #[must_use]
    pub fn days(days: u16) -> Self {
        Self(Period::Days(days))
    }

    #[must_use]
    pub fn period(&self) -> Period {
        self.0
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct TaskLogCleanupConfig {
    cleanup_period: CleanupPeriod,
    retention_period: RetentionPeriod,
}
impl TaskLogCleanupConfig {
    #[must_use]
    pub fn cleanup_period(&self) -> CleanupPeriod {
        self.cleanup_period
    }

    #[must_use]
    pub fn retention_period(&self) -> RetentionPeriod {
        self.retention_period
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
    poll_interval: Duration,
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

async fn cleanup_tasks<C: CatalogStore>(
    catalog_state: C::State,
    task: &TaskLogCleanupTask,
) -> Result<()> {
    let cleanup_period = get_cleanup_period(task);
    let schedule_date = calculate_next_schedule_date(cleanup_period);
    let retention_period = get_retention_period(task);

    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!("Failed to start transaction for `{QN_STR}`Queue."))
        })?;

    C::cleanup_task_logs_older_than(trx.transaction(), retention_period)
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to cleanup old tasks for `{QN_STR}` task. Original Task id was `{}`.",
                task.id
            ))
        })?;

    TaskLogCleanupTask::schedule_task::<C>(
        ScheduleTaskMetadata {
            project_id: task.task_metadata.project_id.clone(),
            parent_task_id: Some(task.task_id()),
            scheduled_for: Some(schedule_date),
            entity: TaskEntity::Project,
        },
        TaskLogCleanupPayload::new(),
        trx.transaction(),
    )
    .await
    .map_err(|e| {
        e.append_detail(format!(
            "Failed to queue next `{QN_STR}` task. Original Task id was `{}`.",
            task.id
        ))
    })?;

    task.record_success_in_transaction::<C>(trx.transaction(), None)
        .await;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction for `{QN_STR}` task. {e}");
        e
    })?;

    Ok(())
}

const DEFAULT_CLEANUP_PERIOD_DAYS: u16 = 1;
fn get_cleanup_period(task: &TaskLogCleanupTask) -> CleanupPeriod {
    match &task.config {
        Some(config) => config.cleanup_period(),
        None => CleanupPeriod(Period::Days(DEFAULT_CLEANUP_PERIOD_DAYS)),
    }
}

const DEFAULT_RETENTION_PERIOD_DAYS: u16 = 90;
fn get_retention_period(task: &TaskLogCleanupTask) -> RetentionPeriod {
    match &task.config {
        Some(config) => config.retention_period(),
        None => RetentionPeriod(Period::Days(DEFAULT_RETENTION_PERIOD_DAYS)),
    }
}

fn calculate_next_schedule_date(cleanup_period: CleanupPeriod) -> DateTime<Utc> {
    match cleanup_period {
        CleanupPeriod(Period::Days(days)) => Utc::now() + chrono::Duration::days(i64::from(days)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parsing_task_cleanup_config_from_json() {
        let config_json = r#"
        {
            "cleanup_period": {
                "days": 7
            },
            "retention_period": {
                "days": 90
            }
        }
        "#;
        let config: TaskLogCleanupConfig = serde_json::from_str(config_json).unwrap();
        assert_eq!(config.cleanup_period, CleanupPeriod(Period::Days(7)));
        assert_eq!(config.retention_period, RetentionPeriod(Period::Days(90)));
    }
}
