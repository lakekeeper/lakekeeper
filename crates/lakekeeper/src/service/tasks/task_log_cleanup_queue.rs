use std::sync::LazyLock;

use chrono::{DateTime, Duration, Utc};
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
        CatalogStore,
        catalog_store::Transaction,
        tasks::{
            ScheduleTaskMetadata, SpecializedTask, TaskConfig, TaskData, TaskEntity,
            TaskExecutionDetails,
        },
    },
};

const QN_STR: &str = "task_log_cleanup";
pub(crate) static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());

#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<QueueApiConfig> = LazyLock::new(|| QueueApiConfig {
    queue_name: &QUEUE_NAME,
    utoipa_type_name: TaskLogCleanupConfig::name(),
    utoipa_schema: TaskLogCleanupConfig::schema(),
});

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

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TaskLogCleanupConfig {
    #[cfg_attr(feature = "open-api", schema(example = "PT1H30M45.5S"))]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    cleanup_period: Option<Duration>,
    #[cfg_attr(feature = "open-api", schema(example = "PT1H30M45.5S"))]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    retention_period: Option<Duration>,
}
impl TaskLogCleanupConfig {
    #[must_use]
    pub fn cleanup_period(&self) -> Option<Duration> {
        self.cleanup_period
    }

    #[must_use]
    pub fn retention_period(&self) -> Option<Duration> {
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
    let cleanup_period = get_cleanup_period(task)?;
    let schedule_date = calculate_next_schedule_date(cleanup_period);
    let retention_period = get_retention_period(task)?;

    let project_id = task.task_metadata.project_id();

    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!("Failed to start transaction for `{QN_STR}` Queue."))
        })?;

    C::cleanup_task_logs_older_than(trx.transaction(), retention_period, project_id)
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to cleanup old tasks for `{QN_STR}` task. Original Task id was `{}`.",
                task.task_id()
            ))
        })?;

    let next_entity = match task.task_metadata.entity {
        TaskEntity::Project => TaskEntity::Project,
        TaskEntity::Warehouse { warehouse_id }
        | TaskEntity::EntityInWarehouse { warehouse_id, .. } => {
            TaskEntity::Warehouse { warehouse_id }
        }
    };

    TaskLogCleanupTask::schedule_task::<C>(
        ScheduleTaskMetadata {
            project_id: task.task_metadata.project_id.clone(),
            parent_task_id: Some(task.task_id()),
            scheduled_for: Some(schedule_date),
            entity: next_entity,
        },
        TaskLogCleanupPayload::new(),
        trx.transaction(),
    )
    .await
    .map_err(|e| {
        e.append_detail(format!(
            "Failed to queue next `{QN_STR}` task. Original Task id was `{}`.",
            task.task_id()
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

const DEFAULT_CLEANUP_PERIOD_DAYS: Duration = Duration::days(1);
fn get_cleanup_period(task: &TaskLogCleanupTask) -> Result<Duration> {
    if let Some(config) = &task.config {
        let Some(cleanup_period) = config.cleanup_period() else {
            return Ok(DEFAULT_CLEANUP_PERIOD_DAYS);
        };
        Ok(cleanup_period)
    } else {
        Ok(DEFAULT_CLEANUP_PERIOD_DAYS)
    }
}

const DEFAULT_RETENTION_PERIOD_DAYS: Duration = Duration::days(90);
fn get_retention_period(task: &TaskLogCleanupTask) -> Result<Duration> {
    if let Some(config) = &task.config {
        let Some(retention_period) = config.retention_period() else {
            return Ok(DEFAULT_RETENTION_PERIOD_DAYS);
        };
        Ok(retention_period)
    } else {
        Ok(DEFAULT_RETENTION_PERIOD_DAYS)
    }
}

fn calculate_next_schedule_date(cleanup_period: Duration) -> DateTime<Utc> {
    Utc::now() + cleanup_period
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
        assert_eq!(config.cleanup_period.unwrap(), Duration::days(7));
        assert_eq!(config.retention_period.unwrap(), Duration::days(90));
    }
}
