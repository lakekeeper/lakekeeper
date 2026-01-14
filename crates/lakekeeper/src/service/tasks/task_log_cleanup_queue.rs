#[cfg(feature = "open-api")]
use std::collections::HashMap;
use std::{sync::LazyLock, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{
    PartialSchema, ToSchema,
    openapi::{RefOr, Schema},
};
use uuid::Uuid;

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
    utils::period::{Period, PeriodData},
};

const QN_STR: &str = "task_log_cleanup";
pub(crate) static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());

#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<QueueApiConfig> = LazyLock::new(|| QueueApiConfig {
    queue_name: &QUEUE_NAME,
    utoipa_type_name: TaskLogCleanupConfig::name(),
    utoipa_schema: TaskLogCleanupConfig::schema(),
});

#[cfg(feature = "open-api")]
pub(crate) static DEPENDENT_SCHEMAS: LazyLock<HashMap<String, RefOr<Schema>>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(CleanupPeriod::name().to_string(), CleanupPeriod::schema());
        map.insert(
            RetentionPeriod::name().to_string(),
            RetentionPeriod::schema(),
        );
        map.insert(Period::name().to_string(), Period::schema());
        map
    });

#[derive(Debug)]
pub enum TaskLogCleanupFilter {
    Project,
    Warehouse { warehouse_id: Uuid },
}

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

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Default, Debug)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
pub struct RetentionPeriod(Period);
impl RetentionPeriod {
    #[must_use]
    pub fn new(period: Period) -> Self {
        Self(period)
    }

    pub fn with_days(days: u16) -> Result<Self> {
        Ok(Self(Period::with_days(days)?))
    }

    #[must_use]
    pub fn period(&self) -> PeriodData {
        self.0.data()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Default, Debug)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
pub struct CleanupPeriod(Period);
impl CleanupPeriod {
    #[must_use]
    pub fn new(period: Period) -> Self {
        Self(period)
    }

    pub fn with_days(days: u16) -> Result<Self> {
        Ok(Self(Period::with_days(days)?))
    }

    #[must_use]
    pub fn period(&self) -> PeriodData {
        self.0.data()
    }
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
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
    let cleanup_period = get_cleanup_period(task)?;
    let schedule_date = calculate_next_schedule_date(cleanup_period);
    let retention_period = get_retention_period(task)?;

    let project_id = &task.task_metadata.project_id;
    let filter = match task.task_metadata.entity {
        TaskEntity::Project => TaskLogCleanupFilter::Project,
        TaskEntity::Warehouse { warehouse_id }
        | TaskEntity::EntityInWarehouse { warehouse_id, .. } => TaskLogCleanupFilter::Warehouse {
            warehouse_id: *warehouse_id,
        },
    };

    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!("Failed to start transaction for `{QN_STR}` Queue."))
        })?;

    C::cleanup_task_logs_older_than(trx.transaction(), retention_period, project_id, filter)
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
fn get_cleanup_period(task: &TaskLogCleanupTask) -> Result<CleanupPeriod> {
    match &task.config {
        Some(config) => Ok(config.cleanup_period()),
        None => Ok(CleanupPeriod(Period::with_days(
            DEFAULT_CLEANUP_PERIOD_DAYS,
        )?)),
    }
}

const DEFAULT_RETENTION_PERIOD_DAYS: u16 = 90;
fn get_retention_period(task: &TaskLogCleanupTask) -> Result<RetentionPeriod> {
    match &task.config {
        Some(config) => Ok(config.retention_period()),
        None => Ok(RetentionPeriod(Period::with_days(
            DEFAULT_RETENTION_PERIOD_DAYS,
        )?)),
    }
}

fn calculate_next_schedule_date(cleanup_period: CleanupPeriod) -> DateTime<Utc> {
    match cleanup_period.period() {
        PeriodData::Days(days) => Utc::now() + chrono::Duration::days(i64::from(days)),
    }
}

#[cfg(test)]
mod test {
    use serde_json::from_str;

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
        let config: TaskLogCleanupConfig = from_str(config_json).unwrap();
        assert_eq!(
            config.cleanup_period,
            CleanupPeriod(Period::with_days(7).unwrap())
        );
        assert_eq!(
            config.retention_period,
            RetentionPeriod(Period::with_days(90).unwrap())
        );
    }
}
