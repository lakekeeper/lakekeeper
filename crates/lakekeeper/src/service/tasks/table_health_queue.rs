//! Queue that measures the health of one Iceberg table per task. See
//! [`crate::service::table_health`].

use std::{
    future::Future,
    ops::ControlFlow,
    sync::{Arc, LazyLock},
    time::Duration,
};

use iceberg::spec::TableMetadata;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::{Location, StorageBackend};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

use super::{
    FailedAttempt, OnWarehouseDelete, QueueRegistration, QueueScope, RetryDecision,
    ScheduleTaskMetadata, SpecializedTask, TaskCheckState, TaskConfig, TaskData, TaskEntity,
    TaskExecutionDetails, TaskQueueName, TaskQueueRegistry, UserConfiguration, UserScheduling,
    WarehouseTaskEntityId, heartbeat::with_heartbeat, retry_backoff,
};
use crate::{
    CONFIG, CancellationToken,
    api::{Result, iceberg::v1::tables::LoadTableFilters},
    operations::{
        manifest_reader::{LakekeeperIoProvider, ManifestReader},
        progress_tracker::ProgressTracker,
    },
    server::maybe_get_secret,
    service::{
        ArcProjectId, CatalogStore, CatalogTableHealthOps, CatalogTableOps, CatalogWarehouseOps,
        NamedEntity, SecretStore, TableInfo, Transaction, WarehouseStatus,
        table_health::{
            MEASUREMENT_VERSION, MeasureError, MeasureOutcome, NotMeasurableReason,
            TableHealthWrite, TableHealthWriteOutcome, measure,
        },
    },
};

const QN_STR: &str = "table_health";
pub static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());

#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<super::QueueApiConfig> =
    LazyLock::new(|| super::QueueApiConfig {
        queue_name: &QUEUE_NAME,
        utoipa_type_name: HealthQueueConfig::name(),
        utoipa_schema: HealthQueueConfig::schema(),
        scope: super::QueueScope::Warehouse,
        user_scheduling: super::UserScheduling::Enabled {
            payload_schema: None,
        },
        user_configuration: super::UserConfiguration::Enabled,
    });

/// How long after a commit the table's measurement is due. All commits within it lead to one
/// measurement.
const COMMIT_DELAY: chrono::TimeDelta = chrono::TimeDelta::hours(1);
/// Run time limit of a measurement if the queue config sets none.
const DEFAULT_MAX_RUN_SECONDS: u64 = 900;
/// How often a running measurement renews its heartbeat, well within
/// [`HealthQueueConfig::max_time_since_last_heartbeat`].
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// Longest wait before a failed measurement is retried.
const MAX_RETRY_BACKOFF: chrono::TimeDelta = chrono::TimeDelta::hours(24);

pub type TableHealthTask =
    SpecializedTask<HealthQueueConfig, HealthQueuePayload, HealthExecutionDetails>;

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct HealthQueueConfig {
    /// How long one measurement may run, in seconds. A table whose scan takes longer is reported
    /// as not measurable. Default: 900
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_run_seconds: Option<u64>,
}

impl HealthQueueConfig {
    #[must_use]
    pub fn max_run_time(&self) -> Duration {
        Duration::from_secs(self.max_run_seconds.unwrap_or(DEFAULT_MAX_RUN_SECONDS))
    }
}

impl TaskConfig for HealthQueueConfig {
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::minutes(5)
    }

    /// No limit: nothing else measures the table again until its next commit.
    fn max_retries() -> i32 {
        i32::MAX
    }

    /// Retries after [`retry_backoff`], at most [`MAX_RETRY_BACKOFF`] apart.
    fn retry_decision(failed: &FailedAttempt<'_>) -> RetryDecision {
        RetryDecision::retry(retry_backoff(failed, MAX_RETRY_BACKOFF))
    }

    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    fn on_warehouse_delete() -> OnWarehouseDelete {
        OnWarehouseDelete::Cancel
    }

    fn check_schedule_eligibility(
        _config: &Self,
        _entity_properties: &std::collections::HashMap<String, String>,
        entity: WarehouseTaskEntityId,
    ) -> Result<(), ErrorModel> {
        match entity {
            WarehouseTaskEntityId::Table { .. } => Ok(()),
            WarehouseTaskEntityId::View { .. } => Err(ErrorModel::bad_request(
                "table_health measures tables only; views have no data files.",
                "EntityTypeUnsupported",
                None,
            )),
            WarehouseTaskEntityId::GenericTable { .. } => Err(ErrorModel::bad_request(
                "table_health measures Iceberg tables only; generic tables have no Iceberg metadata.",
                "EntityTypeUnsupported",
                None,
            )),
        }
    }
}

/// What scheduled a measurement. Only recorded for debugging.
#[derive(Clone, Copy, Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum HealthRunReason {
    /// A commit to the table.
    Commit,
    /// A maintenance run changed the table.
    Maintenance,
    /// A user scheduled it.
    #[default]
    Manual,
    /// All tables of the warehouse were scheduled, after the measurement version changed.
    Enumeration,
    /// A queue config of the warehouse changed.
    Setting,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct HealthQueuePayload {
    #[serde(default)]
    pub reason: HealthRunReason,
}

impl HealthQueuePayload {
    #[must_use]
    pub fn new(reason: HealthRunReason) -> Self {
        Self { reason }
    }
}

impl TaskData for HealthQueuePayload {}

/// Schedule a measurement of each committed table, due [`COMMIT_DELAY`] after the commit, in the
/// commit's transaction. A table that already has an active measurement task keeps it.
///
/// # Errors
/// Returns an error if the tasks cannot be scheduled.
pub(crate) async fn schedule_after_commit<'t, C: CatalogStore>(
    project_id: &ArcProjectId,
    tables: impl Iterator<Item = &'t TableInfo>,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<(), ErrorModel> {
    let scheduled_for = chrono::Utc::now() + COMMIT_DELAY;
    let tasks = tables.map(|table| {
        (
            ScheduleTaskMetadata {
                project_id: project_id.clone(),
                parent_task_id: None,
                scheduled_for: Some(scheduled_for),
                entity: TaskEntity::EntityInWarehouse {
                    warehouse_id: table.warehouse_id,
                    entity_id: WarehouseTaskEntityId::Table {
                        table_id: table.tabular_id,
                    },
                    entity_name: table.tabular_ident.clone().into_name_parts(),
                },
                resource_demand: None,
            },
            HealthQueuePayload::new(HealthRunReason::Commit),
        )
    });
    TableHealthTask::schedule_tasks::<C>(tasks, transaction).await?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct HealthExecutionDetails {
    /// Version of the measurement code that ran.
    pub measurement_version: i32,
    pub scan_duration_ms: i64,
    pub manifests_read: u64,
}

impl TaskExecutionDetails for HealthExecutionDetails {}

/// Register the queue and its workers.
pub(crate) async fn register<C: CatalogStore, S: SecretStore>(
    registry: &TaskQueueRegistry,
    catalog_state: C::State,
    secret_store: S,
    poll_interval: Duration,
) {
    registry
        .register_queue::<HealthQueueConfig, HealthQueuePayload>(QueueRegistration {
            queue_name: &QUEUE_NAME,
            worker_fn: Arc::new(move |cancellation_token| {
                let catalog_state = catalog_state.clone();
                let secret_store = secret_store.clone();
                Box::pin(async move {
                    table_health_worker::<C, S>(
                        catalog_state,
                        secret_store,
                        poll_interval,
                        cancellation_token,
                    )
                    .await;
                })
            }),
            num_workers: CONFIG.task_table_health_workers,
            scope: QueueScope::Warehouse,
            #[cfg(feature = "open-api")]
            user_scheduling: UserScheduling::Enabled {
                payload_schema: None,
            },
            #[cfg(not(feature = "open-api"))]
            user_scheduling: UserScheduling::Enabled,
            user_configuration: UserConfiguration::Enabled,
        })
        .await;
}

async fn table_health_worker<C: CatalogStore, S: SecretStore>(
    catalog_state: C::State,
    secret_state: S,
    poll_interval: Duration,
    cancellation_token: CancellationToken,
) {
    loop {
        let task = TableHealthTask::poll_for_new_task::<C>(
            catalog_state.clone(),
            &poll_interval,
            cancellation_token.clone(),
        )
        .await;
        let Some(task) = task else {
            tracing::info!("Graceful shutdown: exiting `{QN_STR}` worker");
            return;
        };

        let span = if let Some((warehouse_id, entity_id, entity_name)) =
            task.task_metadata.warehouse_task_sub_entity()
        {
            tracing::debug_span!(
                QN_STR,
                warehouse_id = %warehouse_id,
                entity_id = %entity_id.as_uuid(),
                entity_name = %entity_name.join("."),
                attempt = %task.attempt(),
                task_id = %task.task_id(),
            )
        } else {
            tracing::debug_span!(
                QN_STR,
                attempt = %task.attempt(),
                task_id = %task.task_id(),
            )
        };

        run::<C, S>(catalog_state.clone(), &secret_state, &task)
            .instrument(span.or_current())
            .await;
    }
}

/// Measure the task's table and record the outcome.
async fn run<C: CatalogStore, S: SecretStore>(
    catalog_state: C::State,
    secret_state: &S,
    task: &TableHealthTask,
) {
    let table = task.task_metadata.warehouse_task_sub_entity().map(
        |(warehouse_id, entity_id, entity_name)| {
            (warehouse_id, entity_id.as_uuid(), entity_name.join("."))
        },
    );
    let (warehouse_id, table_id, table_name) = match &table {
        Some((warehouse_id, table_id, table_name)) => (
            warehouse_id.to_string(),
            table_id.to_string(),
            table_name.as_str(),
        ),
        None => (String::new(), String::new(), ""),
    };

    match measure_task::<C, S>(catalog_state.clone(), secret_state, task).await {
        Ok(RunEnd::Skipped(reason)) => {
            tracing::info!(%warehouse_id, %table_id, table_name, "Table health not measured: {reason}");
            task.record_success::<C>(catalog_state, Some(reason)).await;
        }
        Ok(RunEnd::Ended(scan)) => {
            finish::<C>(catalog_state, task, scan).await;
        }
        Ok(RunEnd::Stopped) => {
            tracing::info!(%warehouse_id, %table_id, table_name, "Table health measurement stopped");
            task.record_failure::<C>(catalog_state, "Stopped before the measurement finished.")
                .await;
        }
        Ok(RunEnd::NotActive) => {
            tracing::info!(
                %warehouse_id,
                %table_id,
                table_name,
                "Table health measurement ended: another worker holds a newer attempt"
            );
        }
        Err(err) => {
            tracing::error!(%warehouse_id, %table_id, table_name, "Table health measurement failed: {err}");
            let detail = format!("Failed to measure table health.\nError: {}", err.error);
            task.record_failure::<C>(catalog_state, &detail).await;
        }
    }
}

/// Store the result and record the task's success. If a commit changed the table during the run,
/// schedule the next measurement in the same transaction; a missing file is then not stored.
///
/// `write_table_health` holds off commits to the table, which closes the race with a commit: a
/// commit that ran first is seen as a changed location; a commit that comes later waits until the
/// task is no longer active, so its own schedule succeeds. It also keeps the table from being
/// dropped before the result is stored. On an error the task is recorded as failed and retried.
async fn finish<C: CatalogStore>(catalog_state: C::State, task: &TableHealthTask, scan: ScanEnd) {
    if let Err(err) = finish_in_transaction::<C>(catalog_state.clone(), task, scan).await {
        tracing::error!("Failed to finish the table health measurement: {err}");
        let detail = format!(
            "Failed to record the table health measurement.\nError: {}",
            err.error
        );
        task.record_failure::<C>(catalog_state, &detail).await;
    }
}

async fn finish_in_transaction<C: CatalogStore>(
    catalog_state: C::State,
    task: &TableHealthTask,
    scan: ScanEnd,
) -> Result<()> {
    let Some((warehouse_id, &WarehouseTaskEntityId::Table { table_id }, _)) =
        task.task_metadata.warehouse_task_sub_entity()
    else {
        task.record_success::<C>(catalog_state, Some("The task's entity is not a table."))
            .await;
        return Ok(());
    };
    let details = HealthExecutionDetails {
        measurement_version: MEASUREMENT_VERSION,
        scan_duration_ms: i64::try_from(scan.scan_duration.as_millis()).unwrap_or(i64::MAX),
        manifests_read: match &scan.outcome {
            MeasureOutcome::Measured(measurement) => measurement.manifests_read,
            MeasureOutcome::NotMeasurable(_) => 0,
        },
    };
    let mut message = match &scan.outcome {
        MeasureOutcome::Measured(_) => "Measured.".to_string(),
        MeasureOutcome::NotMeasurable(reason) => format!("Not measurable: {reason:?}"),
    };
    let write = TableHealthWrite {
        warehouse_id,
        table_id,
        measurement_version: MEASUREMENT_VERSION,
        // Counted from after a running commit: the next run is due at least this long after it.
        next_run_delay: COMMIT_DELAY,
        outcome: scan.outcome,
        metadata_location: scan.metadata_location,
        scan_duration: scan.scan_duration,
    };
    let mut transaction = C::Transaction::begin_write(catalog_state).await?;
    // Before the heartbeat: a commit holds off the finish, then writes the task row.
    let outcome = C::write_table_health(&write, transaction.transaction()).await?;
    // A success moves the task's execution details into its log.
    if matches!(
        task.heartbeat_in_transaction::<C>(transaction.transaction(), 1.0, Some(details))
            .await?,
        TaskCheckState::NotActive
    ) {
        // Another worker holds a newer attempt and records the outcome. The transaction rolls
        // back, so nothing is written.
        return Ok(());
    }
    // Set if a commit changed the table during the run.
    let follow_up = match outcome {
        TableHealthWriteOutcome::TableGone => {
            message = "Not stored: the table no longer exists.".to_string();
            None
        }
        // The commit can have deleted the missing file; the follow-up measures the new metadata.
        TableHealthWriteOutcome::Raced(next_run) => {
            message =
                "Not stored: a file was missing and a commit changed the table during the run."
                    .to_string();
            Some(next_run.not_before)
        }
        TableHealthWriteOutcome::Written(next_run) => {
            next_run.location_changed.then_some(next_run.not_before)
        }
        TableHealthWriteOutcome::NewerExists(next_run) => {
            message = "Not stored: a newer measurement version measured the table.".to_string();
            next_run.location_changed.then_some(next_run.not_before)
        }
    };
    task.record_success_in_transaction::<C>(transaction.transaction(), Some(&message))
        .await;
    if let Some(next_not_before) = follow_up {
        TableHealthTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id: task.task_metadata.project_id.clone(),
                parent_task_id: None,
                scheduled_for: Some(next_not_before),
                entity: task.task_metadata.entity.clone(),
                resource_demand: None,
            },
            HealthQueuePayload::new(HealthRunReason::Commit),
            transaction.transaction(),
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

/// A scan that ended with an outcome.
struct ScanEnd {
    outcome: MeasureOutcome,
    scan_duration: Duration,
    /// The metadata location that was measured.
    metadata_location: Location,
}

/// How a measurement task ended without an error.
enum RunEnd {
    /// Nothing to measure.
    Skipped(&'static str),
    /// The scan ended with an outcome.
    Ended(ScanEnd),
    /// A stop was requested during the scan.
    Stopped,
    /// Another worker holds a newer attempt of the task, see [`TaskCheckState::NotActive`].
    NotActive,
}

async fn measure_task<C: CatalogStore, S: SecretStore>(
    catalog_state: C::State,
    secret_state: &S,
    task: &TableHealthTask,
) -> Result<RunEnd> {
    let Some((warehouse_id, &WarehouseTaskEntityId::Table { table_id }, _)) =
        task.task_metadata.warehouse_task_sub_entity()
    else {
        return Ok(RunEnd::Skipped("The task's entity is not a table."));
    };

    let Some(warehouse) = C::get_warehouse_by_id(
        warehouse_id,
        WarehouseStatus::active_and_inactive(),
        catalog_state.clone(),
    )
    .await
    .map_err(ErrorModel::from)?
    else {
        return Ok(RunEnd::Skipped("The warehouse no longer exists."));
    };

    // Soft-deleted tables are measured too. The transaction ends before the scan.
    let mut transaction = C::Transaction::begin_read(catalog_state.clone()).await?;
    let tables = C::load_tables(
        warehouse_id,
        [table_id],
        true,
        &LoadTableFilters::default(),
        transaction.transaction(),
    )
    .await?;
    transaction.commit().await?;
    let Some(table) = tables.into_iter().find(|table| table.table_id == table_id) else {
        return Ok(RunEnd::Skipped("The table no longer exists."));
    };
    let Some(metadata_location) = table.metadata_location else {
        return Ok(RunEnd::Skipped("The table is staged."));
    };

    let secret = maybe_get_secret(warehouse.storage_secret_id, secret_state).await?;
    let storage = warehouse
        .storage_profile
        .file_io(secret.as_deref())
        .await
        .map_err(IcebergErrorResponse::from)?;
    let reader = ManifestReader::new(WarehouseStorage(storage));

    scan::<C>(
        catalog_state,
        task,
        &table.table_metadata,
        &metadata_location,
        &reader,
    )
    .await
}

/// Measure the table within the queue's run time limit, heartbeating while the scan runs.
async fn scan<C: CatalogStore>(
    catalog_state: C::State,
    task: &TableHealthTask,
    metadata: &TableMetadata,
    metadata_location: &Location,
    reader: &ManifestReader<WarehouseStorage>,
) -> Result<RunEnd> {
    let tracker = ProgressTracker::new();
    let max_run_time = task.config.as_ref().map_or_else(
        || HealthQueueConfig::default().max_run_time(),
        HealthQueueConfig::max_run_time,
    );
    let started = tokio::time::Instant::now();
    let scan = async {
        let mut measured = std::pin::pin!(measure(metadata, metadata_location, reader, &tracker));
        tokio::select! {
            result = &mut measured => (result, false),
            () = tokio::time::sleep(max_run_time) => {
                tracker.cancel();
                (measured.await, true)
            }
        }
    };
    // A stop request cancels the scan; the task is then retried.
    let heartbeat = || {
        let catalog_state = catalog_state.clone();
        let tracker = tracker.clone();
        async move {
            match task.heartbeat::<C>(catalog_state, 0.0, None).await {
                Ok(TaskCheckState::NotActive) => ControlFlow::Break(()),
                Ok(TaskCheckState::Stop) => {
                    tracker.cancel();
                    ControlFlow::Continue(())
                }
                Ok(TaskCheckState::Continue) => ControlFlow::Continue(()),
                Err(e) => {
                    tracing::warn!(
                        "Failed to heartbeat `{QN_STR}` task {}: {e}",
                        task.task_id()
                    );
                    ControlFlow::Continue(())
                }
            }
        }
    };
    let Some((result, deadline_reached)) =
        with_heartbeat(scan, HEARTBEAT_INTERVAL, heartbeat).await
    else {
        return Ok(RunEnd::NotActive);
    };
    let scan_duration = started.elapsed();

    match result {
        Ok(outcome) => Ok(RunEnd::Ended(ScanEnd {
            outcome,
            scan_duration,
            metadata_location: metadata_location.clone(),
        })),
        Err(MeasureError::Cancelled) if deadline_reached => Ok(RunEnd::Ended(ScanEnd {
            outcome: MeasureOutcome::NotMeasurable(NotMeasurableReason::TimeLimit),
            scan_duration,
            metadata_location: metadata_location.clone(),
        })),
        Err(MeasureError::Cancelled) => Ok(RunEnd::Stopped),
        Err(MeasureError::Read(e)) => Err(ErrorModel::internal(
            "Failed to read the table's metadata files.",
            "TableHealthReadFailed",
            Some(Box::new(e)),
        )
        .into()),
    }
}

/// The storage of a warehouse, for the manifest reader. Its credentials are fixed when the
/// measurement starts.
#[derive(Debug, Clone)]
struct WarehouseStorage(StorageBackend);

impl LakekeeperIoProvider for WarehouseStorage {
    type LakekeeperIO = StorageBackend;

    fn lakekeeper_io(&self) -> impl Future<Output = Self::LakekeeperIO> + Send {
        std::future::ready(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::service::tasks::{FailureKind, RetryPolicy, TaskId};

    /// A failed measurement is retried with no limit on attempts; the wait doubles from 30 s up
    /// to a day, less up to 10% jitter.
    #[test]
    fn a_failed_measurement_is_retried_without_limit_at_most_a_day_apart() {
        let policy = RetryPolicy::of::<HealthQueueConfig>();
        assert_eq!(policy.max_retries, i32::MAX);

        let task_id =
            TaskId::from(Uuid::parse_str("01890a5d-ac96-774b-bcce-b302099a8057").unwrap());
        let delays = [1, 2, 12, 13, 1000].map(|attempt| {
            match policy.decide(&FailedAttempt {
                task_id,
                attempt,
                schedule_id: None,
                kind: FailureKind::Error,
                error: "",
                resource_demand: None,
                execution_details: None,
            }) {
                RetryDecision::Retry { backoff, .. } => backoff,
                decision @ RetryDecision::GiveUp { .. } => panic!("{decision:?}"),
            }
        });
        // Before shortening: 30 s, 60 s, 30 s × 2^11 = 61 440 s, then 24 h; shortened by 4.5%,
        // 5.4%, 2.5%, 3.0% and 0.6%.
        assert_eq!(
            delays,
            [28_650, 56_760, 59_904_000, 83_808_000, 85_881_600]
                .map(chrono::Duration::milliseconds)
        );
    }
}
