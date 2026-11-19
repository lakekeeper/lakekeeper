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
    SpecializedTask, TaskCheckState, TaskConfig, TaskData, TaskExecutionDetails, TaskQueueName,
    TaskQueueRegistry, UserConfiguration, UserScheduling, WarehouseTaskEntityId,
    heartbeat::with_heartbeat, retry_backoff,
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
        CatalogStore, CatalogTableOps, CatalogWarehouseOps, SecretStore, Transaction,
        WarehouseStatus,
        table_health::{
            MEASUREMENT_VERSION, MeasureError, MeasureOutcome, Measurement, NotMeasurableReason,
            measure,
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

/// Measure the task's table, log the result and record the outcome.
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
        Ok(RunEnd::Ended {
            outcome: MeasureOutcome::Measured(measurement),
            scan_duration,
        }) => {
            let details = log_measurement(
                &warehouse_id,
                &table_id,
                table_name,
                &measurement,
                scan_duration,
            );
            task.record_success::<C>(catalog_state, Some(&details))
                .await;
        }
        Ok(RunEnd::Ended {
            outcome: MeasureOutcome::NotMeasurable(reason),
            scan_duration,
        }) => {
            tracing::info!(
                %warehouse_id,
                %table_id,
                table_name,
                ?reason,
                scan_duration_ms = scan_duration.as_millis(),
                measurement_version = MEASUREMENT_VERSION,
                "Table can't be measured"
            );
            let details = format!("Not measurable: {reason:?}");
            task.record_success::<C>(catalog_state, Some(&details))
                .await;
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

/// Log a measured table and its findings. Returns the message for the task log.
fn log_measurement(
    warehouse_id: &str,
    table_id: &str,
    table_name: &str,
    measurement: &Measurement,
    scan_duration: Duration,
) -> String {
    let sizes = measurement.sizes;
    tracing::info!(
        %warehouse_id,
        %table_id,
        table_name,
        live_data_bytes = sizes.live_data_bytes,
        live_delete_bytes = sizes.live_delete_bytes,
        live_metadata_bytes = sizes.live_metadata_bytes,
        snapshot_id = ?measurement.snapshot_id,
        manifests_read = measurement.manifests_read,
        findings = measurement.findings.len(),
        scan_duration_ms = scan_duration.as_millis(),
        measurement_version = MEASUREMENT_VERSION,
        "Measured table health"
    );
    for finding in &measurement.findings {
        tracing::info!(
            %warehouse_id,
            %table_id,
            table_name,
            kind = %finding.kind,
            severity = ?finding.severity,
            evidence = %finding.evidence,
            thresholds = %finding.thresholds,
            "Table health finding"
        );
    }
    let kinds = measurement
        .findings
        .iter()
        .map(|finding| finding.kind.to_string())
        .collect::<Vec<_>>();
    format!(
        "Measured: live data {} bytes, live delete {} bytes, live metadata {} bytes. \
         Findings: {}.",
        sizes.live_data_bytes,
        sizes.live_delete_bytes,
        sizes.live_metadata_bytes,
        if kinds.is_empty() {
            "none".to_string()
        } else {
            kinds.join(", ")
        }
    )
}

/// How a measurement task ended without an error.
enum RunEnd {
    /// Nothing to measure.
    Skipped(&'static str),
    /// The scan ended with an outcome.
    Ended {
        outcome: MeasureOutcome,
        scan_duration: Duration,
    },
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
        Ok(outcome) => Ok(RunEnd::Ended {
            outcome,
            scan_duration,
        }),
        Err(MeasureError::Cancelled) if deadline_reached => Ok(RunEnd::Ended {
            outcome: MeasureOutcome::NotMeasurable(NotMeasurableReason::TimeLimit),
            scan_duration,
        }),
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
