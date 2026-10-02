//! Runs a dataset import off the request path.
//!
//! Listing a prefix with millions of objects takes minutes, which is longer than
//! an HTTP request should live. The scan itself is unchanged: the worker calls
//! the same begin/stage/finish primitives, so the snapshot stays invisible until
//! the pointer move and a crash mid-scan leaves only sweepable debris.
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

use super::{SpecializedTask, TaskConfig, TaskData, TaskExecutionDetails};
use crate::{
    api::{
        Result,
        data::v1::datasets::{ImportDatasetRequest, ImportMode},
    },
    request_metadata::RequestMetadata,
    server::datasets::{
        IMPORT_CANCELLED, ImportHeartbeat, ImportParams, announce_import, run_import,
    },
    service::{
        CatalogStore, CatalogTabularOps, CatalogWarehouseOps, ConstraintViolationPolicy, DatasetId,
        MaterializationReport, SecretStore, SkippedFile, TabularListFlags, WarehouseIdNotFound,
        WarehouseStatus,
        events::EventDispatcher,
        tasks::{TaskCheckState, TaskEntity, TaskQueueName},
    },
};

const QN_STR: &str = "dataset_import";
pub static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());
#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<super::QueueApiConfig> =
    LazyLock::new(|| super::QueueApiConfig {
        queue_name: &QUEUE_NAME,
        utoipa_type_name: DatasetImportQueueConfig::name(),
        utoipa_schema: DatasetImportQueueConfig::schema(),
        scope: super::QueueScope::Warehouse,
        user_scheduling: super::UserScheduling::Disabled,
    });

pub type DatasetImportTask =
    SpecializedTask<DatasetImportQueueConfig, DatasetImportPayload, DatasetImportExecutionDetails>;

/// The request that was authorized, carried forward for the worker to replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetImportPayload {
    pub branch: Option<String>,
    pub sub_prefix: Option<String>,
    pub suffix: Option<String>,
    pub include: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub default_excludes: Option<bool>,
    pub record_versions: Option<bool>,
    pub check_materialization: Option<bool>,
    pub max_files: Option<i64>,
    pub mode: ImportMode,
    pub summary: Option<serde_json::Value>,
    pub on_constraint_violation: ConstraintViolationPolicy,
}

impl TaskData for DatasetImportPayload {}

impl From<&ImportDatasetRequest> for DatasetImportPayload {
    fn from(request: &ImportDatasetRequest) -> Self {
        Self {
            branch: request.branch.clone(),
            sub_prefix: request.sub_prefix.clone(),
            suffix: request.suffix.clone(),
            include: request.include.clone(),
            exclude: request.exclude.clone(),
            default_excludes: request.default_excludes,
            record_versions: request.record_versions,
            check_materialization: request.check_materialization,
            max_files: request.max_files,
            mode: request.mode.unwrap_or_default(),
            summary: request.summary.clone(),
            on_constraint_violation: request.constraint_violation_policy(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct DatasetImportQueueConfig {}

impl TaskConfig for DatasetImportQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    // A scan of a very large prefix runs for minutes; a tighter window would reap
    // a healthy worker mid-listing.
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(3600)
    }
}

/// Where a running import has got.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportPhase {
    /// Listing the prefix. `objects_listed` grows; the total is not known yet.
    #[default]
    Listing,
    /// Comparing what was listed with the branch's manifest.
    Merging,
    /// Moving the branch to the new snapshot.
    Publishing,
    /// Checking the snapshots refs point at against the listing.
    Checking,
    /// Finished; the counts are final.
    Done,
}

/// How far the import has got, and what it registered, recorded on the task while it
/// runs and when it ends, so the outcome survives the worker that produced it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct DatasetImportExecutionDetails {
    #[serde(default)]
    pub phase: ImportPhase,
    /// Objects the listing has found within the scan so far.
    #[serde(default)]
    pub objects_listed: i64,
    pub imported: i64,
    pub modified: i64,
    pub removed: i64,
    pub truncated: bool,
    pub skipped: i64,
    /// The first of the skipped objects, with why.
    pub skipped_files: Vec<SkippedFile>,
    /// What a materialization check found, when one ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization: Option<MaterializationReport>,
}

impl TaskExecutionDetails for DatasetImportExecutionDetails {}

/// `events`, when set, receives a `dataset_committed` event for every import that
/// publishes, attributed to Lakekeeper itself.
pub(crate) async fn dataset_import_worker<C: CatalogStore, S: SecretStore>(
    catalog_state: C::State,
    secret_state: S,
    events: Option<EventDispatcher>,
    poll_interval: Duration,
    cancellation_token: crate::CancellationToken,
) {
    loop {
        let task = DatasetImportTask::poll_for_new_task::<C>(
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
                entity_type = "Not Specified",
                attempt = %task.attempt(),
                task_id = %task.task_id(),
            )
        };

        run_dataset_import_task::<C, S>(
            &task,
            &secret_state,
            events.as_ref(),
            catalog_state.clone(),
        )
        .instrument(span.or_current())
        .await;
    }
}

/// Run one picked-up import task to its end and record the outcome on it: what the
/// worker loop does with each task it polls.
///
/// The import heartbeats every 5,000 objects it lists or merges, so a stop or a
/// cancel reaches it within that many; it then discards what it staged and records
/// the attempt as failed, which retries a stopped task and leaves a cancelled one
/// gone. Once it is past its publish, neither undoes anything: the import runs its
/// materialization check to the end and records the outcome, unless the task is
/// gone.
pub async fn run_dataset_import_task<C: CatalogStore, S: SecretStore>(
    task: &DatasetImportTask,
    secret_state: &S,
    events: Option<&EventDispatcher>,
    catalog_state: C::State,
) {
    match import::<C, S>(task, secret_state, events, catalog_state.clone()).await {
        Ok(details) => {
            // The log copies the task row's execution details, so they go on the row
            // first. The snapshot is already committed; failing here loses only the
            // record, so it must not fail the task.
            match task
                .heartbeat::<C>(catalog_state.clone(), 1.0, Some(details.clone()))
                .await
            {
                Ok(TaskCheckState::NotActive) => {
                    tracing::info!(
                        "`{QN_STR}` task was cancelled once its import was past its publish; there is no task left to record it on."
                    );
                    return;
                }
                Ok(TaskCheckState::Continue | TaskCheckState::Stop) => {}
                Err(e) => tracing::warn!("Failed to record `{QN_STR}` execution details: {e}"),
            }
            tracing::info!(
                "Task of `{QN_STR}` worker exited successfully. Registered {} file(s), {} modified, {} removed.",
                details.imported,
                details.modified,
                details.removed,
            );
            task.record_success::<C>(
                catalog_state,
                Some(&format!(
                    "Registered {} file(s), {} modified, {} removed",
                    details.imported, details.modified, details.removed
                )),
            )
            .await;
        }
        Err(err) if err.error.r#type == IMPORT_CANCELLED => {
            tracing::info!("`{QN_STR}` task was cancelled; its import published nothing.");
        }
        Err(err) => {
            tracing::error!("Error in `{QN_STR}` worker. {err}");
            let detail = format!("Failed to import dataset.\nError: {}", err.error);
            task.record_failure::<C>(catalog_state, &detail).await;
        }
    }
}

/// The import's outcome.
async fn import<C: CatalogStore, S: SecretStore>(
    task: &DatasetImportTask,
    secret_state: &S,
    events: Option<&EventDispatcher>,
    catalog_state: C::State,
) -> Result<DatasetImportExecutionDetails> {
    let (warehouse_id, entity_id) = match &task.task_metadata.entity {
        TaskEntity::Warehouse { .. } | TaskEntity::Project => {
            return Err(ErrorModel::internal(
                format!(
                    "Unexpected task scope for `{QN_STR}` task. Task must have a dataset scope."
                ),
                "UnexpectedTaskScopeForDatasetImport",
                None,
            )
            .into());
        }
        TaskEntity::EntityInWarehouse {
            warehouse_id,
            entity_id,
            entity_name: _,
        } => (*warehouse_id, *entity_id),
    };
    let dataset_id = DatasetId::from(entity_id.as_uuid());

    let warehouse = C::get_warehouse_by_id(
        warehouse_id,
        WarehouseStatus::active_and_inactive(),
        catalog_state.clone(),
    )
    .await
    .map_err(ErrorModel::from)
    .and_then(|w| w.ok_or_else(|| WarehouseIdNotFound::new(warehouse_id).into()))
    .map_err(|e| {
        e.append_detail(format!(
            "Failed to get warehouse {warehouse_id} for Dataset Import task."
        ))
    })?;

    // The dataset may have been dropped between enqueue and run; its location is
    // also what bounds the scan, so it is read at run time — a copy in the payload
    // could go stale.
    let info = C::get_dataset_info(
        warehouse_id,
        dataset_id,
        TabularListFlags::active_and_staged(),
        catalog_state.clone(),
    )
    .await?
    .ok_or_else(|| {
        ErrorModel::internal(
            format!("Dataset {dataset_id} is gone."),
            "DatasetNotFoundForImport",
            None,
        )
    })?;

    let params = ImportParams::from(&task.data);

    let mut heartbeat = ImportHeartbeat::for_task(task);
    let outcome = run_import::<C, S>(
        warehouse.as_ref(),
        secret_state,
        warehouse_id,
        task.task_metadata.entity.clone(),
        info.location.as_ref(),
        &params,
        &mut heartbeat,
        catalog_state.clone(),
    )
    .await?;
    if let (Some(events), Some(published)) = (events, outcome.published.clone()) {
        announce_import::<C>(
            events,
            warehouse,
            dataset_id,
            published,
            Arc::new(RequestMetadata::new_lakekeeper_internal(*task.task_id())),
            catalog_state,
        )
        .await;
    }

    Ok(DatasetImportExecutionDetails {
        phase: ImportPhase::Done,
        objects_listed: outcome.listed,
        imported: outcome.added,
        modified: outcome.modified,
        removed: outcome.removed,
        truncated: outcome.truncated,
        skipped: outcome.skipped,
        skipped_files: outcome.skipped_files,
        materialization: outcome.materialization,
    })
}
