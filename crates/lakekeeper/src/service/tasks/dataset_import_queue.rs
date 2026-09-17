//! Runs a dataset import off the request path.
//!
//! Listing a prefix with millions of objects takes minutes, which is longer than
//! an HTTP request should live. The scan itself is unchanged: the worker calls
//! the same begin/stage/finish primitives, so the snapshot stays invisible until
//! the pointer move and a crash mid-scan leaves only sweepable debris.
use std::{sync::LazyLock, time::Duration};

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

use super::{SpecializedTask, TaskConfig, TaskData, TaskExecutionDetails};
use crate::{
    api::{Result, data::v1::datasets::ImportMode},
    server::datasets::{ImportParams, run_import},
    service::{
        CatalogStore, CatalogTabularOps, CatalogWarehouseOps, DatasetId, SecretStore,
        TabularListFlags, WarehouseStatus,
        tasks::{TaskEntity, TaskQueueName},
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
    pub max_files: Option<i64>,
    pub mode: ImportMode,
    pub summary: Option<serde_json::Value>,
}

impl TaskData for DatasetImportPayload {}

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

/// What the import registered, recorded on the task so the outcome survives the
/// worker that produced it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatasetImportExecutionDetails {
    pub imported: i64,
    pub modified: i64,
    pub removed: i64,
    pub truncated: bool,
}

impl TaskExecutionDetails for DatasetImportExecutionDetails {}

pub(crate) async fn dataset_import_worker<C: CatalogStore, S: SecretStore>(
    catalog_state: C::State,
    secret_state: S,
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

        instrumented_import::<C, S>(catalog_state.clone(), &secret_state, &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_import<C: CatalogStore, S: SecretStore>(
    catalog_state: C::State,
    secret_state: &S,
    task: &DatasetImportTask,
) {
    match import::<C, S>(task, secret_state, catalog_state.clone()).await {
        Ok(details) => {
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
        Err(err) => {
            tracing::error!("Error in `{QN_STR}` worker. {err}");
            let detail = format!("Failed to import dataset.\nError: {}", err.error);
            task.record_failure::<C>(catalog_state, &detail).await;
        }
    }
}

async fn import<C: CatalogStore, S: SecretStore>(
    task: &DatasetImportTask,
    secret_state: &S,
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
    .await?
    .ok_or_else(|| {
        ErrorModel::internal(
            format!("Warehouse {warehouse_id} is gone."),
            "WarehouseNotFoundForDatasetImport",
            None,
        )
    })?;

    // The dataset may have been dropped between enqueue and run; its location is
    // also what bounds the scan, so it is read now rather than carried in the
    // payload where it could go stale.
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

    let params = ImportParams {
        branch: task
            .data
            .branch
            .clone()
            .unwrap_or_else(|| "main".to_string()),
        sub_prefix: task.data.sub_prefix.clone(),
        suffix: task.data.suffix.clone(),
        max_files: task.data.max_files,
        mode: task.data.mode,
        summary: task.data.summary.clone(),
    };

    let outcome = run_import::<C, S>(
        warehouse.as_ref(),
        secret_state,
        warehouse_id,
        dataset_id,
        info.location.as_ref(),
        &params,
        catalog_state,
    )
    .await?;

    Ok(DatasetImportExecutionDetails {
        imported: outcome.added,
        modified: outcome.modified,
        removed: outcome.removed,
        truncated: outcome.truncated,
    })
}
