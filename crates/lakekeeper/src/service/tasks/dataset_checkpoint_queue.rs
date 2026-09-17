//! Folds a dataset branch's delta chain into a checkpoint, off the commit path.
//!
//! Safe to defer in a way most background work is not: a checkpoint only shortens
//! the ancestry walk, never changes what a read returns, so a task that is late,
//! retried or dropped costs read speed and nothing else.
use std::{sync::LazyLock, time::Duration};

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

use super::{SpecializedTask, TaskConfig, TaskData, TaskExecutionDetails};
use crate::{
    api::Result,
    service::{
        CatalogDatasetOps, CatalogStore, DatasetId, Transaction,
        tasks::{TaskEntity, TaskQueueName},
    },
};

const QN_STR: &str = "dataset_checkpoint";
pub static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());
#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<super::QueueApiConfig> =
    LazyLock::new(|| super::QueueApiConfig {
        queue_name: &QUEUE_NAME,
        utoipa_type_name: DatasetCheckpointQueueConfig::name(),
        utoipa_schema: DatasetCheckpointQueueConfig::schema(),
        scope: super::QueueScope::Warehouse,
        user_scheduling: super::UserScheduling::Disabled,
    });

pub type DatasetCheckpointTask = SpecializedTask<
    DatasetCheckpointQueueConfig,
    DatasetCheckpointPayload,
    DatasetCheckpointExecutionDetails,
>;

/// Which branch to fold. Carries no snapshot id: the branch has usually moved on
/// by the time the worker runs, so it resolves the head itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetCheckpointPayload {
    pub(crate) branch: String,
}

impl DatasetCheckpointPayload {
    #[must_use]
    pub fn new(branch: impl Into<String>) -> Self {
        Self {
            branch: branch.into(),
        }
    }
}

impl TaskData for DatasetCheckpointPayload {}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct DatasetCheckpointQueueConfig {}

impl TaskConfig for DatasetCheckpointQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    // A fold over a large manifest runs for minutes; a tighter window would reap
    // a healthy worker mid-run.
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(3600)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatasetCheckpointExecutionDetails {}

impl TaskExecutionDetails for DatasetCheckpointExecutionDetails {}

pub(crate) async fn dataset_checkpoint_worker<C: CatalogStore>(
    catalog_state: C::State,
    poll_interval: Duration,
    cancellation_token: crate::CancellationToken,
) {
    loop {
        let task = DatasetCheckpointTask::poll_for_new_task::<C>(
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
                branch = %task.data.branch,
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

        instrumented_checkpoint::<C>(catalog_state.clone(), &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_checkpoint<C: CatalogStore>(
    catalog_state: C::State,
    task: &DatasetCheckpointTask,
) {
    match checkpoint::<C>(task, catalog_state.clone()).await {
        Ok(Some(snapshot_id)) => {
            tracing::info!(
                "Task of `{QN_STR}` worker exited successfully. Branch `{}` folded into checkpoint {snapshot_id}.",
                task.data.branch,
            );
            task.record_success::<C>(catalog_state, Some("Wrote dataset checkpoint"))
                .await;
        }
        // Already folded, or back under the threshold. Not a failure.
        Ok(None) => {
            tracing::debug!(
                "Task of `{QN_STR}` worker found no checkpoint due for branch `{}`.",
                task.data.branch,
            );
            task.record_success::<C>(catalog_state, Some("No checkpoint due"))
                .await;
        }
        Err(err) => {
            tracing::error!(
                "Error in `{QN_STR}` worker. Failed to checkpoint branch `{}`. {err}",
                task.data.branch,
            );
            let detail = format!(
                "Failed to checkpoint dataset branch `{}`.\nError: {}",
                task.data.branch, err.error
            );
            task.record_failure::<C>(catalog_state, &detail).await;
        }
    }
}

async fn checkpoint<C: CatalogStore>(
    task: &DatasetCheckpointTask,
    catalog_state: C::State,
) -> Result<Option<crate::service::DatasetSnapshotId>> {
    let (warehouse_id, entity_id) = match &task.task_metadata.entity {
        TaskEntity::Warehouse { .. } | TaskEntity::Project => {
            return Err(ErrorModel::internal(
                format!(
                    "Unexpected task scope for `{QN_STR}` task. Task must have a dataset scope."
                ),
                "UnexpectedTaskScopeForDatasetCheckpoint",
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

    let mut t = C::Transaction::begin_write(catalog_state).await?;
    let folded =
        C::checkpoint_dataset_branch(warehouse_id, dataset_id, &task.data.branch, t.transaction())
            .await?;
    t.commit().await?;

    Ok(folded)
}
