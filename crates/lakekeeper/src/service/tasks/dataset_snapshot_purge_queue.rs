//! Purges expired dataset snapshots once their grace period is over.
//!
//! The one step of retention that deletes. It deletes rows, never files: the
//! objects of a purged snapshot stay where they are.
use std::{sync::LazyLock, time::Duration as StdDuration};

use chrono::{Duration, Utc};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

use super::{ScheduleTaskMetadata, SpecializedTask, TaskConfig, TaskData, TaskExecutionDetails};
use crate::{
    CancellationToken,
    api::Result,
    server::datasets::ABANDONED_STAGING_AFTER,
    service::{
        CatalogDatasetOps, CatalogStore, DatasetId, DatasetPurge, Transaction,
        tasks::{TaskEntity, TaskQueueName},
    },
};

const QN_STR: &str = "dataset_snapshot_purge";
pub static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());
#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<super::QueueApiConfig> =
    LazyLock::new(|| super::QueueApiConfig {
        queue_name: &QUEUE_NAME,
        utoipa_type_name: DatasetSnapshotPurgeQueueConfig::name(),
        utoipa_schema: DatasetSnapshotPurgeQueueConfig::schema(),
        scope: super::QueueScope::Warehouse,
        user_scheduling: super::UserScheduling::Disabled,
    });

/// The soonest a purge runs again. A run that cuts history folds the snapshot
/// above the cut into a checkpoint, which restates every file, so runs a day apart
/// let one fold cover a day of expiries. It also keeps a snapshot that is due but
/// still held, by a live grant or a commit staged on it, from spinning the queue.
const PURGE_INTERVAL: Duration = Duration::days(1);

pub type DatasetSnapshotPurgeTask = SpecializedTask<
    DatasetSnapshotPurgeQueueConfig,
    DatasetSnapshotPurgePayload,
    DatasetSnapshotPurgeExecutionDetails,
>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetSnapshotPurgePayload {}

impl TaskData for DatasetSnapshotPurgePayload {}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
pub struct DatasetSnapshotPurgeQueueConfig {}

impl TaskConfig for DatasetSnapshotPurgeQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    // A purge may fold survivors into checkpoints over a large manifest.
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(3600)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatasetSnapshotPurgeExecutionDetails {}

impl TaskExecutionDetails for DatasetSnapshotPurgeExecutionDetails {}

pub(crate) async fn dataset_snapshot_purge_worker<C: CatalogStore>(
    catalog_state: C::State,
    poll_interval: StdDuration,
    cancellation_token: CancellationToken,
) {
    loop {
        let task = DatasetSnapshotPurgeTask::poll_for_new_task::<C>(
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

        instrumented_purge::<C>(catalog_state.clone(), &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_purge<C: CatalogStore>(
    catalog_state: C::State,
    task: &DatasetSnapshotPurgeTask,
) {
    match purge::<C>(task, catalog_state.clone()).await {
        Ok(purge) => {
            tracing::info!(
                "Task of `{QN_STR}` worker exited successfully. Purged {} snapshots.",
                purge.purged
            );
        }
        Err(err) => {
            tracing::error!("Error in `{QN_STR}` worker. Failed to purge dataset snapshots. {err}");
            let detail = format!("Failed to purge dataset snapshots.\nError: {}", err.error);
            task.record_failure::<C>(catalog_state, &detail).await;
        }
    }
}

async fn purge<C: CatalogStore>(
    task: &DatasetSnapshotPurgeTask,
    catalog_state: C::State,
) -> Result<DatasetPurge> {
    let entity = task.task_metadata.entity.clone();
    let TaskEntity::EntityInWarehouse {
        warehouse_id,
        entity_id,
        entity_name: _,
    } = &entity
    else {
        return Err(ErrorModel::internal(
            format!("Unexpected task scope for `{QN_STR}` task. Task must have a dataset scope."),
            "UnexpectedTaskScopeForDatasetSnapshotPurge",
            None,
        )
        .into());
    };
    let dataset_id = DatasetId::from(entity_id.as_uuid());

    // An import staging into the dataset holds the purge; one that died left its
    // staging snapshot behind, swept first as an import sweeps before it begins.
    let mut t = C::Transaction::begin_write(catalog_state.clone()).await?;
    C::expire_staging_snapshots(
        *warehouse_id,
        Some(dataset_id),
        Utc::now() - ABANDONED_STAGING_AFTER,
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    let mut t = C::Transaction::begin_write(catalog_state).await?;
    let purge =
        C::purge_expired_dataset_snapshots(*warehouse_id, dataset_id, t.transaction()).await?;
    task.record_success_in_transaction::<C>(
        t.transaction(),
        Some(&format!("Purged {} snapshots", purge.purged)),
    )
    .await;
    if let Some(next) = purge.next_purge_after {
        DatasetSnapshotPurgeTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id: task.task_metadata.project_id.clone(),
                parent_task_id: Some(task.task_id()),
                scheduled_for: Some(next.max(Utc::now() + PURGE_INTERVAL)),
                entity,
            },
            DatasetSnapshotPurgePayload::default(),
            t.transaction(),
        )
        .await?;
    }
    t.commit().await?;
    Ok(purge)
}
