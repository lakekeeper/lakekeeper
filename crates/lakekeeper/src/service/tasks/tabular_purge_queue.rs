use std::{str::FromStr, sync::LazyLock, time::Duration};

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::Location;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

use super::{SpecializedTask, TaskConfig, TaskData, TaskExecutionDetails};
use crate::{
    api::Result,
    server::{io::remove_all, maybe_get_secret},
    service::{tasks::TaskQueueName, CatalogStore, CatalogWarehouseOps, SecretStore},
};

const QN_STR: &str = "tabular_purge";
pub(crate) static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());
#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<super::QueueApiConfig> =
    LazyLock::new(|| super::QueueApiConfig {
        queue_name: &QUEUE_NAME,
        utoipa_type_name: PurgeQueueConfig::name(),
        utoipa_schema: PurgeQueueConfig::schema(),
    });

pub type TabularPurgeTask =
    SpecializedTask<PurgeQueueConfig, TabularPurgePayload, TabularPurgeExecutionDetails>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularPurgePayload {
    pub(crate) tabular_location: String,
}

impl TabularPurgePayload {
    pub fn new(tabular_location: impl Into<String>) -> Self {
        Self {
            tabular_location: tabular_location.into(),
        }
    }
}

impl TaskData for TabularPurgePayload {}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct PurgeQueueConfig {}

impl TaskConfig for PurgeQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(3600)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TabularPurgeExecutionDetails {}

impl TaskExecutionDetails for TabularPurgeExecutionDetails {}

pub(crate) async fn tabular_purge_worker<C: CatalogStore, S: SecretStore>(
    catalog_state: C::State,
    secret_state: S,
    poll_interval: Duration,
    cancellation_token: crate::CancellationToken,
) {
    loop {
        let task = TabularPurgeTask::poll_for_new_task::<C>(
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
            location = %task.data.tabular_location,
            warehouse_id = %task.task_metadata.warehouse_id,
            entity_type = %task.task_metadata.entity_id.entity_type().to_string(),
            attempt = %task.attempt(),
            task_id = %task.task_id(),
        );

        instrumented_purge::<_, C>(catalog_state.clone(), &secret_state, &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_purge<S: SecretStore, C: CatalogStore>(
    catalog_state: C::State,
    secret_state: &S,
    task: &TabularPurgeTask,
) {
    match purge::<C, S>(task, secret_state, catalog_state.clone()).await {
        Ok(()) => {
            tracing::info!(
                "Task of `{QN_STR}` worker exited successfully. Data at location `{}` deleted.",
                task.data.tabular_location
            );
            task.record_success::<C>(catalog_state, Some("Purged tabular data"))
                .await;
        }
        Err(err) => {
            tracing::error!(
                "Error in `{QN_STR}` worker. Failed to purge location {}. {err}",
                task.data.tabular_location,
            );
            task.record_failure::<C>(
                catalog_state,
                &format!(
                    "Failed to purge tabular at location `{}`.\n{err}",
                    task.data.tabular_location
                ),
            )
            .await;
        }
    };
}

async fn purge<C, S>(
    task: &TabularPurgeTask,
    secret_state: &S,
    catalog_state: C::State,
) -> Result<()>
where
    C: CatalogStore,
    S: SecretStore,
{
    let tabular_location_str = &task.data.tabular_location;
    let warehouse_id = task.task_metadata.warehouse_id;
    let warehouse = C::require_warehouse_by_id(warehouse_id, catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to get warehouse {warehouse_id} for Tabular Purge task."
            ))
        })?;

    let tabular_location = Location::from_str(tabular_location_str).map_err(|e| {
        ErrorModel::internal(
            format!("Failed to parse table location `{tabular_location_str}` to purge table data."),
            "ParseError",
            Some(Box::new(e)),
        )
    })?;

    let secret = maybe_get_secret(warehouse.storage_secret_id, secret_state)
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to get storage secret for warehouse {warehouse_id} for Tabular Purge task."
            ))
        })?;

    let file_io = warehouse
        .storage_profile
        .file_io(secret.as_ref())
        .await
        .map_err(|e| {
            IcebergErrorResponse::from(e).append_detail(format!(
                "Failed to initialize IO for warehouse {warehouse_id} for Tabular Purge task."
            ))
        })?;

    remove_all(&file_io, &tabular_location).await.map_err(|e| {
        IcebergErrorResponse::from(ErrorModel::internal(
            "Failed to remove location.",
            "FileIOError",
            Some(Box::new(e)),
        ))
        .append_detail(format!(
            "Failed to remove location `{tabular_location}` for Tabular Purge task."
        ))
    })?;

    Ok(())
}
