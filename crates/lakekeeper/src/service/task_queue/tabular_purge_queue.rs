use std::{str::FromStr, sync::LazyLock, time::Duration};

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::Location;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use utoipa::{PartialSchema, ToSchema};

use super::{QueueApiConfig, QueueConfig};
use crate::{
    api::{management::v1::TabularType, Result},
    catalog::{io::remove_all, maybe_get_secret},
    service::{
        task_queue::{SpecializedTask, TaskData},
        Catalog, SecretStore, Transaction,
    },
};

pub(crate) const QUEUE_NAME: &str = "tabular_purge";
pub(crate) static API_CONFIG: LazyLock<QueueApiConfig> = LazyLock::new(|| QueueApiConfig {
    queue_name: QUEUE_NAME,
    utoipa_type_name: PurgeQueueConfig::name(),
    utoipa_schema: PurgeQueueConfig::schema(),
});

pub type TabularPurgeTask = SpecializedTask<PurgeQueueConfig, TabularPurgePayload>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularPurgePayload {
    pub(crate) tabular_location: String,
    pub(crate) tabular_type: TabularType,
}

impl TaskData for TabularPurgePayload {}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct PurgeQueueConfig {}

impl QueueConfig for PurgeQueueConfig {
    fn queue_name() -> &'static str {
        QUEUE_NAME
    }
}

pub(crate) async fn tabular_purge_worker<C: Catalog, S: SecretStore>(
    catalog_state: C::State,
    secret_state: S,
    poll_interval: Duration,
    cancellation_token: crate::CancellationToken,
) {
    loop {
        let task =
            SpecializedTask::<PurgeQueueConfig, TabularPurgePayload>::poll_for_new_task::<C>(
                catalog_state.clone(),
                &poll_interval,
                cancellation_token.clone(),
            )
            .await;

        let Some(task) = task else {
            tracing::info!("Graceful shutdown: exiting `{QUEUE_NAME}` worker");
            return;
        };

        let span = tracing::debug_span!(
            QUEUE_NAME,
            location = %task.data.tabular_location,
            warehouse_id = %task.task_metadata.warehouse_id,
            tabular_type = %task.data.tabular_type,
            attempt = %task.attempt,
            task_id = %task.task_id,
        );

        instrumented_purge::<_, C>(catalog_state.clone(), &secret_state, &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_purge<S: SecretStore, C: Catalog>(
    catalog_state: C::State,
    secret_state: &S,
    task: &TabularPurgeTask,
) {
    match purge::<C, S>(task, secret_state, catalog_state.clone()).await {
        Ok(()) => {
            tracing::info!(
                "Task of `{QUEUE_NAME}` worker exited successfully. Data at location `{}` deleted.",
                task.data.tabular_location
            );
            task.record_success::<C>(catalog_state, Some("Purged tabular data"))
                .await;
        }
        Err(err) => {
            tracing::error!(
                "Error in `{QUEUE_NAME}` worker. Failed to purge location {}. {err}",
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
    C: Catalog,
    S: SecretStore,
{
    let tabular_location_str = &task.data.tabular_location;
    let warehouse_id = task.task_metadata.warehouse_id;
    let mut trx = C::Transaction::begin_read(catalog_state.clone())
        .await
        .map_err(|e| e.append_detail("Failed to start DB transaction for Tabular Purge Queue"))?;

    let warehouse = C::require_warehouse(warehouse_id, trx.transaction())
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to get warehouse {warehouse_id} for Tabular Purge task."
            ))
        })?;

    if let Err(e) = trx.commit().await {
        tracing::warn!("Failed to commit read transaction for `{QUEUE_NAME}` before IO. {e}");
    }

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
