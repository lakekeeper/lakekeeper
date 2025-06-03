use std::sync::LazyLock;

use iceberg_ext::{
    catalog::rest::ErrorModel,
    configs::{Location, ParseFromStr},
};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use utoipa::{PartialSchema, ToSchema};

use super::{QueueApiConfig, QueueConfig, TaskMetadata, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT};
use crate::{
    api::{management::v1::TabularType, Result},
    catalog::{io::remove_all, maybe_get_secret},
    service::{task_queue::Task, Catalog, SecretStore, Transaction},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TabularPurgePayload {
    pub(crate) tabular_location: String,
    pub(crate) tabular_type: TabularType,
}

pub(crate) const QUEUE_NAME: &str = "tabular_purge";
pub(crate) static API_CONFIG: LazyLock<QueueApiConfig> = LazyLock::new(|| QueueApiConfig {
    queue_name: QUEUE_NAME,
    utoipa_type_name: PurgeQueueConfig::name(),
    utoipa_schema: PurgeQueueConfig::schema(),
});

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub(crate) struct PurgeQueueConfig {}

impl QueueConfig for PurgeQueueConfig {}

pub(crate) async fn tabular_purge_worker<C: Catalog, S: SecretStore>(
    catalog_state: C::State,
    secret_state: S,
    poll_interval: std::time::Duration,
) {
    loop {
        let task = match C::pick_new_task(
            QUEUE_NAME,
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
            catalog_state.clone(),
        )
        .await
        {
            Ok(expiration) => expiration,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch purge: {:?}", err);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(task) = task else {
            tokio::time::sleep(poll_interval).await;
            continue;
        };
        let state = match task.task_state::<TabularPurgePayload>() {
            Ok(state) => state,
            Err(err) => {
                tracing::error!("Failed to deserialize task state: {:?}", err);
                // TODO: record fatal error
                continue;
            }
        };
        let config = match task.task_config::<PurgeQueueConfig>() {
            Ok(config) => config,
            Err(err) => {
                tracing::error!("Failed to deserialize task config: {:?}", err);
                continue;
            }
        }
        .unwrap_or_default();

        let span = tracing::debug_span!(
            "tabular_purge",
            location = %state.tabular_location,
            warehouse_id = %task.task_metadata.warehouse_id,
            tabular_type = %state.tabular_type,
            queue_name = %task.queue_name,
            task = ?task,
        );

        instrumented_purge::<_, C>(catalog_state.clone(), &secret_state, &state, &task, &config)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_purge<S: SecretStore, C: Catalog>(
    catalog_state: C::State,
    secret_state: &S,
    purge_task: &TabularPurgePayload,
    task: &Task,
    config: &PurgeQueueConfig,
) {
    match purge::<C, S>(purge_task, task, secret_state, catalog_state.clone()).await {
        Ok(()) => {
            tracing::info!(
                "Successfully cleaned up tabular at location {}",
                purge_task.tabular_location
            );
        }
        Err(err) => {
            tracing::error!(
                "Failed to expire tabular at location {} due to: {}",
                purge_task.tabular_location,
                err.error
            );
            super::record_error_with_catalog::<C>(
                catalog_state.clone(),
                &format!("Failed to purge tabular: '{:?}'", err.error),
                config.max_retries(),
                task.task_id,
            )
            .await;
        }
    };
}

async fn purge<C, S>(
    TabularPurgePayload {
        tabular_location,
        tabular_type: _,
    }: &TabularPurgePayload,
    Task {
        task_metadata:
            TaskMetadata {
                entity_id: _,
                warehouse_id,
                parent_task_id: _,
                schedule_for: _,
            },
        queue_name: _,
        task_id,
        status: _,
        picked_up_at: _,
        attempt: _,
        config: _,
        state: _,
    }: &Task,
    secret_state: &S,
    catalog_state: C::State,
) -> Result<()>
where
    C: Catalog,
    S: SecretStore,
{
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to start transaction: {:?}", e);
            e
        })?;

    let warehouse = C::require_warehouse(*warehouse_id, trx.transaction())
        .await
        .map_err(|e| {
            tracing::error!("Failed to get warehouse: {:?}", e);
            e
        })?;
    C::retrying_record_task_success(*task_id, None, trx.transaction()).await;
    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
        e
    })?;

    let secret = maybe_get_secret(warehouse.storage_secret_id, secret_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get secret: {:?}", e);
            e
        })?;

    let file_io = warehouse
        .storage_profile
        .file_io(secret.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to get storage profile: {:?}", e);
            e
        })?;

    let tabular_location = Location::parse_value(tabular_location).map_err(|e| {
        tracing::error!(
            "Failed delete tabular - to parse location {}: {:?}",
            tabular_location,
            e
        );
        ErrorModel::internal(
            "Failed to parse table location of deleted tabular.",
            "ParseError",
            Some(Box::new(e)),
        )
    })?;
    remove_all(&file_io, &tabular_location).await.map_err(|e| {
        tracing::error!(
            ?e,
            "Failed to purge tabular at location: '{tabular_location}'",
        );
        ErrorModel::internal(
            "Failed to remove location.",
            "FileIOError",
            Some(Box::new(e)),
        )
    })?;

    Ok(())
}
