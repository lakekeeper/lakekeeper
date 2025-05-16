use std::sync::Arc;

use iceberg_ext::{
    catalog::rest::ErrorModel,
    configs::{Location, ParseFromStr},
};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

use super::TaskMetadata;
use crate::{
    api::{management::v1::TabularType, Result},
    catalog::{io::remove_all, maybe_get_secret},
    service::{
        task_queue::{Task, TaskQueue},
        Catalog, SecretStore, Transaction,
    },
    WarehouseId,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularPurge {
    pub tabular_location: String,
    pub tabular_type: TabularType,
}

// TODO: concurrent workers
pub async fn purge_task<C: Catalog, S: SecretStore>(
    fetcher: async_channel::Receiver<Task>,
    queues: Arc<dyn TaskQueue + Send + Sync + 'static>,
    catalog_state: C::State,
    secret_state: S,
) {
    while let Ok(task) = fetcher.recv().await {
        let state = match task.task_state::<TabularPurge>() {
            Ok(state) => state,
            Err(err) => {
                tracing::error!("Failed to deserialize task state: {:?}", err);
                // TODO: record fatal error
                continue;
            }
        };
        let span = tracing::debug_span!(
            "tabular_purge",
            location = %state.tabular_location,
            warehouse_id = %task.task_metadata.warehouse_id,
            tabular_type = %state.tabular_type,
            queue_name = %task.queue_name,
            task = ?task,
        );

        instrumented_purge::<_, C>(
            queues.clone(),
            catalog_state.clone(),
            &secret_state,
            &state,
            &task,
        )
        .instrument(span.or_current())
        .await;
    }
}

async fn instrumented_purge<S: SecretStore, C: Catalog>(
    queues: Arc<dyn TaskQueue + Send + Sync + 'static>,
    catalog_state: C::State,
    secret_state: &S,
    purge_task: &TabularPurge,
    task: &Task,
) {
    match purge::<C, S>(
        purge_task,
        &task.task_metadata,
        secret_state,
        catalog_state.clone(),
    )
    .await
    {
        Ok(()) => {
            queues.retrying_record_success(task).await;
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
            queues
                .retrying_record_failure(task, &err.error.to_string())
                .await;
        }
    };
}

async fn purge<C, S>(
    TabularPurge {
        tabular_location,
        tabular_type: _,
    }: &TabularPurge,
    TaskMetadata {
        idempotency_key: _,
        tabular_id: _,
        warehouse_id,
        parent_task_id: _,
        suspend_until: _,
    }: &TaskMetadata,
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

#[derive(Debug)]
pub struct TabularPurgeTask {
    pub tabular_id: Uuid,
    pub tabular_location: String,
    pub warehouse_ident: WarehouseId,
    pub tabular_type: TabularType,
    pub task: Task,
}

#[derive(Debug, Clone)]
pub struct TabularPurgeInput {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseId,
    pub tabular_type: TabularType,
    pub parent_id: Option<Uuid>,
    pub tabular_location: String,
}
