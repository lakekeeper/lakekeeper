use std::{sync::Arc, time::Duration};

use iceberg_ext::{
    catalog::rest::ErrorModel,
    configs::{Location, ParseFromStr},
};
use tracing::Instrument;
use uuid::Uuid;

use crate::{
    api::{management::v1::TabularType, Result},
    catalog::{io::remove_all, maybe_get_secret},
    service::{
        task_queue::{Task, TaskQueue},
        Catalog, SecretStore, Transaction,
    },
    WarehouseIdent,
};

pub type TabularPurgeQueue =
    Arc<dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput> + Send + Sync + 'static>;

// TODO: concurrent workers
pub async fn purge_task<C: Catalog, S: SecretStore>(
    fetcher: TabularPurgeQueue,
    catalog_state: C::State,
    secret_state: S,
) {
    loop {
        // add some jitter to avoid syncing with other queues
        // TODO: probably should have a random number here
        tokio::time::sleep(fetcher.config().poll_interval + Duration::from_millis(13)).await;

        let purge_task = match fetcher.pick_new_task().await {
            Ok(expiration) => expiration,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch deletion: {:?}", err);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(purge_task) = purge_task else {
            continue;
        };

        let span = tracing::debug_span!(
            "tabular_purge",
            tabular_id = %purge_task.tabular_id,
            location = %purge_task.tabular_location,
            warehouse_id = %purge_task.warehouse_ident,
            tabular_type = %purge_task.tabular_type,
            queue_name = %purge_task.task.queue_name,
            task = ?purge_task.task,
        );

        instrumented_purge::<_, C>(
            fetcher.clone(),
            catalog_state.clone(),
            &secret_state,
            &purge_task,
        )
        .instrument(span.or_current())
        .await;
    }
}

async fn instrumented_purge<S: SecretStore, C: Catalog>(
    fetcher: Arc<dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput> + Send + Sync>,
    catalog_state: C::State,
    secret_state: &S,
    purge_task: &TabularPurgeTask,
) {
    match purge::<C, S>(purge_task, secret_state, catalog_state.clone()).await {
        Ok(()) => {
            fetcher.retrying_record_success(&purge_task.task).await;
            tracing::info!(
                "Successfully cleaned up tabular {} at location {}",
                purge_task.tabular_id,
                purge_task.tabular_location
            );
        }
        Err(err) => {
            tracing::error!(
                "Failed to expire table {}: {}",
                purge_task.tabular_id,
                err.error
            );
            fetcher
                .retrying_record_failure(&purge_task.task, &err.error.to_string())
                .await;
        }
    };
}

async fn purge<C, S>(
    TabularPurgeTask {
        tabular_id,
        tabular_location,
        warehouse_ident,
        tabular_type: _,
        task: _,
    }: &TabularPurgeTask,
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

    let warehouse = C::require_warehouse(*warehouse_ident, trx.transaction())
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
            "Failed to purge '{tabular_id}' at location: '{tabular_location}'",
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
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub task: Task,
}

#[derive(Debug, Clone)]
pub struct TabularPurgeInput {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub parent_id: Option<Uuid>,
    pub tabular_location: String,
}
