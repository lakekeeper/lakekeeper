use serde::{Deserialize, Serialize};
use tracing::Instrument;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{EntityId, QueueConfig, TaskMetadata, DEFAULT_MAX_AGE};
use crate::{
    api::{
        management::v1::{DeleteKind, TabularType},
        Result,
    },
    service::{
        authz::Authorizer,
        task_queue::{
            tabular_purge_queue::{PurgeQueueConfig, TabularPurge},
            Task,
        },
        Catalog, TableId, Transaction, ViewId,
    },
};

pub const QUEUE_NAME: &str = "tabular_expiration";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TabularExpiration {
    pub tabular_type: TabularType,
    pub deletion_kind: DeleteKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct ExpirationQueueConfig {}

impl QueueConfig for ExpirationQueueConfig {}

pub async fn tabular_expiration_task<C: Catalog, A: Authorizer>(
    catalog_state: C::State,
    authorizer: A,
    poll_interval: std::time::Duration,
) {
    loop {
        let expiration =
            match C::pick_new_task(QUEUE_NAME, DEFAULT_MAX_AGE, catalog_state.clone()).await {
                Ok(expiration) => expiration,
                Err(err) => {
                    // TODO: add retry counter + exponential backoff
                    tracing::error!("Failed to fetch expiration: {:?}", err);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
        let Some(expiration) = expiration else {
            tokio::time::sleep(poll_interval).await;
            continue;
        };
        let state = match expiration.task_state::<TabularExpiration>() {
            Ok(state) => state,
            Err(err) => {
                tracing::error!("Failed to deserialize task state: {:?}", err);
                // TODO: record fatal error
                continue;
            }
        };
        let config = match expiration.task_config::<ExpirationQueueConfig>() {
            Ok(config) => config,
            Err(err) => {
                tracing::error!("Failed to deserialize task config: {:?}", err);
                continue;
            }
        }
        .unwrap_or_default();

        let EntityId::Tabular(tabular_id) = expiration.task_metadata.entity_id;

        let span = tracing::debug_span!(
            QUEUE_NAME,
            tabular_id = %tabular_id,
            warehouse_id = %expiration.task_metadata.warehouse_id,
            tabular_type = %state.tabular_type,
            deletion_kind = ?state.deletion_kind,
            task = ?expiration,
        );

        instrumented_expire::<C, A>(
            catalog_state.clone(),
            authorizer.clone(),
            tabular_id,
            &state,
            &expiration,
        )
        .instrument(span.or_current())
        .await;
    }
}

async fn instrumented_expire<C: Catalog, A: Authorizer>(
    catalog_state: C::State,
    authorizer: A,
    tabular_id: Uuid,
    expiration: &TabularExpiration,
    task: &Task,
) {
    match handle_table::<C, A>(
        catalog_state.clone(),
        authorizer,
        tabular_id,
        expiration,
        task,
    )
    .await
    {
        Ok(()) => {
            tracing::debug!("Successful {expiration:?}");
        }
        Err(err) => {
            tracing::error!("Failed to handle {expiration:?}: {err:?}");
            super::record_error_with_catalog::<C>(
                catalog_state.clone(),
                &format!("Failed to expire tabular: '{:?}'", err.error),
                5,
                task.task_id,
            )
            .await;
        }
    };
}

async fn handle_table<C, A>(
    catalog_state: C::State,
    authorizer: A,
    tabular_id: Uuid,
    expiration: &TabularExpiration,
    task: &Task,
) -> Result<()>
where
    C: Catalog,
    A: Authorizer,
{
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to start transaction: {:?}", e);
            e
        })?;

    let tabular_location = match expiration.tabular_type {
        TabularType::Table => {
            let table_id = TableId::from(tabular_id);
            let location = C::drop_table(table_id, true, trx.transaction())
                .await
                .map_err(|e| {
                    tracing::error!(?e, "Failed to drop table: {}", e.error);
                    e.error
                })?;

            authorizer
                .delete_table(table_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, "Failed to delete table from authorizer: {}", e.error);
                })
                .ok();
            location
        }
        TabularType::View => {
            let view_id = ViewId::from(tabular_id);
            let location = C::drop_view(view_id, true, trx.transaction())
                .await
                .map_err(|e| {
                    tracing::error!(?e, "Failed to drop view: {}", e.error);
                    e
                })?;
            authorizer
                .delete_view(view_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, "Failed to delete view from authorizer: {}", e.error);
                })
                .ok();
            location
        }
    };

    if matches!(expiration.deletion_kind, DeleteKind::Purge) {
        C::queue_tabular_purge(
            TaskMetadata {
                entity_id: task.task_metadata.entity_id,
                warehouse_id: task.task_metadata.warehouse_id,
                parent_task_id: Some(task.task_id),
                schedule_for: None,
            },
            TabularPurge {
                tabular_type: expiration.tabular_type,
                tabular_location,
            },
            trx.transaction(),
        )
        .await?;
    }
    C::retrying_record_task_success(task.task_id, None, trx.transaction()).await;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
        e
    })?;

    Ok(())
}
