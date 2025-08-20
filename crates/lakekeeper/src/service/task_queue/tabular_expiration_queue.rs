use std::{sync::LazyLock, time::Duration};

use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;

use super::{
    EntityId, QueueApiConfig, QueueConfig, TaskMetadata, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
};
use crate::{
    api::{
        management::v1::{DeleteKind, TabularType},
        Result,
    },
    service::{
        authz::Authorizer,
        task_queue::{tabular_purge_queue::TabularPurgePayload, Task},
        Catalog, TableId, Transaction, ViewId,
    },
};

pub(crate) const QUEUE_NAME: &str = "tabular_expiration";
pub(crate) static API_CONFIG: LazyLock<QueueApiConfig> = LazyLock::new(|| QueueApiConfig {
    queue_name: QUEUE_NAME,
    utoipa_type_name: ExpirationQueueConfig::name(),
    utoipa_schema: ExpirationQueueConfig::schema(),
});

#[derive(Debug, Clone, Deserialize, Serialize)]
/// State stored for a tabular expiration in postgres as `payload` along with the task metadata.
pub(crate) struct TabularExpirationPayload {
    pub(crate) tabular_type: TabularType,
    pub(crate) deletion_kind: DeleteKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
/// Warehouse-specific configuration for the expiration queue.
pub(crate) struct ExpirationQueueConfig {}

impl QueueConfig for ExpirationQueueConfig {}

pub(crate) async fn tabular_expiration_worker<C: Catalog, A: Authorizer>(
    catalog_state: C::State,
    authorizer: A,
    poll_interval: std::time::Duration,
) {
    loop {
        let expiration = match C::pick_new_task(
            QUEUE_NAME,
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
            catalog_state.clone(),
        )
        .await
        {
            Ok(expiration) => expiration,
            Err(err) => {
                tracing::error!("Failed to fetch `{QUEUE_NAME}` task. {err}");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        let Some(expiration) = expiration else {
            let jitter = { rand::rng().next_u64() % 500 };
            tokio::time::sleep(poll_interval + Duration::from_millis(jitter)).await;
            continue;
        };
        let state = match expiration.task_state::<TabularExpirationPayload>() {
            Ok(state) => state,
            Err(err) => {
                tracing::error!("Failed to deserialize `{QUEUE_NAME}` task state. {err}");
                continue;
            }
        };
        let _config = match expiration.task_config::<ExpirationQueueConfig>() {
            Ok(config) => config,
            Err(err) => {
                tracing::error!("Failed to deserialize `{QUEUE_NAME}` task config. {err}");
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
    expiration: &TabularExpirationPayload,
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
            tracing::debug!("Successfully expired  {expiration:?}");
        }
        Err(err) => {
            tracing::error!("Failed to handle {expiration:?}: {err:?}");
            super::record_error_with_catalog::<C>(
                catalog_state.clone(),
                &format!("Failed to expire tabular. {}", err.error),
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
    expiration: &TabularExpirationPayload,
    task: &Task,
) -> Result<()>
where
    C: Catalog,
    A: Authorizer,
{
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to start transaction for `{QUEUE_NAME}` Queue. {e}",);
            e
        })?;

    let tabular_location = match expiration.tabular_type {
        TabularType::Table => {
            let table_id = TableId::from(tabular_id);
            let location = C::drop_table(table_id, true, trx.transaction())
                .await
                .map_err(|e| {
                    tracing::error!("Failed to drop table in `{QUEUE_NAME}` task. {}", e.error);
                    e.error
                })?;

            authorizer
                .delete_table(table_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to delete table from authorizer in `{QUEUE_NAME}` task. {}",
                        e.error
                    );
                })
                .ok();
            location
        }
        TabularType::View => {
            let view_id = ViewId::from(tabular_id);
            let location = C::drop_view(view_id, true, trx.transaction())
                .await
                .inspect_err(|e| {
                    tracing::error!("Failed to drop view in `{QUEUE_NAME}` task. {}", e.error);
                })?;
            authorizer
                .delete_view(view_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to delete view from authorizer in `{QUEUE_NAME}` task. {}",
                        e.error
                    );
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
            TabularPurgePayload {
                tabular_type: expiration.tabular_type,
                tabular_location,
            },
            trx.transaction(),
        )
        .await?;
    }
    C::retrying_record_task_success(task.task_id, None, trx.transaction()).await;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction in `{QUEUE_NAME}` task. {e}");
        e
    })?;

    Ok(())
}
