use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

use super::{TaskInput, TaskMetadata};
use crate::{
    api::{
        management::v1::{DeleteKind, TabularType},
        Result,
    },
    service::{
        authz::Authorizer,
        task_queue::{tabular_purge_queue::TabularPurge, Task, TaskQueue},
        Catalog, TableId, Transaction, ViewId,
    },
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TabularExpiration {
    pub tabular_id: Uuid,
    pub tabular_type: TabularType,
    pub deletion_kind: DeleteKind,
}

// TODO: concurrent workers
pub async fn tabular_expiration_task<C: Catalog, A: Authorizer>(
    fetcher: async_channel::Receiver<Task>,
    queues: Arc<dyn TaskQueue + Send + Sync + 'static>,
    catalog_state: C::State,
    authorizer: A,
) {
    while let Ok(expiration) = fetcher.recv().await {
        let state = match expiration.task_state::<TabularExpiration>() {
            Ok(state) => state,
            Err(err) => {
                tracing::error!("Failed to deserialize task state: {:?}", err);
                // TODO: record fatal error
                continue;
            }
        };
        let span = tracing::debug_span!(
            "tabular_expiration",
            queue_name = %expiration.queue_name,
            tabular_id = %state.tabular_id,
            warehouse_id = %expiration.task_metadata.warehouse_id,
            tabular_type = %state.tabular_type,
            deletion_kind = ?state.deletion_kind,
            task = ?expiration,
        );

        instrumented_expire::<C, A>(
            queues.clone(),
            catalog_state.clone(),
            authorizer.clone(),
            &state,
            &expiration,
        )
        .instrument(span.or_current())
        .await;
    }
}

async fn instrumented_expire<C: Catalog, A: Authorizer>(
    queue: Arc<dyn TaskQueue + Send + Sync + 'static>,
    catalog_state: C::State,
    authorizer: A,
    expiration: &TabularExpiration,
    task: &Task,
) {
    match handle_table::<C, A>(
        catalog_state.clone(),
        authorizer,
        queue.clone(),
        expiration,
        task,
    )
    .await
    {
        Ok(()) => {
            queue.retrying_record_success(task).await;
            tracing::debug!("Successful {expiration:?}");
        }
        Err(e) => {
            tracing::error!("Failed to handle {expiration:?}: {e:?}");
            queue.retrying_record_failure(task, &format!("{e:?}")).await;
        }
    };
}

async fn handle_table<C, A>(
    catalog_state: C::State,
    authorizer: A,
    queue: Arc<dyn TaskQueue + Send + Sync + 'static>,
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
            let table_id = TableId::from(expiration.tabular_id);
            let location = C::drop_table(table_id, true, trx.transaction())
                .await
                .map_err(|e| {
                    tracing::error!("Failed to drop table: {:?}", e);
                    e
                })?;

            authorizer.delete_table(table_id).await?;
            location
        }
        TabularType::View => {
            let view_id = ViewId::from(expiration.tabular_id);
            let location = C::drop_view(view_id, true, trx.transaction())
                .await
                .map_err(|e| {
                    tracing::error!("Failed to drop table: {:?}", e);
                    e
                })?;
            authorizer.delete_view(view_id).await?;
            location
        }
    };

    if matches!(expiration.deletion_kind, DeleteKind::Purge) {
        queue
            .enqueue(
                "tabular_purge",
                TaskInput {
                    task_metadata: TaskMetadata {
                        idempotency_key: expiration.tabular_id,
                        warehouse_id: task.task_metadata.warehouse_id,
                        parent_task_id: Some(task.task_id),
                        suspend_until: None,
                    },
                    payload: serde_json::to_value(TabularPurge {
                        tabular_id: expiration.tabular_id,
                        tabular_type: expiration.tabular_type,
                        tabular_location,
                    })
                    .map_err(|e| {
                        crate::api::ErrorModel::internal(
                            "Failed to serialize tabular purge task.",
                            "SerializationError",
                            Some(Box::new(e)),
                        )
                    })?,
                },
            )
            .await?;
    }

    // Here we commit after the queuing of the deletion since we're in a fault-tolerant workflow
    // which will restart if the commit fails.
    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
        e
    })?;

    Ok(())
}
