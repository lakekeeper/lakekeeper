use std::{fmt, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

use super::{random_ms_duration, TaskQueues};
use crate::{
    api::{
        management::v1::{DeleteKind, TabularType},
        Result,
    },
    service::{
        authz::Authorizer,
        task_queue::{
            tabular_purge_queue::{TabularPurgeInput, TabularPurgeQueue},
            Task, TaskQueue,
        },
        Catalog, TableId, Transaction, ViewId,
    },
    WarehouseId,
};

pub type ExpirationQueue = Arc<
    dyn TaskQueue<Task = TabularExpirationTask, Input = TabularExpirationInput>
        + Send
        + Sync
        + 'static,
>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct State {
    pub tabular_id: Uuid,
    pub tabular_type: TabularType,
    pub deletion_kind: DeleteKind,
}

// TODO: concurrent workers
pub async fn tabular_expiration_task<C: Catalog, A: Authorizer>(
    mut fetcher: tokio::sync::mpsc::Receiver<Task>,
    queues: Arc<dyn TaskQueue>,
    catalog_state: C::State,
    authorizer: A,
) {
    while let Some(expiration) = fetcher.recv().await {
        let state = match expiration.task_state::<State>() {
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
            warehouse_id = %expiration.warehouse_id,
            tabular_type = %state.tabular_type,
            deletion_kind = ?state.deletion_kind,
            task = ?expiration,
        );

        instrumented_expire::<C, A>(
            todo!(),
            queues.clone(),
            catalog_state.clone(),
            authorizer.clone(),
            state,
        )
        .instrument(span.or_current())
        .await;
    }
}

async fn instrumented_expire<C: Catalog, A: Authorizer>(
    fetcher: ExpirationQueue,
    cleaner: Arc<dyn TaskQueue>,
    catalog_state: C::State,
    authorizer: A,
    expiration: &State,
    task: &Task,
) {
    match handle_table::<C, A>(catalog_state.clone(), authorizer, cleaner, expiration).await {
        Ok(()) => {
            fetcher.retrying_record_success(&task).await;
            tracing::debug!("Successful {expiration}");
        }
        Err(e) => {
            tracing::error!("Failed to handle {expiration}: {:?}", e);
            fetcher
                .retrying_record_failure(&task, &format!("{e:?}"))
                .await;
        }
    };
}

async fn handle_table<C, A>(
    catalog_state: C::State,
    authorizer: A,
    queue: Arc<dyn TaskQueue>,
    expiration: &State,
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
            .enqueue(TabularPurgeInput {
                tabular_id: expiration.tabular_id,
                warehouse_ident: expiration.warehouse_ident,
                tabular_type: expiration.tabular_type,
                parent_id: Some(expiration.task.task_id),
                tabular_location,
            })
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

#[derive(Debug)]
pub struct TabularExpirationTask {
    pub deletion_kind: DeleteKind,
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseId,
    pub tabular_type: TabularType,
    pub task: Task,
}

impl fmt::Display for TabularExpirationTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "expiration of {} {} in warehouse {} (task: {}, action: {})",
            self.tabular_type,
            self.tabular_id,
            self.warehouse_ident,
            self.task.task_id,
            self.deletion_kind
        )
    }
}

#[derive(Debug, Clone)]
pub struct TabularExpirationInput {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseId,
    pub tabular_type: TabularType,
    pub purge: bool,
    pub expire_at: chrono::DateTime<chrono::Utc>,
}
