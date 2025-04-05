use std::{sync::Arc, time::Duration};

use tracing::Instrument;
use uuid::Uuid;

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
        Catalog, TableIdentUuid, Transaction, ViewIdentUuid,
    },
    WarehouseIdent,
};

pub type ExpirationQueue = Arc<
    dyn TaskQueue<Task = TabularExpirationTask, Input = TabularExpirationInput>
        + Send
        + Sync
        + 'static,
>;

// TODO: concurrent workers
pub async fn tabular_expiration_task<C: Catalog, A: Authorizer>(
    fetcher: ExpirationQueue,
    cleaner: TabularPurgeQueue,
    catalog_state: C::State,
    authorizer: A,
) {
    loop {
        tokio::time::sleep(fetcher.config().poll_interval).await;

        let expiration = match fetcher.pick_new_task().await {
            Ok(expiration) => expiration,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch deletion: {:?}", err);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(expiration) = expiration else {
            continue;
        };

        let span = tracing::debug_span!(
            "tabular_expiration",
            queue_name = %expiration.task.queue_name,
            tabular_id = %expiration.tabular_id,
            warehouse_id = %expiration.warehouse_ident,
            tabular_type = %expiration.tabular_type,
            deletion_kind = ?expiration.deletion_kind,
            task = ?expiration.task,
        );

        instrumented_expire::<C, A>(
            fetcher.clone(),
            &cleaner,
            catalog_state.clone(),
            authorizer.clone(),
            &expiration,
        )
        .instrument(span.or_current())
        .await;
    }
}

async fn instrumented_expire<C: Catalog, A: Authorizer>(
    fetcher: ExpirationQueue,
    cleaner: &TabularPurgeQueue,
    catalog_state: C::State,
    authorizer: A,
    expiration: &TabularExpirationTask,
) {
    match handle_table::<C, A>(catalog_state.clone(), authorizer, cleaner, expiration).await {
        Ok(()) => {
            fetcher.retrying_record_success(&expiration.task).await;
            tracing::info!("Successfully handled table expiration");
        }
        Err(e) => {
            tracing::error!("Failed to handle table expiration: {:?}", e);
            fetcher
                .retrying_record_failure(&expiration.task, &format!("{e:?}"))
                .await;
        }
    };
}

async fn handle_table<C, A>(
    catalog_state: C::State,
    authorizer: A,
    delete_queue: &TabularPurgeQueue,
    expiration: &TabularExpirationTask,
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
            let table_id = TableIdentUuid::from(expiration.tabular_id);
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
            let view_id = ViewIdentUuid::from(expiration.tabular_id);
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
        delete_queue
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
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub task: Task,
}

#[derive(Debug, Clone)]
pub struct TabularExpirationInput {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub purge: bool,
    pub expire_at: chrono::DateTime<chrono::Utc>,
}
