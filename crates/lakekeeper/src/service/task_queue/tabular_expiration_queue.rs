use std::{sync::LazyLock, time::Duration};

use iceberg::ErrorKind;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;

use super::{EntityId, QueueApiConfig, QueueConfig, TaskMetadata};
use crate::{
    api::{
        management::v1::{DeleteKind, TabularType},
        Result,
    },
    service::{
        authz::Authorizer,
        task_queue::{tabular_purge_queue::TabularPurgePayload, SpecializedTask, TaskData},
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

impl TaskData for TabularExpirationPayload {}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
/// Warehouse-specific configuration for the expiration queue.
pub(crate) struct ExpirationQueueConfig {}

impl QueueConfig for ExpirationQueueConfig {
    fn queue_name() -> &'static str {
        QUEUE_NAME
    }
}

pub(crate) async fn tabular_expiration_worker<C: Catalog, A: Authorizer>(
    catalog_state: C::State,
    authorizer: A,
    poll_interval: &Duration,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    loop {
        let task = SpecializedTask::<ExpirationQueueConfig, TabularExpirationPayload>::poll_for_new_task::<C>(
            catalog_state.clone(),
            poll_interval,
            cancellation_token.clone(),
        )
        .await;

        let Some(task) = task else {
            tracing::info!("Graceful shutdown: exiting tabular expiration worker");
            return;
        };

        let EntityId::Tabular(tabular_id) = task.task_metadata.entity_id;

        let span = tracing::debug_span!(
            QUEUE_NAME,
            tabular_id = %tabular_id,
            warehouse_id = %task.task_metadata.warehouse_id,
            tabular_type = %task.data.tabular_type,
            deletion_kind = ?task.data.deletion_kind,
            attempt = %task.attempt,
            task_id = %task.task_id,
        );

        instrumented_expire::<C, A>(catalog_state.clone(), authorizer.clone(), tabular_id, &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_expire<C: Catalog, A: Authorizer>(
    catalog_state: C::State,
    authorizer: A,
    tabular_id: Uuid,
    task: &SpecializedTask<ExpirationQueueConfig, TabularExpirationPayload>,
) {
    match handle_table::<C, A>(catalog_state.clone(), authorizer, tabular_id, task).await {
        Ok(()) => {
            tracing::debug!(
                "Task of `{QUEUE_NAME}` worker exited successfully. {} with id {tabular_id} deleted.",
                task.data.tabular_type
            );
        }
        Err(err) => {
            tracing::error!(
                "Error in `{QUEUE_NAME}` worker. Expiration of {} with id {tabular_id} failed. Error: {err}",
                task.data.tabular_type
            );
            task.record_failure::<C>(
                catalog_state,
                &format!(
                    "Failed to expire soft-deleted {} with id `{tabular_id}`.\n{err}",
                    task.data.tabular_type
                ),
            )
            .await;
        }
    };
}

async fn handle_table<C, A>(
    catalog_state: C::State,
    authorizer: A,
    tabular_id: Uuid,
    task: &SpecializedTask<ExpirationQueueConfig, TabularExpirationPayload>,
) -> Result<()>
where
    C: Catalog,
    A: Authorizer,
{
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to start transaction for `{QUEUE_NAME}` Queue.",
            ))
        })?;

    let tabular_location = match task.data.tabular_type {
        TabularType::Table => {
            let table_id = TableId::from(tabular_id);
            let drop_result = C::drop_table(table_id, true, trx.transaction()).await;

            let location = match drop_result {
                Err(e) if e.error.r#type == ErrorKind::TableNotFound.to_string() => {
                    tracing::warn!(
                        "Table with id `{table_id}` not found in catalog for `{QUEUE_NAME}` task. Skipping deletion."
                    );
                    None
                }
                Err(e) => return Err(e.append_detail(format!(
                    "Failed to drop table with id `{table_id}` from catalog for `{QUEUE_NAME}` task."
                ))),
                Ok(loc) => Some(loc),
            };

            authorizer
                .delete_table(table_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to delete table from authorizer in `{QUEUE_NAME}` task. {e}"
                    );
                })
                .ok();
            location
        }
        TabularType::View => {
            let view_id = ViewId::from(tabular_id);

            let location = match C::drop_view(view_id, true, trx.transaction()).await {
                Err(e) if e.error.r#type == ErrorKind::TableNotFound.to_string() => {
                    tracing::warn!(
                        "View with id `{view_id}` not found in catalog for `{QUEUE_NAME}` task. Skipping deletion."
                    );
                    None
                }
                Err(e) => {
                    return Err(e.append_detail(format!(
                    "Failed to drop view with id `{view_id}` from catalog for `{QUEUE_NAME}` task."
                )))
                }
                Ok(loc) => Some(loc),
            };

            authorizer
                .delete_view(view_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to delete view from authorizer in `{QUEUE_NAME}` task. {e}"
                    );
                })
                .ok();
            location
        }
    };

    if let Some(tabular_location) = tabular_location {
        if matches!(task.data.deletion_kind, DeleteKind::Purge) {
            C::queue_tabular_purge(
                TaskMetadata {
                    entity_id: task.task_metadata.entity_id,
                    warehouse_id: task.task_metadata.warehouse_id,
                    parent_task_id: Some(task.task_id),
                    schedule_for: None,
                },
                TabularPurgePayload {
                    tabular_type: task.data.tabular_type,
                    tabular_location,
                },
                trx.transaction(),
            )
            .await
            .map_err(|e| {
                e.append_detail(format!(
                    "Failed to queue purge after `{QUEUE_NAME}` task with id `{}`.",
                    task.task_id
                ))
            })?;
        }
    }

    // Record success within the transaction - will be rolled back if commit fails
    task.record_success_in_transaction::<C>(trx.transaction(), None)
        .await;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction in `{QUEUE_NAME}` task. {e}");
        e
    })?;

    Ok(())
}
