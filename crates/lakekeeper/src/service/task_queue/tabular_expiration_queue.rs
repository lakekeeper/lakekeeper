use std::{sync::LazyLock, time::Duration};

use iceberg::ErrorKind;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;

use super::{EntityId, QueueApiConfig, TaskConfig, TaskExecutionDetails, TaskMetadata};
use crate::{
    api::{
        management::v1::{DeleteKind, TabularType},
        Result,
    },
    service::{
        authz::Authorizer,
        task_queue::{
            tabular_purge_queue::TabularPurgePayload, SpecializedTask, TaskData, TaskQueueName,
        },
        Catalog, TableId, Transaction, ViewId,
    },
    CancellationToken,
};

const QN_STR: &str = "tabular_expiration";
pub(crate) static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());
pub(crate) static API_CONFIG: LazyLock<QueueApiConfig> = LazyLock::new(|| QueueApiConfig {
    queue_name: &QUEUE_NAME,
    utoipa_type_name: TabularExpirationQueueConfig::name(),
    utoipa_schema: TabularExpirationQueueConfig::schema(),
});

pub type TabularExpirationTask = SpecializedTask<
    TabularExpirationQueueConfig,
    TabularExpirationPayload,
    TabularExpirationExecutionDetails,
>;

#[derive(Debug, Clone, Deserialize, Serialize)]
/// State stored for a tabular expiration in postgres as `payload` along with the task metadata.
pub struct TabularExpirationPayload {
    pub(crate) tabular_type: TabularType,
    pub(crate) deletion_kind: DeleteKind,
}

impl TabularExpirationPayload {
    #[must_use]
    pub fn new(tabular_type: TabularType, deletion_kind: DeleteKind) -> Self {
        Self {
            tabular_type,
            deletion_kind,
        }
    }
}

impl TaskData for TabularExpirationPayload {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TabularExpirationExecutionDetails {}

impl TaskExecutionDetails for TabularExpirationExecutionDetails {}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
/// Warehouse-specific configuration for the tabular expiration (Soft-Deletion) queue.
pub struct TabularExpirationQueueConfig {}

impl TaskConfig for TabularExpirationQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(120)
    }
}

pub(crate) async fn tabular_expiration_worker<C: Catalog, A: Authorizer>(
    catalog_state: C::State,
    authorizer: A,
    poll_interval: Duration,
    cancellation_token: CancellationToken,
) {
    loop {
        let task = TabularExpirationTask::poll_for_new_task::<C>(
            catalog_state.clone(),
            &poll_interval,
            cancellation_token.clone(),
        )
        .await;

        let Some(task) = task else {
            tracing::info!("Graceful shutdown: exiting tabular expiration worker");
            return;
        };

        let EntityId::Tabular(tabular_id) = task.task_metadata.entity_id;

        let span = tracing::debug_span!(
            QN_STR,
            tabular_id = %tabular_id,
            warehouse_id = %task.task_metadata.warehouse_id,
            tabular_type = %task.data.tabular_type,
            deletion_kind = ?task.data.deletion_kind,
            attempt = %task.attempt(),
            task_id = %task.task_id(),
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
    task: &TabularExpirationTask,
) {
    match handle_table::<C, A>(catalog_state.clone(), authorizer, tabular_id, task).await {
        Ok(()) => {
            tracing::debug!(
                "Task of `{QN_STR}` worker exited successfully. {} with id {tabular_id} deleted.",
                task.data.tabular_type
            );
        }
        Err(err) => {
            tracing::error!(
                "Error in `{QN_STR}` worker. Expiration of {} with id {tabular_id} failed. Error: {err}",
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
    task: &TabularExpirationTask,
) -> Result<()>
where
    C: Catalog,
    A: Authorizer,
{
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!("Failed to start transaction for `{QN_STR}` Queue.",))
        })?;

    let tabular_location = match task.data.tabular_type {
        TabularType::Table => {
            let table_id = TableId::from(tabular_id);
            let drop_result = C::drop_table(table_id, true, trx.transaction()).await;

            let location = match drop_result {
                Err(e) if e.error.r#type == ErrorKind::TableNotFound.to_string() => {
                    tracing::warn!(
                        "Table with id `{table_id}` not found in catalog for `{QN_STR}` task. Skipping deletion."
                    );
                    None
                }
                Err(e) => {
                    return Err(e.append_detail(format!(
                    "Failed to drop table with id `{table_id}` from catalog for `{QN_STR}` task."
                )))
                }
                Ok(loc) => Some(loc),
            };

            authorizer
                .delete_table(table_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to delete table from authorizer in `{QN_STR}` task. {e}"
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
                        "View with id `{view_id}` not found in catalog for `{QN_STR}` task. Skipping deletion."
                    );
                    None
                }
                Err(e) => {
                    return Err(e.append_detail(format!(
                        "Failed to drop view with id `{view_id}` from catalog for `{QN_STR}` task."
                    )))
                }
                Ok(loc) => Some(loc),
            };

            authorizer
                .delete_view(view_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to delete view from authorizer in `{QN_STR}` task. {e}"
                    );
                })
                .ok();
            location
        }
    };

    if let Some(tabular_location) = tabular_location {
        if matches!(task.data.deletion_kind, DeleteKind::Purge) {
            super::tabular_purge_queue::TabularPurgeTask::schedule_task::<C>(
                TaskMetadata {
                    entity_id: task.task_metadata.entity_id,
                    warehouse_id: task.task_metadata.warehouse_id,
                    parent_task_id: Some(task.task_id()),
                    schedule_for: None,
                },
                TabularPurgePayload::new(tabular_location, task.data.tabular_type),
                trx.transaction(),
            )
            .await
            .map_err(|e| {
                e.append_detail(format!(
                    "Failed to queue purge after `{QN_STR}` task with id `{}`.",
                    task.id
                ))
            })?;
        }
    }

    // Record success within the transaction - will be rolled back if commit fails
    task.record_success_in_transaction::<C>(trx.transaction(), None)
        .await;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction in `{QN_STR}` task. {e}");
        e
    })?;

    Ok(())
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use sqlx::PgPool;
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        api::{
            iceberg::v1::PaginationQuery,
            management::v1::{DeleteKind, TabularType},
        },
        implementations::postgres::{
            tabular::table::tests::initialize_table, warehouse::test::initialize_warehouse,
            CatalogState, PostgresCatalog, PostgresTransaction, SecretsState,
        },
        service::{
            authz::AllowAllAuthorizer, storage::MemoryProfile, Catalog, ListFlags, Transaction,
        },
    };

    #[sqlx::test]
    #[traced_test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let queues = crate::service::task_queue::TaskQueueRegistry::new();

        let secrets =
            crate::implementations::postgres::SecretsState::from_pools(pool.clone(), pool);
        let cat = catalog_state.clone();
        let sec = secrets.clone();
        let auth = AllowAllAuthorizer::default();
        queues
            .register_built_in_queues::<PostgresCatalog, SecretsState, AllowAllAuthorizer>(
                cat,
                sec,
                auth,
                Duration::from_millis(100),
            )
            .await;
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let runner = queues.task_queues_runner(cancellation_token.clone()).await;
        let _queue_task = tokio::task::spawn(runner.run_queue_workers(true));

        let warehouse = initialize_warehouse(
            catalog_state.clone(),
            Some(MemoryProfile::default().into()),
            None,
            None,
            true,
        )
        .await;

        let tab = initialize_table(
            warehouse,
            catalog_state.clone(),
            false,
            None,
            Some("tab".to_string()),
        )
        .await;
        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();
        let _ = <PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: true,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .unwrap();
        trx.commit().await.unwrap();
        let mut trx = <PostgresCatalog as Catalog>::Transaction::begin_write(catalog_state.clone())
            .await
            .unwrap();
        TabularExpirationTask::schedule_task::<PostgresCatalog>(
            TaskMetadata {
                warehouse_id: warehouse,
                entity_id: EntityId::Tabular(tab.table_id.0),
                parent_task_id: None,
                schedule_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
            },
            TabularExpirationPayload {
                tabular_type: TabularType::Table,
                deletion_kind: DeleteKind::Purge,
            },
            trx.transaction(),
        )
        .await
        .unwrap();

        <PostgresCatalog as Catalog>::mark_tabular_as_deleted(
            tab.table_id.into(),
            false,
            trx.transaction(),
        )
        .await
        .unwrap();

        trx.commit().await.unwrap();

        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();

        let del = <PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .unwrap()
        .deletion_details;
        del.unwrap();
        trx.commit().await.unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
                .await
                .unwrap();
            let gone = <PostgresCatalog as Catalog>::list_tabulars(
                warehouse,
                None,
                ListFlags {
                    include_active: false,
                    include_staged: false,
                    include_deleted: true,
                },
                trx.transaction(),
                PaginationQuery::empty(),
            )
            .await
            .unwrap()
            .remove(&tab.table_id.into())
            .is_none();
            trx.commit().await.unwrap();
            if gone || std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();

        assert!(<PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .is_none());
        trx.commit().await.unwrap();

        cancellation_token.cancel();
    }
}
