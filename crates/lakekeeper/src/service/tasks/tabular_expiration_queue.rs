use std::{sync::LazyLock, time::Duration};

use iceberg::ErrorKind;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use utoipa::{PartialSchema, ToSchema};

use super::{EntityId, QueueApiConfig, TaskConfig, TaskExecutionDetails, TaskMetadata};
use crate::{
    api::{management::v1::DeleteKind, Result},
    service::{
        authz::Authorizer,
        tasks::{
            tabular_purge_queue::TabularPurgePayload, SpecializedTask, TaskData, TaskQueueName,
        },
        CatalogStore, Transaction,
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
    pub(crate) deletion_kind: DeleteKind,
}

impl TabularExpirationPayload {
    #[must_use]
    pub fn new(deletion_kind: DeleteKind) -> Self {
        Self { deletion_kind }
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

pub(crate) async fn tabular_expiration_worker<C: CatalogStore, A: Authorizer>(
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

        let entity_id = task.task_metadata.entity_id;
        let entity_id_uuid = entity_id.as_uuid();

        let span = tracing::debug_span!(
            QN_STR,
            warehouse_id = %task.task_metadata.warehouse_id,
            entity_type = %entity_id.entity_type().to_string(),
            entity_id = %entity_id_uuid,
            deletion_kind = ?task.data.deletion_kind,
            attempt = %task.attempt(),
            task_id = %task.task_id(),
        );

        instrumented_expire::<C, A>(catalog_state.clone(), authorizer.clone(), &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_expire<C: CatalogStore, A: Authorizer>(
    catalog_state: C::State,
    authorizer: A,
    task: &TabularExpirationTask,
) {
    let entity_id = task.task_metadata.entity_id;
    match handle_table::<C, A>(catalog_state.clone(), authorizer, task).await {
        Ok(()) => {
            tracing::debug!("Task of `{QN_STR}` worker exited successfully. {entity_id} deleted.");
        }
        Err(err) => {
            tracing::error!(
                "Error in `{QN_STR}` worker. Expiration of {entity_id} failed. Error: {err}"
            );
            task.record_failure::<C>(
                catalog_state,
                &format!("Failed to expire soft-deleted {entity_id}.\n{err}"),
            )
            .await;
        }
    };
}

#[allow(clippy::too_many_lines)]
async fn handle_table<C, A>(
    catalog_state: C::State,
    authorizer: A,
    task: &TabularExpirationTask,
) -> Result<()>
where
    C: CatalogStore,
    A: Authorizer,
{
    let entity_id = task.task_metadata.entity_id;
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            e.append_detail(format!("Failed to start transaction for `{QN_STR}` Queue.",))
        })?;

    let tabular_location = match entity_id {
        EntityId::Table(table_id) => {
            let drop_result = C::drop_table(
                task.task_metadata.warehouse_id,
                table_id,
                true,
                trx.transaction(),
            )
            .await;

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
                .delete_table(task.task_metadata.warehouse_id, table_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to delete table from authorizer in `{QN_STR}` task. {e}"
                    );
                })
                .ok();
            location
        }
        EntityId::View(view_id) => {
            let location = match C::drop_view(
                task.task_metadata.warehouse_id,
                view_id,
                true,
                trx.transaction(),
            )
            .await
            {
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
                .delete_view(task.task_metadata.warehouse_id, view_id)
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
                    entity_name: task.task_metadata.entity_name.clone(),
                },
                TabularPurgePayload::new(tabular_location),
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
        api::{iceberg::v1::PaginationQuery, management::v1::DeleteKind},
        implementations::postgres::{
            tabular::table::tests::initialize_table, warehouse::test::initialize_warehouse,
            CatalogState, PostgresBackend, PostgresTransaction, SecretsState,
        },
        service::{
            authz::AllowAllAuthorizer, storage::MemoryProfile, CatalogStore, NamedEntity,
            TabularListFlags, Transaction,
        },
    };

    #[sqlx::test]
    #[traced_test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let queues = crate::service::tasks::TaskQueueRegistry::new();

        let secrets =
            crate::implementations::postgres::SecretsState::from_pools(pool.clone(), pool);
        let cat = catalog_state.clone();
        let sec = secrets.clone();
        let auth = AllowAllAuthorizer::default();
        queues
            .register_built_in_queues::<PostgresBackend, SecretsState, AllowAllAuthorizer>(
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

        let table = initialize_table(
            warehouse,
            catalog_state.clone(),
            false,
            None,
            None,
            Some("tab".to_string()),
        )
        .await;
        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();
        let _ = <PostgresBackend as CatalogStore>::list_tabulars(
            warehouse,
            None,
            TabularListFlags {
                include_active: true,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&table.table_id.into())
        .unwrap();
        trx.commit().await.unwrap();
        let mut trx =
            <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog_state.clone())
                .await
                .unwrap();
        TabularExpirationTask::schedule_task::<PostgresBackend>(
            TaskMetadata {
                warehouse_id: warehouse,
                entity_id: EntityId::Table(table.table_id),
                parent_task_id: None,
                schedule_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
                entity_name: table.table_ident.into_name_parts(),
            },
            TabularExpirationPayload {
                deletion_kind: DeleteKind::Purge,
            },
            trx.transaction(),
        )
        .await
        .unwrap();

        <PostgresBackend as CatalogStore>::mark_tabular_as_deleted(
            warehouse,
            table.table_id.into(),
            false,
            trx.transaction(),
        )
        .await
        .unwrap();

        trx.commit().await.unwrap();

        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();

        let del = <PostgresBackend as CatalogStore>::list_tabulars(
            warehouse,
            None,
            TabularListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&table.table_id.into())
        .unwrap()
        .deletion_details;
        del.unwrap();
        trx.commit().await.unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
                .await
                .unwrap();
            let gone = <PostgresBackend as CatalogStore>::list_tabulars(
                warehouse,
                None,
                TabularListFlags {
                    include_active: false,
                    include_staged: false,
                    include_deleted: true,
                },
                trx.transaction(),
                PaginationQuery::empty(),
            )
            .await
            .unwrap()
            .remove(&table.table_id.into())
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

        assert!(<PostgresBackend as CatalogStore>::list_tabulars(
            warehouse,
            None,
            TabularListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&table.table_id.into())
        .is_none());
        trx.commit().await.unwrap();

        cancellation_token.cancel();
    }
}
