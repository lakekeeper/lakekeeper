use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use iceberg_catalog::{
    api::{CommitTransactionRequest, RequestMetadata},
    service::{
        endpoint_hooks::EndpointHooks,
        task_queue::{EntityId, QueueConfig, TaskInput, TaskMetadata},
        Catalog, TableId, TableIdent, Transaction,
    },
    WarehouseId,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const QUEUE_NAME: &'static str = "test_queue";

pub struct TestHook<C: Catalog> {
    pub state: C::State,
}

impl<C: Catalog> Display for TestHook<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestHook")
    }
}

impl<C: Catalog> Debug for TestHook<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestHook")
            .field("state", &"CatalogState")
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct ExternalQueueConfig {
    pub max_filesize: usize,
}

impl QueueConfig for ExternalQueueConfig {}

impl Default for ExternalQueueConfig {
    fn default() -> Self {
        Self {
            max_filesize: 1024 * 1024 * 1024, // 1GB
        }
    }
}

#[async_trait::async_trait]
impl<C: Catalog> EndpointHooks for TestHook<C> {
    #[tracing::instrument(skip_all)]
    async fn commit_transaction(
        &self,
        warehouse_id: WarehouseId,
        _request: Arc<CommitTransactionRequest>,
        commits: Arc<Vec<iceberg_catalog::catalog::tables::CommitContext>>,
        _table_ident_map: Arc<HashMap<TableIdent, TableId>>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        let mut trx = C::Transaction::begin_write(self.state.clone())
            .await
            .unwrap();
        let c = commits
            .iter()
            .map(|c| TaskInput {
                task_metadata: TaskMetadata {
                    warehouse_id,
                    entity_id: EntityId::Tabular(c.new_metadata.uuid()),
                    parent_task_id: None,
                    schedule_for: None,
                },
                payload: serde_json::json!({"table_id": c.new_metadata.uuid()}),
            })
            .collect();
        let _ = C::enqueue_task_batch(QUEUE_NAME, c, trx.transaction())
            .await
            .map_err(|e| {
                tracing::error!("Failed to enqueue task: {}", e.error);
                e.error
            })?;
        let _ = trx.commit().await.map_err(|e| {
            tracing::error!("Failed to commit transaction: {}", e.error);
            e.error
        })?;
        Ok(())
    }
}
