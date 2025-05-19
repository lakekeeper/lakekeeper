use async_trait::async_trait;
use uuid::Uuid;

use super::{cancel_pending_tasks, queue_task_batch, InputTrait, TaskFilter};
use crate::{
    api::management::v1::TabularType,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::TabularType as DbTabularType,
        task_queues::{pick_task, record_failure, record_success},
    },
    service::task_queue::{
        tabular_purge_queue::{TabularPurgeInput, TabularPurgeTask},
        TaskQueue, TaskQueueConfig,
    },
    WarehouseId,
};

super::impl_pg_task_queue!(TabularPurgeQueue);

impl InputTrait for TabularPurgeInput {
    fn warehouse_ident(&self) -> WarehouseId {
        self.warehouse_ident
    }
}

#[async_trait]
impl TaskQueue for TabularPurgeQueue {
    type Task = TabularPurgeTask;
    type Input = TabularPurgeInput;

    fn config(&self) -> &TaskQueueConfig {
        &self.pg_queue.config
    }

    fn queue_name(&self) -> &'static str {
        "tabular_purges"
    }

    #[tracing::instrument(skip(self))]
    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>> {
        let task = pick_task(
            &self.pg_queue.read_write.write_pool,
            self.queue_name(),
            &self.pg_queue.max_age,
        )
        .await?;

        let Some(task) = task else {
            tracing::debug!("No task found in {}", self.queue_name());
            return Ok(None);
        };

        let purge = sqlx::query!(
            r#"
            SELECT tabular_id, tabular_location, warehouse_id, typ as "tabular_type: DbTabularType"
            FROM tabular_purges
            WHERE task_id = $1
            "#,
            task.task_id
        )
        .fetch_one(&self.pg_queue.read_write.read_pool)
        .await
        .map_err(|e| {
            tracing::error!(?e, "error selecting tabular expiration");
            e.into_error_model("failed to read task after picking one up")
        })?;

        Ok(Some(TabularPurgeTask {
            tabular_id: purge.tabular_id,
            tabular_location: purge.tabular_location,
            warehouse_ident: purge.warehouse_id.into(),
            tabular_type: purge.tabular_type.into(),
            task,
        }))
    }

    async fn record_success(&self, id: Uuid) -> crate::api::Result<()> {
        record_success(id, &self.pg_queue.read_write.write_pool).await
    }

    async fn record_failure(&self, id: Uuid, error_details: &str) -> crate::api::Result<()> {
        record_failure(
            &self.pg_queue.read_write.write_pool,
            id,
            self.config().max_retries,
            error_details,
        )
        .await
    }

    async fn cancel_pending_tasks(&self, filter: TaskFilter) -> crate::api::Result<()> {
        cancel_pending_tasks(&self.pg_queue, filter, self.queue_name()).await
    }

    #[tracing::instrument(skip(self, task))]
    async fn enqueue_batch(&self, task: Vec<Self::Input>) -> crate::api::Result<()> {
        let (idempotency2task, mut idempotency2specific) = super::preprocess_batch(task, |t| {
            Uuid::new_v5(&t.warehouse_ident, t.tabular_id.as_bytes())
        })?;

        let mut transaction = self
            .pg_queue
            .read_write
            .write_pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("failed begin transaction to purge task"))?;
        tracing::debug!("Queuing '{}' purges", idempotency2task.len());
        let queued =
            queue_task_batch(&mut transaction, self.queue_name(), &idempotency2task).await?;

        let mut task_ids = Vec::with_capacity(queued.len());
        let mut tabular_ids = Vec::with_capacity(queued.len());
        let mut warehouse_idents = Vec::with_capacity(queued.len());
        let mut tabular_types: Vec<DbTabularType> = Vec::with_capacity(queued.len());
        let mut tabular_locations = Vec::with_capacity(queued.len());

        for q in queued {
            if let Some(TabularPurgeInput {
                tabular_id,
                warehouse_ident,
                tabular_type,
                tabular_location,
                parent_id: _,
            }) = idempotency2specific.remove(&q.idempotency_key)
            {
                task_ids.push(q.task_id);
                tabular_ids.push(tabular_id);
                warehouse_idents.push(*warehouse_ident);
                tabular_types.push(match tabular_type {
                    TabularType::Table => DbTabularType::Table,
                    TabularType::View => DbTabularType::View,
                });
                tabular_locations.push(tabular_location);
            }
            tracing::debug!("Queued purge task: {:?}", q.task_id);
        }

        sqlx::query!(
            r#"WITH input_rows AS (
    SELECT unnest($1::uuid[]) as task_ids,
           unnest($2::uuid[]) as tabular_ids,
           unnest($3::uuid[]) as warehouse_idents,
           unnest($4::tabular_type[]) as tabular_types,
           unnest($5::text[]) as tabular_locations
    )
    INSERT INTO tabular_purges(task_id, tabular_id, warehouse_id, typ, tabular_location)
SELECT i.task_ids,
       i.tabular_ids,
       i.warehouse_idents,
       i.tabular_types,
       i.tabular_locations
       FROM input_rows i
ON CONFLICT (task_id) DO UPDATE SET tabular_location = EXCLUDED.tabular_location
RETURNING task_id"#,
            &task_ids,
            &tabular_ids,
            &warehouse_idents,
            &tabular_types as _,
            &tabular_locations,
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|e| {
            tracing::error!("failed to queue tasks: {e:?}");
            e.into_error_model("failed queueing tasks")
        })?;
        transaction.commit().await.map_err(|e| {
            tracing::error!("failed to commit transaction: {e:?}");
            e.into_error_model("failed to commit transaction")
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use sqlx::PgPool;

    use super::super::test::setup;
    use crate::service::task_queue::{
        tabular_purge_queue::TabularPurgeInput, TaskQueue, TaskQueueConfig,
    };

    #[sqlx::test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let config = TaskQueueConfig::default();
        let (pg_queue, warehouse_ident) = setup(pool, config).await;
        let queue = super::TabularPurgeQueue { pg_queue };
        let input = TabularPurgeInput {
            tabular_id: uuid::Uuid::new_v4(),
            warehouse_ident,
            tabular_type: crate::api::management::v1::TabularType::Table,
            parent_id: None,
            tabular_location: String::new(),
        };
        queue.enqueue(input.clone()).await.unwrap();
        queue.enqueue(input.clone()).await.unwrap();

        let task = queue
            .pick_new_task()
            .await
            .unwrap()
            .expect("There should be a task");

        assert_eq!(task.warehouse_ident, input.warehouse_ident);
        assert_eq!(task.tabular_id, input.tabular_id);
        assert_eq!(task.tabular_type, input.tabular_type);
        assert_eq!(task.tabular_location, input.tabular_location);

        let task = queue.pick_new_task().await.unwrap();
        assert!(
            task.is_none(),
            "There should only be one task, idempotency didn't work."
        );
    }
}
