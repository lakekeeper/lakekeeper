use async_trait::async_trait;
use uuid::Uuid;

use super::{cancel_pending_tasks, TaskFilter};
use crate::{
    api::management::v1::TabularType,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::TabularType as DbTabularType,
        task_queues::{pick_task, queue_task, record_failure, record_success},
    },
    service::task_queue::{
        tabular_purge_queue::{TabularPurgeInput, TabularPurgeTask},
        TaskQueue, TaskQueueConfig,
    },
};

super::impl_pg_task_queue!(TabularPurgeQueue);

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

    #[tracing::instrument(skip(self))]
    async fn enqueue(
        &self,
        TabularPurgeInput {
            tabular_location,
            tabular_id,
            warehouse_ident,
            tabular_type,
            parent_id,
        }: TabularPurgeInput,
    ) -> crate::api::Result<()> {
        let mut transaction = self
            .pg_queue
            .read_write
            .write_pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("failed begin transaction to purge task"))?;

        tracing::info!(
            "Queuing expiration for '{tabular_id}' of type: '{}' under warehouse: '{warehouse_ident}'",
            tabular_type.to_string(),
        );

        let idempotency_key = Uuid::new_v5(&warehouse_ident, tabular_id.as_bytes());

        let Some(task_id) = queue_task(
            &mut transaction,
            self.queue_name(),
            parent_id,
            idempotency_key,
            warehouse_ident,
            None,
        )
        .await?
        else {
            tracing::debug!("Task already exists");
            transaction.commit().await.map_err(|e| {
                tracing::error!(?e, "failed to commit");
                e.into_error_model("failed commiting transaction")
            })?;
            return Ok(());
        };

        let it = sqlx::query!(
            r#"INSERT INTO tabular_purges(task_id, tabular_id, warehouse_id, typ, tabular_location)
               VALUES ($1, $2, $3, $4, $5)
               -- we update tabular_location since it may have changed from the last time we enqueued
               ON CONFLICT (task_id) DO UPDATE SET tabular_location = $5
               RETURNING task_id"#,
            task_id,
            tabular_id,
            *warehouse_ident,
            match tabular_type {
                TabularType::Table => DbTabularType::Table,
                TabularType::View => DbTabularType::View,
            } as _,
            tabular_location,
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|e| {
            tracing::error!(?e, "failed to insert into tabular_purges");
            e.into_error_model("failed to insert into tabular purges")
        })?;

        if let Some(row) = it {
            tracing::info!("Queued purge task: {:?}", row);
        } else {
            tracing::info!("Purge task already exists.");
        }

        transaction.commit().await.map_err(|e| {
            tracing::error!(?e, "failed to commit");
            e.into_error_model("failed to commit tabular purge task")
        })?;

        Ok(())
    }

    async fn cancel_pending_tasks(&self, filter: TaskFilter) -> crate::api::Result<()> {
        cancel_pending_tasks(&self.pg_queue, filter, self.queue_name()).await
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
        let pg_queue = setup(pool, config);
        let queue = super::TabularPurgeQueue { pg_queue };
        let input = TabularPurgeInput {
            tabular_id: uuid::Uuid::new_v4(),
            warehouse_ident: uuid::Uuid::new_v4().into(),
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
