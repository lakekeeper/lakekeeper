use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::{cancel_pending_tasks, queue_task_batch, InputTrait};
use crate::{
    api::management::v1::TabularType,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::TabularType as DbTabularType,
        task_queues::{pick_task, record_failure, record_success},
        DeletionKind,
    },
    service::task_queue::{
        tabular_expiration_queue::{TabularExpirationInput, TabularExpirationTask},
        TaskFilter, TaskQueue, TaskQueueConfig,
    },
    WarehouseIdent,
};

super::impl_pg_task_queue!(TabularExpirationQueue);

impl InputTrait for TabularExpirationInput {
    fn warehouse_ident(&self) -> WarehouseIdent {
        self.warehouse_ident
    }

    fn suspend_until(&self) -> Option<DateTime<Utc>> {
        Some(self.expire_at)
    }
}

#[async_trait]
impl TaskQueue for TabularExpirationQueue {
    type Task = TabularExpirationTask;
    type Input = TabularExpirationInput;

    fn config(&self) -> &TaskQueueConfig {
        &self.pg_queue.config
    }

    fn queue_name(&self) -> &'static str {
        "tabular_expiration"
    }

    #[tracing::instrument(skip(self, tasks))]
    async fn enqueue_batch(&self, tasks: Vec<Self::Input>) -> crate::api::Result<()> {
        let (idempotency2task, mut idempotency2specific) = super::preprocess_batch(tasks, |t| {
            Uuid::new_v5(&t.warehouse_ident, t.tabular_id.as_bytes())
        })?;

        let mut transaction = self
            .pg_queue
            .read_write
            .write_pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("failed to begin transaction for expiration queue"))?;

        tracing::debug!("Queuing '{}' expirations", idempotency2task.len());
        let queued =
            queue_task_batch(&mut transaction, self.queue_name(), &idempotency2task).await?;
        let mut task_ids = Vec::with_capacity(queued.len());
        let mut tabular_ids = Vec::with_capacity(queued.len());
        let mut warehouse_idents = Vec::with_capacity(queued.len());
        let mut tabular_types: Vec<DbTabularType> = Vec::with_capacity(queued.len());
        let mut deletion_kinds = Vec::with_capacity(queued.len());

        for q in queued {
            if let Some(TabularExpirationInput {
                tabular_id,
                warehouse_ident,
                tabular_type,
                purge,
                expire_at: _,
            }) = idempotency2specific.remove(&q.idempotency_key)
            {
                task_ids.push(q.task_id);
                tabular_ids.push(tabular_id);
                warehouse_idents.push(*warehouse_ident);
                tabular_types.push(match tabular_type {
                    TabularType::Table => DbTabularType::Table,
                    TabularType::View => DbTabularType::View,
                });
                deletion_kinds.push(if purge {
                    DeletionKind::Purge
                } else {
                    DeletionKind::Default
                });
            }
            tracing::debug!("Queued expiration task: {:?}", q.task_id);
        }

        sqlx::query!(
            r#"WITH input_rows AS (
            SELECT
                unnest($1::uuid[]) as task_id,
                unnest($2::uuid[]) as tabular_id,
                unnest($3::uuid[]) as warehouse_id,
                unnest($4::tabular_type[]) as tabular_type,
                unnest($5::deletion_kind[]) as deletion_kind
        )
        INSERT INTO tabular_expirations(task_id, tabular_id, warehouse_id, typ, deletion_kind)
        SELECT
            i.task_id,
            i.tabular_id,
            i.warehouse_id,
            i.tabular_type,
            i.deletion_kind
        FROM input_rows i
        ON CONFLICT (task_id)
        DO UPDATE SET deletion_kind = EXCLUDED.deletion_kind
        RETURNING task_id"#,
            &task_ids,
            &tabular_ids,
            &warehouse_idents,
            &tabular_types as _,
            &deletion_kinds as _
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|e| e.into_error_model("failed queueing tasks"))?;

        transaction.commit().await.map_err(|e| {
            tracing::error!(?e, "failed to commit");
            e.into_error_model("failed to commit transaction inserting tabular expiration task")
        })?;
        Ok(())
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

        let expiration = sqlx::query!(
            r#"
            SELECT tabular_id, warehouse_id, typ as "tabular_type: DbTabularType", deletion_kind as "deletion_kind: DeletionKind"
            FROM tabular_expirations
            WHERE task_id = $1
            "#,
            task.task_id
        )
            .fetch_one(&self.pg_queue.read_write.read_pool)
            .await
            .map_err(|e| {
                tracing::error!(?e, "error selecting tabular expiration");
                // TODO: should we reset task status here?
                e.into_error_model("failed to read task after picking one up")
            })?;

        tracing::info!("Expiration task: {:?}", expiration);
        Ok(Some(TabularExpirationTask {
            deletion_kind: expiration.deletion_kind.into(),
            tabular_id: expiration.tabular_id,
            warehouse_ident: expiration.warehouse_id.into(),
            tabular_type: expiration.tabular_type.into(),
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
}

#[cfg(test)]
mod test {
    use sqlx::PgPool;

    use super::super::test::setup;
    use crate::{
        implementations::postgres::task_queues::TabularExpirationQueue,
        service::task_queue::{
            tabular_expiration_queue::TabularExpirationInput, TaskFilter, TaskQueue,
            TaskQueueConfig,
        },
    };

    #[sqlx::test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let config = TaskQueueConfig::default();
        let (pg_queue, warehouse_ident) = setup(pool, config).await;
        let queue = super::TabularExpirationQueue { pg_queue };
        let input = TabularExpirationInput {
            tabular_id: uuid::Uuid::new_v4(),
            warehouse_ident,
            tabular_type: crate::api::management::v1::TabularType::Table,
            purge: false,
            expire_at: chrono::Utc::now(),
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
        assert_eq!(
            task.deletion_kind,
            crate::implementations::postgres::DeletionKind::Default.into()
        );

        let task = queue.pick_new_task().await.unwrap();
        assert!(
            task.is_none(),
            "There should only be one task, idempotency didn't work."
        );
    }

    #[sqlx::test]
    async fn test_cancel_pending_tasks(pool: PgPool) {
        let config = TaskQueueConfig::default();
        let (pg_queue, warehouse_ident) = setup(pool.clone(), config.clone()).await;
        let queue = TabularExpirationQueue { pg_queue };

        let input = TabularExpirationInput {
            tabular_id: uuid::Uuid::new_v4(),
            warehouse_ident,
            tabular_type: crate::api::management::v1::TabularType::Table,
            purge: false,
            expire_at: chrono::Utc::now(),
        };
        queue.enqueue(input.clone()).await.unwrap();

        queue
            .cancel_pending_tasks(TaskFilter::WarehouseId(warehouse_ident))
            .await
            .unwrap();

        let task = queue.pick_new_task().await.unwrap();
        assert!(task.is_none(), "There should be no tasks");
    }
}
