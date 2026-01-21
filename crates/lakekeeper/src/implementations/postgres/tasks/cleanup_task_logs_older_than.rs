use chrono::Duration;
use sqlx::{PgConnection, query};

use crate::{ProjectId, api::Result, implementations::postgres::dbutils::DBErrorHandler as _};

pub(crate) async fn cleanup_task_logs_older_than(
    transaction: &mut PgConnection,
    retention_period: Duration,
    project_id: &ProjectId,
) -> Result<()> {
    let retention_date = chrono::Utc::now() - retention_period;
    query!(
        r#"
        DELETE FROM task_log
        WHERE created_at < $1
        AND project_id = $2
        "#,
        retention_date,
        project_id.as_str(),
    )
    .execute(transaction)
    .await
    .map_err(|e| e.into_error_model("Failed to delete old task logs."))?;

    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        WarehouseId,
        api::management::v1::{tasks::ListTasksRequest, warehouse::TabularDeleteProfile},
        implementations::{
            CatalogState,
            postgres::{
                PostgresBackend,
                warehouse::{create_project, create_warehouse},
            },
        },
        service::{
            CatalogTaskOps,
            storage::{MemoryProfile, StorageProfile},
            tasks::{
                ScheduleTaskMetadata, SpecializedTask, TaskConfig, TaskData, TaskEntity,
                TaskExecutionDetails, TaskFilter, TaskQueueName,
            },
        },
    };

    use super::*;

    const QN_STR: &str = "dummy";
    static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());

    type DummyTask = SpecializedTask<DummyConfig, DummyPayload, DummyExecutionDetails>;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DummyPayload {}

    impl TaskData for DummyPayload {}

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    struct DummyConfig {}

    impl TaskConfig for DummyConfig {
        fn queue_name() -> &'static TaskQueueName {
            &QUEUE_NAME
        }

        fn max_time_since_last_heartbeat() -> chrono::Duration {
            chrono::Duration::seconds(3600)
        }
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct DummyExecutionDetails {}

    impl TaskExecutionDetails for DummyExecutionDetails {}

    async fn get_remaining_task_log_ids(pool: PgPool, project_ids: impl IntoIterator<Item = &ProjectId>) -> Vec<Uuid> {
        let mut tx = pool.begin().await.unwrap();

        let filters: Vec<TaskFilter> = project_ids
            .into_iter()
            .map(|project_id| TaskFilter::ProjectId {
                project_id: project_id.clone(),
                include_sub_tasks: true,
            })
            .collect();
        let mut task_ids: Vec<Uuid> = Vec::new();
        for filter in filters {
            let tasks: Vec<Uuid> =
                PostgresBackend::list_tasks(&filter, ListTasksRequest::default(), &mut tx)
                    .await
                    .unwrap()
                    .tasks
                    .into_iter()
                    .map(|task| task.task_id().into())
                    .collect();
            task_ids.extend(tasks);
        }

        tx.commit().await.unwrap();

        task_ids
    }

    async fn setup_project(pool: PgPool) -> ProjectId {
        let mut tx = pool.begin().await.unwrap();

        let project_id = ProjectId::new_random();
        create_project(&project_id, "My Project".to_string(), &mut tx)
            .await
            .unwrap();

        tx.commit().await.unwrap();

        project_id
    }

    async fn setup_warehouse(pool: PgPool, project_id: &ProjectId) -> WarehouseId {
        let mut tx = pool.begin().await.unwrap();

        let storage_profile = StorageProfile::Memory(MemoryProfile::default());
        let tabular_delete_profile = TabularDeleteProfile::default();
        let warehouse = create_warehouse(
            "My Warehouse".to_string(),
            project_id,
            storage_profile,
            tabular_delete_profile,
            None,
            &mut tx,
        )
        .await
        .unwrap();

        tx.commit().await.unwrap();

        warehouse.warehouse_id
    }

    async fn schedule_and_finish_task_at(
        pool: PgPool,
        project_id: &ProjectId,
        entity: TaskEntity,
        created_at: DateTime<Utc>,
    ) -> Uuid {
        let mut tx = pool.begin().await.unwrap();

        let task_metadata = ScheduleTaskMetadata {
            project_id: project_id.clone(),
            parent_task_id: None,
            scheduled_for: Some(created_at),
            entity,
        };
        let task_id =
            *DummyTask::schedule_task::<PostgresBackend>(task_metadata, DummyPayload {}, &mut tx)
                .await
                .unwrap()
                .unwrap();
        tx.commit().await.unwrap();

        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
        let task = DummyTask::pick_new_task::<PostgresBackend>(catalog_state.clone())
            .await
            .unwrap()
            .unwrap();

        task.record_success::<PostgresBackend>(catalog_state, None)
            .await;

        let mut conn = pool.acquire().await.unwrap();

        query(
            r#"
            UPDATE task_log
            SET created_at = $2
            WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .bind(created_at)
        .execute(&mut *conn)
        .await
        .unwrap();

        task_id
    }

    #[sqlx::test]
    async fn test_cleanup_task_logs_older_than_removes_project_task_logs_older_than_retention_time(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();

        let project_id = setup_project(pool.clone()).await;

        let retention_period = Duration::days(90);

        let old_created_at = Utc::now() - retention_period - Duration::days(10);
        let new_created_at = Utc::now() - retention_period + Duration::days(10);

        let new_task_id = schedule_and_finish_task_at(
            pool.clone(),
            &project_id,
            TaskEntity::Project,
            new_created_at,
        )
        .await;
        schedule_and_finish_task_at(
            pool.clone(),
            &project_id,
            TaskEntity::Project,
            old_created_at,
        )
        .await;

        cleanup_task_logs_older_than(&mut conn, retention_period, &project_id)
            .await
            .unwrap();

        let remaining_task_logs = get_remaining_task_log_ids(pool, [&project_id]).await;

        assert_eq!(remaining_task_logs.len(), 1);
        assert_eq!(new_task_id, remaining_task_logs[0]);
    }

    #[sqlx::test]
    async fn test_cleanup_task_logs_older_than_removes_warehouse_task_logs_older_than_retention_time(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();

        let project_id = setup_project(pool.clone()).await;
        let warehouse_id = setup_warehouse(pool.clone(), &project_id).await;

        let retention_period = Duration::days(90);

        let old_created_at = Utc::now() - retention_period - Duration::days(10);
        let new_created_at = Utc::now() - retention_period + Duration::days(10);

        let new_task_id = schedule_and_finish_task_at(
            pool.clone(),
            &project_id,
            TaskEntity::Warehouse { warehouse_id },
            new_created_at,
        )
        .await;
        schedule_and_finish_task_at(
            pool.clone(),
            &project_id,
            TaskEntity::Warehouse { warehouse_id },
            old_created_at,
        )
        .await;

        cleanup_task_logs_older_than(&mut conn, retention_period, &project_id)
            .await
            .unwrap();

        let remaining_task_logs = get_remaining_task_log_ids(pool, [&project_id]).await;

        assert_eq!(remaining_task_logs.len(), 1);
        assert_eq!(new_task_id, remaining_task_logs[0]);
    }

    #[sqlx::test]
    async fn test_cleanup_task_logs_older_than_removes_ignores_task_logs_of_other_projects(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();

        let project_id_a = setup_project(pool.clone()).await;
        let project_id_b = setup_project(pool.clone()).await;

        let retention_period = Duration::days(90);

        let created_at = Utc::now() - retention_period - Duration::days(10);

        schedule_and_finish_task_at(pool.clone(), &project_id_a, TaskEntity::Project, created_at)
            .await;
        let task_id_project_b = schedule_and_finish_task_at(
            pool.clone(),
            &project_id_b,
            TaskEntity::Project,
            created_at,
        )
        .await;

        cleanup_task_logs_older_than(&mut conn, retention_period, &project_id_a)
            .await
            .unwrap();

        let remaining_task_logs =
            get_remaining_task_log_ids(pool, [&project_id_a, &project_id_b]).await;

        assert_eq!(remaining_task_logs.len(), 1);
        assert_eq!(task_id_project_b, remaining_task_logs[0]);
    }
}
