use std::collections::HashMap;

use iceberg_ext::catalog::rest::IcebergErrorResponse;
use itertools::Itertools;
use sqlx::PgConnection;
use uuid::Uuid;

use super::EntityType;
use crate::{
    implementations::postgres::dbutils::DBErrorHandler,
    service::task_queue::{TaskEntity, TaskId, TaskQueueName},
    WarehouseId,
};

/// Resolve tasks among all known active and historical tasks.
/// Returns a map of `task_id` to (`TaskEntity`, `queue_name`).
/// Only includes task IDs that exist - missing task IDs are not included in the result.
pub(crate) async fn resolve_tasks(
    warehouse_id: Option<WarehouseId>,
    task_ids: &[TaskId],
    transaction: &mut PgConnection,
) -> Result<HashMap<TaskId, (TaskEntity, TaskQueueName)>, IcebergErrorResponse> {
    if task_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let warehouse_id_is_none = warehouse_id.is_none();
    let warehouse_id = warehouse_id.map_or_else(Uuid::nil, |id| *id);

    // Query both active tasks and historical tasks (task_log)
    let active_tasks = sqlx::query!(
        r#"
        SELECT 
            task_id,
            warehouse_id,
            entity_id,
            entity_type as "entity_type: EntityType",
            queue_name
        FROM task
        WHERE task_id = ANY($1) AND (warehouse_id = $2 OR $3)
        "#,
        &task_ids.iter().map(|id| **id).collect::<Vec<_>>()[..],
        warehouse_id,
        warehouse_id_is_none
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(|e| e.into_error_model("Failed to resolve active tasks"))?;

    let mut result = active_tasks
        .into_iter()
        .map(|record| {
            let task_id = TaskId::from(record.task_id);
            let entity = match record.entity_type {
                EntityType::Tabular => TaskEntity::Table {
                    table_id: record.entity_id.into(),
                    warehouse_id: record.warehouse_id.into(),
                },
            };
            let queue_name = TaskQueueName::from(record.queue_name);
            (task_id, (entity, queue_name))
        })
        .collect::<HashMap<_, _>>();
    let missing_ids = task_ids
        .iter()
        .filter(|id| !result.contains_key(id))
        .map(|id| **id)
        .collect_vec();

    let historical_tasks = sqlx::query!(
        r#"
        SELECT DISTINCT ON (task_id)
            task_id,
            warehouse_id,
            entity_id,
            entity_type as "entity_type: EntityType",
            queue_name
        FROM task_log
        WHERE task_id = ANY($1) AND (warehouse_id = $2 OR $3)
        ORDER BY task_id, attempt DESC
        "#,
        &missing_ids,
        warehouse_id,
        warehouse_id_is_none
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(|e| e.into_error_model("Failed to resolve historical tasks"))?;

    for record in historical_tasks {
        let task_id = TaskId::from(record.task_id);
        let entity = match record.entity_type {
            EntityType::Tabular => TaskEntity::Table {
                table_id: record.entity_id.into(),
                warehouse_id: record.warehouse_id.into(),
            },
        };
        let queue_name = TaskQueueName::from(record.queue_name);
        result.entry(task_id).or_insert((entity, queue_name));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        implementations::postgres::tasks::{
            pick_task, record_failure, record_success,
            test::{setup_two_warehouses, setup_warehouse},
        },
        service::{
            task_queue::{
                EntityId, TaskInput, TaskMetadata, TaskQueueName,
                DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
            },
            TableId,
        },
        WarehouseId,
    };

    async fn queue_task_helper(
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        parent_task_id: Option<TaskId>,
        entity_id: EntityId,
        warehouse_id: WarehouseId,
        schedule_for: Option<chrono::DateTime<chrono::Utc>>,
        payload: Option<serde_json::Value>,
    ) -> Result<Option<TaskId>, IcebergErrorResponse> {
        Ok(super::super::queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: TaskMetadata {
                    warehouse_id,
                    parent_task_id,
                    entity_id,
                    schedule_for,
                },
                payload: payload.unwrap_or(serde_json::json!({})),
            }],
        )
        .await?
        .pop()
        .map(|x| x.task_id))
    }

    fn generate_test_queue_name() -> TaskQueueName {
        TaskQueueName::from(format!("test-{}", Uuid::now_v7()))
    }

    #[sqlx::test]
    async fn test_resolve_tasks_empty_input(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;

        let result = resolve_tasks(Some(warehouse_id), &[], &mut conn)
            .await
            .unwrap();

        assert!(result.is_empty());
    }

    #[sqlx::test]
    async fn test_resolve_tasks_nonexistent_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;

        let nonexistent_task_ids = vec![
            TaskId::from(Uuid::now_v7()),
            TaskId::from(Uuid::now_v7()),
            TaskId::from(Uuid::now_v7()),
        ];

        let result = resolve_tasks(Some(warehouse_id), &nonexistent_task_ids, &mut conn)
            .await
            .unwrap();

        // Should be empty since no tasks exist
        assert!(result.is_empty());
    }

    #[sqlx::test]
    async fn test_resolve_tasks_active_tasks_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity1 = EntityId::Tabular(Uuid::now_v7());
        let entity2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name1 = generate_test_queue_name();
        let tq_name2 = generate_test_queue_name();

        // Queue two tasks
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name1,
            None,
            entity1,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "task1"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task_id2 = queue_task_helper(
            &mut conn,
            &tq_name2,
            None,
            entity2,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "task2"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Pick up the tasks to make them active
        let _task1 = pick_task(&pool, &tq_name1, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        let _task2 = pick_task(&pool, &tq_name2, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Resolve both tasks
        let task_ids = vec![task_id1, task_id2];
        let result = resolve_tasks(Some(warehouse_id), &task_ids, &mut conn)
            .await
            .unwrap();

        // Verify both tasks are resolved
        assert_eq!(result.len(), 2);

        let (entity_result1, queue_name_result1) = &result[&task_id1];
        let (entity_result2, queue_name_result2) = &result[&task_id2];

        // Verify first task
        assert_eq!(queue_name_result1, &tq_name1);
        let TaskEntity::Table {
            table_id: table_id1,
            warehouse_id: wh_id1,
        } = entity_result1;
        assert_eq!(*table_id1, TableId::from(entity1.to_uuid()));
        assert_eq!(*wh_id1, warehouse_id);

        // Verify second task
        assert_eq!(queue_name_result2, &tq_name2);
        let TaskEntity::Table {
            table_id: table_id2,
            warehouse_id: wh_id2,
        } = entity_result2;
        assert_eq!(*table_id2, TableId::from(entity2.to_uuid()));
        assert_eq!(*wh_id2, warehouse_id);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_completed_tasks_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity1 = EntityId::Tabular(Uuid::now_v7());
        let entity2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name1 = generate_test_queue_name();
        let tq_name2 = generate_test_queue_name();

        // Queue and complete two tasks
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name1,
            None,
            entity1,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "completed1"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task_id2 = queue_task_helper(
            &mut conn,
            &tq_name2,
            None,
            entity2,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "completed2"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Pick up and complete both tasks
        let task1 = pick_task(&pool, &tq_name1, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task1, &mut conn, Some("Task 1 completed"))
            .await
            .unwrap();

        let task2 = pick_task(&pool, &tq_name2, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure(&task2, 1, "Task 2 failed", &mut conn)
            .await
            .unwrap();

        // Resolve both completed tasks
        let task_ids = vec![task_id1, task_id2];
        let result = resolve_tasks(Some(warehouse_id), &task_ids, &mut conn)
            .await
            .unwrap();

        // Verify both tasks are resolved from task_log
        assert_eq!(result.len(), 2);

        let (entity_result1, queue_name_result1) = &result[&task_id1];
        let (entity_result2, queue_name_result2) = &result[&task_id2];

        // Verify first task
        assert_eq!(queue_name_result1, &tq_name1);
        let TaskEntity::Table {
            table_id: table_id1,
            warehouse_id: wh_id1,
        } = entity_result1;
        assert_eq!(*table_id1, TableId::from(entity1.to_uuid()));
        assert_eq!(*wh_id1, warehouse_id);

        // Verify second task
        assert_eq!(queue_name_result2, &tq_name2);
        let TaskEntity::Table {
            table_id: table_id2,
            warehouse_id: wh_id2,
        } = entity_result2;
        assert_eq!(*table_id2, TableId::from(entity2.to_uuid()));
        assert_eq!(*wh_id2, warehouse_id);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_mixed_active_and_completed(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity1 = EntityId::Tabular(Uuid::now_v7());
        let entity2 = EntityId::Tabular(Uuid::now_v7());
        let entity3 = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_test_queue_name();

        // Queue three tasks
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity1,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "active"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task_id2 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity2,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "completed"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task_id3 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity3,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "scheduled"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Complete task2, pick up task1 (leave task3 scheduled)
        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task2, &mut conn, Some("Task 2 completed"))
            .await
            .unwrap();

        let _task1 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Resolve all three tasks
        let task_ids = vec![task_id1, task_id2, task_id3];
        let result = resolve_tasks(Some(warehouse_id), &task_ids, &mut conn)
            .await
            .unwrap();

        // All tasks should be resolved
        assert_eq!(result.len(), 3);
        assert!(result.contains_key(&task_id1)); // Active task
        assert!(result.contains_key(&task_id2)); // Completed task (from task_log)
        assert!(result.contains_key(&task_id3)); // Scheduled task

        // Verify queue names are consistent
        let (_, queue_name1) = &result[&task_id1];
        let (_, queue_name2) = &result[&task_id2];
        let (_, queue_name3) = &result[&task_id3];

        assert_eq!(queue_name1, &tq_name);
        assert_eq!(queue_name2, &tq_name);
        assert_eq!(queue_name3, &tq_name);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_with_specific_warehouse(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id1, warehouse_id2) = setup_two_warehouses(pool.clone()).await;
        let entity1 = EntityId::Tabular(Uuid::now_v7());
        let entity2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_test_queue_name();

        // Queue tasks in different warehouses
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity1,
            warehouse_id1,
            None,
            Some(serde_json::json!({"warehouse": "1"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task_id2 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity2,
            warehouse_id2,
            None,
            Some(serde_json::json!({"warehouse": "2"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Resolve tasks with warehouse_id1 filter
        let task_ids = vec![task_id1, task_id2];
        let result = resolve_tasks(Some(warehouse_id1), &task_ids, &mut conn)
            .await
            .unwrap();

        // Only task from warehouse_id1 should be found
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&task_id1)); // Found in warehouse_id1
        assert!(!result.contains_key(&task_id2)); // Not found (wrong warehouse)

        // Verify the found task has correct warehouse_id
        let (entity_result, _) = &result[&task_id1];
        let TaskEntity::Table {
            warehouse_id: wh_id,
            ..
        } = entity_result;
        assert_eq!(*wh_id, warehouse_id1);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_without_warehouse_filter(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id1, warehouse_id2) = setup_two_warehouses(pool.clone()).await;
        let entity1 = EntityId::Tabular(Uuid::now_v7());
        let entity2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_test_queue_name();

        // Queue tasks in different warehouses
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity1,
            warehouse_id1,
            None,
            Some(serde_json::json!({"warehouse": "1"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task_id2 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity2,
            warehouse_id2,
            None,
            Some(serde_json::json!({"warehouse": "2"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Resolve tasks without warehouse filter (None)
        let task_ids = vec![task_id1, task_id2];
        let result = resolve_tasks(None, &task_ids, &mut conn).await.unwrap();

        // Both tasks should be found regardless of warehouse
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&task_id1));
        assert!(result.contains_key(&task_id2));

        // Verify warehouses are preserved
        let (entity_result1, _) = &result[&task_id1];
        let (entity_result2, _) = &result[&task_id2];

        let TaskEntity::Table {
            warehouse_id: wh_id1,
            ..
        } = entity_result1;
        let TaskEntity::Table {
            warehouse_id: wh_id2,
            ..
        } = entity_result2;

        assert_eq!(*wh_id1, warehouse_id1);
        assert_eq!(*wh_id2, warehouse_id2);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_partial_match(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_test_queue_name();

        // Queue one task
        let existing_task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"exists": true})),
        )
        .await
        .unwrap()
        .unwrap();

        // Create a non-existent task ID
        let nonexistent_task_id = TaskId::from(Uuid::now_v7());

        // Resolve both existing and non-existing tasks
        let task_ids = vec![existing_task_id, nonexistent_task_id];
        let result = resolve_tasks(Some(warehouse_id), &task_ids, &mut conn)
            .await
            .unwrap();

        // Should only have the existing task
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&existing_task_id));
        assert!(!result.contains_key(&nonexistent_task_id));

        // Verify the existing task details
        let (entity_result, queue_name_result) = &result[&existing_task_id];
        assert_eq!(queue_name_result, &tq_name);
        let TaskEntity::Table {
            table_id,
            warehouse_id: wh_id,
        } = entity_result;
        assert_eq!(*table_id, TableId::from(entity.to_uuid()));
        assert_eq!(*wh_id, warehouse_id);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_with_retried_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_test_queue_name();

        // Queue a task that will be retried
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"retry": "test"})),
        )
        .await
        .unwrap()
        .unwrap();

        // First attempt - pick and fail
        let task1 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure(&task1, 5, "First attempt failed", &mut conn)
            .await
            .unwrap();

        // Second attempt - pick and keep running
        let _task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Resolve the task (should find it in active tasks, not task_log)
        let task_ids = vec![task_id];
        let result = resolve_tasks(Some(warehouse_id), &task_ids, &mut conn)
            .await
            .unwrap();

        // Task should be resolved (from active tasks table)
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&task_id));

        let (entity_result, queue_name_result) = &result[&task_id];
        assert_eq!(queue_name_result, &tq_name);
        let TaskEntity::Table {
            table_id,
            warehouse_id: wh_id,
        } = entity_result;
        assert_eq!(*table_id, TableId::from(entity.to_uuid()));
        assert_eq!(*wh_id, warehouse_id);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_performance_with_many_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_test_queue_name();

        // Create a moderate number of tasks for performance testing
        let mut task_ids = Vec::new();
        for i in 0..20 {
            let entity = EntityId::Tabular(Uuid::now_v7());
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                None,
                entity,
                warehouse_id,
                None,
                Some(serde_json::json!({"batch": i})),
            )
            .await
            .unwrap()
            .unwrap();
            task_ids.push(task_id);
        }

        // Complete half of the tasks to have them in task_log
        for (i, _) in task_ids.iter().enumerate().take(10) {
            let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();
            if i % 2 == 0 {
                record_success(&task, &mut conn, Some("Completed"))
                    .await
                    .unwrap();
            } else {
                record_failure(&task, 1, "Failed", &mut conn).await.unwrap();
            }
        }

        // Add some non-existent task IDs
        let nonexistent_ids: Vec<TaskId> = (0..5).map(|_| TaskId::from(Uuid::now_v7())).collect();
        task_ids.extend(nonexistent_ids.iter());

        // Resolve all tasks
        let result = resolve_tasks(Some(warehouse_id), &task_ids, &mut conn)
            .await
            .unwrap();

        // Should have results for the 20 existing tasks only
        assert_eq!(result.len(), 20);

        // Verify all found tasks have correct queue name
        for (task_id, (_, queue_name)) in &result {
            assert_eq!(queue_name, &tq_name);
            // Verify this is one of our created tasks
            assert!(task_ids[..20].contains(task_id));
        }
    }
}
