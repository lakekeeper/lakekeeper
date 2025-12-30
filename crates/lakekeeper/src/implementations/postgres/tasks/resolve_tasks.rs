use iceberg_ext::catalog::rest::IcebergErrorResponse;

use super::TaskEntityTypeDB;
use crate::{
    ProjectId,
    api::ErrorModel,
    implementations::postgres::{dbutils::DBErrorHandler, tasks::task_entity_from_db},
    service::{
        ResolvedTask, TableNamed, ViewNamed, build_tabular_ident_from_vec,
        tasks::{
            ResolvedTaskEntity, TaskId, TaskQueueName, TaskResolveScope, WarehouseTaskEntityId,
        },
    },
};

/// Resolve tasks among all known active and historical tasks.
/// Returns a map of `task_id` to (`TaskEntity`, `queue_name`).
/// Only includes task IDs that exist - missing task IDs are not included in the result.
#[allow(clippy::too_many_lines)]
pub(crate) async fn resolve_tasks<'e, 'c: 'e, E>(
    scope: TaskResolveScope,
    task_ids: &[TaskId],
    state: E,
) -> Result<Vec<ResolvedTask>, IcebergErrorResponse>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    let project_id = scope.project_id();

    let project_scoped = matches!(scope, TaskResolveScope::Project { .. });

    let warehouse_id = match scope {
        TaskResolveScope::Warehouse { warehouse_id, .. } => warehouse_id,
        TaskResolveScope::Project { .. } => None,
    };

    // Query both active tasks and historical tasks using CTE and UNION ALL
    let tasks = sqlx::query!(
        r#"
        WITH active_tasks AS (
            SELECT 
                task_id,
                project_id,
                warehouse_id,
                entity_name,
                entity_id,
                entity_type,
                queue_name
            FROM task
            WHERE task_id = ANY($1)
                AND project_id = $3
                AND CASE
                        WHEN $4 THEN warehouse_id IS NULL -- project-level
                        ELSE ($2::uuid IS NULL OR warehouse_id = $2) -- warehouse-level
                    END
        ),
        missing_task_ids AS (
            SELECT unnest($1::uuid[]) as task_id
            EXCEPT
            SELECT task_id FROM active_tasks
        ),
        historical_tasks AS (
            SELECT DISTINCT ON (task_id)
                task_id,
                project_id,
                warehouse_id,
                entity_name,
                entity_id,
                entity_type,
                queue_name
            FROM task_log
            WHERE task_id IN (SELECT task_id FROM missing_task_ids)
                AND project_id = $3
                AND CASE
                        WHEN $4 THEN warehouse_id IS NULL -- project-level
                        ELSE ($2::uuid IS NULL OR warehouse_id = $2) -- warehouse-level
                    END
            ORDER BY task_id, attempt DESC
        )
        SELECT 
            task_id as "task_id!",
            project_id as "project_id!",
            warehouse_id,
            entity_name,
            entity_id,
            entity_type as "entity_type!: TaskEntityTypeDB",
            queue_name as "queue_name!"
        FROM active_tasks
        UNION ALL
        SELECT 
            task_id as "task_id!",
            project_id as "project_id!",
            warehouse_id,
            entity_name,
            entity_id,
            entity_type as "entity_type!: TaskEntityTypeDB",
            queue_name as "queue_name!"
        FROM historical_tasks
        "#,
        &task_ids.iter().map(|id| **id).collect::<Vec<_>>()[..], // $1
        warehouse_id.map(|id| *id),                              // $2
        project_id.as_str(),                                     // $3
        project_scoped,                                          // $4
    )
    .fetch_all(state)
    .await
    .map_err(|e| e.into_error_model("Failed to resolve tasks"))?;

    let result = tasks
        .into_iter()
        .map(|record| {
            let entity = task_entity_from_db(
                record.entity_type,
                record.warehouse_id,
                record.entity_id,
                record.entity_name,
            )?;

            let project_id = ProjectId::from_db_unchecked(record.project_id);
            let task_id = TaskId::from(record.task_id);

            let resolved_entity = match entity {
                crate::service::tasks::TaskEntity::Project => ResolvedTaskEntity::Project,
                crate::service::tasks::TaskEntity::Warehouse { warehouse_id } => {
                    warehouse_id.into()
                }
                crate::service::tasks::TaskEntity::EntityInWarehouse {
                    warehouse_id,
                    entity_id,
                    entity_name,
                } => {
                    let ident = build_tabular_ident_from_vec(&entity_name)?;
                    match entity_id {
                        WarehouseTaskEntityId::Table { table_id } => TableNamed {
                            warehouse_id,
                            table_ident: ident,
                            table_id,
                        }
                        .into(),
                        WarehouseTaskEntityId::View { view_id } => ViewNamed {
                            warehouse_id,
                            view_ident: ident,
                            view_id,
                        }
                        .into(),
                    }
                }
            };

            let queue_name = TaskQueueName::from(record.queue_name);
            Ok(ResolvedTask {
                task_id,
                project_id,
                entity: resolved_entity,
                queue_name,
            })
        })
        .collect::<Result<_, ErrorModel>>()?;

    Ok(result)
}

#[cfg(test)]
mod tests {

    use chrono::{Duration, Utc};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        ProjectId, WarehouseId,
        implementations::postgres::tasks::{
            pick_task, queue_task_batch, record_failure, record_success,
            test::{setup_two_warehouses, setup_warehouse},
        },
        service::tasks::{
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT, ResolvedTaskEntity, ScheduleTaskMetadata,
            TaskEntity, TaskInput, TaskQueueName, WarehouseTaskEntityId,
        },
    };

    #[allow(clippy::too_many_arguments)]
    async fn queue_task_helper(
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        parent_task_id: Option<TaskId>,
        entity_id: WarehouseTaskEntityId,
        project_id: ProjectId,
        warehouse_id: WarehouseId,
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
        payload: Option<serde_json::Value>,
    ) -> Result<Option<TaskId>, IcebergErrorResponse> {
        Ok(super::super::queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    project_id,
                    parent_task_id,
                    entity: TaskEntity::EntityInWarehouse {
                        warehouse_id,
                        entity_id,
                        entity_name: vec![
                            "ns".to_string(),
                            format!("table{}", entity_id.as_uuid()),
                        ],
                    },
                    scheduled_for,
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
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &[],
            &pool,
        )
        .await
        .unwrap();

        assert!(result.is_empty());
    }

    #[sqlx::test]
    async fn test_resolve_tasks_nonexistent_tasks(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let nonexistent_task_ids = vec![
            TaskId::from(Uuid::now_v7()),
            TaskId::from(Uuid::now_v7()),
            TaskId::from(Uuid::now_v7()),
        ];

        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &nonexistent_task_ids,
            &pool,
        )
        .await
        .unwrap();

        // Should be empty since no tasks exist
        assert!(result.is_empty());
    }

    #[sqlx::test]
    async fn test_resolve_tasks_active_tasks_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name1 = generate_test_queue_name();
        let tq_name2 = generate_test_queue_name();

        // Queue two tasks
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name1,
            None,
            entity1,
            project_id.clone(),
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
            project_id.clone(),
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
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();

        // Verify both tasks are resolved
        assert_eq!(result.len(), 2);

        let (entity_result1, queue_name_result1) = &result[&task_id1];
        let (entity_result2, queue_name_result2) = &result[&task_id2];

        // Verify first task
        assert_eq!(queue_name_result1, &tq_name1);
        match entity_result1 {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity1.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse { .. } => panic!("Expected TaskEntity::Table"),
        }

        // Verify second task
        assert_eq!(queue_name_result2, &tq_name2);

        match entity_result2 {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity2.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }
    }

    #[sqlx::test]
    async fn test_resolve_tasks_completed_tasks_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name1 = generate_test_queue_name();
        let tq_name2 = generate_test_queue_name();

        // Queue and complete two tasks
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name1,
            None,
            entity1,
            project_id.clone(),
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
            project_id.clone(),
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
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();

        // Verify both tasks are resolved from task_log
        assert_eq!(result.len(), 2);
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();

        let (entity_result1, queue_name_result1) = &result[&task_id1];
        let (entity_result2, queue_name_result2) = &result[&task_id2];

        // Verify first task
        assert_eq!(queue_name_result1, &tq_name1);
        match entity_result1 {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity1.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }

        // Verify second task
        assert_eq!(queue_name_result2, &tq_name2);
        match entity_result2 {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity2.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }
    }

    #[sqlx::test]
    async fn test_resolve_tasks_mixed_active_and_completed(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity3 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_test_queue_name();

        // Queue three tasks
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity1,
            project_id.clone(),
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
            project_id.clone(),
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
            project_id.clone(),
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
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();

        // All tasks should be resolved
        assert_eq!(result.len(), 3);
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();
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
        let (project_id, warehouse_id1, warehouse_id2) = setup_two_warehouses(pool.clone()).await;
        let entity1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_test_queue_name();

        // Queue tasks in different warehouses
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity1,
            project_id.clone(),
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
            project_id.clone(),
            warehouse_id2,
            None,
            Some(serde_json::json!({"warehouse": "2"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Resolve tasks with warehouse_id1 filter
        let task_ids = vec![task_id1, task_id2];
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id1),
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();

        // Only task from warehouse_id1 should be found
        assert_eq!(result.len(), 1);
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();
        assert!(result.contains_key(&task_id1)); // Found in warehouse_id1
        assert!(!result.contains_key(&task_id2)); // Not found (wrong warehouse)

        // Verify the found task has correct warehouse_id
        let (entity_result, _) = &result[&task_id1];
        match entity_result {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity1.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }
    }

    #[sqlx::test]
    async fn test_resolve_tasks_without_warehouse_filter(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (project_id, warehouse_id1, warehouse_id2) = setup_two_warehouses(pool.clone()).await;
        let entity1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_test_queue_name();

        // Queue tasks in different warehouses
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity1,
            project_id.clone(),
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
            project_id.clone(),
            warehouse_id2,
            None,
            Some(serde_json::json!({"warehouse": "2"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Resolve tasks without warehouse filter (None)
        let task_ids = vec![task_id1, task_id2];
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: None,
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();

        // Both tasks should be found regardless of warehouse
        assert_eq!(result.len(), 2);
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();
        assert!(result.contains_key(&task_id1));
        assert!(result.contains_key(&task_id2));

        // Verify warehouses are preserved
        let (entity_result1, _) = &result[&task_id1];
        let (entity_result2, _) = &result[&task_id2];

        match entity_result1 {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity1.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }

        match entity_result2 {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity2.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }
    }

    #[sqlx::test]
    async fn test_resolve_tasks_partial_match(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_test_queue_name();

        // Queue one task
        let existing_task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity,
            project_id.clone(),
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
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();

        // Should only have the existing task
        assert_eq!(result.len(), 1);
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();
        assert!(result.contains_key(&existing_task_id));
        assert!(!result.contains_key(&nonexistent_task_id));

        // Verify the existing task details
        let (entity_result, queue_name_result) = &result[&existing_task_id];
        assert_eq!(queue_name_result, &tq_name);
        match entity_result {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }
    }

    #[sqlx::test]
    async fn test_resolve_tasks_with_retried_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_test_queue_name();

        // Queue a task that will be retried
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity,
            project_id.clone(),
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
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();

        // Task should be resolved (from active tasks table)
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&task_id));

        let (entity_result, queue_name_result) = &result[&task_id];
        assert_eq!(queue_name_result, &tq_name);
        match entity_result {
            ResolvedTaskEntity::Table(table) => {
                assert_eq!(table.table_id, entity.as_uuid().into());
            }
            ResolvedTaskEntity::View(_)
            | ResolvedTaskEntity::Project
            | ResolvedTaskEntity::Warehouse(_) => panic!("Expected TaskEntity::Table"),
        }
    }

    #[sqlx::test]
    async fn test_resolve_tasks_with_project_filter(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let (project_id, warehouse_id1, warehouse_id2) = setup_two_warehouses(pool.clone()).await;

        let tq_name = generate_test_queue_name();

        let entity_uuid_1 = Uuid::now_v7().into();
        let entity_id_1 = WarehouseTaskEntityId::Table {
            table_id: entity_uuid_1,
        };
        let entity_name_1 = vec!["ns".to_string(), format!("table{}", entity_uuid_1)];

        let entity_uuid_2 = Uuid::now_v7().into();
        let entity_id_2 = WarehouseTaskEntityId::Table {
            table_id: entity_uuid_2,
        };
        let entity_name_2 = vec!["ns".to_string(), format!("table{}", entity_uuid_2)];
        let schedule_for = Some(Utc::now() + Duration::minutes(5));
        let parent_task_id = None;

        let project_payload = serde_json::json!({"data": "project_task"});
        let warehouse_payload = serde_json::json!({"data": "warehouse_task"});

        let results = queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    task_metadata: ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id,
                        entity: TaskEntity::EntityInWarehouse {
                            warehouse_id: warehouse_id1,
                            entity_id: entity_id_1,
                            entity_name: entity_name_1,
                        },
                        scheduled_for: schedule_for,
                    },

                    payload: warehouse_payload.clone(),
                },
                TaskInput {
                    task_metadata: ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id,
                        entity: TaskEntity::Project,
                        scheduled_for: schedule_for,
                    },

                    payload: project_payload.clone(),
                },
                TaskInput {
                    task_metadata: ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id,
                        entity: TaskEntity::EntityInWarehouse {
                            warehouse_id: warehouse_id2,
                            entity_id: entity_id_2,
                            entity_name: entity_name_2,
                        },
                        scheduled_for: schedule_for,
                    },

                    payload: warehouse_payload,
                },
            ],
        )
        .await
        .unwrap();

        let task_ids = results.iter().map(|r| r.task_id).collect::<Vec<_>>();
        let result = resolve_tasks(TaskResolveScope::Project { project_id }, &task_ids, &pool)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].task_id, task_ids[1]);
    }

    #[sqlx::test]
    async fn test_resolve_tasks_performance_with_many_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_test_queue_name();

        // Create a moderate number of tasks for performance testing
        let mut task_ids = Vec::new();
        for i in 0..20 {
            let entity = WarehouseTaskEntityId::Table {
                table_id: Uuid::now_v7().into(),
            };
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                None,
                entity,
                project_id.clone(),
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
        let result = resolve_tasks(
            TaskResolveScope::Warehouse {
                project_id,
                warehouse_id: Some(warehouse_id),
            },
            &task_ids,
            &pool,
        )
        .await
        .unwrap();

        // Should have results for the 20 existing tasks only
        assert_eq!(result.len(), 20);
        let result = result
            .into_iter()
            .map(|t| (t.task_id, (t.entity, t.queue_name)))
            .collect::<std::collections::HashMap<_, _>>();

        // Verify all found tasks have correct queue name
        for (task_id, (_, queue_name)) in &result {
            assert_eq!(queue_name, &tq_name);
            // Verify this is one of our created tasks
            assert!(task_ids[..20].contains(task_id));
        }
    }
}
