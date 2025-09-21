use std::collections::HashSet;

use chrono::DateTime;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use sqlx::PgConnection;
use uuid::Uuid;

use super::EntityType;
use crate::{
    api::management::v1::tasks::{
        ListTasksRequest, ListTasksResponse, Task as APITask, TaskStatus as APITaskStatus,
    },
    implementations::postgres::{
        dbutils::DBErrorHandler,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::task_queue::{TaskEntity, TaskId, TaskOutcome, TaskStatus},
    WarehouseId, CONFIG,
};

#[derive(sqlx::FromRow, Debug)]
struct TaskRow {
    task_id: Uuid,
    warehouse_id: Uuid,
    queue_name: String,
    entity_id: uuid::Uuid,
    entity_type: EntityType,
    task_status: Option<TaskStatus>,
    task_log_status: Option<TaskOutcome>,
    attempt_scheduled_for: DateTime<chrono::Utc>,
    started_at: Option<DateTime<chrono::Utc>>,
    attempt: i32,
    last_heartbeat_at: Option<DateTime<chrono::Utc>>,
    progress: f32,
    parent_task_id: Option<Uuid>,
    task_created_at: DateTime<chrono::Utc>,
    updated_at: Option<DateTime<chrono::Utc>>,
}

fn parse_api_task(row: TaskRow) -> Result<APITask, IcebergErrorResponse> {
    Ok(APITask {
        task_id: row.task_id.into(),
        warehouse_id: row.warehouse_id.into(),
        queue_name: row.queue_name.into(),
        entity: match row.entity_type {
            EntityType::Tabular => TaskEntity::Table {
                warehouse_id: row.warehouse_id.into(),
                table_id: row.entity_id.into(),
            },
        },
        status: row
            .task_status
            .map(Into::into)
            .or(row.task_log_status.map(Into::into))
            .ok_or_else(|| {
                ErrorModel::internal(
                    "Task attempt has neither status nor log status.",
                    "InternalError",
                    None,
                )
            })?,
        picked_up_at: row.started_at,
        attempt: row.attempt,
        parent_task_id: row.parent_task_id.map(TaskId::from),
        scheduled_for: row.attempt_scheduled_for,
        created_at: row.task_created_at,
        last_heartbeat_at: row.last_heartbeat_at,
        updated_at: row.updated_at,
        progress: row.progress,
    })
}

fn categorize_task_statuses(
    status: &[APITaskStatus],
) -> (HashSet<TaskStatus>, HashSet<TaskOutcome>) {
    let (task_status_filter, task_log_status_filter) = status
        .iter()
        .map(APITaskStatus::split)
        .collect::<(Vec<_>, Vec<_>)>();
    let task_status_filter = task_status_filter
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();
    let task_log_status_filter = task_log_status_filter
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();
    (task_status_filter, task_log_status_filter)
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_tasks(
    warehouse_id: WarehouseId,
    query: ListTasksRequest,
    transaction: &mut PgConnection,
) -> Result<ListTasksResponse, IcebergErrorResponse> {
    let ListTasksRequest {
        status,
        queue_name: queue_names,
        entities,
        created_after,
        created_before,
        page_token,
        page_size,
    } = query;

    let page_size = CONFIG.page_size_or_pagination_default(page_size);
    let previous_page_token = page_token.clone();
    let token = page_token.map(PaginateToken::try_from).transpose()?;

    let (pagination_ts, pagination_task_id) = token // token_id is the last returned task_id.
        .as_ref()
        .map(
            |PaginateToken::V1(V1PaginateToken { created_at, id }): &PaginateToken<Uuid>| {
                (created_at, id)
            },
        )
        .map_or((None, None), |(ts, task_id)| (Some(ts), Some(task_id)));

    let queue_names_is_none = queue_names.is_none();
    let queue_names = queue_names
        .unwrap_or_default()
        .into_iter()
        .map(crate::service::task_queue::TaskQueueName::into_string)
        .collect_vec();

    let status_filter_is_none = status.is_none();
    let status_filter = status.unwrap_or_default();
    let (task_status_filter, task_log_status_filter) = categorize_task_statuses(&status_filter);

    let entities_filter_is_none = entities.is_none();
    let (entity_ids, entity_types) = entities
        .unwrap_or_default()
        .into_iter()
        .filter_map(|e| match e {
            TaskEntity::Table {
                table_id,
                warehouse_id: entity_warehouse_id,
            } => (entity_warehouse_id == warehouse_id).then_some((*table_id, EntityType::Tabular)),
        })
        .collect::<(Vec<_>, Vec<_>)>();

    let tasks = sqlx::query_as!(
        TaskRow,
        r#"
        WITH selected_entities AS (
            SELECT entity_id, entity_type
            FROM unnest($10::uuid[], $11::entity_type[]) AS t(entity_id, entity_type)
        ),
        active_tasks AS (
            SELECT
                task_id,
                warehouse_id,
                queue_name,
                entity_id,
                entity_type,
                status as task_status,
                null::task_final_status as task_log_status,
                scheduled_for as attempt_scheduled_for,
                picked_up_at as started_at,
                attempt,
                last_heartbeat_at,
                progress,
                parent_task_id,
                created_at as task_created_at,
                updated_at
            FROM task
            WHERE warehouse_id = $1
                AND ((created_at < $3 OR $3 IS NULL) OR (created_at = $3 AND task_id < $4))
                AND ($6 OR queue_name = ANY($5))
                AND ($9 OR status = ANY($7::task_intermediate_status[]))
                AND ($12 OR (entity_id, entity_type) IN (SELECT entity_id, entity_type FROM selected_entities))
                AND (created_at >= $13 OR $13 IS NULL)
                AND (created_at <= $14 OR $14 IS NULL)
            ORDER BY task_created_at DESC, task_id DESC
            LIMIT $2
        ),
        log_tasks as (
            SELECT DISTINCT ON (task_created_at, task_id)
                task_id,
                warehouse_id,
                queue_name,
                entity_id,
                entity_type,
                null::task_intermediate_status as task_status,
                status as task_log_status,
                attempt_scheduled_for,
                started_at,
                attempt,
                last_heartbeat_at,
                progress,
                parent_task_id,
                task_created_at,
                null::timestamptz as updated_at
            FROM task_log
            WHERE warehouse_id = $1
                AND ((task_created_at < $3 OR $3 IS NULL) OR (task_created_at = $3 AND task_id < $4))
                AND ($6 OR queue_name = ANY($5))
                AND ($9 OR status = ANY($8::task_final_status[]))
                AND ($12 OR (entity_id, entity_type) IN (SELECT entity_id, entity_type FROM selected_entities))
                AND (task_created_at >= $13 OR $13 IS NULL)
                AND (task_created_at <= $14 OR $14 IS NULL)
            ORDER BY task_created_at DESC, task_id DESC, attempt DESC
            LIMIT $2
        )
        SELECT 
            task_id AS "task_id!",
            warehouse_id AS "warehouse_id!",
            queue_name AS "queue_name!",
            entity_id AS "entity_id!",
            entity_type as "entity_type!: EntityType",
            task_status as "task_status: TaskStatus",
            task_log_status as "task_log_status: TaskOutcome",
            attempt_scheduled_for as "attempt_scheduled_for!",
            started_at,
            attempt as "attempt!",
            last_heartbeat_at,
            progress as "progress!",
            parent_task_id,
            task_created_at as "task_created_at!",
            updated_at
         FROM (
            SELECT * FROM active_tasks
            UNION ALL
            SELECT * FROM log_tasks
        ) as combined
        ORDER BY task_created_at DESC, task_id DESC
        LIMIT $2
        "#,
        *warehouse_id,
        page_size as i64,
        pagination_ts,
        pagination_task_id.map(|id| *id),
        &queue_names,
        queue_names_is_none,
        task_status_filter.iter().collect_vec() as Vec<_>,
        task_log_status_filter.iter().collect_vec() as Vec<_>,
        status_filter_is_none,
        entity_ids as Vec<_>, // 11
        entity_types as Vec<_>, // 12
        entities_filter_is_none, // 13
        created_after, // 14
        created_before,
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(|e| e.into_error_model("Failed to list tasks"))?;

    let tasks = tasks
        .into_iter()
        .map(parse_api_task)
        .collect::<Result<Vec<_>, _>>()?;

    let next_page_token = tasks
        .last()
        .map(|last_task| {
            PaginateToken::V1(V1PaginateToken {
                created_at: last_task.created_at,
                id: *last_task.task_id,
            })
            .to_string()
        })
        .or(previous_page_token);

    Ok(ListTasksResponse {
        tasks,
        next_page_token,
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::management::v1::tasks::{ListTasksRequest, TaskStatus as APITaskStatus},
        implementations::postgres::tasks::{
            pick_task, record_failure, record_success, test::setup_warehouse,
        },
        service::task_queue::{
            EntityId, TaskEntity, TaskInput, TaskMetadata, TaskOutcome, TaskQueueName, TaskStatus,
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
        },
        WarehouseId,
    };

    #[test]
    fn test_split_task_status() {
        let (i_status, f_status) = categorize_task_statuses(&[
            APITaskStatus::Failed,
            APITaskStatus::Scheduled,
            APITaskStatus::Running,
            APITaskStatus::Failed,
            APITaskStatus::Success,
        ]);
        assert_eq!(
            i_status,
            HashSet::from([TaskStatus::Scheduled, TaskStatus::Running])
        );
        assert_eq!(
            f_status,
            HashSet::from([TaskOutcome::Failed, TaskOutcome::Success])
        );

        let (i_status, f_status) = categorize_task_statuses(&[APITaskStatus::Scheduled]);
        assert_eq!(i_status, HashSet::from([TaskStatus::Scheduled]));
        assert!(f_status.is_empty());

        let (i_status, f_status) = categorize_task_statuses(&[APITaskStatus::Success]);
        assert!(i_status.is_empty());
        assert_eq!(f_status, HashSet::from([TaskOutcome::Success]));

        let (i_status, f_status) = categorize_task_statuses(&[]);
        assert!(i_status.is_empty());
        assert!(f_status.is_empty());
    }

    async fn queue_task_helper(
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        entity_id: EntityId,
        warehouse_id: WarehouseId,
        payload: Option<serde_json::Value>,
    ) -> Result<crate::service::task_queue::TaskId, IcebergErrorResponse> {
        let result = super::super::queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: TaskMetadata {
                    warehouse_id,
                    parent_task_id: None,
                    entity_id,
                    schedule_for: None,
                },
                payload: payload.unwrap_or(serde_json::json!({})),
            }],
        )
        .await?;
        Ok(result.into_iter().next().unwrap().task_id)
    }

    fn generate_tq_name() -> TaskQueueName {
        TaskQueueName::from(format!("test-{}", Uuid::now_v7()))
    }

    #[sqlx::test]
    async fn test_list_tasks_empty_warehouse(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;

        let request = ListTasksRequest::default();
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert!(result.tasks.is_empty());
        assert!(result.next_page_token.is_none());
    }

    #[sqlx::test]
    async fn test_list_tasks_single_active_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"test": "data"});

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id,
            warehouse_id,
            Some(payload.clone()),
        )
        .await
        .unwrap();

        let request = ListTasksRequest::default();
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        let task = &result.tasks[0];
        assert_eq!(task.task_id, task_id);
        assert_eq!(task.warehouse_id, warehouse_id);
        assert_eq!(task.queue_name.as_str(), tq_name.as_str());
        assert!(matches!(task.status, APITaskStatus::Scheduled));
        assert_eq!(task.attempt, 0);
        assert!(task.picked_up_at.is_none());
        assert!(result.next_page_token.is_some());

        // Verify entity
        let TaskEntity::Table {
            table_id,
            warehouse_id: entity_warehouse_id,
        } = task.entity;
        assert_eq!(*table_id, entity_id.to_uuid());
        assert_eq!(entity_warehouse_id, warehouse_id);
    }

    #[sqlx::test]
    async fn test_list_tasks_multiple_tasks_different_queues(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id1 = EntityId::Tabular(Uuid::now_v7());
        let entity_id2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Queue tasks in different queues
        let task_id1 = queue_task_helper(&mut conn, &tq_name1, entity_id1, warehouse_id, None)
            .await
            .unwrap();
        let task_id2 = queue_task_helper(&mut conn, &tq_name2, entity_id2, warehouse_id, None)
            .await
            .unwrap();

        let request = ListTasksRequest::default();
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 2);
        let task_ids: HashSet<_> = result.tasks.iter().map(|t| t.task_id).collect();
        assert!(task_ids.contains(&task_id1));
        assert!(task_ids.contains(&task_id2));

        // Tasks should be ordered by created_at DESC
        assert!(result.tasks[0].created_at >= result.tasks[1].created_at);
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_queue_name(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id1 = EntityId::Tabular(Uuid::now_v7());
        let entity_id2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Queue tasks in different queues
        let task_id1 = queue_task_helper(&mut conn, &tq_name1, entity_id1, warehouse_id, None)
            .await
            .unwrap();
        let _task_id2 = queue_task_helper(&mut conn, &tq_name2, entity_id2, warehouse_id, None)
            .await
            .unwrap();

        // Filter by first queue only
        let request = ListTasksRequest {
            queue_name: Some(vec![tq_name1.clone()]),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id, task_id1);
        assert_eq!(result.tasks[0].queue_name.as_str(), tq_name1.as_str());
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_status(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id1 = EntityId::Tabular(Uuid::now_v7());
        let entity_id2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue two tasks
        queue_task_helper(&mut conn, &tq_name, entity_id1, warehouse_id, None)
            .await
            .unwrap();
        queue_task_helper(&mut conn, &tq_name, entity_id2, warehouse_id, None)
            .await
            .unwrap();

        // Pick up one task to make it running
        let _picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Filter by running status only
        let request = ListTasksRequest {
            status: Some(vec![APITaskStatus::Running]),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert!(matches!(result.tasks[0].status, APITaskStatus::Running));

        // Filter by scheduled status only
        let request = ListTasksRequest {
            status: Some(vec![APITaskStatus::Scheduled]),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert!(matches!(result.tasks[0].status, APITaskStatus::Scheduled));
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_entity(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id1 = EntityId::Tabular(Uuid::now_v7());
        let entity_id2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue tasks for different entities
        let task_id1 = queue_task_helper(&mut conn, &tq_name, entity_id1, warehouse_id, None)
            .await
            .unwrap();
        let _task_id2 = queue_task_helper(&mut conn, &tq_name, entity_id2, warehouse_id, None)
            .await
            .unwrap();

        // Filter by first entity only
        let request = ListTasksRequest {
            entities: Some(vec![TaskEntity::Table {
                warehouse_id,
                table_id: entity_id1.to_uuid().into(),
            }]),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id, task_id1);
        let TaskEntity::Table {
            table_id,
            warehouse_id: entity_warehouse_id,
        } = result.tasks[0].entity;
        assert_eq!(*table_id, entity_id1.to_uuid());
        assert_eq!(entity_warehouse_id, warehouse_id);
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_created_date_range(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Create a reference time
        let now = Utc::now();
        let before_time = now - chrono::Duration::hours(1);
        let after_time = now + chrono::Duration::hours(1);

        // Queue a task
        let task_id = queue_task_helper(&mut conn, &tq_name, entity_id, warehouse_id, None)
            .await
            .unwrap();

        // Filter with created_after (should include our task)
        let request = ListTasksRequest {
            created_after: Some(before_time),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id, task_id);

        // Filter with created_before (should include our task)
        let request = ListTasksRequest {
            created_before: Some(after_time),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id, task_id);

        // Filter with created_after that excludes our task
        let request = ListTasksRequest {
            created_after: Some(after_time),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        assert!(result.tasks.is_empty());

        // Filter with created_before that excludes our task
        let request = ListTasksRequest {
            created_before: Some(before_time),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        assert!(result.tasks.is_empty());
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Queue 5 tasks
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();
        for _ in 0..5 {
            let entity_id = EntityId::Tabular(Uuid::now_v7());
            let task_id = queue_task_helper(&mut conn, &tq_name, entity_id, warehouse_id, None)
                .await
                .unwrap();
            task_ids.push(task_id);
        }

        // Get first page with page_size=2
        let request = ListTasksRequest {
            page_size: Some(2),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        seen_ids.extend(result.tasks.iter().map(|t| t.task_id));

        assert_eq!(result.tasks.len(), 2);
        assert!(result.next_page_token.is_some());

        // Get second page
        let request = ListTasksRequest {
            page_size: Some(2),
            page_token: result.next_page_token,
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        seen_ids.extend(result.tasks.iter().map(|t| t.task_id));

        assert_eq!(result.tasks.len(), 2);
        assert!(result.next_page_token.is_some());

        // Get third page (should have 1 task)
        let request = ListTasksRequest {
            page_size: Some(2),
            page_token: result.next_page_token,
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        seen_ids.extend(result.tasks.iter().map(|t| t.task_id));

        assert_eq!(result.tasks.len(), 1);
        assert!(result.next_page_token.is_some());

        // Get fourth page (should be empty)
        let request = ListTasksRequest {
            page_size: Some(2),
            page_token: result.next_page_token,
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(
            result.tasks,
            vec![],
            "Expected no tasks on fourth page, got {:?}",
            result.tasks
        );
        // No token would indicate that we didn't paginate yet, thus returning the most recent tasks
        assert!(result.next_page_token.is_some());

        // Try yet again (should still be empty)
        let request = ListTasksRequest {
            page_size: Some(2),
            page_token: result.next_page_token,
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();
        assert!(result.tasks.is_empty());
        assert!(result.next_page_token.is_some());

        // Verify all task IDs were seen
        assert_eq!(seen_ids.len(), 5);
        for task_id in task_ids {
            assert!(seen_ids.contains(&task_id));
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination_mixed_active_completed(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Create 10 tasks - mix of active and completed
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        for i in 0..10 {
            let entity_id = EntityId::Tabular(Uuid::now_v7());
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                entity_id,
                warehouse_id,
                Some(serde_json::json!({"index": i})),
            )
            .await
            .unwrap();
            task_ids.push(task_id);
        }

        // Complete some tasks (first 4)
        for &task_id in &task_ids[0..4] {
            let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(picked_task.task_id(), task_id);
            record_success(&picked_task, &mut conn, Some("Completed successfully"))
                .await
                .unwrap();
        }

        // Fail some tasks (next 2)
        for &task_id in &task_ids[4..6] {
            let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(picked_task.task_id(), task_id);
            record_failure(&picked_task, 1, "Task failed", &mut conn)
                .await
                .unwrap();
        }

        // Cancel some tasks (next 2)
        super::super::cancel_scheduled_tasks(
            &mut conn,
            crate::service::task_queue::TaskFilter::TaskIds(task_ids[6..8].to_vec()),
            Some(&tq_name),
            false,
        )
        .await
        .unwrap();

        // Leave remaining tasks (2) as scheduled

        // Test pagination through all tasks with page_size=3
        let mut all_tasks = Vec::new();
        let mut page_token = None;
        let mut page_count = 0;

        loop {
            let request = ListTasksRequest {
                page_size: Some(3),
                page_token: page_token.clone(),
                ..Default::default()
            };
            let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(all_tasks.iter().map(|t| t.task_id));
            page_count += 1;

            // Prevent infinite loops in case of issues
            assert!(page_count <= 5, "Too many pages, possible infinite loop");

            if result.next_page_token.is_some() && has_more_tasks {
                page_token = result.next_page_token;
            } else {
                break;
            }
        }

        // Verify we got all 10 tasks
        assert_eq!(all_tasks.len(), 10);
        assert_eq!(seen_ids.len(), 10);

        // Verify all original task IDs are present
        for task_id in &task_ids {
            assert!(seen_ids.contains(task_id), "Missing task_id: {task_id}");
        }

        // Verify task statuses - should have mix of Success, Failed, Cancelled, and Scheduled
        let status_types: Vec<_> = all_tasks.iter().map(|t| &t.status).collect();
        let has_success = status_types
            .iter()
            .any(|s| matches!(s, APITaskStatus::Success));
        let has_failed = status_types
            .iter()
            .any(|s| matches!(s, APITaskStatus::Failed));
        let has_cancelled = status_types
            .iter()
            .any(|s| matches!(s, APITaskStatus::Cancelled));
        let has_scheduled = status_types
            .iter()
            .any(|s| matches!(s, APITaskStatus::Scheduled));

        assert!(has_success);
        assert!(has_failed);
        assert!(has_cancelled);
        assert!(has_scheduled);
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination_only_completed_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Create and complete 6 tasks
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        for i in 0..6 {
            let entity_id = EntityId::Tabular(Uuid::now_v7());
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                entity_id,
                warehouse_id,
                Some(serde_json::json!({"completed_index": i})),
            )
            .await
            .unwrap();
            task_ids.push(task_id);

            // Pick up and complete immediately
            let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(picked_task.task_id(), task_id);
            record_success(
                &picked_task,
                &mut conn,
                Some(&format!("Completed task {i}")),
            )
            .await
            .unwrap();
        }

        // Test pagination with page_size=2
        let mut all_tasks = Vec::new();
        let mut page_token = None;
        let mut page_count = 0;

        loop {
            let request = ListTasksRequest {
                page_size: Some(2),
                page_token: page_token.clone(),
                ..Default::default()
            };
            let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(all_tasks.iter().map(|t| t.task_id));
            page_count += 1;

            assert!(page_count <= 5, "Too many pages, possible infinite loop");

            if result.next_page_token.is_some() && has_more_tasks {
                page_token = result.next_page_token;
            } else {
                break;
            }
        }

        // Verify all tasks are completed and in task_log
        assert_eq!(all_tasks.len(), 6);
        assert_eq!(seen_ids.len(), 6);

        for task in &all_tasks {
            assert!(matches!(task.status, APITaskStatus::Success));
            assert!(task.picked_up_at.is_some());
        }

        // Verify all original task IDs are present
        for task_id in &task_ids {
            assert!(seen_ids.contains(task_id));
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination_mixed_scenarios_with_retries(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Create 8 tasks with different outcomes
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        for i in 0..8 {
            let entity_id = EntityId::Tabular(Uuid::now_v7());
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                entity_id,
                warehouse_id,
                Some(serde_json::json!({"retry_test_index": i})),
            )
            .await
            .unwrap();
            task_ids.push(task_id);
        }

        // Task 0: Success on first try
        let task0 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task0, &mut conn, Some("Success on first try"))
            .await
            .unwrap();

        // Task 1: Fail once, then succeed
        let task1 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure(&task1, 2, "First attempt failed", &mut conn)
            .await
            .unwrap();

        let task1_retry = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task1_retry.task_id(), task1.task_id());
        record_success(&task1_retry, &mut conn, Some("Success on retry"))
            .await
            .unwrap();

        // Task 2: Fail multiple times, eventually fail permanently
        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure(&task2, 1, "Failed permanently", &mut conn)
            .await
            .unwrap();

        // Task 3: Cancel while scheduled
        super::super::cancel_scheduled_tasks(
            &mut conn,
            crate::service::task_queue::TaskFilter::TaskIds(vec![task_ids[3]]),
            Some(&tq_name),
            false,
        )
        .await
        .unwrap();

        // Task 4: Pick up and leave running
        let _task4_running = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Tasks 5,6,7: Leave as scheduled

        // Test pagination through all tasks
        let mut all_tasks = Vec::new();
        let mut page_token = None;
        let mut page_count = 0;

        loop {
            let request = ListTasksRequest {
                page_size: Some(2),
                page_token: page_token.clone(),
                ..Default::default()
            };
            let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(all_tasks.iter().map(|t| t.task_id));
            page_count += 1;

            assert!(page_count <= 6, "Too many pages, possible infinite loop");

            if result.next_page_token.is_some() && has_more_tasks {
                page_token = result.next_page_token;
            } else {
                break;
            }
        }

        // Verify we got all 8 tasks
        assert_eq!(all_tasks.len(), 8);
        assert_eq!(seen_ids.len(), 8);

        // Verify all original task IDs are present
        for task_id in &task_ids {
            assert!(seen_ids.contains(task_id));
        }

        // Verify we have expected status distribution
        let mut status_counts = std::collections::HashMap::new();
        for task in &all_tasks {
            match task.status {
                APITaskStatus::Success => *status_counts.entry("Success").or_insert(0) += 1,
                APITaskStatus::Failed => *status_counts.entry("Failed").or_insert(0) += 1,
                APITaskStatus::Cancelled => *status_counts.entry("Cancelled").or_insert(0) += 1,
                APITaskStatus::Running => *status_counts.entry("Running").or_insert(0) += 1,
                APITaskStatus::Scheduled => *status_counts.entry("Scheduled").or_insert(0) += 1,
                APITaskStatus::Stopping => *status_counts.entry("Stopping").or_insert(0) += 1,
            }
        }

        // Should have: 2 Success, 1 Failed, 1 Cancelled, 1 Running, 3 Scheduled
        assert_eq!(*status_counts.get("Success").unwrap_or(&0), 2);
        assert_eq!(*status_counts.get("Failed").unwrap_or(&0), 1);
        assert_eq!(*status_counts.get("Cancelled").unwrap_or(&0), 1);
        assert_eq!(*status_counts.get("Running").unwrap_or(&0), 1);
        assert_eq!(*status_counts.get("Scheduled").unwrap_or(&0), 3);
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination_across_multiple_queues(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Create tasks in two different queues
        let mut all_task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        // Queue 1: 3 tasks
        for i in 0..3 {
            let entity_id = EntityId::Tabular(Uuid::now_v7());
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name1,
                entity_id,
                warehouse_id,
                Some(serde_json::json!({"queue1_index": i})),
            )
            .await
            .unwrap();
            all_task_ids.push(task_id);
        }

        // Queue 2: 4 tasks
        for i in 0..4 {
            let entity_id = EntityId::Tabular(Uuid::now_v7());
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name2,
                entity_id,
                warehouse_id,
                Some(serde_json::json!({"queue2_index": i})),
            )
            .await
            .unwrap();
            all_task_ids.push(task_id);
        }

        // Complete some tasks from queue 1
        let task_q1_1 = pick_task(&pool, &tq_name1, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task_q1_1, &mut conn, Some("Queue 1 completed"))
            .await
            .unwrap();

        // Fail a task from queue 2
        let task_q2_1 = pick_task(&pool, &tq_name2, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure(&task_q2_1, 1, "Queue 2 failed", &mut conn)
            .await
            .unwrap();

        // Test pagination across all queues (default behavior should include all queues)
        let mut all_tasks = Vec::new();
        let mut page_token = None;
        let mut page_count = 0;

        loop {
            let request = ListTasksRequest {
                page_size: Some(2),
                page_token: page_token.clone(),
                ..Default::default()
            };
            let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(all_tasks.iter().map(|t| t.task_id));
            page_count += 1;

            assert!(page_count <= 5, "Too many pages, possible infinite loop");

            if result.next_page_token.is_some() && has_more_tasks {
                page_token = result.next_page_token;
            } else {
                break;
            }
        }

        // Verify we got all 7 tasks from both queues
        assert_eq!(all_tasks.len(), 7);
        assert_eq!(seen_ids.len(), 7);

        // Verify all original task IDs are present
        for task_id in &all_task_ids {
            assert!(seen_ids.contains(task_id));
        }

        // Verify we have tasks from both queues
        let queue_names: HashSet<_> = all_tasks.iter().map(|t| &t.queue_name).collect();
        assert!(queue_names.contains(&tq_name1));
        assert!(queue_names.contains(&tq_name2));
    }

    #[sqlx::test]
    async fn test_list_tasks_completed_tasks_from_log(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue and complete a task
        let task_id = queue_task_helper(&mut conn, &tq_name, entity_id, warehouse_id, None)
            .await
            .unwrap();

        // Pick up the task
        let picked_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Complete the task successfully
        record_success(&picked_task, &mut conn, Some("Task completed"))
            .await
            .unwrap();

        // List all tasks
        let request = ListTasksRequest::default();
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        let task = &result.tasks[0];
        assert_eq!(task.task_id, task_id);
        assert!(matches!(task.status, APITaskStatus::Success));
        assert!(task.picked_up_at.is_some());
    }

    #[sqlx::test]
    async fn test_list_tasks_mixed_active_and_completed(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id1 = EntityId::Tabular(Uuid::now_v7());
        let entity_id2 = EntityId::Tabular(Uuid::now_v7());
        let entity_id3 = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue three tasks
        let task_id1 = queue_task_helper(&mut conn, &tq_name, entity_id1, warehouse_id, None)
            .await
            .unwrap();
        let task_id2 = queue_task_helper(&mut conn, &tq_name, entity_id2, warehouse_id, None)
            .await
            .unwrap();
        let task_id3 = queue_task_helper(&mut conn, &tq_name, entity_id3, warehouse_id, None)
            .await
            .unwrap();

        // Complete first task
        let picked_task1 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&picked_task1, &mut conn, Some("Completed"))
            .await
            .unwrap();

        // Pick up second task (keep it running)
        let _picked_task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Third task remains scheduled

        // List all tasks
        let request = ListTasksRequest::default();
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 3);

        // Find tasks by ID and verify their status
        let task_statuses: std::collections::HashMap<_, _> = result
            .tasks
            .iter()
            .map(|t| (t.task_id, &t.status))
            .collect();

        assert!(matches!(
            task_statuses.get(&task_id1).unwrap(),
            APITaskStatus::Success
        ));
        assert!(matches!(
            task_statuses.get(&task_id2).unwrap(),
            APITaskStatus::Running
        ));
        assert!(matches!(
            task_statuses.get(&task_id3).unwrap(),
            APITaskStatus::Scheduled
        ));
    }

    #[sqlx::test]
    async fn test_list_tasks_with_retries(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue a task
        let task_id = queue_task_helper(&mut conn, &tq_name, entity_id, warehouse_id, None)
            .await
            .unwrap();

        // First attempt - pick and fail
        let task1 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure(&task1, 5, "First attempt failed", &mut conn)
            .await
            .unwrap();

        // Second attempt - pick and succeed
        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task2, &mut conn, Some("Second attempt succeeded"))
            .await
            .unwrap();

        // List all tasks - should show the successful attempt
        let request = ListTasksRequest::default();
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        let task = &result.tasks[0];
        assert_eq!(task.task_id, task_id);
        assert!(matches!(task.status, APITaskStatus::Success));
        assert_eq!(task.attempt, 2); // Should show the successful attempt
    }

    #[sqlx::test]
    async fn test_list_tasks_wrong_warehouse(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let wrong_warehouse_id = WarehouseId::from(Uuid::now_v7());
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue a task in the correct warehouse
        let _task_id = queue_task_helper(&mut conn, &tq_name, entity_id, warehouse_id, None)
            .await
            .unwrap();

        // Try to list tasks from wrong warehouse
        let request = ListTasksRequest::default();
        let result = list_tasks(wrong_warehouse_id, request, &mut conn)
            .await
            .unwrap();

        // Should return empty list
        assert!(result.tasks.is_empty());
        assert!(result.next_page_token.is_none());
    }

    #[sqlx::test]
    async fn test_list_tasks_complex_filters(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id1 = EntityId::Tabular(Uuid::now_v7());
        let entity_id2 = EntityId::Tabular(Uuid::now_v7());
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Queue tasks in different queues and entities
        let task_id1 = queue_task_helper(&mut conn, &tq_name1, entity_id1, warehouse_id, None)
            .await
            .unwrap();
        let _task_id2 = queue_task_helper(&mut conn, &tq_name1, entity_id2, warehouse_id, None)
            .await
            .unwrap();
        let _task_id3 = queue_task_helper(&mut conn, &tq_name2, entity_id1, warehouse_id, None)
            .await
            .unwrap();

        // Filter by queue_name AND entity
        let request = ListTasksRequest {
            queue_name: Some(vec![tq_name1.clone()]),
            entities: Some(vec![TaskEntity::Table {
                warehouse_id,
                table_id: entity_id1.to_uuid().into(),
            }]),
            ..Default::default()
        };
        let result = list_tasks(warehouse_id, request, &mut conn).await.unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id, task_id1);
        assert_eq!(result.tasks[0].queue_name.as_str(), tq_name1.as_str());

        let TaskEntity::Table {
            table_id,
            warehouse_id: entity_warehouse_id,
        } = result.tasks[0].entity;
        assert_eq!(*table_id, entity_id1.to_uuid());
        assert_eq!(entity_warehouse_id, warehouse_id);
    }
}
