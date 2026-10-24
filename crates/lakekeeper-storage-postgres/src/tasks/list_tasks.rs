use std::{collections::HashSet, sync::Arc};

use chrono::DateTime;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use lakekeeper::{
    CONFIG, ProjectId,
    api::management::v1::tasks::{ListTasksRequest, TaskStatus, WarehouseTaskEntityFilter},
    service::{
        TaskList,
        tasks::{
            TaskAttemptId, TaskFilter, TaskId, TaskInfo, TaskIntermediateStatus, TaskMetadata,
            TaskOutcome,
        },
    },
};
use sqlx::PgConnection;
use uuid::Uuid;

use super::TaskEntityTypeDB;
use crate::{
    dbutils::DBErrorHandler,
    pagination::{PaginateToken, V1PaginateToken},
    tasks::{task_entity_from_db, task_schedule_id_from_db, task_status_from_db},
};

#[derive(sqlx::FromRow, Debug)]
struct TaskRow {
    task_id: Uuid,
    project_id: String,
    warehouse_id: Option<Uuid>,
    queue_name: String,
    entity_id: Option<uuid::Uuid>,
    entity_type: TaskEntityTypeDB,
    entity_name: Option<Vec<String>>,
    task_status: Option<TaskIntermediateStatus>,
    task_log_status: Option<TaskOutcome>,
    attempt_scheduled_for: DateTime<chrono::Utc>,
    started_at: Option<DateTime<chrono::Utc>>,
    attempt: i32,
    last_heartbeat_at: Option<DateTime<chrono::Utc>>,
    progress: f32,
    parent_task_id: Option<Uuid>,
    task_created_at: DateTime<chrono::Utc>,
    updated_at: Option<DateTime<chrono::Utc>>,
    resource_demand: Option<i64>,
}

fn parse_task(row: TaskRow) -> Result<TaskInfo, IcebergErrorResponse> {
    let scope = task_entity_from_db(
        row.entity_type,
        row.warehouse_id,
        row.entity_id,
        row.entity_name.clone(),
    )
    .map_err(ErrorModel::from)?;

    let status = task_status_from_db(row.task_status, row.task_log_status)?;

    Ok(TaskInfo {
        id: TaskAttemptId {
            task_id: row.task_id.into(),
            attempt: row.attempt,
        },
        queue_name: row.queue_name.into(),
        task_metadata: TaskMetadata {
            resource_demand: row.resource_demand,
            project_id: Arc::new(ProjectId::from_db_unchecked(row.project_id)),
            entity: scope,
            parent_task_id: row.parent_task_id.map(TaskId::from),
            scheduled_for: row.attempt_scheduled_for,
            schedule_id: task_schedule_id_from_db(row.entity_type, row.entity_id),
        },
        status,
        picked_up_at: row.started_at,
        created_at: row.task_created_at,
        last_heartbeat_at: row.last_heartbeat_at,
        updated_at: row.updated_at,
        progress: row.progress,
    })
}

fn categorize_task_statuses(
    status: &[TaskStatus],
) -> (HashSet<TaskIntermediateStatus>, HashSet<TaskOutcome>) {
    let (task_status_filter, task_log_status_filter) = status
        .iter()
        .map(TaskStatus::split)
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

/// Splits the entity filter into the id and type arrays (`$10`, `$11`) of [`list_tasks`], without repeated entries.
fn entity_filter_arrays(
    entities: Vec<WarehouseTaskEntityFilter>,
) -> (Vec<Option<Uuid>>, Vec<TaskEntityTypeDB>) {
    entities
        .into_iter()
        .map(|e| match e {
            WarehouseTaskEntityFilter::Table { table_id } => {
                (Some(*table_id), TaskEntityTypeDB::Table)
            }
            WarehouseTaskEntityFilter::View { view_id } => (Some(*view_id), TaskEntityTypeDB::View),
            WarehouseTaskEntityFilter::GenericTable { generic_table_id } => {
                (Some(*generic_table_id), TaskEntityTypeDB::GenericTable)
            }
            WarehouseTaskEntityFilter::Warehouse => (None, TaskEntityTypeDB::Warehouse),
        })
        // A task matching two entries would be listed twice.
        .unique()
        .unzip()
}

#[allow(clippy::too_many_lines)] // The SQL literal alone is over the limit.
pub(crate) async fn list_tasks(
    filter: &TaskFilter,
    query: &ListTasksRequest,
    transaction: &mut PgConnection,
) -> Result<TaskList, IcebergErrorResponse> {
    let ListTasksRequest {
        status,
        queue_name: queue_names,
        entities,
        created_after,
        created_before,
        parent_task_id,
        page_token,
        page_size,
    } = (*query).clone();

    let (warehouse_id, project_id, include_sub_tasks) = match filter {
        TaskFilter::WarehouseId {
            warehouse_id,
            project_id,
        } => (Some(warehouse_id), Some(project_id), false),
        TaskFilter::ProjectId {
            project_id,
            include_sub_tasks,
        } => (None, Some(project_id), *include_sub_tasks),
        TaskFilter::TaskIds(_) => Err(ErrorModel::internal(
            "TaskFilter for TaskIds not implemented for list_tasks.",
            "InternalError",
            None,
        ))?,
    };

    let page_size = CONFIG.page_size_or_pagination_default(page_size);
    let previous_page_token = page_token.clone();
    let token: Option<PaginateToken<Uuid>> = page_token.map(PaginateToken::try_from).transpose()?;

    // token_id is the last returned task_id.
    let (pagination_ts, pagination_task_id) = token
        .as_ref()
        .map(PaginateToken::v1_parts)
        .transpose()?
        .unzip();

    let queue_names_is_none = queue_names.is_none();
    let queue_names = queue_names
        .unwrap_or_default()
        .into_iter()
        .map(lakekeeper::service::tasks::TaskQueueName::into_string)
        .collect_vec();

    let status_filter_is_none = status.is_none();
    let status_filter = status.clone().unwrap_or_default();
    let (task_status_filter, task_log_status_filter) = categorize_task_statuses(&status_filter);

    let entities_filter_is_none = entities.is_none();
    let (entity_ids, entity_types) = entity_filter_arrays(entities.unwrap_or_default());

    let tasks = sqlx::query_as!(
        TaskRow,
        r#"
        WITH selected_entities AS (
            SELECT entity_id, entity_type
            FROM unnest($10::uuid[], $11::entity_type[]) AS t(entity_id, entity_type)
        ),
        -- Copied into each branch below, so that each branch uses its own index.
        active_rows AS NOT MATERIALIZED (
            SELECT
                task_id,
                warehouse_id,
                project_id,
                queue_name,
                entity_id,
                entity_type,
                entity_name,
                status as task_status,
                null::task_final_status as task_log_status,
                scheduled_for as attempt_scheduled_for,
                picked_up_at as started_at,
                attempt,
                last_heartbeat_at,
                progress,
                parent_task_id,
                created_at as task_created_at,
                updated_at,
                resource_demand
            FROM task
            WHERE ($18 OR project_id = $15)
                AND CASE
                    WHEN $16 THEN $17 OR warehouse_id IS NULL -- project-level tasks
                    ELSE warehouse_id = $1 -- warehouse-level tasks
                END
                AND ((created_at < $3 OR $3 IS NULL) OR (created_at = $3 AND task_id < $4))
                -- Implied by the line above; lets the index scan start at the page token.
                AND (created_at <= $3 OR $3 IS NULL)
                AND ($6 OR queue_name = ANY($5))
                AND ($9 OR status = ANY($7::task_intermediate_status[]))
                AND (created_at >= $13 OR $13 IS NULL)
                AND (created_at <= $14 OR $14 IS NULL)
                AND ($19::uuid IS NULL OR parent_task_id = $19)
        ),
        -- Three disjoint branches, each limited on its own; without an entity filter the entity list is empty, so only the first returns rows.
        active_tasks AS (
            -- Evaluated at run time: Postgres 16 would otherwise cost the skipped branch as a full scan.
            (SELECT * FROM active_rows WHERE (SELECT $12::bool)
            ORDER BY task_created_at DESC, task_id DESC LIMIT $2)
            UNION ALL
            SELECT r.* FROM selected_entities se
            CROSS JOIN LATERAL (
                SELECT * FROM active_rows
                WHERE entity_type = se.entity_type AND entity_id = se.entity_id
                ORDER BY task_created_at DESC, task_id DESC LIMIT $2
            ) r
            WHERE se.entity_type <> 'warehouse'
            UNION ALL
            -- A warehouse-scoped task may carry the id of its schedule.
            (SELECT * FROM active_rows WHERE entity_type = 'warehouse'
                AND EXISTS (SELECT 1 FROM selected_entities se WHERE se.entity_type = 'warehouse')
            ORDER BY task_created_at DESC, task_id DESC LIMIT $2)
        ),
        log_rows AS NOT MATERIALIZED (
            SELECT
                task_id,
                warehouse_id,
                project_id,
                queue_name,
                entity_id,
                entity_type,
                entity_name,
                null::task_intermediate_status as task_status,
                status as task_log_status,
                attempt_scheduled_for,
                started_at,
                attempt,
                last_heartbeat_at,
                progress,
                parent_task_id,
                task_created_at,
                null::timestamptz as updated_at,
                resource_demand
            FROM task_log
            WHERE ($18 OR project_id = $15)
                AND CASE
                    WHEN $16 THEN $17 OR warehouse_id IS NULL -- project-level tasks
                    ELSE warehouse_id = $1
                END
                AND ((task_created_at < $3 OR $3 IS NULL) OR (task_created_at = $3 AND task_id < $4))
                AND (task_created_at <= $3 OR $3 IS NULL)
                AND ($6 OR queue_name = ANY($5))
                AND ($9 OR status = ANY($8::task_final_status[]))
                AND (task_created_at >= $13 OR $13 IS NULL)
                AND (task_created_at <= $14 OR $14 IS NULL)
                AND ($19::uuid IS NULL OR parent_task_id = $19)
        ),
        -- Same branches as active_tasks; DISTINCT ON keeps the newest attempt of each task.
        log_tasks AS (
            (SELECT DISTINCT ON (task_created_at, task_id) * FROM log_rows WHERE (SELECT $12::bool)
            ORDER BY task_created_at DESC, task_id DESC, attempt DESC LIMIT $2)
            UNION ALL
            SELECT r.* FROM selected_entities se
            CROSS JOIN LATERAL (
                SELECT DISTINCT ON (task_created_at, task_id) * FROM log_rows
                WHERE entity_type = se.entity_type AND entity_id = se.entity_id
                ORDER BY task_created_at DESC, task_id DESC, attempt DESC LIMIT $2
            ) r
            WHERE se.entity_type <> 'warehouse'
            UNION ALL
            (SELECT DISTINCT ON (task_created_at, task_id) * FROM log_rows WHERE entity_type = 'warehouse'
                AND EXISTS (SELECT 1 FROM selected_entities se WHERE se.entity_type = 'warehouse')
            ORDER BY task_created_at DESC, task_id DESC, attempt DESC LIMIT $2)
        )
        SELECT 
            task_id AS "task_id!",
            warehouse_id,
            project_id AS "project_id!",
            queue_name AS "queue_name!",
            entity_id,
            entity_type as "entity_type!: TaskEntityTypeDB",
            entity_name as "entity_name: Vec<String>",
            task_status as "task_status: TaskIntermediateStatus",
            task_log_status as "task_log_status: TaskOutcome",
            attempt_scheduled_for as "attempt_scheduled_for!",
            started_at,
            attempt as "attempt!",
            last_heartbeat_at,
            progress as "progress!",
            parent_task_id,
            task_created_at as "task_created_at!",
            updated_at,
            resource_demand
         FROM (
            SELECT * FROM active_tasks
            UNION ALL
            SELECT * FROM log_tasks
        ) as combined
        ORDER BY task_created_at DESC, task_id DESC
        LIMIT $2
        "#,
        warehouse_id.map(|id| **id), // 1
        page_size as i64, // 2
        pagination_ts, // 3
        pagination_task_id.copied(), // 4
        &queue_names, // 5
        queue_names_is_none, // 6
        task_status_filter.iter().collect_vec() as Vec<_>, // 7
        task_log_status_filter.iter().collect_vec() as Vec<_>, // 8
        status_filter_is_none, // 9
        entity_ids as Vec<_>, // 10
        entity_types as Vec<_>, // 11
        entities_filter_is_none, // 12
        created_after, // 13
        created_before, // 14
        &project_id.map(|p| p.as_str()).unwrap_or_default(), // 15
        warehouse_id.is_none(), // 16
        include_sub_tasks, // 17
        project_id.is_none(), // 18
        parent_task_id.map(|id| *id), // 19
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(|e| e.into_error_model("Failed to list tasks"))?;

    let tasks = tasks
        .into_iter()
        .map(parse_task)
        .collect::<Result<Vec<_>, _>>()?;

    let next_page_token = tasks
        .last()
        .map(|last_task| {
            PaginateToken::V1(V1PaginateToken {
                created_at: last_task.created_at,
                id: *last_task.task_id(),
            })
            .to_string()
        })
        .or(previous_page_token);

    Ok(TaskList {
        tasks,
        next_page_token,
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use lakekeeper::{
        ProjectId, WarehouseId,
        api::{
            RequestMetadata,
            management::v1::{
                ApiServer,
                project::{CreateProjectRequest, Service},
                tasks::{ListTasksRequest, TaskStatus},
            },
        },
        service::{
            ArcProjectId,
            authz::AllowAllAuthorizer,
            tasks::{
                DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT, EnqueueOutcome, ScheduleTaskMetadata,
                TaskEntity, TaskInput, TaskIntermediateStatus, TaskOutcome, TaskQueueName,
                TaskScheduleUpsert, WarehouseTaskEntityId,
            },
        },
    };
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        tasks::{
            pick_task, queue_task_batch, record_failure_without_backoff, record_success,
            test::setup_warehouse, trigger_task_schedule, upsert_task_schedule,
        },
        tests::get_api_context,
    };

    #[test]
    fn test_split_task_status() {
        let (i_status, f_status) = categorize_task_statuses(&[
            TaskStatus::Failed,
            TaskStatus::Scheduled,
            TaskStatus::Running,
            TaskStatus::Failed,
            TaskStatus::Success,
        ]);
        assert_eq!(
            i_status,
            HashSet::from([
                TaskIntermediateStatus::Scheduled,
                TaskIntermediateStatus::Running
            ])
        );
        assert_eq!(
            f_status,
            HashSet::from([TaskOutcome::Failed, TaskOutcome::Success])
        );

        let (i_status, f_status) = categorize_task_statuses(&[TaskStatus::Scheduled]);
        assert_eq!(i_status, HashSet::from([TaskIntermediateStatus::Scheduled]));
        assert!(f_status.is_empty());

        let (i_status, f_status) = categorize_task_statuses(&[TaskStatus::Success]);
        assert!(i_status.is_empty());
        assert_eq!(f_status, HashSet::from([TaskOutcome::Success]));

        let (i_status, f_status) = categorize_task_statuses(&[]);
        assert!(i_status.is_empty());
        assert!(f_status.is_empty());
    }

    async fn queue_task_helper(
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        entity_id: WarehouseTaskEntityId,
        project_id: ArcProjectId,
        warehouse_id: WarehouseId,
        payload: Option<serde_json::Value>,
    ) -> Result<lakekeeper::service::tasks::TaskId, IcebergErrorResponse> {
        queue_wh_task_helper_with_entity_name(
            conn,
            queue_name,
            entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id,
            warehouse_id,
            payload,
        )
        .await
    }

    async fn queue_wh_task_helper_with_entity_name(
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        entity_id: WarehouseTaskEntityId,
        entity_name: Vec<String>,
        project_id: ArcProjectId,
        warehouse_id: WarehouseId,
        payload: Option<serde_json::Value>,
    ) -> Result<lakekeeper::service::tasks::TaskId, IcebergErrorResponse> {
        let result = super::super::queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand: None,
                    project_id,
                    parent_task_id: None,
                    entity: TaskEntity::EntityInWarehouse {
                        warehouse_id,
                        entity_id,
                        entity_name,
                    },
                    scheduled_for: None,
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

    async fn queue_warehouse_task_helper(
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        project_id: ArcProjectId,
        warehouse_id: WarehouseId,
    ) -> TaskId {
        queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand: None,
                    project_id,
                    parent_task_id: None,
                    entity: TaskEntity::Warehouse { warehouse_id },
                    scheduled_for: None,
                },
                payload: serde_json::json!({}),
            }],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
        .task_id
    }

    /// Picks the next task of the queue, which must be `expected`, and records its success.
    async fn complete_next_task(
        pool: &PgPool,
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        expected: TaskId,
    ) {
        let task = pick_task(pool, queue_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id(), expected);
        record_success(&task, conn, None).await.unwrap();
    }

    async fn list_warehouse_tasks(
        conn: &mut sqlx::PgConnection,
        warehouse_id: WarehouseId,
        project_id: &ArcProjectId,
        request: &ListTasksRequest,
    ) -> TaskList {
        list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            request,
            conn,
        )
        .await
        .unwrap()
    }

    fn listed_attempts(list: &TaskList) -> Vec<(TaskId, i32, TaskStatus)> {
        list.tasks
            .iter()
            .map(|t| (t.task_id(), t.id.attempt, t.status))
            .collect()
    }

    #[sqlx::test]
    async fn test_list_tasks_empty_warehouse(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert!(result.tasks.is_empty());
        assert!(result.next_page_token.is_none());
    }

    #[sqlx::test]
    async fn test_list_tasks_single_active_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_name = vec!["ns".to_string(), "table".to_string()];
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"test": "data"});

        // Queue a task
        let task_id = queue_wh_task_helper_with_entity_name(
            &mut conn,
            &tq_name,
            entity_id,
            entity_name.clone(),
            project_id.clone(),
            warehouse_id,
            Some(payload.clone()),
        )
        .await
        .unwrap();

        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        let task = &result.tasks[0];
        assert_eq!(task.task_id(), task_id);
        assert_eq!(task.task_metadata.entity_name(), Some(entity_name).as_ref());
        assert_eq!(task.task_metadata.warehouse_id(), Some(warehouse_id));
        assert_eq!(task.queue_name.as_str(), tq_name.as_str());
        assert!(matches!(task.status, TaskStatus::Scheduled));
        assert_eq!(task.id.attempt, 0);
        assert!(task.picked_up_at.is_none());
        assert!(result.next_page_token.is_some());

        match task.task_metadata.entity_id() {
            Some(WarehouseTaskEntityId::Table { table_id }) => {
                assert_eq!(*table_id, entity_id.as_uuid());
            }
            _ => {
                panic!("Expected TaskEntity::Table")
            }
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_multiple_tasks_different_queues(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Queue tasks in different queues
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name1,
            entity_id1,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let task_id2 = queue_task_helper(
            &mut conn,
            &tq_name2,
            entity_id2,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 2);
        let task_ids: HashSet<_> = result
            .tasks
            .iter()
            .map(lakekeeper::service::tasks::TaskInfo::task_id)
            .collect();
        assert!(task_ids.contains(&task_id1));
        assert!(task_ids.contains(&task_id2));

        // Tasks should be ordered by created_at DESC
        assert!(result.tasks[0].created_at >= result.tasks[1].created_at);
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_queue_name(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Queue tasks in different queues
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name1,
            entity_id1,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let _task_id2 = queue_task_helper(
            &mut conn,
            &tq_name2,
            entity_id2,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Filter by first queue only
        let request = ListTasksRequest {
            queue_name: Some(vec![tq_name1.clone()]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id(), task_id1);
        assert_eq!(result.tasks[0].queue_name.as_str(), tq_name1.as_str());
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_status(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue two tasks
        queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id1,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id2,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Pick up one task to make it running
        let _picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Filter by running status only
        let request = ListTasksRequest {
            status: Some(vec![TaskStatus::Running]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert!(matches!(result.tasks[0].status, TaskStatus::Running));

        // Filter by scheduled status only
        let request = ListTasksRequest {
            status: Some(vec![TaskStatus::Scheduled]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert!(matches!(result.tasks[0].status, TaskStatus::Scheduled));
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_entity(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue tasks for different entities
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id1,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let _task_id2 = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id2,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Filter by first entity only
        let request = ListTasksRequest {
            entities: Some(vec![WarehouseTaskEntityFilter::Table {
                table_id: entity_id1.as_uuid().into(),
            }]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id(), task_id1);

        match result.tasks[0].task_metadata.entity_id() {
            Some(WarehouseTaskEntityId::Table { table_id }) => {
                assert_eq!(*table_id, entity_id1.as_uuid());
            }
            _ => {
                panic!("Expected TaskEntity::Table")
            }
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_with_repeated_entity_filters_lists_each_task_once(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();
        let table_task = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let warehouse_task = super::super::queue_task_batch(
            &mut conn,
            &tq_name,
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand: None,
                    project_id: project_id.clone(),
                    parent_task_id: None,
                    entity: TaskEntity::Warehouse { warehouse_id },
                    scheduled_for: None,
                },
                payload: serde_json::json!({}),
            }],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
        .task_id;

        let table = WarehouseTaskEntityFilter::Table {
            table_id: entity_id.as_uuid().into(),
        };
        let request = ListTasksRequest {
            entities: Some(vec![
                table.clone(),
                WarehouseTaskEntityFilter::Warehouse,
                table,
                WarehouseTaskEntityFilter::Warehouse,
            ]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(
            result
                .tasks
                .iter()
                .map(lakekeeper::service::tasks::TaskInfo::task_id)
                .collect_vec(),
            vec![warehouse_task, table_task]
        );
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_generic_table_entity(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let table_entity = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let gt_entity = WarehouseTaskEntityId::GenericTable {
            generic_table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        let _table_task = queue_task_helper(
            &mut conn,
            &tq_name,
            table_entity,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let gt_task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            gt_entity,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        let request = ListTasksRequest {
            entities: Some(vec![WarehouseTaskEntityFilter::GenericTable {
                generic_table_id: gt_entity.as_uuid().into(),
            }]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id(), gt_task_id);
        match result.tasks[0].task_metadata.entity_id() {
            Some(WarehouseTaskEntityId::GenericTable { generic_table_id }) => {
                assert_eq!(*generic_table_id, gt_entity.as_uuid());
            }
            other => panic!("Expected TaskEntity::GenericTable, got {other:?}"),
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_filter_by_created_date_range(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Create a reference time
        let now = Utc::now();
        let before_time = now - chrono::Duration::hours(1);
        let after_time = now + chrono::Duration::hours(1);

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Filter with created_after (should include our task)
        let request = ListTasksRequest {
            created_after: Some(before_time),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id(), task_id);

        // Filter with created_before (should include our task)
        let request = ListTasksRequest {
            created_before: Some(after_time),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id(), task_id);

        // Filter with created_after that excludes our task
        let request = ListTasksRequest {
            created_after: Some(after_time),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
        assert!(result.tasks.is_empty());

        // Filter with created_before that excludes our task
        let request = ListTasksRequest {
            created_before: Some(before_time),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
        assert!(result.tasks.is_empty());
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Queue 5 tasks
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();
        for _ in 0..5 {
            let entity_id = WarehouseTaskEntityId::Table {
                table_id: Uuid::now_v7().into(),
            };
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                entity_id,
                project_id.clone(),
                warehouse_id,
                None,
            )
            .await
            .unwrap();
            task_ids.push(task_id);
        }

        // Get first page with page_size=2
        let request = ListTasksRequest {
            page_size: Some(2),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
        seen_ids.extend(
            result
                .tasks
                .iter()
                .map(lakekeeper::service::tasks::TaskInfo::task_id),
        );

        assert_eq!(result.tasks.len(), 2);
        assert!(result.next_page_token.is_some());

        // Get second page
        let request = ListTasksRequest {
            page_size: Some(2),
            page_token: result.next_page_token,
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
        seen_ids.extend(
            result
                .tasks
                .iter()
                .map(lakekeeper::service::tasks::TaskInfo::task_id),
        );

        assert_eq!(result.tasks.len(), 2);
        assert!(result.next_page_token.is_some());

        // Get third page (should have 1 task)
        let request = ListTasksRequest {
            page_size: Some(2),
            page_token: result.next_page_token,
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
        seen_ids.extend(
            result
                .tasks
                .iter()
                .map(lakekeeper::service::tasks::TaskInfo::task_id),
        );

        assert_eq!(result.tasks.len(), 1);
        assert!(result.next_page_token.is_some());

        // Get fourth page (should be empty)
        let request = ListTasksRequest {
            page_size: Some(2),
            page_token: result.next_page_token,
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id: project_id.clone(),
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

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
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();
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
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Create 10 tasks - mix of active and completed
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        for i in 0..10 {
            let entity_id = WarehouseTaskEntityId::Table {
                table_id: Uuid::now_v7().into(),
            };
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                entity_id,
                project_id.clone(),
                warehouse_id,
                Some(serde_json::json!({"index": i})),
            )
            .await
            .unwrap();
            task_ids.push(task_id);
        }

        // Complete some tasks (first 4)
        for &task_id in &task_ids[0..4] {
            let picked_task =
                pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
            let picked_task =
                pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(picked_task.task_id(), task_id);
            record_failure_without_backoff(&picked_task, 1, "Task failed", &mut conn)
                .await
                .unwrap();
        }

        // Cancel some tasks (next 2)
        super::super::cancel_scheduled_tasks(
            &mut conn,
            lakekeeper::service::tasks::CancelTasksFilter::TaskIds(task_ids[6..8].to_vec()),
            Some(&tq_name),
            &[],
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
            let result = list_tasks(
                &TaskFilter::WarehouseId {
                    warehouse_id,
                    project_id: project_id.clone(),
                },
                &request,
                &mut conn,
            )
            .await
            .unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(
                all_tasks
                    .iter()
                    .map(lakekeeper::service::tasks::TaskInfo::task_id),
            );
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
        let outcome_types = all_tasks.iter().map(|t| &t.status).collect_vec();
        let has_success = outcome_types
            .iter()
            .any(|s| matches!(s, TaskStatus::Success));
        let has_failed = outcome_types
            .iter()
            .any(|s| matches!(s, TaskStatus::Failed));
        let has_cancelled = outcome_types
            .iter()
            .any(|s| matches!(s, TaskStatus::Cancelled));
        let status_types: Vec<_> = all_tasks.iter().map(|t| &t.status).collect();
        let has_scheduled = status_types
            .iter()
            .any(|s| matches!(s, TaskStatus::Scheduled));

        assert!(has_success);
        assert!(has_failed);
        assert!(has_cancelled);
        assert!(has_scheduled);
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination_only_completed_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Create and complete 6 tasks
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        for i in 0..6 {
            let entity_id = WarehouseTaskEntityId::Table {
                table_id: Uuid::now_v7().into(),
            };
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                entity_id,
                project_id.clone(),
                warehouse_id,
                Some(serde_json::json!({"completed_index": i})),
            )
            .await
            .unwrap();
            task_ids.push(task_id);

            // Pick up and complete immediately
            let picked_task =
                pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
            let result = list_tasks(
                &TaskFilter::WarehouseId {
                    warehouse_id,
                    project_id: project_id.clone(),
                },
                &request,
                &mut conn,
            )
            .await
            .unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(
                all_tasks
                    .iter()
                    .map(lakekeeper::service::tasks::TaskInfo::task_id),
            );
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
            assert!(matches!(task.status, TaskStatus::Success));
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
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();

        // Create 8 tasks with different outcomes
        let mut task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        for i in 0..8 {
            let entity_id = WarehouseTaskEntityId::Table {
                table_id: Uuid::now_v7().into(),
            };
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name,
                entity_id,
                project_id.clone(),
                warehouse_id,
                Some(serde_json::json!({"retry_test_index": i})),
            )
            .await
            .unwrap();
            task_ids.push(task_id);
        }

        // Task 0: Success on first try
        let task0 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task0, &mut conn, Some("Success on first try"))
            .await
            .unwrap();

        // Task 1: Fail once, then succeed
        let task1 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure_without_backoff(&task1, 2, "First attempt failed", &mut conn)
            .await
            .unwrap();

        let task1_retry = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task1_retry.task_id(), task1.task_id());
        record_success(&task1_retry, &mut conn, Some("Success on retry"))
            .await
            .unwrap();

        // Task 2: Fail multiple times, eventually fail permanently
        let task2 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure_without_backoff(&task2, 1, "Failed permanently", &mut conn)
            .await
            .unwrap();

        // Task 3: Cancel while scheduled
        super::super::cancel_scheduled_tasks(
            &mut conn,
            lakekeeper::service::tasks::CancelTasksFilter::TaskIds(vec![task_ids[3]]),
            Some(&tq_name),
            &[],
            false,
        )
        .await
        .unwrap();

        // Task 4: Pick up and leave running
        let _task4_running = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
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
            let result = list_tasks(
                &TaskFilter::WarehouseId {
                    warehouse_id,
                    project_id: project_id.clone(),
                },
                &request,
                &mut conn,
            )
            .await
            .unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(
                all_tasks
                    .iter()
                    .map(lakekeeper::service::tasks::TaskInfo::task_id),
            );
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
                TaskStatus::Success => *status_counts.entry("Success").or_insert(0) += 1,
                TaskStatus::Failed => *status_counts.entry("Failed").or_insert(0) += 1,
                TaskStatus::Cancelled => *status_counts.entry("Cancelled").or_insert(0) += 1,
                TaskStatus::Running => *status_counts.entry("Running").or_insert(0) += 1,
                TaskStatus::Scheduled => *status_counts.entry("Scheduled").or_insert(0) += 1,
                TaskStatus::Stopping => *status_counts.entry("Stopping").or_insert(0) += 1,
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
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Create tasks in two different queues
        let mut all_task_ids = Vec::new();
        let mut seen_ids = HashSet::new();

        // Queue 1: 3 tasks
        for i in 0..3 {
            let entity_id = WarehouseTaskEntityId::Table {
                table_id: Uuid::now_v7().into(),
            };
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name1,
                entity_id,
                project_id.clone(),
                warehouse_id,
                Some(serde_json::json!({"queue1_index": i})),
            )
            .await
            .unwrap();
            all_task_ids.push(task_id);
        }

        // Queue 2: 4 tasks
        for i in 0..4 {
            let entity_id = WarehouseTaskEntityId::Table {
                table_id: Uuid::now_v7().into(),
            };
            let task_id = queue_task_helper(
                &mut conn,
                &tq_name2,
                entity_id,
                project_id.clone(),
                warehouse_id,
                Some(serde_json::json!({"queue2_index": i})),
            )
            .await
            .unwrap();
            all_task_ids.push(task_id);
        }

        // Complete some tasks from queue 1
        let task_q1_1 = pick_task(&pool, &tq_name1, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task_q1_1, &mut conn, Some("Queue 1 completed"))
            .await
            .unwrap();

        // Fail a task from queue 2
        let task_q2_1 = pick_task(&pool, &tq_name2, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure_without_backoff(&task_q2_1, 1, "Queue 2 failed", &mut conn)
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
            let result = list_tasks(
                &TaskFilter::WarehouseId {
                    warehouse_id,
                    project_id: project_id.clone(),
                },
                &request,
                &mut conn,
            )
            .await
            .unwrap();

            let has_more_tasks = !result.tasks.is_empty();
            all_tasks.extend(result.tasks);
            seen_ids.extend(
                all_tasks
                    .iter()
                    .map(lakekeeper::service::tasks::TaskInfo::task_id),
            );
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
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue and complete a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Pick up the task
        let picked_task = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Complete the task successfully
        record_success(&picked_task, &mut conn, Some("Task completed"))
            .await
            .unwrap();

        // List all tasks
        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        let task = &result.tasks[0];
        assert_eq!(task.task_id(), task_id);
        assert!(matches!(task.status, TaskStatus::Success));
        assert!(task.picked_up_at.is_some());
    }

    #[sqlx::test]
    async fn test_list_tasks_mixed_active_and_completed(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id3 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue three tasks
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id1,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let task_id2 = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id2,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let task_id3 = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id3,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Complete first task
        let picked_task1 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&picked_task1, &mut conn, Some("Completed"))
            .await
            .unwrap();

        // Pick up second task (keep it running)
        let _picked_task2 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Third task remains scheduled

        // List all tasks
        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 3);

        // Find tasks by ID and verify their status
        let task_statuses: std::collections::HashMap<_, _> = result
            .tasks
            .iter()
            .map(|t| (t.task_id(), &t.status))
            .collect();

        assert!(matches!(
            task_statuses.get(&task_id1).unwrap(),
            TaskStatus::Success
        ));
        assert!(matches!(
            task_statuses.get(&task_id2).unwrap(),
            TaskStatus::Running
        ));
        assert!(matches!(
            task_statuses.get(&task_id3).unwrap(),
            TaskStatus::Scheduled
        ));
    }

    #[sqlx::test]
    async fn test_list_tasks_with_retries(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // First attempt - pick and fail
        let task1 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure_without_backoff(&task1, 5, "First attempt failed", &mut conn)
            .await
            .unwrap();

        // Second attempt - pick and succeed
        let task2 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_success(&task2, &mut conn, Some("Second attempt succeeded"))
            .await
            .unwrap();

        // List all tasks - should show the successful attempt
        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        let task = &result.tasks[0];
        assert_eq!(task.task_id(), task_id);
        assert!(matches!(task.status, TaskStatus::Success));
        assert_eq!(task.id.attempt, 2); // Should show the successful attempt
    }

    #[sqlx::test]
    async fn test_list_tasks_wrong_warehouse(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let wrong_warehouse_id = WarehouseId::from(Uuid::now_v7());
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue a task in the correct warehouse
        let _task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            entity_id,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Try to list tasks from wrong warehouse
        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id: wrong_warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        // Should return empty list
        assert!(result.tasks.is_empty());
        assert!(result.next_page_token.is_none());
    }

    #[sqlx::test]
    async fn test_list_tasks_complex_filters(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name1 = generate_tq_name();
        let tq_name2 = generate_tq_name();

        // Queue tasks in different queues and entities
        let task_id1 = queue_task_helper(
            &mut conn,
            &tq_name1,
            entity_id1,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let _task_id2 = queue_task_helper(
            &mut conn,
            &tq_name1,
            entity_id2,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let _task_id3 = queue_task_helper(
            &mut conn,
            &tq_name2,
            entity_id1,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        // Filter by queue_name AND entity
        let request = ListTasksRequest {
            queue_name: Some(vec![tq_name1.clone()]),
            entities: Some(vec![WarehouseTaskEntityFilter::Table {
                table_id: entity_id1.as_uuid().into(),
            }]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].task_id(), task_id1);
        assert_eq!(result.tasks[0].queue_name.as_str(), tq_name1.as_str());

        match result.tasks[0].task_metadata.entity_id() {
            Some(WarehouseTaskEntityId::Table { table_id }) => {
                assert_eq!(*table_id, entity_id1.as_uuid());
            }
            _ => {
                panic!("Expected TaskEntity::Table")
            }
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_with_warehouse_filter_only_shows_warehouse_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let tq_name = generate_tq_name();

        // Queue warehouse and project task
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    payload: serde_json::json!({"data": "project_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        entity: TaskEntity::Project,
                        scheduled_for: None,
                    },
                },
                TaskInput {
                    payload: serde_json::json!({"data": "warehouse_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        entity: TaskEntity::EntityInWarehouse {
                            entity_id,
                            entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
                            warehouse_id,
                        },
                        scheduled_for: None,
                    },
                },
            ],
        )
        .await
        .unwrap();
        // Queue project task

        // Try to list tasks from wrong warehouse
        let request = ListTasksRequest::default();
        let result = list_tasks(
            &TaskFilter::WarehouseId {
                warehouse_id,
                project_id,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        let task = result.tasks.first();
        let Some(task) = task else {
            panic!("Expected to find a task");
        };
        assert_eq!(task.task_metadata.warehouse_id(), Some(warehouse_id));
        assert_eq!(result.tasks.len(), 1);
    }

    #[sqlx::test]
    async fn test_list_tasks_with_project_filter_only_shows_project_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let tq_name = generate_tq_name();

        // Queue warehouse and project task
        let entity_id_1 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_id_2 = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    payload: serde_json::json!({"data": "warehouse_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        entity: TaskEntity::EntityInWarehouse {
                            entity_id: entity_id_1,
                            entity_name: vec![format!("entity-{}", entity_id_1.as_uuid())],
                            warehouse_id,
                        },
                        scheduled_for: None,
                    },
                },
                TaskInput {
                    payload: serde_json::json!({"data": "project_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        entity: TaskEntity::Project,
                        scheduled_for: None,
                    },
                },
                TaskInput {
                    payload: serde_json::json!({"data": "warehouse_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        entity: TaskEntity::EntityInWarehouse {
                            warehouse_id,
                            entity_id: entity_id_2,
                            entity_name: vec![format!("entity-{}", entity_id_2.as_uuid())],
                        },
                        scheduled_for: None,
                    },
                },
            ],
        )
        .await
        .unwrap();

        // Try to list tasks - filter by queue_name to exclude bootstrap tasks
        let request = ListTasksRequest {
            queue_name: Some(vec![tq_name.clone()]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::ProjectId {
                project_id,
                include_sub_tasks: false,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        let task = result.tasks.first();
        let Some(task) = task else {
            panic!("Expected to find a task");
        };

        assert_eq!(task.task_metadata.warehouse_id(), None);
        assert_eq!(result.tasks.len(), 1);
    }

    #[sqlx::test]
    async fn test_list_tasks_with_project_filter_and_include_sub_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let context = get_api_context(&pool, AllowAllAuthorizer::default()).await;

        // Prepare second project to ignore
        let other_project = ApiServer::create_project(
            CreateProjectRequest {
                project_id: Some(ProjectId::new_random()),
                project_name: "Other Project".to_string(),
            },
            context,
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tq_name = generate_tq_name();

        // Queue warehouse and project task
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        queue_task_batch(
            &mut conn,
            &tq_name,
            vec![
                TaskInput {
                    payload: serde_json::json!({"data": "other_project_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: other_project.project_id,
                        parent_task_id: None,
                        entity: TaskEntity::Project,
                        scheduled_for: None,
                    },
                },
                TaskInput {
                    payload: serde_json::json!({"data": "warehouse_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        entity: TaskEntity::EntityInWarehouse {
                            warehouse_id,
                            entity_id,
                            entity_name: vec![format!("entity-{}", entity_id.as_uuid())],
                        },
                        scheduled_for: None,
                    },
                },
                TaskInput {
                    payload: serde_json::json!({"data": "project_task"}),
                    task_metadata: ScheduleTaskMetadata {
                        resource_demand: None,
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        entity: TaskEntity::Project,
                        scheduled_for: None,
                    },
                },
            ],
        )
        .await
        .unwrap();

        // List tasks - filter by queue_name to exclude bootstrap tasks
        let request = ListTasksRequest {
            queue_name: Some(vec![tq_name.clone()]),
            ..Default::default()
        };
        let result = list_tasks(
            &TaskFilter::ProjectId {
                project_id: project_id.clone(),
                include_sub_tasks: true,
            },
            &request,
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(result.tasks.len(), 2);
        for task in &result.tasks {
            if let Some(task_wh_id) = task.task_metadata.warehouse_id() {
                assert_eq!(task_wh_id, warehouse_id);
            }
            assert_eq!(task.project_id(), &*project_id);
        }
    }

    #[sqlx::test]
    async fn test_list_tasks_with_table_and_warehouse_filter_lists_active_and_finished(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let table = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let other_table = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // One finished and one active task each for the table, the other table and the warehouse.
        let table_done = queue_task_helper(
            &mut conn,
            &tq_name,
            table,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        complete_next_task(&pool, &mut conn, &tq_name, table_done).await;
        let other_done = queue_task_helper(
            &mut conn,
            &tq_name,
            other_table,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        complete_next_task(&pool, &mut conn, &tq_name, other_done).await;
        let warehouse_done =
            queue_warehouse_task_helper(&mut conn, &tq_name, project_id.clone(), warehouse_id)
                .await;
        complete_next_task(&pool, &mut conn, &tq_name, warehouse_done).await;
        let table_active = queue_task_helper(
            &mut conn,
            &tq_name,
            table,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let _other_active = queue_task_helper(
            &mut conn,
            &tq_name,
            other_table,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();
        let warehouse_active =
            queue_warehouse_task_helper(&mut conn, &tq_name, project_id.clone(), warehouse_id)
                .await;

        let request = ListTasksRequest {
            entities: Some(vec![
                WarehouseTaskEntityFilter::Table {
                    table_id: table.as_uuid().into(),
                },
                WarehouseTaskEntityFilter::Warehouse,
            ]),
            ..Default::default()
        };
        let result = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;

        assert_eq!(
            listed_attempts(&result),
            vec![
                (warehouse_active, 0, TaskStatus::Scheduled),
                (table_active, 0, TaskStatus::Scheduled),
                (warehouse_done, 1, TaskStatus::Success),
                (table_done, 1, TaskStatus::Success),
            ]
        );
    }

    #[sqlx::test]
    async fn test_list_tasks_scheduled_warehouse_task_matches_warehouse_filter_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let schedule = upsert_task_schedule(
            &mut conn,
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .warehouse_id(Some(warehouse_id))
                .queue_name(tq_name.clone())
                .schedule_key("list-tasks")
                .interval(chrono::Duration::minutes(1))
                .build(),
        )
        .await
        .unwrap();

        // Tasks of a schedule carry the schedule id as their entity id.
        let EnqueueOutcome::Inserted(done) = trigger_task_schedule(&mut conn, schedule.schedule_id)
            .await
            .unwrap()
        else {
            panic!("Expected a new task");
        };
        complete_next_task(&pool, &mut conn, &tq_name, done).await;
        let EnqueueOutcome::Inserted(active) =
            trigger_task_schedule(&mut conn, schedule.schedule_id)
                .await
                .unwrap()
        else {
            panic!("Expected a new task");
        };

        let request = ListTasksRequest {
            entities: Some(vec![WarehouseTaskEntityFilter::Warehouse]),
            ..Default::default()
        };
        let result = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        assert_eq!(
            result
                .tasks
                .iter()
                .map(|t| (t.task_id(), t.task_metadata.schedule_id))
                .collect_vec(),
            vec![
                (active, Some(schedule.schedule_id)),
                (done, Some(schedule.schedule_id)),
            ]
        );

        // A table filter with the same id matches neither.
        let request = ListTasksRequest {
            entities: Some(vec![WarehouseTaskEntityFilter::Table {
                table_id: (*schedule.schedule_id).into(),
            }]),
            ..Default::default()
        };
        let result = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        assert_eq!(listed_attempts(&result), vec![]);
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination_with_table_filter_over_finished_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let table = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let other_table = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };

        // Finished tasks of the two tables, alternating in time.
        let mut table_tasks = Vec::new();
        for _ in 0..3 {
            for entity_id in [table, other_table] {
                let task_id = queue_task_helper(
                    &mut conn,
                    &tq_name,
                    entity_id,
                    project_id.clone(),
                    warehouse_id,
                    None,
                )
                .await
                .unwrap();
                complete_next_task(&pool, &mut conn, &tq_name, task_id).await;
                if entity_id == table {
                    table_tasks.push(task_id);
                }
            }
        }

        let entities = Some(vec![WarehouseTaskEntityFilter::Table {
            table_id: table.as_uuid().into(),
        }]);
        let request = ListTasksRequest {
            entities: entities.clone(),
            page_size: Some(2),
            ..Default::default()
        };
        let page_1 = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        assert_eq!(
            listed_attempts(&page_1),
            vec![
                (table_tasks[2], 1, TaskStatus::Success),
                (table_tasks[1], 1, TaskStatus::Success),
            ]
        );

        let request = ListTasksRequest {
            entities,
            page_size: Some(2),
            page_token: page_1.next_page_token,
            ..Default::default()
        };
        let page_2 = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        assert_eq!(
            listed_attempts(&page_2),
            vec![(table_tasks[0], 1, TaskStatus::Success)]
        );
    }

    #[sqlx::test]
    async fn test_list_tasks_pagination_keeps_order_across_active_and_finished(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let done_queue = generate_tq_name();
        // Never picked, so its tasks stay active.
        let active_queue = generate_tq_name();

        let mut expected = Vec::new();
        for _ in 0..2 {
            let done = queue_task_helper(
                &mut conn,
                &done_queue,
                WarehouseTaskEntityId::Table {
                    table_id: Uuid::now_v7().into(),
                },
                project_id.clone(),
                warehouse_id,
                None,
            )
            .await
            .unwrap();
            complete_next_task(&pool, &mut conn, &done_queue, done).await;
            expected.push((done, 1, TaskStatus::Success));
            let active = queue_task_helper(
                &mut conn,
                &active_queue,
                WarehouseTaskEntityId::Table {
                    table_id: Uuid::now_v7().into(),
                },
                project_id.clone(),
                warehouse_id,
                None,
            )
            .await
            .unwrap();
            expected.push((active, 0, TaskStatus::Scheduled));
        }
        expected.reverse();

        let request = ListTasksRequest {
            page_size: Some(3),
            ..Default::default()
        };
        let page_1 = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        assert_eq!(listed_attempts(&page_1), expected[..3]);

        let request = ListTasksRequest {
            page_size: Some(3),
            page_token: page_1.next_page_token,
            ..Default::default()
        };
        let page_2 = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        assert_eq!(listed_attempts(&page_2), expected[3..]);
    }

    #[sqlx::test]
    async fn test_list_tasks_with_table_filter_lists_newest_finished_attempt(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let table = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            table,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        let attempt_1 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure_without_backoff(&attempt_1, 5, "First attempt failed", &mut conn)
            .await
            .unwrap();
        complete_next_task(&pool, &mut conn, &tq_name, task_id).await;

        let request = ListTasksRequest {
            entities: Some(vec![WarehouseTaskEntityFilter::Table {
                table_id: table.as_uuid().into(),
            }]),
            ..Default::default()
        };
        let result = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        assert_eq!(
            listed_attempts(&result),
            vec![(task_id, 2, TaskStatus::Success)]
        );
    }

    #[sqlx::test]
    async fn test_list_tasks_with_table_filter_lists_failed_attempt_and_active_retry(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let tq_name = generate_tq_name();
        let table = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            table,
            project_id.clone(),
            warehouse_id,
            None,
        )
        .await
        .unwrap();

        let attempt_1 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        record_failure_without_backoff(&attempt_1, 5, "First attempt failed", &mut conn)
            .await
            .unwrap();
        let attempt_2 = pick_task(&pool, &tq_name, &[], DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(attempt_2.task_id(), task_id);

        let request = ListTasksRequest {
            entities: Some(vec![WarehouseTaskEntityFilter::Table {
                table_id: table.as_uuid().into(),
            }]),
            ..Default::default()
        };
        let result = list_warehouse_tasks(&mut conn, warehouse_id, &project_id, &request).await;
        // Listed once from `task` and once from `task_log`. Both rows share the sort key, so
        // their order is not defined.
        let mut listed = listed_attempts(&result);
        listed.sort_by_key(|(_, attempt, _)| std::cmp::Reverse(*attempt));
        assert_eq!(
            listed,
            vec![
                (task_id, 2, TaskStatus::Running),
                (task_id, 1, TaskStatus::Failed),
            ]
        );
    }
}
