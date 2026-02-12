use chrono::{DateTime, Duration};
use iceberg_ext::catalog::rest::{ErrorModel};
use itertools::Itertools;
use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

use super::TaskEntityTypeDB;
use crate::{
    ProjectId,
    api::management::v1::tasks::TaskAttempt,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tasks::{task_entity_from_db, task_status_from_db},
    },
    service::{
        DatabaseIntegrityError, GetTaskDetailsError, TaskDetails,
        tasks::{
            TaskAttemptId, TaskDetailsScope, TaskId, TaskInfo, TaskIntermediateStatus,
            TaskMetadata, TaskOutcome,
        },
    },
};

#[derive(sqlx::FromRow, Debug)]
struct TaskDetailsRow {
    pub queue_name: String,
    pub entity_id: Option<uuid::Uuid>,
    pub entity_type: TaskEntityTypeDB,
    pub entity_name: Option<Vec<String>>,
    pub task_status: Option<TaskIntermediateStatus>,
    pub task_log_status: Option<TaskOutcome>,
    pub attempt_scheduled_for: DateTime<chrono::Utc>,
    pub started_at: Option<DateTime<chrono::Utc>>,
    pub attempt: i32,
    pub last_heartbeat_at: Option<DateTime<chrono::Utc>>,
    pub progress: f32,
    pub parent_task_id: Option<Uuid>,
    pub task_created_at: DateTime<chrono::Utc>,
    pub attempt_created_at: Option<DateTime<chrono::Utc>>,
    pub updated_at: Option<DateTime<chrono::Utc>>,
    pub task_data: serde_json::Value,
    pub execution_details: Option<serde_json::Value>,
    pub duration: Option<PgInterval>,
    pub message: Option<String>,
    pub project_id: String,
    pub warehouse_id: Option<uuid::Uuid>,
}

#[allow(clippy::too_many_lines)]
fn parse_task_details(
    task_id: TaskId,
    mut records: Vec<TaskDetailsRow>,
) -> Result<Option<TaskDetails>, GetTaskDetailsError> {
    // Sort by attempt descending
    records.sort_by_key(|r| -r.attempt);
    if records.is_empty() {
        return Ok(None);
    }

    let most_recent = records.remove(0);
    let attempts = records
        .into_iter()
        .map(|r| {
            Result::<_, GetTaskDetailsError>::Ok(TaskAttempt {
                attempt: r.attempt,
                status: task_status_from_db(r.task_status, r.task_log_status)
                    .map_err(|e| DatabaseIntegrityError::new(e.to_string()))?,
                started_at: r.started_at,
                scheduled_for: r.attempt_scheduled_for,
                duration: r
                    .duration
                    .map(pg_interval_to_duration)
                    .transpose()
                    .map_err(|e| {
                        DatabaseIntegrityError::new(e.to_string())
                            .append_detail("Failed to parse task duration")
                    })?,
                message: r.message,
                created_at: r.attempt_created_at.ok_or_else(|| {
                    DatabaseIntegrityError::new("Task attempt is missing created_at timestamp.")
                })?,
                progress: r.progress,
                execution_details: r.execution_details,
            })
        })
        .try_collect()?;

    let scope = task_entity_from_db(
        most_recent.entity_type,
        most_recent.warehouse_id,
        most_recent.entity_id,
        most_recent.entity_name.clone(),
    )?;

    let status = task_status_from_db(most_recent.task_status, most_recent.task_log_status)
        .map_err(|e| DatabaseIntegrityError::new(e.to_string()))?;

    let task = TaskInfo {
        queue_name: most_recent.queue_name.into(),
        id: TaskAttemptId {
            task_id,
            attempt: most_recent.attempt,
        },
        task_metadata: TaskMetadata {
            project_id: ProjectId::from_db_unchecked(most_recent.project_id),
            parent_task_id: most_recent.parent_task_id.map(TaskId::from),
            scheduled_for: most_recent.attempt_scheduled_for,
            entity: scope,
        },
        status,
        picked_up_at: most_recent.started_at,
        created_at: most_recent.task_created_at,
        last_heartbeat_at: most_recent.last_heartbeat_at,
        updated_at: most_recent.updated_at,
        progress: most_recent.progress,
    };

    Ok(Some(TaskDetails {
        task,
        data: most_recent.task_data,
        attempts,
        execution_details: most_recent.execution_details,
    }))
}

fn pg_interval_to_duration(interval: PgInterval) -> Result<Duration, ErrorModel> {
    let PgInterval {
        months,
        days,
        microseconds,
    } = interval;

    if months != 0 {
        return Err(ErrorModel::internal(
            "Cannot convert PgInterval with non-zero months to Duration",
            "InternalError",
            None,
        ));
    }

    Ok(Duration::days(days.into()) + Duration::microseconds(microseconds))
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn get_task_details<'e, 'c: 'e, E>(
    task_id: TaskId,
    scope: TaskDetailsScope,
    num_attempts: u16,
    state: E,
) -> Result<Option<TaskDetails>, GetTaskDetailsError>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let project_id = scope.project_id();
    let warehouse_id = scope.warehouse_id();

    // Overwrite necessary due to:
    // https://github.com/launchbadge/sqlx/issues/1266
    let records = sqlx::query_as!(
        TaskDetailsRow,
        r#"
        SELECT
            project_id as "project_id!",
            warehouse_id,
            queue_name AS "queue_name!",
            entity_id AS "entity_id",
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
            attempt_created_at,
            updated_at,
            task_data as "task_data!",
            execution_details,
            duration,
            message
         FROM (
        SELECT
            project_id,
            warehouse_id,
            queue_name,
            entity_id,
            entity_type,
            entity_name,
            status as task_status,
            null as task_log_status,
            scheduled_for as attempt_scheduled_for,
            picked_up_at as started_at,
            attempt,
            last_heartbeat_at,
            progress,
            parent_task_id,
            created_at as task_created_at,
            null::timestamptz as attempt_created_at,
            updated_at,
            task_data,
            execution_details,
            case when picked_up_at is not null
                then now() - picked_up_at
                else null
            end as duration,
            null as message
        FROM task
        WHERE task_id = $1
            AND project_id = $2
            AND CASE
                    WHEN $3::uuid IS NULL THEN warehouse_id IS NULL -- project-level tasks
                    ELSE warehouse_id = $3 -- warehouse-level tasks
                END
        UNION ALL
        (SELECT
            project_id,
            warehouse_id,
            queue_name,
            entity_id,
            entity_type,
            entity_name,
            null as task_status,
            status as task_log_status,
            attempt_scheduled_for,
            started_at,
            attempt,
            last_heartbeat_at,
            progress,
            parent_task_id,
            task_created_at,
            created_at as attempt_created_at,
            null as updated_at,
            task_data,
            execution_details,
            duration,
            message
        FROM task_log        
        WHERE task_id = $1
            AND project_id = $2
            AND CASE
                    WHEN $3::uuid IS NULL THEN warehouse_id IS NULL -- project-level tasks
                    ELSE warehouse_id = $3 -- warehouse-level tasks
                END
        ORDER BY attempt desc 
        LIMIT $4 + 1
        )) as combined
        "#,
        *task_id,
        project_id.as_str(),
        warehouse_id.map(|id| *id),
        i32::from(num_attempts), // Query limit is num_attempts + 1 to handle the case where there's an active task plus historical attempts
    )
    .fetch_all(state)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let result = parse_task_details(task_id, records)?;

    Ok(if let Some(mut result) = result {
        if result.attempts.len() > num_attempts as usize {
            result.attempts.truncate(num_attempts as usize);
        }
        Some(result)
    } else {
        None
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use sqlx::{PgPool, postgres::types::PgInterval};
    use uuid::Uuid;

    use super::*;
    use crate::{
        ProjectId, WarehouseId,
        api::management::v1::tasks::TaskStatus,
        implementations::postgres::tasks::{
            check_and_heartbeat_task, pick_task, queue_task_batch, record_failure, record_success,
            test::setup_warehouse,
        },
        service::tasks::{
            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT, ScheduleTaskMetadata, TaskCheckState,
            TaskEntity, TaskInput, TaskIntermediateStatus, TaskOutcome, TaskQueueName,
            WarehouseTaskEntityId,
        },
    };

    #[allow(clippy::too_many_arguments)]
    fn create_test_row(
        queue_name: &str,
        entity_id: Uuid,
        attempt: i32,
        task_status: Option<TaskIntermediateStatus>,
        task_log_status: Option<TaskOutcome>,
        scheduled_for: DateTime<Utc>,
        started_at: Option<DateTime<Utc>>,
        task_created_at: DateTime<Utc>,
        attempt_created_at: Option<DateTime<Utc>>,
        project_id: &str,
        warehouse_id: Option<Uuid>,
    ) -> TaskDetailsRow {
        TaskDetailsRow {
            queue_name: queue_name.to_string(),
            entity_id: Some(entity_id),
            entity_type: TaskEntityTypeDB::Table,
            entity_name: Some(vec!["ns1".to_string(), "table1".to_string()]),
            task_status,
            task_log_status,
            attempt_scheduled_for: scheduled_for,
            started_at,
            attempt,
            last_heartbeat_at: None,
            progress: 0.5,
            parent_task_id: None,
            task_created_at,
            attempt_created_at,
            updated_at: None,
            task_data: serde_json::json!({"test": "data"}),
            execution_details: Some(serde_json::json!({"details": "test"})),
            duration: Some(PgInterval {
                months: 0,
                days: 0,
                microseconds: 3_600_000_000, // 1 hour
            }),
            message: Some("Test message".to_string()),
            project_id: project_id.to_string(),
            warehouse_id,
        }
    }

    #[test]
    fn test_parse_task_details_empty_records() {
        let task_id = TaskId::from(Uuid::now_v7());
        let records = vec![];

        let result = parse_task_details(task_id, records).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_task_details_active_task_only() {
        let task_id = TaskId::from(Uuid::now_v7());
        let warehouse_id = WarehouseId::from(Uuid::now_v7());
        let project_id = ProjectId::new_random();
        let entity_id = Uuid::now_v7();
        let scheduled_for = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let started_at = Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap());
        let task_created_at = Utc.with_ymd_and_hms(2024, 1, 1, 11, 0, 0).unwrap();

        let records = vec![create_test_row(
            "test-queue",
            entity_id,
            1,
            Some(TaskIntermediateStatus::Running),
            None,
            scheduled_for,
            started_at,
            task_created_at,
            None, // No attempt_created_at for active tasks,
            project_id.as_str(),
            Some(*warehouse_id),
        )];

        let result = parse_task_details(task_id, records).unwrap().unwrap();

        // Verify main task details
        assert_eq!(result.task.task_id(), task_id);
        assert_eq!(result.task.task_metadata.warehouse_id(), Some(warehouse_id));
        assert_eq!(result.task.queue_name.as_str(), "test-queue");
        assert_eq!(result.task.id.attempt, 1);
        assert_eq!(result.task.task_metadata.scheduled_for, scheduled_for);
        assert_eq!(result.task.picked_up_at, started_at);
        assert_eq!(result.task.created_at, task_created_at);
        assert!((result.task.progress - 0.5).abs() < f32::EPSILON);
        assert!(matches!(result.task.status, TaskStatus::Running));

        match result.task.task_metadata.entity_id() {
            Some(WarehouseTaskEntityId::Table { table_id }) => {
                assert_eq!(*table_id, entity_id);
            }
            _ => {
                panic!("Expected TaskEntity::Table")
            }
        }

        // Verify task data
        assert_eq!(result.data, serde_json::json!({"test": "data"}));
        assert_eq!(
            result.execution_details,
            Some(serde_json::json!({"details": "test"}))
        );

        // No historical attempts for active task only
        assert!(result.attempts.is_empty());
    }

    #[test]
    fn test_parse_task_details_active_with_history() {
        let task_id = TaskId::from(Uuid::now_v7());
        let warehouse_id = WarehouseId::from(Uuid::now_v7());
        let project_id = ProjectId::new_random();
        let entity_id = Uuid::now_v7();
        let scheduled_for = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let task_created_at = Utc.with_ymd_and_hms(2024, 1, 1, 11, 0, 0).unwrap();
        let attempt1_created_at = Utc.with_ymd_and_hms(2024, 1, 1, 11, 30, 0).unwrap();

        let records = vec![
            // Current running attempt (attempt 2)
            create_test_row(
                "test-queue",
                entity_id,
                2,
                Some(TaskIntermediateStatus::Running),
                None,
                scheduled_for,
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 10, 0).unwrap()),
                task_created_at,
                None,
                project_id.as_str(),
                Some(*warehouse_id),
            ),
            // Previous failed attempt (attempt 1)
            create_test_row(
                "test-queue",
                entity_id,
                1,
                None,
                Some(TaskOutcome::Failed),
                Utc.with_ymd_and_hms(2024, 1, 1, 11, 45, 0).unwrap(),
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 11, 50, 0).unwrap()),
                task_created_at,
                Some(attempt1_created_at),
                project_id.as_str(),
                Some(*warehouse_id),
            ),
        ];

        let result = parse_task_details(task_id, records).unwrap().unwrap();

        // Verify main task details (should be the most recent attempt)
        assert_eq!(result.task.id.attempt, 2);
        assert!(matches!(result.task.status, TaskStatus::Running));

        // Verify we have one historical attempt
        assert_eq!(result.attempts.len(), 1);
        let historical_attempt = &result.attempts[0];
        assert_eq!(historical_attempt.attempt, 1);
        assert!(matches!(historical_attempt.status, TaskStatus::Failed));
        assert_eq!(historical_attempt.created_at, attempt1_created_at);
        assert_eq!(
            historical_attempt.scheduled_for,
            Utc.with_ymd_and_hms(2024, 1, 1, 11, 45, 0).unwrap()
        );
        assert_eq!(
            historical_attempt.started_at,
            Some(Utc.with_ymd_and_hms(2024, 1, 1, 11, 50, 0).unwrap())
        );
        assert_eq!(historical_attempt.message, Some("Test message".to_string()));
        assert!((historical_attempt.progress - 0.5).abs() < f32::EPSILON);
        assert_eq!(
            historical_attempt.execution_details,
            Some(serde_json::json!({"details": "test"}))
        );

        // Verify duration parsing
        let expected_duration = chrono::Duration::hours(1);
        assert_eq!(historical_attempt.duration, Some(expected_duration));
    }

    #[test]
    fn test_parse_task_details_log_only() {
        let task_id = TaskId::from(Uuid::now_v7());
        let warehouse_id = WarehouseId::from(Uuid::now_v7());
        let project_id = ProjectId::new_random();
        let entity_id = Uuid::now_v7();
        let scheduled_for = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let started_at = Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap());
        let task_created_at = Utc.with_ymd_and_hms(2024, 1, 1, 11, 0, 0).unwrap();
        let attempt_created_at = Utc.with_ymd_and_hms(2024, 1, 1, 11, 30, 0).unwrap();

        let records = vec![create_test_row(
            "cleanup-queue",
            entity_id,
            1,
            None,
            Some(TaskOutcome::Success),
            scheduled_for,
            started_at,
            task_created_at,
            Some(attempt_created_at),
            project_id.as_str(),
            Some(*warehouse_id),
        )];

        let result = parse_task_details(task_id, records).unwrap().unwrap();

        // Verify main task details
        assert_eq!(result.task.task_id(), task_id);
        assert_eq!(result.task.task_metadata.warehouse_id(), Some(warehouse_id));
        assert_eq!(result.task.queue_name.as_str(), "cleanup-queue");
        assert_eq!(result.task.id.attempt, 1);
        assert_eq!(result.task.task_metadata.scheduled_for, scheduled_for);
        assert_eq!(result.task.picked_up_at, started_at);
        assert_eq!(result.task.created_at, task_created_at);
        assert!(matches!(result.task.status, TaskStatus::Success));

        // No historical attempts for completed task
        assert!(result.attempts.is_empty());
    }

    #[test]
    fn test_parse_task_details_multiple_historical_attempts() {
        let task_id = TaskId::from(Uuid::now_v7());
        let warehouse_id = WarehouseId::from(Uuid::now_v7());
        let project_id = ProjectId::new_random();
        let entity_id = Uuid::now_v7();
        let task_created_at = Utc.with_ymd_and_hms(2024, 1, 1, 11, 0, 0).unwrap();

        let records = vec![
            // Most recent completed attempt (attempt 3)
            create_test_row(
                "retry-queue",
                entity_id,
                3,
                None,
                Some(TaskOutcome::Success),
                Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 0).unwrap(),
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 35, 0).unwrap()),
                task_created_at,
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 20, 0).unwrap()),
                project_id.as_str(),
                Some(*warehouse_id),
            ),
            // Second failed attempt (attempt 2)
            create_test_row(
                "retry-queue",
                entity_id,
                2,
                None,
                Some(TaskOutcome::Failed),
                Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap(),
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap()),
                task_created_at,
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 11, 50, 0).unwrap()),
                project_id.as_str(),
                Some(*warehouse_id),
            ),
            // First failed attempt (attempt 1)
            create_test_row(
                "retry-queue",
                entity_id,
                1,
                None,
                Some(TaskOutcome::Failed),
                Utc.with_ymd_and_hms(2024, 1, 1, 11, 30, 0).unwrap(),
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 11, 35, 0).unwrap()),
                task_created_at,
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 11, 20, 0).unwrap()),
                project_id.as_str(),
                Some(*warehouse_id),
            ),
        ];

        let result = parse_task_details(task_id, records).unwrap().unwrap();

        // Verify main task details (should be the most recent attempt)
        assert_eq!(result.task.id.attempt, 3);
        assert!(matches!(result.task.status, TaskStatus::Success));

        // Verify we have two historical attempts, sorted by attempt descending
        assert_eq!(result.attempts.len(), 2);

        let attempt2 = &result.attempts[0];
        assert_eq!(attempt2.attempt, 2);
        assert!(matches!(attempt2.status, TaskStatus::Failed));

        let attempt1 = &result.attempts[1];
        assert_eq!(attempt1.attempt, 1);
        assert!(matches!(attempt1.status, TaskStatus::Failed));
    }

    #[allow(clippy::too_many_arguments)]
    async fn queue_wh_task_helper(
        conn: &mut sqlx::PgConnection,
        queue_name: &TaskQueueName,
        parent_task_id: Option<TaskId>,
        entity_id: WarehouseTaskEntityId,
        entity_name: Vec<String>,
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
                        entity_id,
                        entity_name,
                        warehouse_id,
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

    fn generate_tq_name() -> TaskQueueName {
        TaskQueueName::from(format!("test-{}", Uuid::now_v7()))
    }

    #[sqlx::test]
    async fn test_get_task_details_nonexistent_task(pool: PgPool) {
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let task_id = TaskId::from(Uuid::now_v7());

        let result = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[sqlx::test]
    async fn test_get_task_details_active_task_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let entity_name = vec!["ns".to_string(), "table".to_string()];
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"test": "data"});
        let scheduled_for = Utc::now() - chrono::Duration::minutes(1);
        // Truncate scheduled_for to seconds as postgres does not store nanoseconds
        let scheduled_for = scheduled_for
            - chrono::Duration::nanoseconds(i64::from(scheduled_for.timestamp_subsec_nanos()));

        // Queue a task
        let task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            entity_name.clone(),
            project_id.clone(),
            warehouse_id,
            Some(scheduled_for),
            Some(payload.clone()),
        )
        .await
        .unwrap()
        .unwrap();

        // Pick up the task to make it active
        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id(), task_id);
        assert_eq!(task.task_metadata.entity_name().unwrap(), &entity_name);

        // Update progress and execution details
        let execution_details = serde_json::json!({"progress": "in progress"});
        let check_result =
            check_and_heartbeat_task(&mut conn, &task, 0.5, Some(execution_details.clone()))
                .await
                .unwrap();
        assert_eq!(check_result, TaskCheckState::Continue);

        // Get task details
        let result = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();

        // Verify task details
        assert_eq!(result.task.task_id(), task_id);
        assert_eq!(result.task.task_metadata.warehouse_id(), Some(warehouse_id));
        assert_eq!(result.task.queue_name.as_str(), tq_name.as_str());
        assert_eq!(result.task.id.attempt, 1);
        assert!(matches!(result.task.status, TaskStatus::Running));
        assert!(result.task.picked_up_at.is_some());
        assert!((result.task.progress - 0.5).abs() < f32::EPSILON);
        assert!(result.task.last_heartbeat_at.is_some());
        assert!(result.task.task_metadata.parent_task_id.is_none());
        assert_eq!(result.task.task_metadata.scheduled_for, scheduled_for);
        // Check that created is now +- a few seconds
        let now = Utc::now();
        assert!(result.task.created_at <= now + chrono::Duration::seconds(10));
        assert!(result.task.created_at >= now - chrono::Duration::seconds(10));

        match result.task.task_metadata.entity_id() {
            Some(WarehouseTaskEntityId::Table { table_id }) => {
                assert_eq!(*table_id, entity_id.as_uuid());
            }
            _ => {
                panic!("Expected TaskEntity::Table")
            }
        }

        // Verify task data and execution details
        assert_eq!(result.data, payload);
        assert_eq!(result.execution_details, Some(execution_details));

        // No historical attempts for active task only
        assert!(result.attempts.is_empty());
    }

    #[sqlx::test]
    async fn test_get_task_details_completed_task_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"cleanup": "data"});

        // Queue a task
        let task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id.clone(),
            warehouse_id,
            None,
            Some(payload.clone()),
        )
        .await
        .unwrap()
        .unwrap();

        // Pick up the task
        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Complete the task successfully
        record_success(&task, &mut conn, Some("Task completed successfully"))
            .await
            .unwrap();

        // Get task details
        let result = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();

        // Verify task details
        assert_eq!(result.task.task_id(), task_id);
        assert_eq!(result.task.task_metadata.warehouse_id(), Some(warehouse_id));
        assert_eq!(result.task.queue_name.as_str(), tq_name.as_str());
        assert_eq!(result.task.attempt(), 1);
        assert!(matches!(result.task.status, TaskStatus::Success));
        assert!(result.task.picked_up_at.is_some());

        // Verify task data
        assert_eq!(result.data, payload);

        // No historical attempts for single completed task
        assert!(result.attempts.is_empty());
    }

    #[sqlx::test]
    async fn test_get_task_details_with_retry_history(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"retry": "test"});

        // Queue a task
        let task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id.clone(),
            warehouse_id,
            None,
            Some(payload.clone()),
        )
        .await
        .unwrap()
        .unwrap();

        // First attempt - pick and fail
        let task1 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task1.attempt(), 1);

        record_failure(&task1, 5, "First attempt failed", &mut conn)
            .await
            .unwrap();

        // Second attempt - pick and fail
        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task2.attempt(), 2);

        record_failure(&task2, 5, "Second attempt failed", &mut conn)
            .await
            .unwrap();

        // Third attempt - pick and succeed
        let task3 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task3.attempt(), 3);

        record_success(&task3, &mut conn, Some("Third attempt succeeded"))
            .await
            .unwrap();

        // Get task details
        let result = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();

        // Verify main task details (should be the most recent successful attempt)
        assert_eq!(result.task.task_id(), task_id);
        assert_eq!(result.task.attempt(), 3);
        assert!(matches!(result.task.status, TaskStatus::Success));

        // Verify we have 2 historical attempts (failed attempts 1 and 2)
        assert_eq!(result.attempts.len(), 2);

        // Check attempts are sorted by attempt number descending
        let attempt2 = &result.attempts[0];
        assert_eq!(attempt2.attempt, 2);
        assert!(matches!(attempt2.status, TaskStatus::Failed));
        assert_eq!(attempt2.message, Some("Second attempt failed".to_string()));

        let attempt1 = &result.attempts[1];
        assert_eq!(attempt1.attempt, 1);
        assert!(matches!(attempt1.status, TaskStatus::Failed));
        assert_eq!(attempt1.message, Some("First attempt failed".to_string()));
    }

    #[sqlx::test]
    async fn test_get_task_details_active_with_history(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"active_with_history": "test"});

        // Queue a task
        let task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id.clone(),
            warehouse_id,
            None,
            Some(payload.clone()),
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

        // Second attempt - pick but keep running
        let task2 = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task2.attempt(), 2);

        // Update progress for the active task
        let execution_details = serde_json::json!({"current_step": "processing"});
        let _ = check_and_heartbeat_task(&mut conn, &task2, 0.7, Some(execution_details.clone()))
            .await
            .unwrap();

        // Get task details
        let result = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();

        // Verify main task details (should be the currently running attempt)
        assert_eq!(result.task.task_id(), task_id);
        assert_eq!(result.task.attempt(), 2);
        assert!(matches!(result.task.status, TaskStatus::Running));
        assert!((result.task.progress - 0.7).abs() < f32::EPSILON);
        assert_eq!(result.execution_details, Some(execution_details));

        // Verify we have 1 historical attempt (failed attempt 1)
        assert_eq!(result.attempts.len(), 1);

        let attempt1 = &result.attempts[0];
        assert_eq!(attempt1.attempt, 1);
        assert!(matches!(attempt1.status, TaskStatus::Failed));
        assert_eq!(attempt1.message, Some("First attempt failed".to_string()));
    }

    #[sqlx::test]
    async fn test_get_task_details_limit_attempts(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue a task
        let task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id.clone(),
            warehouse_id,
            None,
            Some(serde_json::json!({"many_attempts": "test"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Create 5 failed attempts
        for i in 1..=5 {
            let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
                .await
                .unwrap()
                .unwrap();

            record_failure(&task, 10, &format!("Attempt {i} failed"), &mut conn)
                .await
                .unwrap();
        }

        // 6th attempt succeeds
        let task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        record_success(&task, &mut conn, Some("Final attempt succeeded"))
            .await
            .unwrap();

        // Get task details with limit of 3 attempts
        let result = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            3,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();

        // Should have only 3 historical attempts (most recent ones: attempts 5, 4, 3)
        assert_eq!(result.attempts.len(), 3);
        assert_eq!(result.attempts[0].attempt, 5);
        assert_eq!(result.attempts[1].attempt, 4);
        assert_eq!(result.attempts[2].attempt, 3);

        // Main task should be the successful 6th attempt
        assert_eq!(result.task.attempt(), 6);
        assert!(matches!(result.task.status, TaskStatus::Success));
    }

    #[sqlx::test]
    async fn test_get_task_details_with_parent_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let parent_entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let child_entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue parent task
        let parent_task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            None,
            parent_entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id.clone(),
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "parent"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Queue child task with parent
        let child_task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            Some(parent_task_id),
            child_entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id.clone(),
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "child"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Pick up child task
        let _child_task = pick_task(&pool, &tq_name, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT)
            .await
            .unwrap()
            .unwrap();

        // Get child task details
        let result = get_task_details(
            child_task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();

        // Verify child task has parent reference
        assert_eq!(result.task.task_id(), child_task_id);
        assert_eq!(result.task.parent_task_id(), Some(parent_task_id));
        assert_eq!(result.data, serde_json::json!({"type": "child"}));
    }

    #[sqlx::test]
    async fn test_get_task_details_wrong_warehouse(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;
        let wrong_warehouse_id = WarehouseId::from(Uuid::now_v7());
        let entity_id = WarehouseTaskEntityId::Table {
            table_id: Uuid::now_v7().into(),
        };
        let tq_name = generate_tq_name();

        // Queue a task in the correct warehouse
        let task_id = queue_wh_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            vec!["ns".to_string(), "table".to_string()],
            project_id.clone(),
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "data"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Try to get task details with wrong warehouse ID
        let result = get_task_details(
            task_id,
            TaskDetailsScope::Warehouse {
                project_id,
                warehouse_id: wrong_warehouse_id,
            },
            10,
            &pool,
        )
        .await
        .unwrap();

        // Should return None since task doesn't exist in the wrong warehouse
        assert!(result.is_none());
    }

    #[sqlx::test]
    async fn test_get_task_details_retrieve_project_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let (warehouse_id, project_id) = setup_warehouse(pool.clone()).await;

        let tq_name = generate_tq_name();

        let entity_uuid_1 = Uuid::now_v7().into();
        let entity_id_1 = WarehouseTaskEntityId::Table {
            table_id: entity_uuid_1,
        };
        let entity_name_1 = vec![format!("entity-{}", entity_uuid_1)];

        let entity_uuid_2 = Uuid::now_v7().into();
        let entity_id_2 = WarehouseTaskEntityId::Table {
            table_id: entity_uuid_2,
        };
        let entity_name_2 = vec![format!("entity-{}", entity_uuid_2)];

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
                            warehouse_id,
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
                            warehouse_id,
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

        // Retrieve the project-level task
        let task_ids = results.iter().map(|result| result.task_id).collect_vec();
        let project_task_id = task_ids[1];

        let result = get_task_details(
            project_task_id,
            TaskDetailsScope::Project { project_id },
            10,
            &pool,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(result.task.task_metadata.warehouse_id().is_none());
        assert_eq!(result.data, project_payload);
    }
}
