use chrono::{DateTime, Duration};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use sqlx::{postgres::types::PgInterval, PgConnection};
use uuid::Uuid;

use super::EntityType;
use crate::{
    api::management::v1::tasks::{GetTaskDetailsResponse, Task as APITask, TaskAttempt},
    implementations::postgres::dbutils::DBErrorHandler,
    service::task_queue::{TaskEntity, TaskId, TaskOutcome, TaskStatus},
    WarehouseId,
};

#[derive(sqlx::FromRow, Debug)]
struct TaskDetailsRow {
    pub queue_name: String,
    pub entity_id: uuid::Uuid,
    pub entity_type: EntityType,
    pub task_status: Option<TaskStatus>,
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
}

fn parse_task_details(
    task_id: TaskId,
    warehouse_id: WarehouseId,
    mut records: Vec<TaskDetailsRow>,
) -> Result<Option<GetTaskDetailsResponse>, IcebergErrorResponse> {
    // Sort by attempt descending
    records.sort_by_key(|r| -r.attempt);
    if records.is_empty() {
        return Ok(None);
    }

    let most_recent = records.remove(0);
    let attempts = records
        .into_iter()
        .map(|r| {
            Result::<_, ErrorModel>::Ok(TaskAttempt {
                attempt: r.attempt,
                status: r
                    .task_status
                    .map(Into::into)
                    .or(r.task_log_status.map(Into::into))
                    .ok_or_else(|| {
                        ErrorModel::internal(
                            "Task attempt has neither status nor log status.",
                            "Unexpected",
                            None,
                        )
                    })?,
                started_at: r.started_at,
                scheduled_for: r.attempt_scheduled_for,
                duration: r
                    .duration
                    .map(pg_interval_to_duration)
                    .transpose()
                    .map_err(|e| e.append_detail("Failed to parse task duration"))?,
                message: r.message,
                created_at: r.attempt_created_at.ok_or_else(|| {
                    ErrorModel::internal(
                        "Task attempt is missing created_at timestamp.",
                        "Unexpected",
                        None,
                    )
                })?,
                progress: r.progress,
                execution_details: r.execution_details,
            })
        })
        .try_collect()?;

    let task = APITask {
        task_id,
        warehouse_id,
        queue_name: most_recent.queue_name.into(),
        entity: match most_recent.entity_type {
            EntityType::Tabular => TaskEntity::Table {
                warehouse_id,
                table_id: most_recent.entity_id.into(),
            },
        },
        status: most_recent
            .task_status
            .map(Into::into)
            .or(most_recent.task_log_status.map(Into::into))
            .ok_or_else(|| {
                ErrorModel::internal(
                    "Most recent task record has neither status nor log status.",
                    "InternalError",
                    None,
                )
            })?,
        picked_up_at: most_recent.started_at,
        attempt: most_recent.attempt,
        parent_task_id: most_recent.parent_task_id.map(TaskId::from),
        scheduled_for: most_recent.attempt_scheduled_for,
        created_at: most_recent.task_created_at,
        last_heartbeat_at: most_recent.last_heartbeat_at,
        updated_at: most_recent.updated_at,
        progress: most_recent.progress,
    };

    Ok(Some(GetTaskDetailsResponse {
        task,
        attempts,
        task_data: most_recent.task_data,
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

pub(crate) async fn get_task_details(
    warehouse_id: WarehouseId,
    task_id: TaskId,
    num_attempts: u16,
    transaction: &mut PgConnection,
) -> Result<Option<GetTaskDetailsResponse>, IcebergErrorResponse> {
    // Overwrite necessary due to:
    // https://github.com/launchbadge/sqlx/issues/1266
    let records = sqlx::query_as!(
        TaskDetailsRow,
        r#"
        SELECT 
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
            attempt_created_at,
            updated_at,
            task_data as "task_data!",
            execution_details,
            duration,
            message
         FROM (
        SELECT
            queue_name,
            entity_id,
            entity_type,
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
        WHERE warehouse_id = $1 AND task_id = $2
        UNION ALL
        (SELECT
            queue_name,
            entity_id,
            entity_type,
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
        WHERE warehouse_id = $1 AND task_id = $2
        ORDER BY attempt desc 
        LIMIT $3 + 1
        )) as combined
        "#,
        *warehouse_id,
        *task_id,
        i32::from(num_attempts) // Query limit is num_attempts + 1 to handle the case where there's an active task plus historical attempts
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(|e| e.into_error_model("Failed to get task details"))?;

    let result = parse_task_details(task_id, warehouse_id, records)?;

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
    use sqlx::{postgres::types::PgInterval, PgPool};
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::management::v1::tasks::TaskStatus as APITaskStatus,
        implementations::postgres::tasks::{
            check_and_heartbeat_task, pick_task, record_failure, record_success,
            test::setup_warehouse,
        },
        service::task_queue::{
            EntityId, TaskCheckState, TaskEntity, TaskInput, TaskMetadata, TaskOutcome,
            TaskQueueName, TaskStatus, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
        },
        WarehouseId,
    };

    #[allow(clippy::too_many_arguments)]
    fn create_test_row(
        queue_name: &str,
        entity_id: Uuid,
        attempt: i32,
        task_status: Option<TaskStatus>,
        task_log_status: Option<TaskOutcome>,
        scheduled_for: DateTime<Utc>,
        started_at: Option<DateTime<Utc>>,
        task_created_at: DateTime<Utc>,
        attempt_created_at: Option<DateTime<Utc>>,
    ) -> TaskDetailsRow {
        TaskDetailsRow {
            queue_name: queue_name.to_string(),
            entity_id,
            entity_type: EntityType::Tabular,
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
        }
    }

    #[test]
    fn test_parse_task_details_empty_records() {
        let task_id = TaskId::from(Uuid::now_v7());
        let warehouse_id = WarehouseId::from(Uuid::now_v7());
        let records = vec![];

        let result = parse_task_details(task_id, warehouse_id, records).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_task_details_active_task_only() {
        let task_id = TaskId::from(Uuid::now_v7());
        let warehouse_id = WarehouseId::from(Uuid::now_v7());
        let entity_id = Uuid::now_v7();
        let scheduled_for = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let started_at = Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap());
        let task_created_at = Utc.with_ymd_and_hms(2024, 1, 1, 11, 0, 0).unwrap();

        let records = vec![create_test_row(
            "test-queue",
            entity_id,
            1,
            Some(TaskStatus::Running),
            None,
            scheduled_for,
            started_at,
            task_created_at,
            None, // No attempt_created_at for active tasks
        )];

        let result = parse_task_details(task_id, warehouse_id, records)
            .unwrap()
            .unwrap();

        // Verify main task details
        assert_eq!(result.task.task_id, task_id);
        assert_eq!(result.task.warehouse_id, warehouse_id);
        assert_eq!(result.task.queue_name.as_str(), "test-queue");
        assert_eq!(result.task.attempt, 1);
        assert_eq!(result.task.scheduled_for, scheduled_for);
        assert_eq!(result.task.picked_up_at, started_at);
        assert_eq!(result.task.created_at, task_created_at);
        assert!((result.task.progress - 0.5).abs() < f32::EPSILON);
        assert!(matches!(result.task.status, APITaskStatus::Running));

        // Verify entity
        let TaskEntity::Table {
            table_id,
            warehouse_id: entity_warehouse_id,
        } = result.task.entity;
        assert_eq!(*table_id, entity_id);
        assert_eq!(entity_warehouse_id, warehouse_id);

        // Verify task data
        assert_eq!(result.task_data, serde_json::json!({"test": "data"}));
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
                Some(TaskStatus::Running),
                None,
                scheduled_for,
                Some(Utc.with_ymd_and_hms(2024, 1, 1, 12, 10, 0).unwrap()),
                task_created_at,
                None,
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
            ),
        ];

        let result = parse_task_details(task_id, warehouse_id, records)
            .unwrap()
            .unwrap();

        // Verify main task details (should be the most recent attempt)
        assert_eq!(result.task.attempt, 2);
        assert!(matches!(result.task.status, APITaskStatus::Running));

        // Verify we have one historical attempt
        assert_eq!(result.attempts.len(), 1);
        let historical_attempt = &result.attempts[0];
        assert_eq!(historical_attempt.attempt, 1);
        assert!(matches!(historical_attempt.status, APITaskStatus::Failed));
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
        )];

        let result = parse_task_details(task_id, warehouse_id, records)
            .unwrap()
            .unwrap();

        // Verify main task details
        assert_eq!(result.task.task_id, task_id);
        assert_eq!(result.task.warehouse_id, warehouse_id);
        assert_eq!(result.task.queue_name.as_str(), "cleanup-queue");
        assert_eq!(result.task.attempt, 1);
        assert_eq!(result.task.scheduled_for, scheduled_for);
        assert_eq!(result.task.picked_up_at, started_at);
        assert_eq!(result.task.created_at, task_created_at);
        assert!(matches!(result.task.status, APITaskStatus::Success));

        // No historical attempts for completed task
        assert!(result.attempts.is_empty());
    }

    #[test]
    fn test_parse_task_details_multiple_historical_attempts() {
        let task_id = TaskId::from(Uuid::now_v7());
        let warehouse_id = WarehouseId::from(Uuid::now_v7());
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
            ),
        ];

        let result = parse_task_details(task_id, warehouse_id, records)
            .unwrap()
            .unwrap();

        // Verify main task details (should be the most recent attempt)
        assert_eq!(result.task.attempt, 3);
        assert!(matches!(result.task.status, APITaskStatus::Success));

        // Verify we have two historical attempts, sorted by attempt descending
        assert_eq!(result.attempts.len(), 2);

        let attempt2 = &result.attempts[0];
        assert_eq!(attempt2.attempt, 2);
        assert!(matches!(attempt2.status, APITaskStatus::Failed));

        let attempt1 = &result.attempts[1];
        assert_eq!(attempt1.attempt, 1);
        assert!(matches!(attempt1.status, APITaskStatus::Failed));
    }

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

    fn generate_tq_name() -> TaskQueueName {
        TaskQueueName::from(format!("test-{}", Uuid::now_v7()))
    }

    #[sqlx::test]
    async fn test_get_task_details_nonexistent_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let task_id = TaskId::from(Uuid::now_v7());

        let result = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[sqlx::test]
    async fn test_get_task_details_active_task_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"test": "data"});
        let scheduled_for = Utc::now() - chrono::Duration::minutes(1);
        // Truncate scheduled_for to seconds as postgres does not store nanoseconds
        let scheduled_for = scheduled_for
            - chrono::Duration::nanoseconds(i64::from(scheduled_for.timestamp_subsec_nanos()));

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
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

        // Update progress and execution details
        let execution_details = serde_json::json!({"progress": "in progress"});
        let check_result =
            check_and_heartbeat_task(&mut conn, &task, 0.5, Some(execution_details.clone()))
                .await
                .unwrap();
        assert_eq!(check_result, TaskCheckState::Continue);

        // Get task details
        let result = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .unwrap();

        // Verify task details
        assert_eq!(result.task.task_id, task_id);
        assert_eq!(result.task.warehouse_id, warehouse_id);
        assert_eq!(result.task.queue_name.as_str(), tq_name.as_str());
        assert_eq!(result.task.attempt, 1);
        assert!(matches!(result.task.status, APITaskStatus::Running));
        assert!(result.task.picked_up_at.is_some());
        assert!((result.task.progress - 0.5).abs() < f32::EPSILON);
        assert!(result.task.last_heartbeat_at.is_some());
        assert!(result.task.parent_task_id.is_none());
        assert_eq!(result.task.scheduled_for, scheduled_for);
        // Check that created is now +- a few seconds
        let now = Utc::now();
        assert!(result.task.created_at <= now + chrono::Duration::seconds(10));
        assert!(result.task.created_at >= now - chrono::Duration::seconds(10));

        // Verify entity
        let TaskEntity::Table {
            table_id,
            warehouse_id: entity_warehouse_id,
        } = result.task.entity;
        assert_eq!(*table_id, entity_id.to_uuid());
        assert_eq!(entity_warehouse_id, warehouse_id);

        // Verify task data and execution details
        assert_eq!(result.task_data, payload);
        assert_eq!(result.execution_details, Some(execution_details));

        // No historical attempts for active task only
        assert!(result.attempts.is_empty());
    }

    #[sqlx::test]
    async fn test_get_task_details_completed_task_only(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"cleanup": "data"});

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
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
        let result = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .unwrap();

        // Verify task details
        assert_eq!(result.task.task_id, task_id);
        assert_eq!(result.task.warehouse_id, warehouse_id);
        assert_eq!(result.task.queue_name.as_str(), tq_name.as_str());
        assert_eq!(result.task.attempt, 1);
        assert!(matches!(result.task.status, APITaskStatus::Success));
        assert!(result.task.picked_up_at.is_some());

        // Verify task data
        assert_eq!(result.task_data, payload);

        // No historical attempts for single completed task
        assert!(result.attempts.is_empty());
    }

    #[sqlx::test]
    async fn test_get_task_details_with_retry_history(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"retry": "test"});

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
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
        let result = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .unwrap();

        // Verify main task details (should be the most recent successful attempt)
        assert_eq!(result.task.task_id, task_id);
        assert_eq!(result.task.attempt, 3);
        assert!(matches!(result.task.status, APITaskStatus::Success));

        // Verify we have 2 historical attempts (failed attempts 1 and 2)
        assert_eq!(result.attempts.len(), 2);

        // Check attempts are sorted by attempt number descending
        let attempt2 = &result.attempts[0];
        assert_eq!(attempt2.attempt, 2);
        assert!(matches!(attempt2.status, APITaskStatus::Failed));
        assert_eq!(attempt2.message, Some("Second attempt failed".to_string()));

        let attempt1 = &result.attempts[1];
        assert_eq!(attempt1.attempt, 1);
        assert!(matches!(attempt1.status, APITaskStatus::Failed));
        assert_eq!(attempt1.message, Some("First attempt failed".to_string()));
    }

    #[sqlx::test]
    async fn test_get_task_details_active_with_history(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();
        let payload = serde_json::json!({"active_with_history": "test"});

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
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
        let result = get_task_details(warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap()
            .unwrap();

        // Verify main task details (should be the currently running attempt)
        assert_eq!(result.task.task_id, task_id);
        assert_eq!(result.task.attempt, 2);
        assert!(matches!(result.task.status, APITaskStatus::Running));
        assert!((result.task.progress - 0.7).abs() < f32::EPSILON);
        assert_eq!(result.execution_details, Some(execution_details));

        // Verify we have 1 historical attempt (failed attempt 1)
        assert_eq!(result.attempts.len(), 1);

        let attempt1 = &result.attempts[0];
        assert_eq!(attempt1.attempt, 1);
        assert!(matches!(attempt1.status, APITaskStatus::Failed));
        assert_eq!(attempt1.message, Some("First attempt failed".to_string()));
    }

    #[sqlx::test]
    async fn test_get_task_details_limit_attempts(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue a task
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
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
        let result = get_task_details(warehouse_id, task_id, 3, &mut conn)
            .await
            .unwrap()
            .unwrap();

        // Should have only 3 historical attempts (most recent ones: attempts 5, 4, 3)
        assert_eq!(result.attempts.len(), 3);
        assert_eq!(result.attempts[0].attempt, 5);
        assert_eq!(result.attempts[1].attempt, 4);
        assert_eq!(result.attempts[2].attempt, 3);

        // Main task should be the successful 6th attempt
        assert_eq!(result.task.attempt, 6);
        assert!(matches!(result.task.status, APITaskStatus::Success));
    }

    #[sqlx::test]
    async fn test_get_task_details_with_parent_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let parent_entity_id = EntityId::Tabular(Uuid::now_v7());
        let child_entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue parent task
        let parent_task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            parent_entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"type": "parent"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Queue child task with parent
        let child_task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            Some(parent_task_id),
            child_entity_id,
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
        let result = get_task_details(warehouse_id, child_task_id, 10, &mut conn)
            .await
            .unwrap()
            .unwrap();

        // Verify child task has parent reference
        assert_eq!(result.task.task_id, child_task_id);
        assert_eq!(result.task.parent_task_id, Some(parent_task_id));
        assert_eq!(result.task_data, serde_json::json!({"type": "child"}));
    }

    #[sqlx::test]
    async fn test_get_task_details_wrong_warehouse(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let warehouse_id = setup_warehouse(pool.clone()).await;
        let wrong_warehouse_id = WarehouseId::from(Uuid::now_v7());
        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let tq_name = generate_tq_name();

        // Queue a task in the correct warehouse
        let task_id = queue_task_helper(
            &mut conn,
            &tq_name,
            None,
            entity_id,
            warehouse_id,
            None,
            Some(serde_json::json!({"test": "data"})),
        )
        .await
        .unwrap()
        .unwrap();

        // Try to get task details with wrong warehouse ID
        let result = get_task_details(wrong_warehouse_id, task_id, 10, &mut conn)
            .await
            .unwrap();

        // Should return None since task doesn't exist in the wrong warehouse
        assert!(result.is_none());
    }
}
