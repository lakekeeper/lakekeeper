use sqlx::{PgConnection, query};

use crate::{
    ProjectId,
    api::Result,
    implementations::postgres::dbutils::DBErrorHandler as _,
    service::tasks::task_log_cleanup_queue::{RetentionPeriod, TaskLogCleanupFilter},
    utils::period::PeriodData,
};

pub(crate) async fn cleanup_task_logs_older_than(
    transaction: &mut PgConnection,
    retention_period: RetentionPeriod,
    project_id: &ProjectId,
    filter: TaskLogCleanupFilter,
) -> Result<()> {
    let warehouse_id = match filter {
        TaskLogCleanupFilter::Project => None,
        TaskLogCleanupFilter::Warehouse { warehouse_id } => Some(warehouse_id),
    };
    let query = match retention_period.period().data() {
        PeriodData::Days(days) => query!(
            r#"
            DELETE FROM task_log
            WHERE created_at < now() - make_interval(days => $1)
            AND project_id = $2
            AND ($3::uuid IS NULL OR $3 = warehouse_id)
            "#,
            i32::from(days),
            project_id.as_str(),
            warehouse_id
        ),
    };

    query
        .execute(transaction)
        .await
        .map_err(|e| e.into_error_model("Failed to delete old task logs."))?;

    Ok(())
}

#[cfg(test)]
mod test {
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug)]
    struct TaskLogRecord {
        task_id: Uuid,
        _attempt: i32,
    }

    #[sqlx::test]
    async fn test_cleanup_task_logs_older_than_removes_task_logs_older_than_retention_time(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();

        let project_id = ProjectId::new_random();
        let filter = TaskLogCleanupFilter::Project;

        let retention_period = RetentionPeriod::with_days(90).unwrap();
        let PeriodData::Days(days) = retention_period.period().data();

        let kept_task_log_ids = [
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440003").unwrap(),
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440004").unwrap(),
        ];

        query!(
            r#"
            WITH insert_project AS (
                INSERT INTO project (project_id, project_name)
                VALUES ($1, 'My Project')
                RETURNING project_id
            ),
            insert_warehouse AS (
                INSERT INTO warehouse (warehouse_name, project_id, status, "tabular_delete_mode")
                SELECT 'My Warehouse', project_id, 'active', 'hard'
                FROM insert_project
                RETURNING warehouse_id
            )
            INSERT INTO task_log (
                task_id,
                attempt,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_name,
                entity_type,
                project_id,
                attempt_scheduled_for,
                task_created_at,
                created_at
            )
            VALUES
            (
                '550e8400-e29b-41d4-a716-446655440001',
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'metadata_sync',
                '{"table_id": "customers"}',
                'success',
                '550e8400-e29b-41d4-a716-446655440101',
                ARRAY['customers'],
                'table',
                (SELECT project_id FROM insert_project),
                now() - make_interval(days => ($2 + 5)),
                now() - make_interval(days => ($2 + 5)),
                now() - make_interval(days => ($2 + 5))
            ),
            (
                '550e8400-e29b-41d4-a716-446655440002',
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'garbage_collection',
                '{"schema": "prod_analytics"}',
                'success',
                NULL,
                NULL,
                'warehouse',
                (SELECT project_id FROM insert_project),
                now() - make_interval(days => ($2 + 30)),
                now() - make_interval(days => ($2 + 30)),
                now() - make_interval(days => ($2 + 30))
            ),
            (
                $3,
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'schema_evolution',
                '{"column_name": "new_discount_column"}',
                'success',
                '550e8400-e29b-41d4-a716-446655440103',
                ARRAY['orders'],
                'table',
                (SELECT project_id FROM insert_project),
                now() - make_interval(days => ($2 - 60)),
                now() - make_interval(days => ($2 - 60)),
                now() - make_interval(days => ($2 - 60))
            ),
            (
                $4,
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'file_compaction',
                '{"path": "/warehouse/parquet"}',
                'failed',
                NULL,
                NULL,
                'warehouse',
                (SELECT project_id FROM insert_project),
                now() - make_interval(days => ($2 - 83)),
                now() - make_interval(days => ($2 - 83)),
                now() - make_interval(days => ($2 - 83))
            )
            "#,
            project_id.as_str(),
            i32::from(days),
            kept_task_log_ids[0],
            kept_task_log_ids[1],
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        cleanup_task_logs_older_than(&mut conn, retention_period, &project_id, filter)
            .await
            .unwrap();

        let remaining_task_logs = sqlx::query_as!(
            TaskLogRecord,
            r#"
            SELECT task_id, attempt as _attempt FROM task_log
        "#
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();

        assert_eq!(remaining_task_logs.len(), 2);
        for record in remaining_task_logs {
            assert!(kept_task_log_ids.contains(&record.task_id));
        }
    }

    #[sqlx::test]
    async fn test_cleanup_task_logs_older_than_ignores_logs_for_other_projects(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let project_id = ProjectId::new_random();
        let other_project_id = ProjectId::new_random();

        let my_task_id = Uuid::new_v4();
        let other_task_id = Uuid::new_v4();

        let my_warehouse_id = Uuid::new_v4();
        let other_warehouse_id = Uuid::new_v4();

        let filter = TaskLogCleanupFilter::Project;

        let retention_period = RetentionPeriod::with_days(90).unwrap();
        let PeriodData::Days(days) = retention_period.period().data();

        query!(
            r#"
            WITH insert_projects AS (
                INSERT INTO project (project_id, project_name)
                VALUES
                (
                    $1,
                    'My Project'
                ),
                (
                    $2,
                    'Other Project'
                )
            ),
            insert_warehouse AS (
                INSERT INTO warehouse (warehouse_id, warehouse_name, project_id, status, "tabular_delete_mode")
                VALUES
                (
                    $3,
                    'My Warehouse',
                    $1,
                    'active',
                    'hard'
                ),
                (
                    $4,
                    'Other Warehouse',
                    $2,
                    'active',
                    'hard'
                )
            )
            INSERT INTO task_log (
                task_id,
                attempt,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_name,
                entity_type,
                project_id,
                attempt_scheduled_for,
                task_created_at,
                created_at
            )
            VALUES
            (
                $5,
                1,
                $3,
                'metadata_sync',
                '{"table_id": "customers"}',
                'success'::task_final_status,
                '550e8400-e29b-41d4-a716-446655440101',
                ARRAY['customers'],
                'table',
                $1,
                now() - make_interval(days => ($7 + 5)),
                now() - make_interval(days => ($7 + 5)),
                now() - make_interval(days => ($7 + 5))
            ),
            (
                $6,
                1,
                $4,
                'file_compaction',
                '{"path": "/warehouse/parquet"}',
                'failed',
                NULL,
                NULL,
                'warehouse',
                $2,
                now() - make_interval(days => ($7 + 5)),
                now() - make_interval(days => ($7 + 5)),
                now() - make_interval(days => ($7 + 5))
            )
            "#,
            project_id.as_str(),
            other_project_id.as_str(),
            my_warehouse_id,
            other_warehouse_id,
            my_task_id,
            other_task_id,
            i32::from(days)
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        cleanup_task_logs_older_than(&mut conn, retention_period, &project_id, filter)
            .await
            .unwrap();

        let remaining_task_logs = sqlx::query_as!(
            TaskLogRecord,
            r#"
            SELECT task_id, attempt as _attempt FROM task_log
        "#
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();

        assert_eq!(remaining_task_logs.len(), 1);
        assert_eq!(remaining_task_logs[0].task_id, other_task_id);
    }

    #[sqlx::test]
    async fn test_cleanup_task_logs_older_than_only_cleanup_logs_for_warehouse_tasks_when_filter_is_warehouse(
        pool: PgPool,
    ) {
        let mut conn = pool.acquire().await.unwrap();

        let project_id = ProjectId::new_random();
        let warehouse_id = Uuid::new_v4();
        let filter = TaskLogCleanupFilter::Warehouse { warehouse_id };

        let project_task_id = Uuid::new_v4();
        let warehouse_task_id = Uuid::new_v4();
        let tabular_task_id = Uuid::new_v4();

        let retention_period = RetentionPeriod::with_days(90).unwrap();
        let PeriodData::Days(days) = retention_period.period().data();

        query!(
            r#"
            WITH insert_projects AS (
                INSERT INTO project (project_id, project_name)
                VALUES
                (
                    $1,
                    'My Project'
                )
            ),
            insert_warehouse AS (
                INSERT INTO warehouse (warehouse_id, warehouse_name, project_id, status, "tabular_delete_mode")
                VALUES
                (
                    $2,
                    'My Warehouse',
                    $1,
                    'active',
                    'hard'
                )
            )
            INSERT INTO task_log (
                task_id,
                attempt,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_name,
                entity_type,
                project_id,
                attempt_scheduled_for,
                task_created_at,
                created_at
            )
            VALUES
            (
                $3,
                1,
                NULL,
                'clean_tasks',
                '{}'::jsonb,
                'success'::task_final_status,
                NULL,
                NULL,
                'project'::entity_type,
                $1,
                now() - make_interval(days => ($6 + 5)::int),
                now() - make_interval(days => ($6 + 5)::int),
                now() - make_interval(days => ($6 + 5)::int)
            ),
            (
                $4,
                1,
                $2,
                'remove_orphaned_files',
                '{}'::jsonb,
                'success'::task_final_status,
                NULL,
                NULL,
                'warehouse'::entity_type,
                $1,
                now() - make_interval(days => ($6 + 5)::int),
                now() - make_interval(days => ($6 + 5)::int),
                now() - make_interval(days => ($6 + 5)::int)
            ),
            (
                $5,
                1,
                $2,
                'remove_tabular',
                '{}'::jsonb,
                'success'::task_final_status,
                '550e8400-e29b-41d4-a716-446655440101'::uuid,
                ARRAY['customers']::text[],
                'table'::entity_type,
                $1,
                now() - make_interval(days => ($6 + 5)::int),
                now() - make_interval(days => ($6 + 5)::int),
                now() - make_interval(days => ($6 + 5)::int)
            )
            "#,
            project_id.as_str(),
            warehouse_id,
            project_task_id,
            warehouse_task_id,
            tabular_task_id,
            i32::from(days)
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        cleanup_task_logs_older_than(&mut conn, retention_period, &project_id, filter)
            .await
            .unwrap();

        let remaining_task_logs = sqlx::query_as!(
            TaskLogRecord,
            r#"
            SELECT task_id, attempt as _attempt FROM task_log
        "#
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();

        assert_eq!(remaining_task_logs.len(), 1);
        assert_eq!(remaining_task_logs[0].task_id, project_task_id);
    }

    #[sqlx::test]
    async fn test_cleanup_task_logs_with_warehouse_filter_respects_retention_period(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let project_id = ProjectId::new_random();
        let warehouse_id = Uuid::new_v4();
        let filter = TaskLogCleanupFilter::Warehouse { warehouse_id };

        let old_task_id = Uuid::new_v4();
        let recent_task_id = Uuid::new_v4();

        let retention_period = RetentionPeriod::with_days(90).unwrap();
        let PeriodData::Days(days) = retention_period.period().data();

        query!(
            r#"
            WITH insert_projects AS (
                INSERT INTO project (project_id, project_name)
                VALUES (
                    $1,
                    'My Project'
                )
            ),
            insert_warehouse AS (
                INSERT INTO warehouse (warehouse_id, warehouse_name, project_id, status, "tabular_delete_mode")
                VALUES (
                    $2,
                    'My Warehouse',
                    $1,
                    'active',
                    'hard'
                )
            )
            INSERT INTO task_log (
                task_id,
                attempt,
                warehouse_id,
                queue_name,
                task_data,
                status,
                entity_id,
                entity_name,
                entity_type,
                project_id,
                attempt_scheduled_for,
                task_created_at,
                created_at
            )
            VALUES
            (
                $3,
                1,
                $2,
                'old_task',
                '{}',
                'success',
                NULL,
                NULL,
                'warehouse',
                $1,
                now() - make_interval(days => ($5 + 10)),
                now() - make_interval(days => ($5 + 10)),
                now() - make_interval(days => ($5 + 10))
            ),
            (
                $4,
                1,
                $2,
                'recent_task',
                '{}',
                'success',
                NULL,
                NULL,
                'warehouse',
                $1,
                now() - make_interval(days => ($5 - 10)),
                now() - make_interval(days => ($5 - 10)),
                now() - make_interval(days => ($5 - 10))
            )
            "#,
            project_id.as_str(),
            warehouse_id,
            old_task_id,
            recent_task_id,
            i32::from(days),
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        cleanup_task_logs_older_than(&mut conn, retention_period, &project_id, filter)
            .await
            .unwrap();

        let remaining_logs = sqlx::query_as!(
            TaskLogRecord,
            r#"SELECT task_id, attempt as _attempt FROM task_log"#
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();

        assert_eq!(remaining_logs.len(), 1);
        assert_eq!(remaining_logs[0].task_id, recent_task_id);
    }
}
