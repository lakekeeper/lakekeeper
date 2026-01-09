use sqlx::{PgConnection, query};

use crate::{
    api::Result, implementations::postgres::dbutils::DBErrorHandler as _,
    service::tasks::task_log_cleanup_queue::RetentionPeriod, utils::period::Period,
};

pub(crate) async fn cleanup_task_logs_older_than(
    transaction: &mut PgConnection,
    retention_period: RetentionPeriod,
) -> Result<()> {
    let query = match retention_period.period() {
        Period::Days(days) => query!(
            r#"
            DELETE FROM task_log
            WHERE created_at < now() - make_interval(days => $1)
            "#,
            days
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

        let retention_period = RetentionPeriod::days(90);
        let Period::Days(days) = retention_period.period();

        let kept_task_log_ids = [
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440003").unwrap(),
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440004").unwrap(),
        ];

        query!(
            r#"
            WITH insert_project AS (
                INSERT INTO project (project_id, project_name)
                VALUES ('e78398f7-c481-4fd0-b3c4-23a20bdb9648', 'My Project')
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
                '550e8400-e29b-41d4-a716-446655440001'::uuid,
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'metadata_sync',
                '{"table_id": "customers"}'::jsonb,
                'success'::task_final_status,
                '550e8400-e29b-41d4-a716-446655440101'::uuid,
                ARRAY['customers']::text[],
                'table'::entity_type,
                'e78398f7-c481-4fd0-b3c4-23a20bdb9648'::uuid,
                now() - make_interval(days => ($1 + 5)::int),
                now() - make_interval(days => ($1 + 5)::int),
                now() - make_interval(days => ($1 + 5)::int)
            ),
            (
                '550e8400-e29b-41d4-a716-446655440002'::uuid,
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'garbage_collection',
                '{"schema": "prod_analytics"}'::jsonb,
                'success'::task_final_status,
                NULL::uuid,
                NULL::text[],
                'warehouse'::entity_type,
                'e78398f7-c481-4fd0-b3c4-23a20bdb9648'::uuid,
                now() - make_interval(days => ($1 + 30)::int),
                now() - make_interval(days => ($1 + 30)::int),
                now() - make_interval(days => ($1 + 30)::int)
            ),
            (
                $2,
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'schema_evolution',
                '{"column_name": "new_discount_column"}'::jsonb,
                'success'::task_final_status,
                '550e8400-e29b-41d4-a716-446655440103'::uuid,
                ARRAY['orders']::text[],
                'table'::entity_type,
                'e78398f7-c481-4fd0-b3c4-23a20bdb9648'::uuid,
                now() - make_interval(days => ($1 - 60)::int),
                now() - make_interval(days => ($1 - 60)::int),
                now() - make_interval(days => ($1 - 60)::int)
            ),
            (
                $3,
                1,
                (SELECT warehouse_id FROM insert_warehouse),
                'file_compaction',
                '{"path": "/warehouse/parquet"}'::jsonb,
                'failed'::task_final_status,
                NULL::uuid,
                NULL::text[],
                'warehouse'::entity_type,
                'e78398f7-c481-4fd0-b3c4-23a20bdb9648'::uuid,
                now() - make_interval(days => ($1 - 83)::int),
                now() - make_interval(days => ($1 - 83)::int),
                now() - make_interval(days => ($1 - 83)::int)
            )
            "#,
            days,
            kept_task_log_ids[0],
            kept_task_log_ids[1],
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        let tasks = query!(
            r#"
                SELECT task_id, attempt, created_at FROM task_log;
            "#
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();

        cleanup_task_logs_older_than(&mut conn, retention_period)
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
}
