use crate::{
    api::Result,
    implementations::postgres::dbutils::DBErrorHandler,
    service::stats_retention::{StatisticsRetentionConfig, StatisticsRetentionService},
};
use chrono::{DateTime, Utc};
use std::time::Duration;

/// PostgreSQL implementation of statistics retention service
#[derive(Debug)]
pub struct PostgresStatisticsRetentionService {
    pool: sqlx::PgPool,
}

impl PostgresStatisticsRetentionService {
    /// Create a new PostgreSQL statistics retention service
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl StatisticsRetentionService for PostgresStatisticsRetentionService {
    async fn cleanup_endpoint_statistics(&self, config: &StatisticsRetentionConfig) -> Result<u64> {
        let mut transaction = self.pool.begin().await.map_err(|e| {
            tracing::error!("Failed to start transaction: {e}");
            e.into_error_model("failed to start transaction")
        })?;

        let mut total_deleted = 0u64;

        // Time-based cleanup: Delete entries older than max_age
        if config.endpoint_max_age > Duration::ZERO {
            let cutoff_time = chrono::Utc::now() - chrono::Duration::from_std(config.endpoint_max_age)
                .map_err(|e| {
                    tracing::error!("Failed to convert duration: {e}");
                    crate::api::ErrorModel::bad_request(
                        "Invalid endpoint max age duration".to_string(),
                        "DurationConversionError".to_string(),
                        None,
                    )
                })?;

            let deleted_by_age = sqlx::query!(
                "DELETE FROM endpoint_statistics WHERE timestamp < $1",
                cutoff_time
            )
            .execute(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!("Failed to delete old endpoint statistics: {e}");
                e.into_error_model("failed to delete old endpoint statistics")
            })?
            .rows_affected();

            total_deleted += deleted_by_age;
            tracing::debug!(
                deleted_by_age,
                cutoff_time = %cutoff_time,
                "Deleted endpoint statistics older than cutoff time"
            );
        }

        // Count-based cleanup: Keep only the latest N entries per unique combination
        if config.endpoint_max_entries > 0 {
            let deleted_by_count = sqlx::query!(
                r#"
                WITH entries_to_delete AS (
                    SELECT endpoint_statistics_id
                    FROM (
                        SELECT 
                            endpoint_statistics_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY project_id, warehouse_id, matched_path, status_code 
                                ORDER BY timestamp DESC
                            ) AS rn
                        FROM endpoint_statistics
                    ) ranked
                    WHERE rn > $1
                )
                DELETE FROM endpoint_statistics 
                WHERE endpoint_statistics_id IN (SELECT endpoint_statistics_id FROM entries_to_delete)
                "#,
                config.endpoint_max_entries as i64
            )
            .execute(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!("Failed to delete excess endpoint statistics: {e}");
                e.into_error_model("failed to delete excess endpoint statistics")
            })?
            .rows_affected();

            total_deleted += deleted_by_count;
            tracing::debug!(
                deleted_by_count,
                max_entries = config.endpoint_max_entries,
                "Deleted excess endpoint statistics beyond max entries limit"
            );
        }

        transaction.commit().await.map_err(|e| {
            tracing::error!("Failed to commit transaction: {e}");
            e.into_error_model("failed to commit transaction")
        })?;

        Ok(total_deleted)
    }

    async fn cleanup_warehouse_statistics(&self, config: &StatisticsRetentionConfig) -> Result<u64> {
        let mut transaction = self.pool.begin().await.map_err(|e| {
            tracing::error!("Failed to start transaction: {e}");
            e.into_error_model("failed to start transaction")
        })?;

        let mut total_deleted = 0u64;

        // Time-based cleanup: Delete entries older than max_age
        if config.warehouse_max_age > Duration::ZERO {
            let cutoff_time = chrono::Utc::now() - chrono::Duration::from_std(config.warehouse_max_age)
                .map_err(|e| {
                    tracing::error!("Failed to convert duration: {e}");
                    crate::api::ErrorModel::bad_request(
                        "Invalid warehouse max age duration".to_string(),
                        "DurationConversionError".to_string(),
                        None,
                    )
                })?;

            let deleted_by_age = sqlx::query!(
                "DELETE FROM warehouse_statistics_history WHERE timestamp < $1",
                cutoff_time
            )
            .execute(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!("Failed to delete old warehouse statistics: {e}");
                e.into_error_model("failed to delete old warehouse statistics")
            })?
            .rows_affected();

            total_deleted += deleted_by_age;
            tracing::debug!(
                deleted_by_age,
                cutoff_time = %cutoff_time,
                "Deleted warehouse statistics older than cutoff time"
            );
        }

        // Count-based cleanup: Keep only the latest N entries per warehouse
        if config.warehouse_max_entries > 0 {
            let deleted_by_count = sqlx::query!(
                r#"
                WITH entries_to_delete AS (
                    SELECT warehouse_id, timestamp
                    FROM (
                        SELECT 
                            warehouse_id,
                            timestamp,
                            ROW_NUMBER() OVER (
                                PARTITION BY warehouse_id 
                                ORDER BY timestamp DESC
                            ) AS rn
                        FROM warehouse_statistics_history
                    ) ranked
                    WHERE rn > $1
                )
                DELETE FROM warehouse_statistics_history 
                WHERE (warehouse_id, timestamp) IN (
                    SELECT warehouse_id, timestamp FROM entries_to_delete
                )
                "#,
                config.warehouse_max_entries as i64
            )
            .execute(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!("Failed to delete excess warehouse statistics: {e}");
                e.into_error_model("failed to delete excess warehouse statistics")
            })?
            .rows_affected();

            total_deleted += deleted_by_count;
            tracing::debug!(
                deleted_by_count,
                max_entries = config.warehouse_max_entries,
                "Deleted excess warehouse statistics beyond max entries limit"
            );
        }

        transaction.commit().await.map_err(|e| {
            tracing::error!("Failed to commit transaction: {e}");
            e.into_error_model("failed to commit transaction")
        })?;

        Ok(total_deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::endpoints::{Endpoint, EndpointFlat},
        implementations::postgres::PostgresStatisticsSink,
        service::{endpoint_statistics::EndpointIdentifier, authz::AllowAllAuthorizer},
        DEFAULT_PROJECT_ID,
    };
    use chrono::{Duration as ChronoDuration, Utc};
    use http::StatusCode;
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_endpoint_statistics_time_based_retention(pool: sqlx::PgPool) {
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            crate::api::management::v1::warehouse::TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;

        let retention_service = PostgresStatisticsRetentionService::new(pool.clone());
        let stats_sink = PostgresStatisticsSink::new(pool.clone());

        // Insert test endpoint statistics
        let project_id = DEFAULT_PROJECT_ID.clone().unwrap();
        let mut stats = HashMap::new();
        stats.insert(project_id.clone(), HashMap::new());
        let project_stats = stats.get_mut(&project_id).unwrap();

        let ident = EndpointIdentifier {
            uri: Endpoint::CatalogGetConfig,
            status_code: StatusCode::OK,
            warehouse: Some(warehouse.warehouse_id),
            warehouse_name: Some(warehouse.warehouse_name.clone()),
        };
        project_stats.insert(ident, 5);

        stats_sink.process_stats(Arc::new(stats)).await.unwrap();

        // Create old entries by manually updating timestamps
        let old_timestamp = Utc::now() - ChronoDuration::days(10);
        sqlx::query!(
            "UPDATE endpoint_statistics SET timestamp = $1 WHERE project_id = $2",
            old_timestamp,
            project_id.to_string()
        )
        .execute(&pool)
        .await
        .unwrap();

        // Test time-based retention
        let config = StatisticsRetentionConfig {
            endpoint_max_entries: 0, // Disable count-based retention
            endpoint_max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            warehouse_max_entries: 0,
            warehouse_max_age: Duration::ZERO,
        };

        let deleted = retention_service
            .cleanup_endpoint_statistics(&config)
            .await
            .unwrap();

        assert_eq!(deleted, 1, "Should have deleted 1 old endpoint statistics entry");

        // Verify the entry was deleted
        let remaining = sqlx::query!(
            "SELECT COUNT(*) as count FROM endpoint_statistics WHERE project_id = $1",
            project_id.to_string()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(remaining.count.unwrap(), 0, "No endpoint statistics should remain");
    }

    #[sqlx::test]
    async fn test_endpoint_statistics_count_based_retention(pool: sqlx::PgPool) {
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            crate::api::management::v1::warehouse::TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;

        let retention_service = PostgresStatisticsRetentionService::new(pool.clone());

        // Insert multiple endpoint statistics entries manually
        let project_id = DEFAULT_PROJECT_ID.clone().unwrap();
        
        for i in 0..5 {
            let timestamp = Utc::now() - ChronoDuration::hours(i);
            sqlx::query!(
                r#"
                INSERT INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                project_id.to_string(),
                warehouse.warehouse_id,
                EndpointFlat::CatalogGetConfig as EndpointFlat,
                200,
                1i64,
                timestamp
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test count-based retention (keep only 2 entries)
        let config = StatisticsRetentionConfig {
            endpoint_max_entries: 2,
            endpoint_max_age: Duration::ZERO, // Disable time-based retention
            warehouse_max_entries: 0,
            warehouse_max_age: Duration::ZERO,
        };

        let deleted = retention_service
            .cleanup_endpoint_statistics(&config)
            .await
            .unwrap();

        assert_eq!(deleted, 3, "Should have deleted 3 excess endpoint statistics entries");

        // Verify only 2 entries remain
        let remaining = sqlx::query!(
            "SELECT COUNT(*) as count FROM endpoint_statistics WHERE project_id = $1",
            project_id.to_string()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(remaining.count.unwrap(), 2, "Should have 2 endpoint statistics entries remaining");
    }

    #[sqlx::test]
    async fn test_warehouse_statistics_time_based_retention(pool: sqlx::PgPool) {
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            crate::api::management::v1::warehouse::TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;

        let retention_service = PostgresStatisticsRetentionService::new(pool.clone());

        // Insert old warehouse statistics entries
        let old_timestamp = Utc::now() - ChronoDuration::days(10);
        sqlx::query!(
            r#"
            INSERT INTO warehouse_statistics_history (number_of_views, number_of_tables, warehouse_id, timestamp)
            VALUES ($1, $2, $3, $4)
            "#,
            5i64,
            10i64,
            warehouse.warehouse_id,
            old_timestamp
        )
        .execute(&pool)
        .await
        .unwrap();

        // Test time-based retention
        let config = StatisticsRetentionConfig {
            endpoint_max_entries: 0,
            endpoint_max_age: Duration::ZERO,
            warehouse_max_entries: 0, // Disable count-based retention
            warehouse_max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
        };

        let deleted = retention_service
            .cleanup_warehouse_statistics(&config)
            .await
            .unwrap();

        assert_eq!(deleted, 1, "Should have deleted 1 old warehouse statistics entry");

        // Verify the entry was deleted
        let remaining = sqlx::query!(
            "SELECT COUNT(*) as count FROM warehouse_statistics_history WHERE warehouse_id = $1",
            warehouse.warehouse_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(remaining.count.unwrap(), 0, "No warehouse statistics should remain");
    }

    #[sqlx::test]
    async fn test_warehouse_statistics_count_based_retention(pool: sqlx::PgPool) {
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            crate::api::management::v1::warehouse::TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;

        let retention_service = PostgresStatisticsRetentionService::new(pool.clone());

        // Insert multiple warehouse statistics entries
        for i in 0..5 {
            let timestamp = Utc::now() - ChronoDuration::hours(i);
            sqlx::query!(
                r#"
                INSERT INTO warehouse_statistics_history (number_of_views, number_of_tables, warehouse_id, timestamp)
                VALUES ($1, $2, $3, $4)
                "#,
                i as i64,
                (i * 2) as i64,
                warehouse.warehouse_id,
                timestamp
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test count-based retention (keep only 3 entries)
        let config = StatisticsRetentionConfig {
            endpoint_max_entries: 0,
            endpoint_max_age: Duration::ZERO,
            warehouse_max_entries: 3,
            warehouse_max_age: Duration::ZERO, // Disable time-based retention
        };

        let deleted = retention_service
            .cleanup_warehouse_statistics(&config)
            .await
            .unwrap();

        assert_eq!(deleted, 2, "Should have deleted 2 excess warehouse statistics entries");

        // Verify only 3 entries remain
        let remaining = sqlx::query!(
            "SELECT COUNT(*) as count FROM warehouse_statistics_history WHERE warehouse_id = $1",
            warehouse.warehouse_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(remaining.count.unwrap(), 3, "Should have 3 warehouse statistics entries remaining");
    }

    #[sqlx::test]
    async fn test_cleanup_all_statistics(pool: sqlx::PgPool) {
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            crate::api::management::v1::warehouse::TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;

        let retention_service = PostgresStatisticsRetentionService::new(pool.clone());
        let project_id = DEFAULT_PROJECT_ID.clone().unwrap();

        // Insert test data for both endpoint and warehouse statistics
        // Endpoint statistics
        sqlx::query!(
            r#"
            INSERT INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            project_id.to_string(),
            warehouse.warehouse_id,
            EndpointFlat::CatalogGetConfig as EndpointFlat,
            200,
            1i64,
            Utc::now() - ChronoDuration::days(10)
        )
        .execute(&pool)
        .await
        .unwrap();

        // Warehouse statistics
        sqlx::query!(
            r#"
            INSERT INTO warehouse_statistics_history (number_of_views, number_of_tables, warehouse_id, timestamp)
            VALUES ($1, $2, $3, $4)
            "#,
            5i64,
            10i64,
            warehouse.warehouse_id,
            Utc::now() - ChronoDuration::days(10)
        )
        .execute(&pool)
        .await
        .unwrap();

        // Test cleanup of both
        let config = StatisticsRetentionConfig {
            endpoint_max_entries: 0,
            endpoint_max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            warehouse_max_entries: 0,
            warehouse_max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
        };

        let (endpoint_deleted, warehouse_deleted) = retention_service
            .cleanup_all_statistics(&config)
            .await
            .unwrap();

        assert_eq!(endpoint_deleted, 1, "Should have deleted 1 endpoint statistics entry");
        assert_eq!(warehouse_deleted, 1, "Should have deleted 1 warehouse statistics entry");
    }

    #[sqlx::test]
    async fn test_disabled_retention_policies(pool: sqlx::PgPool) {
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            crate::api::management::v1::warehouse::TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;

        let retention_service = PostgresStatisticsRetentionService::new(pool.clone());
        let project_id = DEFAULT_PROJECT_ID.clone().unwrap();

        // Insert old data
        sqlx::query!(
            r#"
            INSERT INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            project_id.to_string(),
            warehouse.warehouse_id,
            EndpointFlat::CatalogGetConfig as EndpointFlat,
            200,
            1i64,
            Utc::now() - ChronoDuration::days(10)
        )
        .execute(&pool)
        .await
        .unwrap();

        // Test with all retention policies disabled
        let config = StatisticsRetentionConfig {
            endpoint_max_entries: 0, // Disabled
            endpoint_max_age: Duration::ZERO, // Disabled
            warehouse_max_entries: 0, // Disabled
            warehouse_max_age: Duration::ZERO, // Disabled
        };

        let deleted = retention_service
            .cleanup_endpoint_statistics(&config)
            .await
            .unwrap();

        assert_eq!(deleted, 0, "Should not delete anything when retention is disabled");

        // Verify data still exists
        let remaining = sqlx::query!(
            "SELECT COUNT(*) as count FROM endpoint_statistics WHERE project_id = $1",
            project_id.to_string()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(remaining.count.unwrap(), 1, "Data should still exist when retention is disabled");
    }
} 