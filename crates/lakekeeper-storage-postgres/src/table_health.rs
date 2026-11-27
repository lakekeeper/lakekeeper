use lakekeeper::{
    api::Result,
    service::table_health::{Severity, TableHealthWrite, TableHealthWriteOutcome},
};
use sqlx::types::Json;

use crate::dbutils::DBErrorHandler as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "health_status", rename_all = "kebab-case")]
enum DbHealthStatus {
    Measured,
    NotMeasurable,
}

/// `max_severity` as stored: 0 for no finding, then increasing with the severity.
fn db_severity(severity: Option<Severity>) -> i16 {
    match severity {
        None => 0,
        Some(Severity::Info) => 1,
        Some(Severity::Warning) => 2,
        Some(Severity::Critical) => 3,
    }
}

pub(crate) async fn write_table_health(
    write: &TableHealthWrite,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<TableHealthWriteOutcome> {
    let sizes = write.sizes();
    let status = if sizes.is_some() {
        DbHealthStatus::Measured
    } else {
        DbHealthStatus::NotMeasurable
    };
    let finding_count = i32::try_from(write.findings().len()).unwrap_or(i32::MAX);
    let written = sqlx::query_scalar!(
        r#"
        INSERT INTO health_iceberg_table (warehouse_id, table_id, measurement_version, status,
                                          measured_at, next_not_before, max_severity,
                                          finding_count, result, live_data_bytes,
                                          live_delete_bytes, live_metadata_bytes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (warehouse_id, table_id) DO UPDATE SET
            measurement_version = excluded.measurement_version,
            status              = excluded.status,
            measured_at         = excluded.measured_at,
            next_not_before     = excluded.next_not_before,
            max_severity        = excluded.max_severity,
            finding_count       = excluded.finding_count,
            result              = excluded.result,
            live_data_bytes     = excluded.live_data_bytes,
            live_delete_bytes   = excluded.live_delete_bytes,
            live_metadata_bytes = excluded.live_metadata_bytes
        WHERE health_iceberg_table.measurement_version <= excluded.measurement_version
        RETURNING true AS "written!"
        "#,
        *write.warehouse_id,
        *write.table_id,
        write.measurement_version,
        status as _,
        write.measured_at,
        write.next_not_before,
        db_severity(write.max_severity()),
        finding_count,
        Json(write.result()) as _,
        sizes.map(|sizes| sizes.live_data_bytes),
        sizes.map(|sizes| sizes.live_delete_bytes),
        sizes.map(|sizes| sizes.live_metadata_bytes),
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error writing table health".to_string()))?;

    Ok(if written.is_some() {
        TableHealthWriteOutcome::Written
    } else {
        TableHealthWriteOutcome::NewerExists
    })
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use chrono::{DateTime, Utc};
    use lakekeeper::{
        WarehouseId,
        service::{
            TableId, TabularId,
            table_health::{
                Finding, FindingKind, MeasureOutcome, Measurement, NotMeasurableReason, Severity,
                Sizes, TableHealthWrite, TableHealthWriteOutcome,
            },
        },
    };
    use lakekeeper_io::Location;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::write_table_health;
    use crate::{
        CatalogState,
        tabular::{
            drop_tabular, mark_tabular_as_deleted,
            table::tests::{InitializedTable, initialize_table},
        },
        warehouse::test::initialize_warehouse,
    };

    struct Row {
        status: &'static str,
        max_severity: i16,
        /// Live data, delete and metadata bytes.
        sizes: Option<(i64, i64, i64)>,
        result: Value,
    }

    fn measured() -> Row {
        Row {
            status: "measured",
            max_severity: 2,
            sizes: Some((100, 0, 10)),
            result: json!({"not-measurable": null, "findings": []}),
        }
    }

    fn not_measurable() -> Row {
        Row {
            status: "not-measurable",
            max_severity: 2,
            sizes: None,
            result: json!({"not-measurable": {"reason": "encrypted"}, "findings": []}),
        }
    }

    async fn insert(
        pool: &sqlx::PgPool,
        warehouse_id: WarehouseId,
        table_id: Uuid,
        row: Row,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO health_iceberg_table (warehouse_id, table_id, measurement_version, \
             status, measured_at, next_not_before, max_severity, finding_count, result, \
             live_data_bytes, live_delete_bytes, live_metadata_bytes) \
             VALUES ($1, $2, 1, $3::health_status, now(), now(), $4, 0, $5, $6, $7, $8)",
        )
        .bind(*warehouse_id)
        .bind(table_id)
        .bind(row.status)
        .bind(row.max_severity)
        .bind(row.result)
        .bind(row.sizes.map(|sizes| sizes.0))
        .bind(row.sizes.map(|sizes| sizes.1))
        .bind(row.sizes.map(|sizes| sizes.2))
        .execute(pool)
        .await
        .map(|_| ())
    }

    /// Name of the constraint that `row` violates.
    async fn violated_constraint(
        pool: &sqlx::PgPool,
        warehouse_id: WarehouseId,
        table_id: Uuid,
        row: Row,
    ) -> String {
        let err = insert(pool, warehouse_id, table_id, row)
            .await
            .expect_err("the row must be rejected");
        err.as_database_error()
            .and_then(|e| e.constraint())
            .expect("a constraint violation")
            .to_string()
    }

    async fn row_count(pool: &sqlx::PgPool, table_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM health_iceberg_table WHERE table_id = $1")
            .bind(table_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn setup(pool: &sqlx::PgPool) -> (CatalogState, WarehouseId, InitializedTable) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;
        (state, warehouse_id, table)
    }

    #[lakekeeper_storage_postgres::db_test]
    async fn measured_and_not_measurable_rows_are_stored(pool: sqlx::PgPool) {
        let (state, warehouse_id, table) = setup(&pool).await;
        let other = initialize_table(warehouse_id, state, false, None, None, None).await;

        insert(&pool, warehouse_id, *table.table_id, measured())
            .await
            .unwrap();
        insert(&pool, warehouse_id, *other.table_id, not_measurable())
            .await
            .unwrap();

        assert_eq!(row_count(&pool, *table.table_id).await, 1);
        assert_eq!(row_count(&pool, *other.table_id).await, 1);
    }

    #[lakekeeper_storage_postgres::db_test]
    async fn checks_reject_inconsistent_rows(pool: sqlx::PgPool) {
        let (_, warehouse_id, table) = setup(&pool).await;
        let id = *table.table_id;

        let with_size = |sizes| Row {
            sizes,
            ..measured()
        };
        let unknown_severity = Row {
            max_severity: 4,
            ..measured()
        };
        let oversized_result = Row {
            result: json!({"findings": [], "padding": "x".repeat(1_048_576)}),
            ..measured()
        };

        for (row, constraint) in [
            (with_size(None), "health_iceberg_table_sizes_check"),
            (
                with_size(Some((-1, 0, 10))),
                "health_iceberg_table_sizes_check",
            ),
            (
                Row {
                    sizes: Some((0, 0, 10)),
                    ..not_measurable()
                },
                "health_iceberg_table_sizes_check",
            ),
            (unknown_severity, "health_iceberg_table_max_severity_check"),
            (oversized_result, "health_iceberg_table_result_check"),
        ] {
            assert_eq!(
                violated_constraint(&pool, warehouse_id, id, row).await,
                constraint
            );
        }
        assert_eq!(row_count(&pool, id).await, 0);
    }

    /// Only an Iceberg table can have a row: views and generic tables have no row in `"table"`.
    #[lakekeeper_storage_postgres::db_test]
    async fn rows_need_an_iceberg_table(pool: sqlx::PgPool) {
        let (_, warehouse_id, _) = setup(&pool).await;

        assert_eq!(
            violated_constraint(&pool, warehouse_id, Uuid::now_v7(), measured()).await,
            "health_iceberg_table_table_id_fkey"
        );
    }

    /// A soft-deleted table keeps its row. Dropping the table removes it, and the other table
    /// keeps its row.
    #[lakekeeper_storage_postgres::db_test]
    async fn a_hard_drop_removes_the_row(pool: sqlx::PgPool) {
        let (state, warehouse_id, table) = setup(&pool).await;
        let other = initialize_table(warehouse_id, state, false, None, None, None).await;
        for id in [*table.table_id, *other.table_id] {
            insert(&pool, warehouse_id, id, measured()).await.unwrap();
        }
        let tabular_id = TabularId::Table(table.table_id);

        let mut transaction = pool.begin().await.unwrap();
        mark_tabular_as_deleted(warehouse_id, tabular_id, false, None, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(row_count(&pool, *table.table_id).await, 1);

        let mut transaction = pool.begin().await.unwrap();
        drop_tabular(warehouse_id, tabular_id, false, None, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(row_count(&pool, *table.table_id).await, 0);
        assert_eq!(row_count(&pool, *other.table_id).await, 1);
    }

    fn measured_at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn health_write(
        warehouse_id: WarehouseId,
        table_id: TableId,
        version: i32,
        outcome: MeasureOutcome,
    ) -> TableHealthWrite {
        TableHealthWrite {
            warehouse_id,
            table_id,
            measurement_version: version,
            measured_at: measured_at(),
            next_not_before: measured_at() + chrono::TimeDelta::hours(1),
            outcome,
            metadata_location: Location::from_str("s3://bucket/t/metadata/00001.metadata.json")
                .unwrap(),
            scan_duration: Duration::from_millis(4100),
        }
    }

    fn measured_with_finding() -> MeasureOutcome {
        MeasureOutcome::Measured(Measurement {
            sizes: Sizes {
                live_data_bytes: 100,
                live_delete_bytes: 20,
                live_metadata_bytes: 3,
            },
            findings: vec![Finding {
                kind: FindingKind::ManifestLengthMismatch,
                severity: Severity::Warning,
                evidence: json!({"count": 1}),
                thresholds: json!({}),
            }],
            snapshot_id: Some(7),
            manifests_read: 2,
        })
    }

    async fn write(pool: &sqlx::PgPool, write: &TableHealthWrite) -> TableHealthWriteOutcome {
        let mut transaction = pool.begin().await.unwrap();
        let outcome = write_table_health(write, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();
        outcome
    }

    /// The stored row: version, status, measured at, next not before, max severity, finding
    /// count, live data, delete and metadata bytes, result.
    type StoredRow = (
        i32,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        i16,
        i32,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Value,
    );

    async fn stored(pool: &sqlx::PgPool, table_id: TableId) -> StoredRow {
        sqlx::query_as(
            "SELECT measurement_version, status::text, measured_at, next_not_before, \
             max_severity, finding_count, live_data_bytes, live_delete_bytes, \
             live_metadata_bytes, result \
             FROM health_iceberg_table WHERE table_id = $1",
        )
        .bind(*table_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[lakekeeper_storage_postgres::db_test]
    async fn a_measured_table_is_stored(pool: sqlx::PgPool) {
        let (_, warehouse_id, table) = setup(&pool).await;

        let outcome = write(
            &pool,
            &health_write(warehouse_id, table.table_id, 1, measured_with_finding()),
        )
        .await;

        assert_eq!(outcome, TableHealthWriteOutcome::Written);
        assert_eq!(
            stored(&pool, table.table_id).await,
            (
                1,
                "measured".to_string(),
                measured_at(),
                measured_at() + chrono::TimeDelta::hours(1),
                2,
                1,
                Some(100),
                Some(20),
                Some(3),
                json!({
                    "not-measurable": null,
                    "findings": [{
                        "kind": "manifest-length-mismatch",
                        "severity": "warning",
                        "evidence": {"count": 1},
                        "thresholds": {}
                    }],
                    "metadata-location": "s3://bucket/t/metadata/00001.metadata.json",
                    "snapshot-id": 7,
                    "scan-duration-ms": 4100,
                    "manifests-read": 2
                }),
            )
        );
    }

    /// A table that can't be measured has no sizes, and ranks as a warning.
    #[lakekeeper_storage_postgres::db_test]
    async fn a_not_measurable_table_replaces_its_measurement(pool: sqlx::PgPool) {
        let (_, warehouse_id, table) = setup(&pool).await;
        write(
            &pool,
            &health_write(warehouse_id, table.table_id, 1, measured_with_finding()),
        )
        .await;

        let outcome = write(
            &pool,
            &health_write(
                warehouse_id,
                table.table_id,
                1,
                MeasureOutcome::NotMeasurable(NotMeasurableReason::MissingFileSize {
                    path: "s3://bucket/t/data/a.parquet".to_string(),
                }),
            ),
        )
        .await;

        assert_eq!(outcome, TableHealthWriteOutcome::Written);
        let (_, status, _, _, max_severity, finding_count, data, delete, metadata, result) =
            stored(&pool, table.table_id).await;
        assert_eq!(
            (
                status.as_str(),
                max_severity,
                finding_count,
                data,
                delete,
                metadata
            ),
            ("not-measurable", 2, 0, None, None, None)
        );
        assert_eq!(
            result,
            json!({
                "not-measurable": {
                    "reason": "missing-file-size",
                    "path": "s3://bucket/t/data/a.parquet"
                },
                "findings": [],
                "metadata-location": "s3://bucket/t/metadata/00001.metadata.json",
                "snapshot-id": null,
                "scan-duration-ms": 4100,
                "manifests-read": null
            })
        );
    }

    /// An older measurement version keeps a newer row, findings included. The same or a newer
    /// version replaces it.
    #[lakekeeper_storage_postgres::db_test]
    async fn an_older_version_keeps_a_newer_row(pool: sqlx::PgPool) {
        let (_, warehouse_id, table) = setup(&pool).await;
        let no_findings = || {
            MeasureOutcome::Measured(Measurement {
                sizes: Sizes::default(),
                findings: vec![],
                snapshot_id: None,
                manifests_read: 0,
            })
        };
        write(
            &pool,
            &health_write(warehouse_id, table.table_id, 2, measured_with_finding()),
        )
        .await;
        let newer = stored(&pool, table.table_id).await;

        let outcome = write(
            &pool,
            &health_write(warehouse_id, table.table_id, 1, no_findings()),
        )
        .await;
        assert_eq!(outcome, TableHealthWriteOutcome::NewerExists);
        assert_eq!(stored(&pool, table.table_id).await, newer);

        for version in [2, 3] {
            let outcome = write(
                &pool,
                &health_write(warehouse_id, table.table_id, version, no_findings()),
            )
            .await;
            assert_eq!(outcome, TableHealthWriteOutcome::Written);
            let (stored_version, _, _, _, max_severity, finding_count, _, _, _, _) =
                stored(&pool, table.table_id).await;
            assert_eq!(
                (stored_version, max_severity, finding_count),
                (version, 0, 0)
            );
        }
    }
}
