use base64::Engine as _;
use chrono::{DateTime, Utc};
use lakekeeper::{
    WarehouseId,
    api::{
        Result,
        iceberg::v1::{PaginatedMapping, PaginationQuery},
        management::v1::health::{
            FindingSeverity, NotMeasurableReason, TableHealthOrderBy, TableHealthStatus,
            TableHealthSummary,
        },
    },
    service::{
        InvalidPaginationToken, TableId,
        table_health::{Severity, TableHealthWrite, TableHealthWriteOutcome},
    },
};
use sqlx::types::Json;
use uuid::Uuid;

use crate::dbutils::DBErrorHandler as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "health_status", rename_all = "kebab-case")]
enum DbHealthStatus {
    Measured,
    NotMeasurable,
}

impl From<DbHealthStatus> for TableHealthStatus {
    fn from(status: DbHealthStatus) -> Self {
        match status {
            DbHealthStatus::Measured => TableHealthStatus::Measured,
            DbHealthStatus::NotMeasurable => TableHealthStatus::NotMeasurable,
        }
    }
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

/// The severity of a stored `max_severity`. The table's check allows 0 to 3 only.
fn severity_from_db(severity: i16) -> Option<FindingSeverity> {
    match severity {
        1 => Some(FindingSeverity::Info),
        2 => Some(FindingSeverity::Warning),
        3.. => Some(FindingSeverity::Critical),
        _ => None,
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

/// Position in the ranking: the last row's sort keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RankToken {
    max_severity: i16,
    live_data_bytes: i64,
    table_id: Uuid,
}

impl RankToken {
    fn encode(self) -> String {
        let RankToken {
            max_severity,
            live_data_bytes,
            table_id,
        } = self;
        base64::prelude::BASE64_URL_SAFE_NO_PAD
            .encode(format!("s&{max_severity}&{live_data_bytes}&{table_id}"))
    }

    /// Reads a token of the ranking in `order_by`. A token of another order is invalid.
    fn decode(token: &str, order_by: TableHealthOrderBy) -> Result<Self, InvalidPaginationToken> {
        let invalid = || InvalidPaginationToken::new("Invalid table health page token", token);
        let decoded = base64::prelude::BASE64_URL_SAFE_NO_PAD
            .decode(token)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .ok_or_else(invalid)?;
        let parts = decoded.split('&').collect::<Vec<_>>();
        let parsed = match (order_by, parts.as_slice()) {
            (TableHealthOrderBy::Severity, ["s", severity, bytes, id]) => (|| {
                Some(RankToken {
                    max_severity: severity.parse().ok()?,
                    live_data_bytes: bytes.parse().ok()?,
                    table_id: id.parse().ok()?,
                })
            })(),
            _ => None,
        };
        parsed.ok_or_else(invalid)
    }
}

/// A row of the ranking. `rank_bytes` is the second sort key.
struct RankingRow {
    table_id: Uuid,
    namespace: Vec<String>,
    name: String,
    status: DbHealthStatus,
    max_severity: i16,
    finding_count: i32,
    live_data_bytes: Option<i64>,
    live_delete_bytes: Option<i64>,
    live_metadata_bytes: Option<i64>,
    not_measurable: Option<Json<NotMeasurableReason>>,
    measured_at: DateTime<Utc>,
    measurement_version: i32,
    rank_bytes: i64,
}

/// The ranking's sort expressions are the same as in the index
/// `health_iceberg_table_rank_severity`, so that a page reads only its rows from the index.
pub(crate) async fn list_table_health(
    warehouse_id: WarehouseId,
    order_by: TableHealthOrderBy,
    include_soft_deleted: bool,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
    connection: &mut sqlx::PgConnection,
) -> Result<PaginatedMapping<TableId, TableHealthSummary>> {
    let page_size = lakekeeper::CONFIG.page_size_or_pagination_default(page_size);
    let token = page_token
        .as_option()
        .map(|token| RankToken::decode(token, order_by))
        .transpose()?;

    let rows = match order_by {
        TableHealthOrderBy::Severity => {
            sqlx::query_as!(
                RankingRow,
                r#"
                SELECT h.table_id,
                       t.tabular_namespace_name AS namespace,
                       t.name,
                       h.status AS "status: DbHealthStatus",
                       h.max_severity,
                       h.finding_count,
                       h.live_data_bytes,
                       h.live_delete_bytes,
                       h.live_metadata_bytes,
                       CASE WHEN h.status = 'not-measurable' THEN h.result -> 'not-measurable' END
                           AS "not_measurable: Json<NotMeasurableReason>",
                       h.measured_at,
                       h.measurement_version,
                       coalesce(h.live_data_bytes, -1) AS "rank_bytes!"
                FROM health_iceberg_table h
                JOIN tabular t ON t.warehouse_id = h.warehouse_id AND t.tabular_id = h.table_id
                WHERE h.warehouse_id = $1
                  AND t.typ = 'table'
                  AND ($2 OR t.deleted_at IS NULL)
                  AND ($3::smallint IS NULL
                       OR (h.max_severity, coalesce(h.live_data_bytes, -1), h.table_id)
                          < ($3, $4, $5))
                ORDER BY h.max_severity DESC,
                         coalesce(h.live_data_bytes, -1) DESC,
                         h.table_id DESC
                LIMIT $6
                "#,
                *warehouse_id,
                include_soft_deleted,
                token.map(|token| token.max_severity),
                token.map(|token| token.live_data_bytes),
                token.map(|token| token.table_id),
                page_size,
            )
            .fetch_all(connection)
            .await
        }
    }
    .map_err(|e| e.into_error_model("Error listing table health".to_string()))?;

    let mut page = PaginatedMapping::with_capacity(rows.len());
    for row in rows {
        let token = RankToken {
            max_severity: row.max_severity,
            live_data_bytes: row.rank_bytes,
            table_id: row.table_id,
        };
        let table_id = TableId::from(row.table_id);
        page.insert(
            table_id,
            TableHealthSummary {
                table_id,
                namespace: row.namespace,
                name: row.name,
                status: row.status.into(),
                not_measurable_reason: row.not_measurable.map(|Json(reason)| reason),
                max_severity: severity_from_db(row.max_severity),
                finding_count: row.finding_count,
                live_data_bytes: row.live_data_bytes,
                live_delete_bytes: row.live_delete_bytes,
                live_metadata_bytes: row.live_metadata_bytes,
                measured_at: row.measured_at,
                measurement_version: row.measurement_version,
            },
            token.encode(),
        );
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use base64::Engine as _;
    use chrono::{DateTime, Utc};
    use lakekeeper::{
        WarehouseId,
        api::{
            iceberg::{types::PageToken, v1::PaginationQuery},
            management::v1::health::{
                FindingSeverity, NotMeasurableReason as ApiReason, TableHealthOrderBy,
                TableHealthStatus, TableHealthSummary,
            },
        },
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

    use super::{list_table_health, write_table_health};
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

    /// Ids of the ranking, read page by page.
    async fn ranking(
        pool: &sqlx::PgPool,
        warehouse_id: WarehouseId,
        include_soft_deleted: bool,
    ) -> Vec<TableId> {
        let mut connection = pool.acquire().await.unwrap();
        let mut ids = vec![];
        let mut page_token = PageToken::NotSpecified;
        loop {
            let page = list_table_health(
                warehouse_id,
                TableHealthOrderBy::Severity,
                include_soft_deleted,
                PaginationQuery {
                    page_size: Some(2),
                    page_token: page_token.clone(),
                },
                &mut connection,
            )
            .await
            .unwrap();
            let Some(token) = page.next_token().map(ToString::to_string) else {
                return ids;
            };
            ids.extend(page.into_iter().map(|(id, _)| id));
            page_token = PageToken::Present(token);
        }
    }

    fn measured_row(max_severity: i16, live_data_bytes: i64) -> Row {
        Row {
            max_severity,
            sizes: Some((live_data_bytes, 1_000_000, 1_000_000)),
            ..measured()
        }
    }

    /// Highest severity first, then most live data bytes; a table that can't be measured ranks
    /// as a warning without bytes. The other sizes don't count. Every row comes once over pages
    /// of two. Soft-deleted tables only with `include_soft_deleted`.
    #[lakekeeper_storage_postgres::db_test]
    async fn the_ranking_pages_through_every_table_in_order(pool: sqlx::PgPool) {
        let (state, warehouse_id, healthy) = setup(&pool).await;
        let table = || initialize_table(warehouse_id, state.clone(), false, None, None, None);
        let (small_warning, not_measurable_table, large_warning, deleted) =
            (table().await, table().await, table().await, table().await);
        let other_project = lakekeeper::ProjectId::from(Uuid::now_v7());
        let other_warehouse =
            initialize_warehouse(state.clone(), None, Some(&other_project), None, true)
                .await
                .1;
        let elsewhere =
            initialize_table(other_warehouse, state.clone(), false, None, None, None).await;
        for (warehouse, table, row) in [
            (warehouse_id, &healthy, measured_row(0, 100)),
            (warehouse_id, &small_warning, measured_row(2, 50)),
            (warehouse_id, &not_measurable_table, not_measurable()),
            (warehouse_id, &large_warning, measured_row(2, 500)),
            (warehouse_id, &deleted, measured_row(0, 1000)),
            (other_warehouse, &elsewhere, measured_row(3, 1)),
        ] {
            insert(&pool, warehouse, *table.table_id, row)
                .await
                .unwrap();
        }
        let mut transaction = pool.begin().await.unwrap();
        mark_tabular_as_deleted(
            warehouse_id,
            TabularId::Table(deleted.table_id),
            false,
            None,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let ids =
            |tables: &[&InitializedTable]| tables.iter().map(|t| t.table_id).collect::<Vec<_>>();
        assert_eq!(
            ranking(&pool, warehouse_id, false).await,
            ids(&[
                &large_warning,
                &small_warning,
                &not_measurable_table,
                &healthy
            ])
        );
        assert_eq!(
            ranking(&pool, warehouse_id, true).await,
            ids(&[
                &large_warning,
                &small_warning,
                &not_measurable_table,
                &deleted,
                &healthy
            ])
        );
    }

    #[lakekeeper_storage_postgres::db_test]
    async fn a_ranking_row_carries_the_stored_values(pool: sqlx::PgPool) {
        let (state, warehouse_id, table) = setup(&pool).await;
        let other = initialize_table(warehouse_id, state, false, None, None, None).await;
        write(
            &pool,
            &health_write(warehouse_id, table.table_id, 1, measured_with_finding()),
        )
        .await;
        insert(&pool, warehouse_id, *other.table_id, not_measurable())
            .await
            .unwrap();

        let mut connection = pool.acquire().await.unwrap();
        let page = list_table_health(
            warehouse_id,
            TableHealthOrderBy::Severity,
            false,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::NotSpecified,
            },
            &mut connection,
        )
        .await
        .unwrap();
        let rows = page.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                TableHealthSummary {
                    table_id: table.table_id,
                    namespace: table.namespace.clone().inner(),
                    name: table.table_ident.name.clone(),
                    status: TableHealthStatus::Measured,
                    not_measurable_reason: None,
                    max_severity: Some(FindingSeverity::Warning),
                    finding_count: 1,
                    live_data_bytes: Some(100),
                    live_delete_bytes: Some(20),
                    live_metadata_bytes: Some(3),
                    measured_at: measured_at(),
                    measurement_version: 1,
                },
                TableHealthSummary {
                    table_id: other.table_id,
                    namespace: other.namespace.clone().inner(),
                    name: other.table_ident.name.clone(),
                    status: TableHealthStatus::NotMeasurable,
                    not_measurable_reason: Some(ApiReason::Encrypted),
                    max_severity: Some(FindingSeverity::Warning),
                    finding_count: 0,
                    live_data_bytes: None,
                    live_delete_bytes: None,
                    live_metadata_bytes: None,
                    measured_at: rows[1].measured_at,
                    measurement_version: 1,
                },
            ]
        );
    }

    /// A token of another order, or one that isn't a token of the ranking, is a bad request.
    #[lakekeeper_storage_postgres::db_test]
    async fn a_page_token_of_another_order_is_rejected(pool: sqlx::PgPool) {
        let (_, warehouse_id, table) = setup(&pool).await;
        insert(&pool, warehouse_id, *table.table_id, measured())
            .await
            .unwrap();
        let other_order =
            base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(format!("r&100&{}", table.table_id));

        for token in [other_order, "not-a-token".to_string()] {
            let mut connection = pool.acquire().await.unwrap();
            let error = list_table_health(
                warehouse_id,
                TableHealthOrderBy::Severity,
                false,
                PaginationQuery {
                    page_size: Some(1),
                    page_token: PageToken::Present(token),
                },
                &mut connection,
            )
            .await
            .unwrap_err();
            assert_eq!(error.error.code, 400);
        }
    }

    /// Plan of `sql`, with sequential and bitmap scans and sorts off. The test tables are empty,
    /// so the planner would read them whole and sort; this way it reads an index in order if
    /// one gives the order.
    async fn plan(pool: &sqlx::PgPool, sql: &str) -> String {
        let mut transaction = pool.begin().await.unwrap();
        for setting in [
            "SET LOCAL enable_seqscan = off",
            "SET LOCAL enable_bitmapscan = off",
            "SET LOCAL enable_sort = off",
        ] {
            sqlx::query(setting)
                .execute(&mut *transaction)
                .await
                .unwrap();
        }
        // `sql` is a literal of the test below.
        let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN {sql}")))
            .fetch_all(&mut *transaction)
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        rows.join("\n")
    }

    /// The ranking query can read its index in order, on the first page and after a token: the
    /// sort expressions match the index. The SQL is that of `list_table_health`, with values for
    /// the parameters.
    #[lakekeeper_storage_postgres::db_test]
    async fn the_ranking_query_reads_its_index(pool: sqlx::PgPool) {
        let severity = |token: &str| {
            format!(
                "SELECT h.table_id FROM health_iceberg_table h \
                 JOIN tabular t ON t.warehouse_id = h.warehouse_id AND t.tabular_id = h.table_id \
                 WHERE h.warehouse_id = '00000000-0000-0000-0000-000000000001' \
                 AND t.typ = 'table' AND (false OR t.deleted_at IS NULL) \
                 AND ({token}::smallint IS NULL \
                      OR (h.max_severity, coalesce(h.live_data_bytes, -1), h.table_id) \
                         < ({token}, 10, '00000000-0000-0000-0000-000000000002')) \
                 ORDER BY h.max_severity DESC, \
                          coalesce(h.live_data_bytes, -1) DESC, \
                          h.table_id DESC \
                 LIMIT 100"
            )
        };
        for sql in [severity("NULL"), severity("2")] {
            let plan = plan(&pool, &sql).await;
            assert!(
                plan.contains("Index Scan using health_iceberg_table_rank_severity"),
                "{plan}"
            );
            assert!(!plan.contains("Sort"), "{plan}");
        }
    }
}
