use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::service::Result;
use iceberg_ext::catalog::rest::ReportMetricsRequest;

pub(crate) async fn create_metric(
    report_metrics_request: ReportMetricsRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let query = match report_metrics_request {
        ReportMetricsRequest::ScanReport(report) => {
            let report_filter = serde_json::to_value(report.filter).unwrap();
            let report_metrics = serde_json::to_value(report.metrics).unwrap();
            let report_metadata = serde_json::to_value(report.metadata).unwrap();

            sqlx::query_scalar!(
                r#"
                WITH tabular_id_selector AS (
                    SELECT tabular_id FROM tabular WHERE name = $1)
                INSERT INTO table_metrics_scan_report (table_id, snapshot_id, filter, schema_id, projected_field_ids, projected_field_names, metrics, metadata)
                SELECT tabular_id_selector.tabular_id, $2, $3, $4, $5, $6, $7, $8
                FROM tabular_id_selector
                RETURNING metric_id;
                "#,
                report.table_name,
                report.snapshot_id,
                report_filter,
                report.schema_id,
                &report.projected_field_ids,
                &report.projected_field_names,
                report_metrics,
                report_metadata,
            )
        }
        ReportMetricsRequest::CommitReport(report) => {
            let report_metrics = serde_json::to_value(report.metrics).unwrap();
            let report_metadata = serde_json::to_value(report.metadata).unwrap();
            sqlx::query_scalar!(
                r#"
                WITH tabular_id_selector AS (
                    SELECT tabular_id FROM tabular WHERE name = $1)
                INSERT INTO table_metrics_commit_report (table_id, snapshot_id, sequence_number, operation, metrics, metadata)
                SELECT tabular_id_selector.tabular_id, $2, $3, $4, $5, $6
                FROM tabular_id_selector
                RETURNING metric_id;
                "#,
                report.table_name,
                report.snapshot_id,
                report.sequence_number,
                report.operation,
                report_metrics,
                report_metadata,
            )
        }
    };

    let _ = query.fetch_one(&mut **transaction).await.map_err(|e| {
        tracing::warn!(?e, "Error");
        e.into_error_model(format!("Error creating metric"))
    })?;

    Ok(())
}
