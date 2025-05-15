use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::service::Result;
use iceberg_ext::catalog::rest::ReportMetricsRequest;

pub(crate) async fn create_metric(
    report_metrics_request: ReportMetricsRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let query = match report_metrics_request {
        ReportMetricsRequest::ScanReport(_) => {
            todo!()
        }
        ReportMetricsRequest::CommitReport(report) => {
            sqlx::query_scalar!(
                r#"
                INSERT INTO table_metrics_commit_report (field)
                VALUES ($1)
                "#,
                "Foo"
            )
        }
        ReportMetricsRequest::DatabaseTest(_) => {
            todo!()
        }
    };

    query.fetch_one(&mut **transaction).await.map_err(|e| {
        tracing::warn!(?e, "Error");
        e.into_error_model(format!("Error creating metric"))
    })?;

    Ok(())
}
