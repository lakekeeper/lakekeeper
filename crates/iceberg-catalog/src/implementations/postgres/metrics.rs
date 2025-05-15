use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::service::Result;
use iceberg_ext::catalog::rest::ReportMetricsRequest;

pub(crate) async fn create_metric(
    report_metrics_request: ReportMetricsRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let _ = sqlx::query_scalar!(
        r#"
        INSERT INTO table_metrics (field)
        VALUES ($1)
        "#,
        "Foo"
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!(?e, "Error");
        e.into_error_model(format!("Error creating metric"))
    })?;

    Ok(())
}
