use super::CatalogServer;
use crate::{
    api::iceberg::v1::{ApiContext, ErrorModel, Result, TableParameters},
    catalog::require_warehouse_id,
    request_metadata::RequestMetadata,
    service::{
        authz::Authorizer, secrets::SecretStore, Catalog, ListFlags, State, Transaction as _,
    },
};
use iceberg_ext::spec::Metrics as ExtMetrics;

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::metrics::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn report_metrics(
        params: TableParameters,
        metrics_value: serde_json::Value,
        api_context: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        let parsed_metrics: ExtMetrics = serde_json::from_value(metrics_value).map_err(|e| {
            ErrorModel::bad_request(
                format!("Failed to parse metrics JSON: {}", e),
                "MetricsParsingFailed",
                Some(Box::new(e)),
            )
        })?;

        let warehouse_id = require_warehouse_id(params.prefix)?;

        let mut catalog_transaction =
            C::Transaction::begin_write(api_context.inner().catalog.clone()).await?;

        let table_id = C::table_to_id(
            warehouse_id,
            &params.table,
            ListFlags::default(),
            catalog_transaction.transaction(),
        )
        .await?
        .ok_or_else(|| {
            ErrorModel::not_found(
                format!(
                    "Table {} not found in warehouse {}",
                    params.table, warehouse_id
                ),
                "TableNotFound",
                None,
            )
        })?;

        C::report_table_metrics(
            table_id,
            parsed_metrics,
            catalog_transaction.transaction(),
        )
        .await?;

        catalog_transaction.commit().await?;

        Ok(())
    }
}
