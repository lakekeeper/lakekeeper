use super::CatalogServer;
use crate::api::iceberg::v1::tables::TablesService;
use crate::api::management::v1::bootstrap::Service;
use crate::api::management::v1::warehouse::TabularDeleteProfile;
use crate::service::{Transaction, UserId};
use crate::{
    api::iceberg::v1::{ApiContext, Result, TableParameters},
    request_metadata::RequestMetadata,
    service::{authz::Authorizer, secrets::SecretStore, Catalog, State},
};
use iceberg_ext::catalog::rest::ReportMetricsRequest;

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::metrics::MetricsService<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn report_metrics(
        _: TableParameters,
        _report_metrics_request: ReportMetricsRequest,
        api_context: ApiContext<State<A, C, S>>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        // Authorisierung authz
        // validation
        // BL innerer catalog
        let mut transaction = C::Transaction::begin_write(api_context.v1_state.catalog).await?;
        C::create_metric(transaction.transaction()).await;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::api::iceberg::v1::metrics::MetricsService;
    use crate::service::authz::tests::HidingAuthorizer;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::metrics::CommitReport;
    use iceberg_ext::catalog::rest::ReportMetricsRequest;

    #[sqlx::test]
    async fn test_store_metric(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::test_io_profile();

        let authz = HidingAuthorizer::new();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let table_params = TableParameters {
            prefix: None,
            table: TableIdent::new(ns.namespace, "test".into()),
        };

        let metrics = ReportMetricsRequest::CommitReport(CommitReport {
            table_name: "".to_string(),
            snapshot_id: 0,
            sequence_number: 0,
            operation: "".to_string(),
            metrics: Default::default(),
            metadata: None,
        });
        // create 10 staged tables
        let _ = CatalogServer::report_metrics(
            table_params,
            metrics,
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }
}
