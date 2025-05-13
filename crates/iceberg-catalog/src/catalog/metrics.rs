use super::CatalogServer;
use crate::service::Transaction;
use crate::{
    api::iceberg::v1::{ApiContext, Result, TableParameters},
    request_metadata::RequestMetadata,
    service::{authz::Authorizer, secrets::SecretStore, Catalog, State},
};

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::metrics::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn report_metrics(
        _: TableParameters,
        _: serde_json::Value,
        context: ApiContext<State<A, C, S>>,
        _: RequestMetadata,
    ) -> Result<()> {
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        C::create_metric(transaction.transaction()).await;
        Ok(())
    }
}
