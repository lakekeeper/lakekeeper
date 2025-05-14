use axum::Json;
use http::StatusCode;
use super::CatalogServer;
use crate::service::Transaction;
use crate::{
    api::iceberg::v1::{ApiContext, Result, TableParameters},
    request_metadata::RequestMetadata,
    service::{authz::Authorizer, secrets::SecretStore, Catalog, State},
};
use crate::api::management::v1::ApiServer;
use crate::api::management::v1::bootstrap::Service;

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::metrics::MetricsService<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn report_metrics(
        _: TableParameters,
        _: serde_json::Value,
        api_context: ApiContext<State<A, C, S>>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let mut transaction = C::Transaction::begin_write(api_context.v1_state.catalog).await?;
        C::create_metric(transaction.transaction()).await;

        // ApiServer::<C, A, S>::server_info(api_context, metadata)
        //     .await
        //     .map(|user| (StatusCode::OK, Json(user)))?;
        // 
        Ok(())
    }
}
