use async_trait::async_trait;
use axum::{
    extract::{Query, State},
    routing::get,
    Extension, Router,
};
use iceberg_ext::catalog::rest::{CatalogConfig, IcebergErrorResponse};

use crate::{
    api::{ApiContext, Result},
    request_metadata::RequestMetadata,
};

#[async_trait]
pub trait Service<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<CatalogConfig, IcebergErrorResponse>;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GetConfigQueryParams {
    /// Warehouse location or identifier to request from the service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
}

pub fn router<I: Service<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new().route(
        "/config",
        get(
            |Query(query): Query<GetConfigQueryParams>,
             State(api_context): State<ApiContext<S>>,
             Extension(metadata): Extension<RequestMetadata>| {
                I::get_config(query, api_context, metadata)
            },
        ),
    )
}
