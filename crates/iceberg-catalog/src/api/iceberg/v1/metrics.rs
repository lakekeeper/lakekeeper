use super::namespace::NamespaceIdentUrl;
use crate::api::iceberg::types::Prefix;
use crate::api::iceberg::v1::tables::TableParameters;
use crate::api::{ApiContext, Result};
use crate::request_metadata::RequestMetadata;
use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Json, Router};
use http::StatusCode;
use iceberg_ext::TableIdent;

#[async_trait]
pub trait Service<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        parameters: TableParameters,
        request: serde_json::Value,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;
}

pub fn router<I: Service<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables/{table}/metrics
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
            post(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<serde_json::Value>| async {
                    {
                        I::report_metrics(
                            TableParameters {
                                prefix: Some(prefix),
                                table: TableIdent {
                                    namespace: namespace.into(),
                                    name: table,
                                },
                            },
                            request,
                            api_context,
                            metadata,
                        )
                    }
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
}
