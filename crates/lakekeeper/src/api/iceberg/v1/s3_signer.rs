use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use iceberg_ext::catalog::rest::{S3SignRequest, S3SignResponse};

use super::{ApiContext, Prefix, Result, TableIdent, namespace::NamespaceIdentUrl};
use crate::request_metadata::RequestMetadata;

/// The spec's per-table signer route, relative to the catalog v1 mount.
///
/// Shared with the test that checks it still resolves to an [`Endpoint`], so the
/// route and the path endpoint statistics are keyed on cannot drift apart.
pub const SIGN_TABLE_ROUTE: &str = "/{prefix}/namespaces/{namespace}/tables/{table}/sign";

/// How a sign request identifies the table it is signing for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignTarget {
    /// The pre-standard global and prefix routes carry no table, so it has to be
    /// recovered from the location in the request URI.
    FromRequestUri,
    /// The pre-standard per-table route, addressed by tabular id.
    TabularId(uuid::Uuid),
    /// The spec's `…/tables/{table}/sign` route, addressed by name.
    Table(Box<TableIdent>),
}

#[async_trait]
pub trait Service<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// Sign a request to object storage.
    ///
    /// Reachable at the spec's `…/namespaces/{namespace}/tables/{table}/sign`,
    /// and at Lakekeeper's older `/aws/s3/sign` routes. The older ones stay
    /// because `signer.endpoint` — still the only endpoint any released client
    /// reads — points at them.
    async fn sign(
        prefix: Option<Prefix>,
        target: SignTarget,
        request: S3SignRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<S3SignResponse>;
}

pub fn router<I: Service<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        .route(
            "/aws/s3/sign",
            post(
                |State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<S3SignRequest>| {
                    I::sign(
                        None,
                        SignTarget::FromRequestUri,
                        request,
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        .route(
            "/{prefix}/v1/aws/s3/sign",
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<S3SignRequest>| {
                    I::sign(
                        Some(prefix),
                        SignTarget::FromRequestUri,
                        request,
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        .route(
            "/signer/{prefix}/tabular-id/{tabular_id}/v1/aws/s3/sign",
            post(
                |Path((prefix, tabular_id)): Path<(Prefix, uuid::Uuid)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<S3SignRequest>| {
                    I::sign(
                        Some(prefix),
                        SignTarget::TabularId(tabular_id),
                        request,
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        .route(
            SIGN_TABLE_ROUTE,
            post(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<S3SignRequest>| {
                    I::sign(
                        Some(prefix),
                        SignTarget::Table(Box::new(TableIdent {
                            namespace: namespace.into(),
                            name: table,
                        })),
                        request,
                        api_context,
                        metadata,
                    )
                },
            ),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::endpoints::Endpoint;

    /// Endpoint statistics key on axum's matched path, so if the route literal
    /// and the endpoint table drift apart the route silently stops being counted.
    #[test]
    fn the_standard_sign_route_resolves_to_an_endpoint() {
        let matched = format!("/catalog/v1{SIGN_TABLE_ROUTE}");
        let endpoint = Endpoint::from_method_and_matched_path(&http::Method::POST, &matched)
            .expect("the standard sign route must be in the endpoint table");
        assert_eq!(endpoint.as_http_route(), format!("POST {matched}"));
    }
}
