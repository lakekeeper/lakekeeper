use async_trait::async_trait;
use axum::{extract::State, routing::post, Extension, Form, Router};
use iceberg_ext::catalog::rest::{OAuthTokenRequest, OAuthTokenResponse};

use crate::{
    api::{ApiContext, Result},
    request_metadata::RequestMetadata,
};

#[async_trait]
pub trait Service<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    async fn get_token(
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
        // application/x-www-form-urlencoded
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse>;
}

pub fn router<I: Service<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new().route(
        "/oauth/tokens",
        post(
            |State(api_context): State<ApiContext<S>>,
             Extension(metadata): Extension<RequestMetadata>,
             // application/x-www-form-urlencoded
             Form(request): Form<OAuthTokenRequest>| {
                I::get_token(api_context, metadata, request)
            },
        ),
    )
}
