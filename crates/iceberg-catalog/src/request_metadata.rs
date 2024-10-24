use crate::service::token_verification::{Actor, AuthDetails};
use axum::middleware::Next;
use axum::response::Response;
use http::HeaderMap;
use std::str::FromStr;
use uuid::Uuid;

/// A struct to hold metadata about a request.
///
/// Currently, it only holds the `request_id`, later it can be expanded to hold more metadata for
/// Authz etc.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    pub request_id: Uuid,
    pub auth_details: AuthDetails,
}

impl RequestMetadata {
    #[cfg(test)]
    #[must_use]
    pub fn new_random() -> Self {
        Self {
            request_id: Uuid::new_v4(),
            auth_details: AuthDetails::Unauthenticated,
        }
    }

    #[must_use]
    pub fn actor(&self) -> &Actor {
        self.auth_details.actor()
    }
}
#[cfg(feature = "router")]
pub(crate) async fn create_request_metadata_with_trace_id_fn(
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id: Uuid = headers
        .get("x-request-id")
        .and_then(|hv| {
            hv.to_str()
                .map(Uuid::from_str)
                .ok()
                .transpose()
                .ok()
                .flatten()
        })
        .unwrap_or(Uuid::now_v7());
    request.extensions_mut().insert(RequestMetadata {
        request_id,
        auth_details: AuthDetails::Unauthenticated,
    });
    next.run(request).await
}
