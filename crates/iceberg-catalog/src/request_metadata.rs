use std::str::FromStr;

use axum::{extract::MatchedPath, middleware::Next, response::Response};
use http::{HeaderMap, Method};
use uuid::Uuid;

use crate::{
    service::authn::{Actor, AuthDetails},
    ProjectIdent, DEFAULT_PROJECT_ID,
};

/// A struct to hold metadata about a request.
///
/// Currently, it only holds the `request_id`, later it can be expanded to hold more metadata for
/// Authz etc.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    pub request_id: Uuid,
    pub request_method: Method,
    pub matched_path: Option<MatchedPath>,
    pub uri: String,
    pub auth_details: AuthDetails,
    pub project_id_header: Option<ProjectIdent>,
}

impl RequestMetadata {
    #[cfg(test)]
    #[must_use]
    pub fn new_random() -> Self {
        Self {
            request_id: Uuid::new_v4(),
            auth_details: AuthDetails::Unauthenticated,
            matched_path: None,
            uri: "/".to_string(),
            project_id_header: None,
            request_method: Method::GET,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn random_human(user_id: crate::service::UserId) -> Self {
        use crate::service::authn::Principal;

        Self {
            request_id: Uuid::now_v7(),
            request_method: Method::GET,
            matched_path: None,
            uri: String::new(),
            auth_details: AuthDetails::Principal(Principal::random_human(user_id)),
            project_id_header: None,
        }
    }

    #[must_use]
    pub fn actor(&self) -> &Actor {
        self.auth_details.actor()
    }

    #[must_use]
    pub fn project_id(&self) -> Option<ProjectIdent> {
        self.project_id_header.or(*DEFAULT_PROJECT_ID)
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

    let project_id = headers
        .get("x-project-id")
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| ProjectIdent::from_str(s).ok());

    let matched_path = request.extensions().get::<MatchedPath>().cloned();
    let uri = request.uri().to_string();
    let method = request.method().clone();
    request.extensions_mut().insert(RequestMetadata {
        request_id,
        matched_path,
        uri,
        request_method: method,
        auth_details: AuthDetails::Unauthenticated,
        project_id_header: project_id,
    });
    next.run(request).await
}
