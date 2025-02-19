use std::str::FromStr;

use axum::{
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::HeaderMap;
use limes::Authentication;
use uuid::Uuid;

use crate::{service::authn::Actor, ProjectIdent, DEFAULT_PROJECT_ID};

pub const PROJECT_ID_HEADER: &str = "x-project-ident";
pub const X_REQUEST_ID_HEADER: &str = "x-request-id";

const X_FORWARDED_FOR_HEADER: &str = "x-forwarded-for";
const X_FORWARDED_PROTO_HEADER: &str = "x-forwarded-proto";
const X_FORWARDED_PORT_HEADER: &str = "x-forwarded-port";

/// A struct to hold metadata about a request.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    request_id: Uuid,
    project_id: Option<ProjectIdent>,
    authentication: Option<Authentication>,
    host: Option<String>,
    actor: Actor,
}

impl RequestMetadata {
    /// Set authentication information for the request.
    pub fn set_authentication(
        &mut self,
        actor: Actor,
        authentication: Authentication,
    ) -> &mut Self {
        self.actor = actor;
        self.authentication = Some(authentication);
        self
    }

    /// ID of the user performing the request.
    /// This returns the underlying user-id, even if a role is assumed.
    /// Please use `actor()` to get the full actor for `AuthZ` decisions.
    #[must_use]
    pub fn user_id(&self) -> Option<&crate::service::UserId> {
        match &self.actor {
            Actor::Principal(user_id) => Some(user_id),
            Actor::Role { principal, .. } => Some(principal),
            Actor::Anonymous => None,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn new_unauthenticated() -> Self {
        Self {
            request_id: Uuid::now_v7(),
            project_id: None,
            authentication: None,
            host: Some("localhost".to_string()),
            actor: Actor::Anonymous,
        }
    }

    #[must_use]
    pub fn preferred_project_id(&self) -> Option<ProjectIdent> {
        self.project_id.or(*DEFAULT_PROJECT_ID)
    }

    #[cfg(test)]
    #[must_use]
    pub fn random_human(user_id: crate::service::UserId) -> Self {
        Self {
            request_id: Uuid::now_v7(),
            authentication: Some(
                Authentication::builder()
                    .token_header(None)
                    .claims(serde_json::json!({}))
                    .subject(user_id.clone().into())
                    .name(Some("Test User".to_string()))
                    .email(None)
                    .principal_type(None)
                    .build(),
            ),
            host: Some("localhost".to_string()),
            actor: Actor::Principal(user_id),
            project_id: None,
        }
    }

    #[must_use]
    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    #[must_use]
    pub fn authentication(&self) -> Option<&Authentication> {
        self.authentication.as_ref()
    }

    #[must_use]
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.actor.is_authenticated()
    }

    /// Determine the Project ID, return an error if none is provided.
    ///
    /// Resolution order:
    /// 1. User-provided project ID
    /// 2. Project ID from headers
    /// 3. Default project ID
    ///
    /// # Errors
    /// Fails if none of the above methods provide a project ID.
    pub fn require_project_id(
        &self,
        user_project: Option<ProjectIdent>, // Explicitly requested via an API parameter
    ) -> crate::api::Result<ProjectIdent> {
        user_project.or(self.preferred_project_id()).ok_or_else(|| {
            crate::api::ErrorModel::bad_request(
                format!("No project provided. Please provide the `{PROJECT_ID_HEADER}` header"),
                "NoProjectIdProvided",
                None,
            )
            .into()
        })
    }

    /// Get the host that the request was made to.
    ///
    /// Contains the value of the `x-forwarded-for` header if present, otherwise the `host` header.
    /// If either contained invalid utf-8 data, it will be `None`.
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }
}

#[cfg(feature = "router")]
/// Initializes request metadata with a random request ID as an axum Extension.
/// Does not authenticate the request.
///
/// Run this middleware before running [`auth_middleware_fn`](crate::service::authn::auth_middleware_fn).
pub(crate) async fn create_request_metadata_with_trace_and_project_fn(
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id: Uuid = headers
        .get(X_REQUEST_ID_HEADER)
        .and_then(|hv| {
            hv.to_str()
                .map(Uuid::from_str)
                .ok()
                .transpose()
                .ok()
                .flatten()
        })
        .unwrap_or(Uuid::now_v7());

    let host = determine_host(&headers);

    let project_id = headers
        .get(PROJECT_ID_HEADER)
        .and_then(|hv| hv.to_str().ok())
        .map(ProjectIdent::from_str)
        .transpose();
    let project_id = match project_id {
        Ok(ident) => ident,
        Err(err) => return err.into_response(),
    };
    request.extensions_mut().insert(RequestMetadata {
        request_id,
        authentication: None,
        host,
        actor: Actor::Anonymous,
        project_id,
    });
    next.run(request).await
}

fn determine_host(headers: &HeaderMap) -> Option<String> {
    let x_forwarded_for = headers
        .get(X_FORWARDED_FOR_HEADER)
        .and_then(|hv| hv.to_str().ok());
    let x_forwarded_proto = headers
        .get(X_FORWARDED_PROTO_HEADER)
        .and_then(|hv| hv.to_str().ok());
    let x_forwarded_port = headers
        .get(X_FORWARDED_PORT_HEADER)
        .and_then(|hv| hv.to_str().ok());

    let x_forwarded_host = if let Some(forwarded_for) = x_forwarded_for {
        let mut x_forwarded_host = String::new();
        if let Some(proto) = x_forwarded_proto {
            x_forwarded_host.push_str(&format!("{proto}://"));
        } else {
            // we default to https since we assume that a reverse proxy did tls termination
            // leaving out protocol would break at least iceberg java which requires a protocol.
            x_forwarded_host.push_str("https://");
        }

        x_forwarded_host.push_str(forwarded_for);
        if let Some(port) = x_forwarded_port {
            x_forwarded_host.push(':');
            x_forwarded_host.push_str(port);
        }
        Some(x_forwarded_host)
    } else {
        None
    };

    let host = x_forwarded_host.or(headers
        .get(http::header::HOST)
        .map(|hv| hv.to_str().map(ToString::to_string))
        .transpose()
        .ok()
        .flatten()
        .map(|host| {
            if host.starts_with("http://") || host.starts_with("https://") {
                host
            } else {
                format!("http://{host}")
            }
        }));
    host
}

#[cfg(test)]
mod test {
    use http::{header::HeaderValue, HeaderMap};

    use crate::request_metadata::{
        determine_host, X_FORWARDED_FOR_HEADER, X_FORWARDED_PORT_HEADER, X_FORWARDED_PROTO_HEADER,
    };

    #[test]
    fn test_determine_host_with_x_forwarded_complete() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_FOR_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(X_FORWARDED_PORT_HEADER, HeaderValue::from_static("8080"));

        let result = determine_host(&headers);
        assert_eq!(result, Some("https://example.com:8080".to_string()));
    }

    #[test]
    fn test_determine_host_with_x_forwarded_no_port() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_FOR_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));

        let result = determine_host(&headers);
        assert_eq!(result, Some("https://example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_x_forwarded_no_proto() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_FOR_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert("x-forwarded-port", HeaderValue::from_static("8080"));

        let result = determine_host(&headers);
        assert_eq!(result, Some("example.com:8080".to_string()));
    }

    #[test]
    fn test_determine_host_with_only_x_forwarded_for() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_FOR_HEADER,
            HeaderValue::from_static("example.com"),
        );

        let result = determine_host(&headers);
        assert_eq!(result, Some("example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("example.com"));

        let result = determine_host(&headers);
        assert_eq!(result, Some("http://example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_host_header_with_protocol() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("https://example.com"),
        );

        let result = determine_host(&headers);
        assert_eq!(result, Some("https://example.com".to_string()));
    }

    #[test]
    fn test_determine_host_empty_headers() {
        let headers = HeaderMap::new();
        let result = determine_host(&headers);
        assert_eq!(result, None);
    }

    #[test]
    fn test_determine_host_invalid_header_values() {
        let mut headers = HeaderMap::new();
        // Insert an invalid UTF-8 sequence as header value
        headers.insert(
            X_FORWARDED_FOR_HEADER,
            HeaderValue::from_bytes(&[0xFF]).unwrap(),
        );

        let result = determine_host(&headers);
        assert_eq!(result, None);
    }

    #[test]
    fn test_determine_host_prefers_x_forwarded() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_FOR_HEADER,
            HeaderValue::from_static("forwarded.example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("host.example.com"),
        );

        let result = determine_host(&headers);
        assert_eq!(result, Some("https://forwarded.example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_port_in_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("example.com:8080"),
        );

        let result = determine_host(&headers);
        assert_eq!(result, Some("http://example.com:8080".to_string()));
    }
}
