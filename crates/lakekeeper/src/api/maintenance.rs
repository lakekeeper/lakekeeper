//! Middleware that rejects mutating requests while the service is in
//! maintenance mode. Intended to support zero-downtime upgrades performed by a
//! Kubernetes operator: the operator does a rolling restart that sets
//! [`MaintenanceMode::ReadOnly`] on every pod, then runs database migrations,
//! then does a second rolling restart that removes the flag.
//!
//! The flag is captured once at startup (see [`crate::config::CONFIG`]) and is
//! not dynamic. Mutating means anything other than `GET`, `HEAD`, or `OPTIONS`.
//! Read endpoints with write side-effects (e.g. user auto-registration on
//! `GET /v1/config`) suppress those side-effects in their own handlers.

use axum::{
    body::Body,
    extract::Request,
    http::{Method, StatusCode, header::RETRY_AFTER},
    middleware::Next,
    response::{IntoResponse, Response},
};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

/// HTTP `Retry-After` value (seconds) returned with maintenance-mode 503s.
///
/// Iceberg Java honors `Retry-After` for idempotent requests and for
/// non-idempotent (`updateTable`) requests only when the header is present.
/// `PyIceberg` ignores it. Sixty seconds is a sensible default for a brief
/// migration window; jitter is up to the operator (e.g. by staggering new
/// pod readiness).
pub const MAINTENANCE_RETRY_AFTER_SECONDS: u64 = 60;

/// Error code returned in [`ErrorModel::r#type`] when a request is blocked
/// because the server is in maintenance mode. Clients can branch on this to
/// distinguish a maintenance window from a generic outage.
pub const MAINTENANCE_ERROR_TYPE: &str = "MaintenanceModeError";

/// Returns `true` for HTTP methods we treat as writes. We deliberately do not
/// inspect the route: `POST /v1/search` style endpoints are blocked during
/// maintenance, which is acceptable for a planned upgrade window. The one
/// `GET`-with-write side-effect we know of (`GET /v1/config` user auto-register)
/// is suppressed in the handler itself, not here.
fn is_mutating(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// Build the standardized 503 response.
fn maintenance_response() -> Response {
    let err: IcebergErrorResponse = ErrorModel::builder()
        .code(StatusCode::SERVICE_UNAVAILABLE.as_u16())
        .r#type(MAINTENANCE_ERROR_TYPE.to_string())
        .message(
            "Lakekeeper is in read-only maintenance mode. Mutating requests are temporarily rejected; retry after the maintenance window completes."
                .to_string(),
        )
        .build()
        .into();

    let mut response = (StatusCode::SERVICE_UNAVAILABLE, axum::Json(err)).into_response();
    response.headers_mut().insert(
        RETRY_AFTER,
        MAINTENANCE_RETRY_AFTER_SECONDS.to_string().parse().expect(
            "MAINTENANCE_RETRY_AFTER_SECONDS formats as ASCII digits, always a valid header value",
        ),
    );
    response
}

/// Axum middleware. Apply with [`axum::middleware::from_fn`] to a router that
/// contains only routes that should be subject to the gate (i.e. the merged
/// `/catalog/v1` + `/management/v1` nest, *not* `/health`).
#[cfg(feature = "router")]
pub(crate) async fn maintenance_middleware_fn(request: Request<Body>, next: Next) -> Response {
    if crate::CONFIG.maintenance_mode.is_read_only() && is_mutating(request.method()) {
        tracing::debug!(
            method = %request.method(),
            path = request.uri().path(),
            "Rejecting mutating request: maintenance mode active",
        );
        return maintenance_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use axum::{Router, body::Body, middleware, routing::get};
    use http::{Request, StatusCode};
    use tower::ServiceExt as _;

    use super::*;
    use crate::config::MaintenanceMode;

    /// We can't easily flip the global `CONFIG.maintenance_mode` from a test
    /// without process-wide side effects, so we test the building blocks
    /// directly: `is_mutating`, `maintenance_response`, and a thin wrapper
    /// middleware that takes the mode as a parameter.
    #[test]
    fn is_mutating_classifies_methods() {
        assert!(!is_mutating(&Method::GET));
        assert!(!is_mutating(&Method::HEAD));
        assert!(!is_mutating(&Method::OPTIONS));
        assert!(is_mutating(&Method::POST));
        assert!(is_mutating(&Method::PUT));
        assert!(is_mutating(&Method::PATCH));
        assert!(is_mutating(&Method::DELETE));
    }

    #[test]
    fn maintenance_response_has_retry_after_and_error_model() {
        let resp = maintenance_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let retry_after = resp
            .headers()
            .get(RETRY_AFTER)
            .expect("Retry-After header is set on maintenance responses");
        assert_eq!(retry_after.to_str().unwrap(), "60");
    }

    /// Parameterized version of [`maintenance_middleware_fn`] used only by
    /// tests so they can drive the mode explicitly.
    async fn gate(mode: MaintenanceMode, request: Request<Body>, next: Next) -> Response {
        if mode.is_read_only() && is_mutating(request.method()) {
            return maintenance_response();
        }
        next.run(request).await
    }

    fn router_with_mode(mode: MaintenanceMode) -> Router {
        Router::new()
            .route("/r", get(|| async { "ok" }).post(|| async { "ok" }))
            .layer(middleware::from_fn(move |req, next| gate(mode, req, next)))
    }

    #[tokio::test]
    async fn read_only_blocks_post_returns_503_with_retry_after() {
        let app = router_with_mode(MaintenanceMode::ReadOnly);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/r")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers().get(RETRY_AFTER).unwrap().to_str().unwrap(),
            "60"
        );

        // Body parses as an IcebergErrorResponse with the maintenance type.
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: IcebergErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.error.r#type, MAINTENANCE_ERROR_TYPE);
        assert_eq!(parsed.error.code, StatusCode::SERVICE_UNAVAILABLE.as_u16());
    }

    #[tokio::test]
    async fn read_only_passes_get() {
        let app = router_with_mode(MaintenanceMode::ReadOnly);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/r")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn off_passes_post() {
        let app = router_with_mode(MaintenanceMode::Off);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/r")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
