//! Middleware that rejects mutating requests while the service is in
//! maintenance mode. Intended to support zero-downtime upgrades performed by a
//! Kubernetes operator: the operator does a rolling restart that sets
//! [`MaintenanceMode::ReadOnly`] on every pod, then runs database migrations,
//! then does a second rolling restart that removes the flag.
//!
//! The gate follows the effective mode of this process
//! ([`crate::service::maintenance_mode::MAINTENANCE`]): the startup mode is a
//! floor, and an instance admin can switch the process into read-only at runtime.
//! Mutating means anything other than `GET`, `HEAD`, or `OPTIONS`.
//! Read endpoints with write side-effects (e.g. user auto-registration on
//! `GET /v1/config`) suppress those side-effects in their own handlers.

use std::{collections::HashSet, sync::Arc};

use axum::{
    Router,
    body::Body,
    extract::{MatchedPath, Request, State},
    http::{Method, StatusCode, header::RETRY_AFTER},
    middleware::Next,
    response::{IntoResponse, Response},
};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::endpoints::{
        CatalogV1Endpoint, ManagementV1Endpoint, SET_MAINTENANCE_MODE_PATH, SignEndpoint,
    },
    service::maintenance_mode::MAINTENANCE,
};

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

/// Returns `true` for HTTP methods we treat as writes. The one `GET`-with-write
/// side-effect we know of (`GET /v1/config` user auto-register) is suppressed
/// in the handler itself, not here.
fn is_mutating(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// `POST` routes that only read, by matched path: searches and checks whose
/// input does not fit a query string, and S3 request signing, which readers
/// that use remote signing need. The gate serves them while read-only.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadOnlyPostPaths(Arc<HashSet<&'static str>>);

impl ReadOnlyPostPaths {
    /// Lakekeeper's own read-only `POST` routes plus `extra`, such as an
    /// authorizer's ([`Authorizer::read_only_post_paths`](crate::service::authz::Authorizer::read_only_post_paths)).
    pub(crate) fn built_in_and(extra: &[&'static str]) -> Self {
        let built_in = [
            SignEndpoint::S3RequestGlobal.path(),
            SignEndpoint::S3RequestPrefix.path(),
            SignEndpoint::S3RequestTabular.path(),
            SignEndpoint::S3RequestByTableName.path(),
            CatalogV1Endpoint::ReportMetrics.path(),
            ManagementV1Endpoint::SearchUser.path(),
            ManagementV1Endpoint::SearchRole.path(),
            ManagementV1Endpoint::SearchTabular.path(),
            ManagementV1Endpoint::LoadEndpointStatistics.path(),
            ManagementV1Endpoint::ListTasks.path(),
            ManagementV1Endpoint::ListProjectTasks.path(),
            ManagementV1Endpoint::BatchCheckActions.path(),
        ];
        Self(Arc::new(
            built_in.into_iter().chain(extra.iter().copied()).collect(),
        ))
    }
}

/// Whether the gate rejects a request: `read_only` is the effective mode,
/// `matched_path` the route the request matched. A `POST` in
/// `read_only_posts` and the maintenance-mode route are never rejected; the
/// latter so read-only can be switched off again.
fn rejects(
    read_only: bool,
    method: &Method,
    matched_path: Option<&str>,
    read_only_posts: &ReadOnlyPostPaths,
) -> bool {
    if !read_only || !is_mutating(method) {
        return false;
    }
    match matched_path {
        Some(path) if path == SET_MAINTENANCE_MODE_PATH => false,
        Some(path) if *method == Method::POST => !read_only_posts.0.contains(path),
        _ => true,
    }
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

/// Gate `router` with read-only maintenance mode: while this process is
/// read-only, mutating requests get 503 with `Retry-After`, except the
/// read-only `POST` routes, Lakekeeper's own plus `authorizer_read_only_posts`.
/// Apply to a router that contains only routes subject to the gate (i.e. the
/// merged `/catalog/v1` + `/management/v1` nest, *not* `/health`).
#[cfg(feature = "router")]
pub(crate) fn with_maintenance_gate<S: Clone + Send + Sync + 'static>(
    router: Router<S>,
    authorizer_read_only_posts: &'static [&'static str],
) -> Router<S> {
    router.layer(axum::middleware::from_fn_with_state(
        ReadOnlyPostPaths::built_in_and(authorizer_read_only_posts),
        maintenance_middleware_fn,
    ))
}

#[cfg(feature = "router")]
async fn maintenance_middleware_fn(
    State(read_only_posts): State<ReadOnlyPostPaths>,
    request: Request<Body>,
    next: Next,
) -> Response {
    gate(MAINTENANCE.is_read_only(), &read_only_posts, request, next).await
}

/// The gate with the effective mode passed in as `read_only`.
async fn gate(
    read_only: bool,
    read_only_posts: &ReadOnlyPostPaths,
    request: Request<Body>,
    next: Next,
) -> Response {
    // The common case: no lookup beyond the flag.
    if !read_only {
        return next.run(request).await;
    }
    if rejects(
        read_only,
        request.method(),
        request
            .extensions()
            .get::<MatchedPath>()
            .map(MatchedPath::as_str),
        read_only_posts,
    ) {
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
    use axum::{
        Router,
        body::Body,
        middleware,
        routing::{get, post},
    };
    use http::{Request, StatusCode};
    use tower::ServiceExt as _;

    use super::*;

    /// Flipping the process-wide switch from a test would affect every test in
    /// the process, so these drive [`rejects`] with an explicit flag.
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

    /// The production gate with the flag passed in, so tests never touch the
    /// process-wide switch.
    fn router_with(read_only: bool) -> Router {
        Router::new()
            .route("/r", get(|| async { "ok" }).post(|| async { "ok" }))
            .layer(middleware::from_fn(move |req, next| {
                let paths = ReadOnlyPostPaths::built_in_and(&[]);
                async move { gate(read_only, &paths, req, next).await }
            }))
    }

    #[test]
    fn rejects_follows_flag_and_method() {
        let none = ReadOnlyPostPaths::default();
        assert!(rejects(true, &Method::POST, None, &none));
        assert!(!rejects(true, &Method::GET, None, &none));
        assert!(!rejects(false, &Method::POST, None, &none));
    }

    #[test]
    fn rejects_serves_listed_read_only_posts() {
        let paths = ReadOnlyPostPaths::built_in_and(&["/management/v1/ext/search"]);
        let search = Some("/management/v1/search/user");
        assert!(!rejects(true, &Method::POST, search, &paths));
        assert!(!rejects(
            true,
            &Method::POST,
            Some("/management/v1/ext/search"),
            &paths
        ));
        // Only POST is served: other mutating methods on a listed path stay rejected.
        assert!(rejects(true, &Method::PUT, search, &paths));
        assert!(rejects(true, &Method::DELETE, search, &paths));
        assert!(rejects(
            true,
            &Method::POST,
            Some("/management/v1/user"),
            &paths
        ));
    }

    #[test]
    fn built_in_read_only_posts_are_exactly_the_reading_endpoints() {
        let mut paths = ReadOnlyPostPaths::built_in_and(&[])
            .0
            .iter()
            .copied()
            .collect::<Vec<_>>();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![
                "/catalog/v1/aws/s3/sign",
                "/catalog/v1/signer/{prefix}/tabular-id/{tabular_id}/v1/aws/s3/sign",
                "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
                "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign",
                "/catalog/v1/{prefix}/v1/aws/s3/sign",
                "/management/v1/action/batch-check",
                "/management/v1/endpoint-statistics",
                "/management/v1/project/task/list",
                "/management/v1/search/role",
                "/management/v1/search/user",
                "/management/v1/warehouse/{warehouse_id}/search-tabular",
                "/management/v1/warehouse/{warehouse_id}/task/list",
            ]
        );
    }

    #[tokio::test]
    async fn read_only_blocks_post_returns_503_with_retry_after() {
        let app = router_with(true);
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
        let app = router_with(true);
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
        let app = router_with(false);
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

    /// The production gate over the management nest, as `new_full_router`
    /// mounts it, with the maintenance-mode route and a sibling.
    fn management_router_with(read_only: bool) -> Router {
        let management = Router::new()
            .route("/server/maintenance-mode", post(|| async { "ok" }))
            .route("/server/other", post(|| async { "ok" }))
            .route("/search/user", post(|| async { "ok" }));
        Router::new()
            .nest("/management/v1", management)
            .layer(middleware::from_fn(move |req, next| {
                let paths = ReadOnlyPostPaths::built_in_and(&[]);
                async move { gate(read_only, &paths, req, next).await }
            }))
    }

    async fn post_status(app: Router, uri: &str) -> (StatusCode, Option<String>) {
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let error_type = serde_json::from_slice::<IcebergErrorResponse>(&body)
            .ok()
            .map(|e| e.error.r#type);
        (status, error_type)
    }

    #[tokio::test]
    async fn exempt_route_passes_gate_while_read_only() {
        assert_eq!(
            crate::api::endpoints::SET_MAINTENANCE_MODE_PATH,
            "/management/v1/server/maintenance-mode"
        );
        let (status, error_type) = post_status(
            management_router_with(true),
            "/management/v1/server/maintenance-mode",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(error_type, None);
    }

    #[tokio::test]
    async fn read_only_post_passes_gate_while_read_only() {
        let (status, error_type) =
            post_status(management_router_with(true), "/management/v1/search/user").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(error_type, None);
    }

    #[tokio::test]
    async fn sibling_route_stays_blocked_while_read_only() {
        let (status, error_type) =
            post_status(management_router_with(true), "/management/v1/server/other").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_type.as_deref(), Some(MAINTENANCE_ERROR_TYPE));
    }
}
