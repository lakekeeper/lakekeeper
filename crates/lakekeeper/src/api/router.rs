use std::{fmt::Debug, sync::Arc};

use axum::{
    Json, Router, extract::DefaultBodyLimit, response::IntoResponse, routing::get, serve::Listener,
};
use axum_extra::{either::Either, middleware::option_layer};
use axum_prometheus::{PrometheusMetricLayer, metrics};
use http::{HeaderName, HeaderValue, Method, StatusCode, header};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::{conn::auto, graceful::GracefulShutdown},
    service::TowerToHyperService,
};
use limes::Authenticator;
use tower::ServiceBuilder;
use tower_http::{
    ServiceBuilderExt,
    catch_panic::CatchPanicLayer,
    compression::CompressionLayer,
    cors::AllowOrigin,
    sensitive_headers::SetSensitiveHeadersLayer,
    set_header::SetResponseHeaderLayer,
    timeout::TimeoutLayer,
    trace::{self, TraceLayer},
};

#[cfg(feature = "open-api")]
use crate::api::management::v1::api_doc as v1_api_doc;
use crate::{
    CONFIG, CancellationToken,
    api::{
        ApiContext,
        iceberg::v1::{
            new_v1_full_router,
            tables::{DATA_ACCESS_HEADER_NAME, ETAG_HEADER_NAME, IF_NONE_MATCH_HEADER_NAME},
        },
        management::v1::ApiServer,
    },
    request_metadata::{
        X_PROJECT_ID_HEADER_NAME, X_REQUEST_ID_HEADER_NAME,
        create_request_metadata_with_trace_and_project_fn,
    },
    request_tracing::{MakeRequestUuid7, RestMakeSpan},
    service::{
        CatalogStore, EndpointStatisticsTrackerTx, SecretStore, State,
        admission::AdmissionGates,
        authn::{AuthMiddlewareState, auth_middleware_fn},
        authz::{Authorizer, InstanceAdminMembership},
        health::{HealthState, HealthStatus, ServiceHealthProvider},
        tasks::QueueApiConfig,
    },
};

pub const X_USER_AGENT_HEADER_NAME: HeaderName = HeaderName::from_static("x-user-agent");

/// Every API response is specific to the authenticated principal: bodies are
/// filtered by the caller's permissions and `loadTable` may embed storage
/// credentials vended for them alone. `private` keeps such a response out of
/// shared caches while still allowing the client-side storage that
/// `ETag` / `If-None-Match` revalidation depends on — `no-store` would defeat
/// the conditional-request support the catalog implements.
static CACHE_CONTROL_PRIVATE: HeaderValue = HeaderValue::from_static("private");

/// Request headers that select between different bodies for the same URL, so
/// that a cache does not serve one variant in place of another. The
/// credentials in `authorization` decide both the principal and the vended
/// storage credentials, `x-project-id` selects the project a warehouse is
/// resolved in, and `x-iceberg-access-delegation` picks the form of storage
/// access returned by `loadTable`.
static VARY_ON_REQUEST_IDENTITY: HeaderValue =
    HeaderValue::from_static("authorization, x-project-id, x-iceberg-access-delegation");

/// Attaches the cache directives above to every route already mounted on
/// `router`, and to every response short-circuited by a layer already applied
/// to it. Call this before mounting `/health`, which is not
/// principal-specific.
///
/// `Cache-Control` is set `if_not_present` so that routes with stricter needs
/// — the S3 signer sets `private, no-cache` — keep their own directive.
/// `Vary` is *appended*: it is a list-valued header, so a route that names a
/// further request header must not lose the fields below. Caches combine the
/// field lines, which is also how `CompressionLayer` adds `accept-encoding`.
fn set_cache_directives<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            CACHE_CONTROL_PRIVATE.clone(),
        ))
        .layer(SetResponseHeaderLayer::appending(
            header::VARY,
            VARY_ON_REQUEST_IDENTITY.clone(),
        ))
}

#[cfg(feature = "open-api")]
static ICEBERG_OPENAPI_SPEC_YAML: std::sync::LazyLock<serde_json::Value> =
    std::sync::LazyLock::new(|| {
        let mut yaml_str =
            include_str!("../../../../docs/docs/api/rest-catalog-open-api.yaml").to_string();
        yaml_str = yaml_str.replace("  /v1/", "  /catalog/v1/");
        serde_norway::from_str(&yaml_str).expect("Failed to parse Iceberg API model V1 as JSON")
    });

pub struct RouterArgs<C: CatalogStore, A: Authorizer + Clone, S: SecretStore, N: Authenticator> {
    pub authenticator: Option<N>,
    pub state: ApiContext<State<A, C, S>>,
    pub service_health_provider: ServiceHealthProvider,
    pub cors_origins: Option<&'static [HeaderValue]>,
    pub metrics_layer: Option<PrometheusMetricLayer<'static>>,
    pub endpoint_statistics_tracker_tx: EndpointStatisticsTrackerTx,
    /// Source of instance-admin membership. Use
    /// `Arc::new(`[`ConfiguredInstanceAdmins`]`)` for the static
    /// `LAKEKEEPER__INSTANCE_ADMINS` default, or inject a custom (e.g.
    /// database-backed) implementation to manage instance admins at runtime.
    ///
    /// [`ConfiguredInstanceAdmins`]: crate::service::authz::ConfiguredInstanceAdmins
    pub instance_admin_membership: Arc<dyn InstanceAdminMembership>,
    /// Post-authentication admission gates. Empty by default (admits every
    /// request); host binaries may register gates that reject already
    /// authenticated principals before they reach any handler.
    pub admission_gates: AdmissionGates,
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore, N: Authenticator + Debug> Debug
    for RouterArgs<C, A, S, N>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterArgs")
            .field("authorizer", &"Authorizer")
            .field("state", &self.state)
            .field("authenticator", &self.authenticator)
            .field("service_health_provider", &self.service_health_provider)
            .field("cors_origins", &self.cors_origins)
            .field(
                "metrics_layer",
                &self.metrics_layer.as_ref().map(|_| "PrometheusMetricLayer"),
            )
            .field(
                "endpoint_statistics_tracker_tx",
                &self.endpoint_statistics_tracker_tx,
            )
            .field("instance_admin_membership", &self.instance_admin_membership)
            .field("admission_gates", &self.admission_gates)
            .finish()
    }
}

/// Create a new router with the given `RouterArgs`
///
/// # Errors
/// - Fails if the token verifier chain cannot be created
pub async fn new_full_router<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
    N: Authenticator + 'static,
>(
    RouterArgs {
        authenticator,
        state,
        service_health_provider,
        cors_origins,
        metrics_layer,
        endpoint_statistics_tracker_tx,
        instance_admin_membership,
        admission_gates,
        // registered_task_queues,
    }: RouterArgs<C, A, S, N>,
) -> anyhow::Result<Router> {
    let v1_routes = new_v1_full_router::<crate::server::CatalogServer<C, A, S>, State<A, C, S>>();

    let generic_table_routes = crate::api::data::v1::generic_tables::router::<
        crate::server::CatalogServer<C, A, S>,
        State<A, C, S>,
    >();

    let authorizer = state.v1_state.authz.clone();
    let management_routes = Router::new().merge(ApiServer::new_v1_router(&authorizer));
    let maybe_cors_layer = get_cors_layer(cors_origins);

    let maybe_auth_layer = if let Some(authenticator) = authenticator {
        option_layer(Some(axum::middleware::from_fn_with_state(
            AuthMiddlewareState {
                authenticator,
                authorizer: state.v1_state.authz.clone(),
                events: state.v1_state.events.clone(),
                catalog_state: state.v1_state.catalog.clone(),
                instance_admin_membership,
                admission_gates,
            },
            auth_middleware_fn::<C, _, _>,
        )))
    } else {
        option_layer(None)
    };

    let mut router = Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .nest("/lakekeeper/v1", generic_table_routes)
        // Maintenance gate: rejects mutating requests (POST/PUT/PATCH/DELETE)
        // with 503 + Retry-After when MAINTENANCE_MODE=read-only. Applied
        // before `/health` is added so liveness/readiness probes are
        // unaffected.
        .layer(axum::middleware::from_fn(
            crate::api::maintenance::maintenance_middleware_fn,
        ))
        .layer(DefaultBodyLimit::max(CONFIG.max_request_body_size));

    // Apply request body logging middleware FIRST, before any other middleware that might consume the body
    if CONFIG.debug.log_request_bodies {
        router = router.layer(axum::middleware::from_fn(print_request_body));
    }

    // Wraps the authentication layer, so that a request rejected there carries
    // the cache directives too. `/health` is mounted below, outside the wrap.
    let router = set_cache_directives(
        router
            .layer(axum::middleware::from_fn_with_state(
                endpoint_statistics_tracker_tx,
                crate::service::endpoint_statistics::endpoint_statistics_middleware_fn,
            ))
            .layer(maybe_auth_layer),
    )
    // Add health later so that it is not authenticated
    .route(
        "/health",
        get(|| async move {
            let health = service_health_provider.collect_health().await;
            health_response(health)
        }),
    );

    let registered_api_configs = state.v1_state.registered_task_queues.api_config().await;
    let (warehouse_task_api_configs, project_task_api_configs) = registered_api_configs
        .iter()
        .partition::<Vec<_>, _>(|config| {
            matches!(config.scope, crate::service::tasks::QueueScope::Warehouse)
        });

    let router = maybe_merge_swagger_router(
        router,
        &warehouse_task_api_configs,
        &project_task_api_configs,
    );
    let router = router
        .layer(axum::middleware::from_fn(
            create_request_metadata_with_trace_and_project_fn,
        ))
        .layer(
            ServiceBuilder::new()
                .set_x_request_id(MakeRequestUuid7)
                .layer(SetSensitiveHeadersLayer::new([
                    axum::http::header::AUTHORIZATION,
                ]))
                .layer(CompressionLayer::new())
                .layer(
                    TraceLayer::new_for_http()
                        .on_failure(())
                        .make_span_with(
                            RestMakeSpan::new(tracing::Level::INFO).with_log_authorization_header(
                                CONFIG.debug.log_authorization_header,
                            ),
                        )
                        .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
                )
                .layer(TimeoutLayer::with_status_code(
                    http::StatusCode::REQUEST_TIMEOUT,
                    CONFIG.max_request_time,
                ))
                .layer(CatchPanicLayer::new())
                .layer(maybe_cors_layer)
                .propagate_x_request_id(),
        )
        .with_state(state);

    Ok(if let Some(metrics_layer) = metrics_layer {
        router.layer(metrics_layer)
    } else {
        router
    })
}

fn health_response(health: HealthState) -> axum::response::Response {
    let status = match health.health {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Unhealthy | HealthStatus::Unknown => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status, Json(health)).into_response()
}

async fn print_request_body(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<impl IntoResponse, axum::response::Response> {
    let path = request.uri().path().to_string();
    let method = request.method().to_string();
    let request_id = request
        .headers()
        .get(crate::api::X_REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("MISSING-REQUEST-ID")
        .to_string();
    let user_agent = request
        .headers()
        .get(http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let request = buffer_request_body(request, &method, &path, &request_id, &user_agent).await?;
    let response = next.run(request).await;
    buffer_response_body(response, &method, &path, &request_id, &user_agent).await
}

async fn buffer_response_body(
    response: axum::response::Response,
    method: &str,
    path: &str,
    request_id: &str,
    user_agent: &str,
) -> Result<axum::response::Response, axum::response::Response> {
    let (parts, body) = response.into_parts();

    let bytes = http_body_util::BodyExt::collect(body)
        .await
        .map_err(|err| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )
                .into_response()
        })?
        .to_bytes();

    let s = String::from_utf8_lossy(&bytes).to_string();
    let status = parts.status;

    tracing::debug!(
        method = method,
        path = path,
        request_id = request_id,
        user_agent = user_agent,
        status = %status,
        response_body = s,
    );

    Ok(axum::response::Response::from_parts(
        parts,
        axum::body::Body::from(bytes),
    ))
}

// This function is expensive and should only be used for debugging purposes.
async fn buffer_request_body(
    request: axum::extract::Request,
    method: &str,
    path: &str,
    request_id: &str,
    user_agent: &str,
) -> Result<axum::extract::Request, axum::response::Response> {
    let (parts, body) = request.into_parts();

    // this won't work if the body is an long running stream
    let bytes = http_body_util::BodyExt::collect(body)
        .await
        .map_err(|err| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )
                .into_response()
        })?
        .to_bytes();

    let s = String::from_utf8_lossy(&bytes).to_string();
    tracing::debug!(
        method = method,
        path = path,
        request_body = s,
        request_id = request_id,
        user_agent = user_agent
    );

    Ok(axum::extract::Request::from_parts(
        parts,
        axum::body::Body::from(bytes),
    ))
}

fn get_cors_layer(
    cors_origins: Option<&'static [HeaderValue]>,
) -> axum_extra::either::Either<
    (
        axum::middleware::ResponseAxumBodyLayer,
        tower_http::cors::CorsLayer,
    ),
    tower::layer::util::Identity,
> {
    tracing::info!("Configuring CORS layer for origins: {:?}", cors_origins);
    let maybe_cors_layer = option_layer(cors_origins.map(|origins| {
        let allowed_origin = if origins
            .iter()
            .any(|origin| origin == HeaderValue::from_static("*"))
        {
            AllowOrigin::any()
        } else {
            AllowOrigin::list(origins.iter().cloned())
        };
        tower_http::cors::CorsLayer::new()
            .allow_origin(allowed_origin)
            .allow_headers(vec![
                header::AUTHORIZATION,
                header::CONTENT_TYPE,
                header::ACCEPT,
                header::USER_AGENT,
                X_PROJECT_ID_HEADER_NAME,
                X_REQUEST_ID_HEADER_NAME,
                IF_NONE_MATCH_HEADER_NAME,
                X_USER_AGENT_HEADER_NAME,
                DATA_ACCESS_HEADER_NAME,
            ])
            .expose_headers(vec![ETAG_HEADER_NAME])
            .allow_methods(vec![
                Method::GET,
                Method::HEAD,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ])
    }));
    match &maybe_cors_layer {
        Either::E1(cors_layer) => {
            tracing::debug!("CORS layer enabled: {cors_layer:?}");
        }
        Either::E2(_) => {
            tracing::info!("CORS layer not enabled for REST API");
        }
    }
    maybe_cors_layer
}

#[cfg_attr(not(feature = "open-api"), allow(unused_variables))]
fn maybe_merge_swagger_router<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    router: Router<ApiContext<State<A, C, S>>>,
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Router<ApiContext<State<A, C, S>>> {
    #[cfg(feature = "open-api")]
    if CONFIG.serve_swagger_ui {
        router.merge(
            utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
                .url(
                    "/api-docs/management/v1/openapi.json",
                    v1_api_doc::<A>(queue_api_configs, project_queue_api_configs),
                )
                .external_url_unchecked(
                    "/api-docs/catalog/v1/openapi.json",
                    ICEBERG_OPENAPI_SPEC_YAML.clone(),
                ),
        )
    } else {
        router
    }
    #[cfg(not(feature = "open-api"))]
    {
        router
    }
}

/// Serve the given router on the given listener until `cancellation_token` is
/// cancelled, then drain in-flight connections.
///
/// # Errors
/// Returns `Ok` once draining finishes; the error type is kept for callers.
pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    // One accept loop of our own, because `axum::serve` exposes no way to reach
    // hyper's per-connection settings. Three of them matter here, and all three
    // are per-connection state that lives until the connection closes:
    //
    // * `timer` — hyper defaults `header_read_timeout` to 30s but silently
    //   disables it when no timer is installed, which is what `axum::serve`
    //   does. Installing one activates it, and because hyper arms it whenever
    //   the connection is waiting for a request head, it also bounds how long
    //   an idle kept-alive connection is held.
    // * `max_buf_size` — hyper's read buffer grows adaptively to 408 KiB and
    //   keeps that capacity for the connection's lifetime, so one large commit
    //   permanently inflates the connection that carried it.
    // * TCP keepalive — lets the kernel retire a connection whose peer vanished
    //   without FIN or RST, which nothing else here would ever notice.
    //
    // `Listener::accept` is axum's, so accept-error backoff (EMFILE and
    // friends) still comes from upstream.
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(CONNECTION_IDLE_TIMEOUT)
        .max_buf_size(MAX_CONNECTION_BUFFER_SIZE);
    // CONNECT protocol carries websockets over HTTP/2.
    builder
        .http2()
        .timer(TokioTimer::new())
        .enable_connect_protocol();

    let keepalive = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_IDLE)
        .with_interval(TCP_KEEPALIVE_PROBE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_PROBE_RETRIES);

    let graceful = GracefulShutdown::new();
    let mut listener = listener;

    // Separates "many connections open" from "we are leaking tasks":
    // `tokio_live_tasks_count` counts every task on the runtime, so on its own it
    // cannot tell one from the other.
    metrics::describe_gauge!(
        METRIC_HTTP_CONNECTIONS,
        "Currently open HTTP connections, one tokio task each"
    );
    metrics::gauge!(METRIC_HTTP_CONNECTIONS).set(0.0);

    loop {
        let (stream, peer) = tokio::select! {
            biased;
            () = cancellation_token.cancelled() => {
                tracing::info!("HTTP server shutdown requested (cancellation token)");
                break;
            }
            accepted = Listener::accept(&mut listener) => accepted,
        };

        // Nagle would otherwise delay small responses waiting for an ACK.
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!(%peer, "Failed to set TCP_NODELAY: {e}");
        }
        if let Err(e) = socket2::SockRef::from(&stream).set_tcp_keepalive(&keepalive) {
            tracing::debug!(%peer, "Failed to set TCP keepalive: {e}");
        }

        let open_connection = OpenConnection::new();
        let connection = builder
            .serve_connection_with_upgrades(
                TokioIo::new(stream),
                TowerToHyperService::new(router.clone()),
            )
            .into_owned();
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            let _open_connection = open_connection;
            if let Err(e) = connection.await {
                tracing::debug!(%peer, "Connection closed with error: {e}");
            }
        });
    }

    // Refuse new connections immediately. Held open, the socket keeps
    // completing handshakes into the backlog for the whole drain, and those
    // clients wait for a reply that never comes instead of failing fast.
    drop(listener);

    let in_flight = graceful.count();
    tracing::info!(in_flight, "Draining connections");
    tokio::select! {
        // `GracefulShutdown::shutdown` signals the connections on its first
        // poll, so it must be polled before the deadline can win.
        biased;
        () = graceful.shutdown() => tracing::info!("All connections drained"),
        () = tokio::time::sleep(SHUTDOWN_GRACE_PERIOD) => tracing::warn!(
            "Grace period elapsed; abandoning connections still open"
        ),
    }
    Ok(())
}

/// Open HTTP connections. Compare against `tokio_live_tasks_count`: a gap that
/// grows is a task leak somewhere other than the accept loop.
const METRIC_HTTP_CONNECTIONS: &str = "lakekeeper_http_connections";

/// Holds [`METRIC_HTTP_CONNECTIONS`] up for as long as it is alive.
///
/// The decrement happens in `Drop` so that it cannot be skipped. This gauge is
/// the signal for spotting a task leak, so a path that leaks the gauge itself
/// would fabricate the very thing it is meant to detect.
struct OpenConnection;

impl OpenConnection {
    fn new() -> Self {
        metrics::gauge!(METRIC_HTTP_CONNECTIONS).increment(1.0);
        Self
    }
}

impl Drop for OpenConnection {
    fn drop(&mut self) {
        metrics::gauge!(METRIC_HTTP_CONNECTIONS).decrement(1.0);
    }
}

/// How long an HTTP/1 connection may wait for a request head before the server
/// closes it.
///
/// hyper arms this timer whenever the connection is waiting for a request head,
/// so it covers both a client dribbling headers and a connection sitting idle
/// between requests. 75s matches nginx's `keepalive_timeout` and sits just above
/// the 60s idle timeout common to cloud load balancers, which retire a
/// connection before we do.
///
/// Two gaps: hyper has no equivalent for HTTP/2, and the timer is armed only
/// once `auto` has sniffed the protocol, so a peer that connects and sends
/// nothing at all is bounded by neither this nor TCP keepalive.
const CONNECTION_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(75);
/// Ceiling on hyper's per-connection HTTP/1 read/write buffer.
///
/// hyper grows the buffer adaptively and keeps the capacity for the connection's
/// lifetime, so this is the steady-state cost of a connection that has carried a
/// large request. hyper's own default is 408 `KiB`. Measured on 4 `MiB` bodies,
/// 64 `KiB` costs nothing: 1828 req/s here against 1742 at hyper's default.
///
/// This also caps the request head, which hyper rejects with `431` once it no
/// longer fits.
const MAX_CONNECTION_BUFFER_SIZE: usize = 64 * 1024;
/// Idle time before the kernel starts TCP keepalive probing.
///
/// Covers the states the idle timeout above does not: a peer that disappears
/// while we are reading a request body or writing a response leaves no
/// wait-for-head timer armed.
const TCP_KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_mins(1);
/// Gap between TCP keepalive probes once the idle time has elapsed.
const TCP_KEEPALIVE_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
/// Unanswered TCP keepalive probes before the kernel drops the connection.
const TCP_KEEPALIVE_PROBE_RETRIES: u32 = 6;
/// How long `serve` waits for in-flight connections after cancellation.
const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use axum::{Router, body::Body, http::Request, response::IntoResponse as _, routing::get};
    use http::{HeaderMap, HeaderValue, StatusCode, header};
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    use crate::{
        config::MaintenanceMode,
        service::health::{Health, HealthState, HealthStatus},
    };

    fn test_health_state(health: HealthStatus) -> HealthState {
        HealthState {
            health,
            services: HashMap::from([(
                "catalog".to_string(),
                vec![Health::now("read_pool", health)],
            )]),
            maintenance_mode: MaintenanceMode::default(),
        }
    }

    async fn request_health(health: HealthStatus) -> (StatusCode, HealthState) {
        let app = Router::new().route(
            "/health",
            get(move || async move { super::health_response(test_health_state(health)) }),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: HealthState = serde_json::from_slice(&body).unwrap();

        (status, body)
    }

    /// Routes a `GET /t` through the cache directives and returns the response
    /// headers. `preset` is applied by the inner handler, standing in for a
    /// route that sets its own directives.
    async fn cache_directive_headers(preset: &[(http::HeaderName, &'static str)]) -> HeaderMap {
        let preset = preset.to_vec();
        let app = super::set_cache_directives(Router::new().route(
            "/t",
            get(move || {
                let preset = preset.clone();
                async move {
                    let mut headers = HeaderMap::new();
                    for (name, value) in preset {
                        headers.insert(name, HeaderValue::from_static(value));
                    }
                    (headers, "ok")
                }
            }),
        ));

        app.oneshot(Request::builder().uri("/t").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .headers()
            .clone()
    }

    /// A cache combines every `Vary` field line, so collect them all rather
    /// than reading only the first.
    fn vary_fields(headers: &HeaderMap) -> String {
        headers
            .get_all(header::VARY)
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn assert_varies_on_the_request_identity(headers: &HeaderMap) {
        let vary = vary_fields(headers);
        for varied_on in [
            header::AUTHORIZATION.as_str(),
            crate::request_metadata::X_PROJECT_ID_HEADER,
            crate::api::iceberg::v1::tables::DATA_ACCESS_HEADER,
        ] {
            assert!(vary.contains(varied_on), "{varied_on} missing from {vary}");
        }
    }

    #[tokio::test]
    async fn responses_are_private_and_vary_on_the_request_identity() {
        let headers = cache_directive_headers(&[]).await;

        assert_eq!(headers[header::CACHE_CONTROL], "private");
        assert_varies_on_the_request_identity(&headers);
    }

    /// `Vary` is list-valued: a route naming a further request header must
    /// keep it *and* the identity fields, or a cache loses one of the two.
    #[tokio::test]
    async fn a_route_vary_is_merged_with_the_identity_fields() {
        let headers = cache_directive_headers(&[(header::VARY, "accept")]).await;

        let vary = vary_fields(&headers);
        assert!(vary.contains("accept"), "route's own field lost: {vary}");
        assert_varies_on_the_request_identity(&headers);
    }

    /// `private` permits client-side storage — without it the `ETag` /
    /// `If-None-Match` revalidation the catalog implements would never be
    /// exercised.
    #[tokio::test]
    async fn responses_are_not_marked_no_store() {
        let headers = cache_directive_headers(&[]).await;

        let cache_control = headers[header::CACHE_CONTROL].to_str().unwrap();
        assert!(!cache_control.contains("no-store"), "{cache_control}");
    }

    /// The S3 signer relies on this: it sets `private, no-cache` itself.
    #[tokio::test]
    async fn a_route_keeps_its_own_cache_directives() {
        let headers =
            cache_directive_headers(&[(header::CACHE_CONTROL, "private, no-cache")]).await;

        assert_eq!(headers[header::CACHE_CONTROL], "private, no-cache");
    }

    /// Mirrors `new_full_router`: authentication is wrapped by the cache
    /// directives, `/health` is mounted outside them. A request rejected
    /// before it reaches a handler never produces a body, so the directives
    /// have to come from the wrap rather than from the route.
    fn app_with_rejecting_auth() -> Router {
        super::set_cache_directives(Router::new().route("/t", get(|| async { "ok" })).layer(
            axum::middleware::from_fn(
                async |_req: Request<Body>, _next: axum::middleware::Next| {
                    StatusCode::UNAUTHORIZED.into_response()
                },
            ),
        ))
        .route("/health", get(|| async { "ok" }))
    }

    #[tokio::test]
    async fn a_rejected_request_still_gets_the_cache_directives() {
        let response = app_with_rejecting_auth()
            .oneshot(Request::builder().uri("/t").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "private");
        assert_varies_on_the_request_identity(response.headers());
    }

    #[tokio::test]
    async fn health_is_outside_the_cache_directives() {
        let response = app_with_rejecting_auth()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key(header::CACHE_CONTROL));
        assert!(!response.headers().contains_key(header::VARY));
    }

    #[tokio::test]
    async fn health_endpoint_returns_ok_for_healthy_state() {
        let (status, body) = request_health(HealthStatus::Healthy).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.health, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn health_endpoint_returns_service_unavailable_for_unhealthy_state() {
        let (status, body) = request_health(HealthStatus::Unhealthy).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.health, HealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn health_endpoint_returns_service_unavailable_for_unknown_state() {
        let (status, body) = request_health(HealthStatus::Unknown).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.health, HealthStatus::Unknown);
    }

    #[cfg(feature = "open-api")]
    #[test]
    fn test_openapi_spec_can_be_parsed() {
        let _ = super::ICEBERG_OPENAPI_SPEC_YAML.clone();
    }
}
