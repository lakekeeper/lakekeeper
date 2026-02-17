use std::fmt::Debug;

use axum::{Json, Router, extract::DefaultBodyLimit, response::IntoResponse, routing::get};
use axum_extra::{either::Either, middleware::option_layer};
use axum_prometheus::PrometheusMetricLayer;
use http::{HeaderName, HeaderValue, Method, header};
use limes::Authenticator;
use tower::ServiceBuilder;
use tower_http::{
    ServiceBuilderExt,
    catch_panic::CatchPanicLayer,
    compression::CompressionLayer,
    cors::AllowOrigin,
    sensitive_headers::SetSensitiveHeadersLayer,
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
        authn::{AuthMiddlewareState, auth_middleware_fn},
        authz::Authorizer,
        health::ServiceHealthProvider,
        tasks::QueueApiConfig,
    },
};

pub const X_USER_AGENT_HEADER_NAME: HeaderName = HeaderName::from_static("x-user-agent");

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
        // registered_task_queues,
    }: RouterArgs<C, A, S, N>,
) -> anyhow::Result<Router> {
    let v1_routes = new_v1_full_router::<crate::server::CatalogServer<C, A, S>, State<A, C, S>>();

    let authorizer = state.v1_state.authz.clone();
    let management_routes = Router::new().merge(ApiServer::new_v1_router(&authorizer));
    let maybe_cors_layer = get_cors_layer(cors_origins);

    let maybe_auth_layer = if let Some(authenticator) = authenticator {
        option_layer(Some(axum::middleware::from_fn_with_state(
            AuthMiddlewareState {
                authenticator,
                authorizer: state.v1_state.authz.clone(),
                events: state.v1_state.events.clone(),
            },
            auth_middleware_fn,
        )))
    } else {
        option_layer(None)
    };

    let mut router = Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .layer(DefaultBodyLimit::max(CONFIG.max_request_body_size));

    // Apply request body logging middleware FIRST, before any other middleware that might consume the body
    if CONFIG.debug.log_request_bodies {
        router = router.layer(axum::middleware::from_fn(print_request_body));
    }

    let router = router
        .layer(axum::middleware::from_fn_with_state(
            endpoint_statistics_tracker_tx,
            crate::service::endpoint_statistics::endpoint_statistics_middleware_fn,
        ))
        .layer(maybe_auth_layer)
        // Add health later so that it is not authenticated
        .route(
            "/health",
            get(|| async move {
                let health = service_health_provider.collect_health().await;
                Json(health).into_response()
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
                        .make_span_with(RestMakeSpan::new(tracing::Level::INFO).with_log_authorization_header(CONFIG.debug.log_authorization_header))
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

/// Serve the given router on the given listener
///
/// # Errors
/// Fails if the webserver panics
pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    let cancellation_future = async move {
        cancellation_token.cancelled().await;
        tracing::info!("HTTP server shutdown requested (cancellation token)");
    };
    axum::serve(listener, router)
        .with_graceful_shutdown(cancellation_future)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("error running HTTP server"))
}

#[cfg(test)]
mod test {

    #[cfg(feature = "open-api")]
    #[test]
    fn test_openapi_spec_can_be_parsed() {
        let _ = super::ICEBERG_OPENAPI_SPEC_YAML.clone();
    }
}
