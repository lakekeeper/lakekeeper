use std::{fmt::Debug, sync::LazyLock};

use axum::{response::IntoResponse, routing::get, Json, Router};
use axum_extra::middleware::option_layer;
use axum_prometheus::PrometheusMetricLayer;
use http::{header, HeaderValue, Method};
use limes::Authenticator;
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer, cors::AllowOrigin,
    sensitive_headers::SetSensitiveHeadersLayer, timeout::TimeoutLayer, trace, trace::TraceLayer,
    ServiceBuilderExt,
};

use crate::{
    api::{
        iceberg::v1::new_v1_full_router,
        management::v1::{api_doc as v1_api_doc, ApiServer},
        shutdown_signal, ApiContext,
    },
    request_metadata::create_request_metadata_with_trace_and_project_fn,
    service::{
        authn::{auth_middleware_fn, AuthMiddlewareState},
        authz::Authorizer,
        contract_verification::ContractVerifiers,
        endpoint_hooks::EndpointHookCollection,
        health::ServiceHealthProvider,
        task_queue::TaskQueues,
        Catalog, EndpointStatisticsTrackerTx, SecretStore, State,
    },
    tracing::{MakeRequestUuid7, RestMakeSpan},
    CONFIG,
};

static ICEBERG_OPENAPI_SPEC_YAML: LazyLock<serde_json::Value> = LazyLock::new(|| {
    let mut yaml_str =
        include_str!("../../../../docs/docs/api/rest-catalog-open-api.yaml").to_string();
    yaml_str = yaml_str.replace("  /v1/", "  /catalog/v1/");
    serde_yml::from_str(&yaml_str).expect("Failed to parse Iceberg API model V1 as JSON")
});

pub struct RouterArgs<C: Catalog, A: Authorizer + Clone, S: SecretStore, N: Authenticator> {
    pub authenticator: Option<N>,
    pub authorizer: A,
    pub catalog_state: C::State,
    pub secrets_state: S,
    pub queues: TaskQueues,
    pub table_change_checkers: ContractVerifiers,
    pub service_health_provider: ServiceHealthProvider,
    pub cors_origins: Option<&'static [HeaderValue]>,
    pub metrics_layer: Option<PrometheusMetricLayer<'static>>,
    pub endpoint_statistics_tracker_tx: EndpointStatisticsTrackerTx,
    pub hooks: EndpointHookCollection,
}

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore, N: Authenticator + Debug> Debug
    for RouterArgs<C, A, S, N>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterArgs")
            .field("authorizer", &"Authorizer")
            .field("catalog_state", &"CatalogState")
            .field("secrets_state", &"SecretsState")
            .field("queues", &self.queues)
            .field("table_change_checkers", &self.table_change_checkers)
            .field("authenticator", &self.authenticator)
            .field("svhp", &self.service_health_provider)
            .field("cors_origins", &self.cors_origins)
            .field(
                "metrics_layer",
                &self.metrics_layer.as_ref().map(|_| "PrometheusMetricLayer"),
            )
            .field(
                "endpoint_statistics_tracker_tx",
                &self.endpoint_statistics_tracker_tx,
            )
            .field("endpoint_hooks", &self.hooks)
            .finish()
    }
}

/// Create a new router with the given `RouterArgs`
///
/// # Errors
/// - Fails if the token verifier chain cannot be created
pub fn new_full_router<
    C: Catalog,
    A: Authorizer + Clone,
    S: SecretStore,
    N: Authenticator + 'static,
>(
    RouterArgs {
        authenticator,
        authorizer,
        catalog_state,
        secrets_state,
        queues,
        table_change_checkers,
        service_health_provider,
        cors_origins,
        metrics_layer,
        endpoint_statistics_tracker_tx,
        hooks,
    }: RouterArgs<C, A, S, N>,
) -> anyhow::Result<Router> {
    let v1_routes = new_v1_full_router::<crate::catalog::CatalogServer<C, A, S>, State<A, C, S>>();

    let management_routes = Router::new().merge(ApiServer::new_v1_router(&authorizer));
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
            ])
            .allow_methods(vec![
                Method::GET,
                Method::HEAD,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ])
    }));

    let maybe_auth_layer = if let Some(authenticator) = authenticator {
        option_layer(Some(axum::middleware::from_fn_with_state(
            AuthMiddlewareState {
                authenticator,
                authorizer: authorizer.clone(),
            },
            auth_middleware_fn,
        )))
    } else {
        option_layer(None)
    };

    let router = Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .layer(axum::middleware::from_fn_with_state(
            endpoint_statistics_tracker_tx,
            crate::service::endpoint_statistics::endpoint_statistics_middleware_fn,
        ))
        .layer(maybe_auth_layer)
        .route(
            "/health",
            get(|| async move {
                let health = service_health_provider.collect_health().await;
                Json(health).into_response()
            }),
        );
    let router = maybe_merge_swagger_router(router)
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
                        .make_span_with(RestMakeSpan::new(tracing::Level::INFO))
                        .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
                )
                .layer(TimeoutLayer::new(std::time::Duration::from_secs(30)))
                .layer(CatchPanicLayer::new())
                .layer(maybe_cors_layer)
                .propagate_x_request_id(),
        )
        .with_state(ApiContext {
            v1_state: State {
                authz: authorizer,
                catalog: catalog_state,
                secrets: secrets_state,
                contract_verifiers: table_change_checkers,
                queues,
                hooks,
            },
        });

    Ok(if let Some(metrics_layer) = metrics_layer {
        router.layer(metrics_layer)
    } else {
        router
    })
}

fn maybe_merge_swagger_router<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    router: Router<ApiContext<State<A, C, S>>>,
) -> Router<ApiContext<State<A, C, S>>> {
    if CONFIG.serve_swagger_ui {
        router.merge(
            utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
                .url("/api-docs/management/v1/openapi.json", v1_api_doc::<A>())
                .external_url_unchecked(
                    "/api-docs/catalog/v1/openapi.json",
                    ICEBERG_OPENAPI_SPEC_YAML.clone(),
                ),
        )
    } else {
        router
    }
}

/// Serve the given router on the given listener
///
/// # Errors
/// Fails if the webserver panics
pub async fn serve(listener: tokio::net::TcpListener, router: Router) -> anyhow::Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("error running HTTP server"))
}

#[cfg(test)]
mod test {
    #[test]
    fn test_openapi_spec_can_be_parsed() {
        let _ = super::ICEBERG_OPENAPI_SPEC_YAML.clone();
    }
}
