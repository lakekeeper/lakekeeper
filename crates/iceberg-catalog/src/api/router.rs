use crate::service::event_publisher::CloudEventsPublisher;
use crate::tracing::{MakeRequestUuid7, RestMakeSpan};

use crate::api::management::v1::{api_doc as v1_api_doc, ApiServer};
use crate::api::{iceberg::v1::new_v1_full_router, shutdown_signal, ApiContext};
use crate::service::contract_verification::ContractVerifiers;
use crate::service::health::ServiceHealthProvider;
use crate::service::task_queue::TaskQueues;
use crate::service::token_verification::Verifier;
use crate::service::{authz::Authorizer, Catalog, SecretStore, State};
use axum::response::IntoResponse;
use axum::{routing::get, Json, Router};
use axum_extra::middleware::option_layer;
use axum_prometheus::PrometheusMetricLayer;
use http::{header, HeaderValue, Method};
use tower::ServiceBuilder;
use tower_http::cors::AllowOrigin;
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer,
    sensitive_headers::SetSensitiveHeadersLayer, timeout::TimeoutLayer, trace, trace::TraceLayer,
    ServiceBuilderExt,
};

lazy_static::lazy_static! {
    static ref ICEBERG_OPENAPI_SPEC_YAML: serde_json::Value = {
        let mut yaml_str = include_str!("../../../../openapi/rest-catalog-open-api.yaml").to_string();
        yaml_str = yaml_str.replace("  /v1/", "  /catalog/v1/");
        serde_yml::from_str(&yaml_str).expect("Failed to parse Iceberg API model V1 as JSON")
    };
}

#[allow(clippy::module_name_repetitions, clippy::too_many_arguments)]
pub fn new_full_router<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    authorizer: A,
    catalog_state: C::State,
    secrets_state: S,
    queues: TaskQueues,
    publisher: CloudEventsPublisher,
    table_change_checkers: ContractVerifiers,
    token_verifier: Option<Verifier>,
    svhp: ServiceHealthProvider,
    cors_origins: Option<&[HeaderValue]>,
    metrics_layer: Option<PrometheusMetricLayer<'static>>,
) -> Router {
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
    let maybe_auth_layer = option_layer(token_verifier.map(|o| {
        axum::middleware::from_fn_with_state(
            o,
            crate::service::token_verification::auth_middleware_fn,
        )
    }));

    let router = Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .layer(maybe_auth_layer)
        .route(
            "/health",
            get(|| async move {
                let health = svhp.collect_health().await;
                Json(health).into_response()
            }),
        )
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
                .url("/api-docs/management/v1/openapi.json", v1_api_doc::<A>())
                .external_url_unchecked(
                    "/api-docs/catalog/v1/openapi.json",
                    ICEBERG_OPENAPI_SPEC_YAML.clone(),
                ),
        )
        .layer(axum::middleware::from_fn(
            crate::request_metadata::create_request_metadata_with_trace_id_fn,
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
                publisher,
                contract_verifiers: table_change_checkers,
                queues,
            },
        });

    if let Some(metrics_layer) = metrics_layer {
        router.layer(metrics_layer)
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
