use crate::service::event_publisher::CloudEventsPublisher;
use crate::tracing::{MakeRequestUuid7, RestMakeSpan};

use crate::api::management::v1::{api_doc as v1_api_doc, ApiServer};
use crate::api::{iceberg::v1::new_v1_full_router, shutdown_signal, ApiContext};
use crate::service::authn::IdpVerifier;
use crate::service::authn::K8sVerifier;
use crate::service::authn::VerifierChain;
use crate::service::contract_verification::ContractVerifiers;
use crate::service::health::ServiceHealthProvider;
use crate::service::task_queue::TaskQueues;
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

pub struct RouterArgs<C: Catalog, A: Authorizer + Clone, S: SecretStore> {
    pub authorizer: A,
    pub catalog_state: C::State,
    pub secrets_state: S,
    pub queues: TaskQueues,
    pub publisher: CloudEventsPublisher,
    pub table_change_checkers: ContractVerifiers,
    pub token_verifier: Option<IdpVerifier>,
    pub k8s_token_verifier: Option<K8sVerifier>,
    pub service_health_provider: ServiceHealthProvider,
    pub cors_origins: Option<&'static [HeaderValue]>,
    pub metrics_layer: Option<PrometheusMetricLayer<'static>>,
}

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> std::fmt::Debug for RouterArgs<C, A, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterArgs")
            .field("authorizer", &"Authorizer")
            .field("catalog_state", &"CatalogState")
            .field("secrets_state", &"SecretsState")
            .field("queues", &self.queues)
            .field("publisher", &self.publisher)
            .field("table_change_checkers", &self.table_change_checkers)
            .field("token_verifier", &self.token_verifier)
            .field("k8s_token_verifier", &self.k8s_token_verifier)
            .field("svhp", &self.service_health_provider)
            .field("cors_origins", &self.cors_origins)
            .field(
                "metrics_layer",
                &self.metrics_layer.as_ref().map(|_| "PrometheusMetricLayer"),
            )
            .finish()
    }
}

/// Create a new router with the given `RouterArgs`
///
/// # Errors
/// - Fails if the token verifier chain cannot be created
pub fn new_full_router<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    RouterArgs {
        authorizer,
        catalog_state,
        secrets_state,
        queues,
        publisher,
        table_change_checkers,
        token_verifier,
        k8s_token_verifier,
        service_health_provider,
        cors_origins,
        metrics_layer,
    }: RouterArgs<C, A, S>,
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
    let maybe_auth_layer = match (token_verifier, k8s_token_verifier) {
        (None, None) => option_layer(None),
        (idp_verifier, k8s_verifier) => option_layer(Some(axum::middleware::from_fn_with_state(
            VerifierChain::try_new(idp_verifier, k8s_verifier)?,
            crate::service::authn::auth_middleware_fn,
        ))),
    };

    let router = Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .layer(maybe_auth_layer)
        .route(
            "/health",
            get(|| async move {
                let health = service_health_provider.collect_health().await;
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

    Ok(if let Some(metrics_layer) = metrics_layer {
        router.layer(metrics_layer)
    } else {
        router
    })
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
