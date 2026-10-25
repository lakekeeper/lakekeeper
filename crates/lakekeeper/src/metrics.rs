use std::{future::Future, pin::Pin};

use axum::response::IntoResponse;
use axum_prometheus::{
    AXUM_HTTP_REQUESTS_DURATION_SECONDS, EndpointLabel, PREFIXED_HTTP_REQUESTS_DURATION_SECONDS,
    PrometheusMetricLayer, PrometheusMetricLayerBuilder, metrics,
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle},
    utils,
};

use crate::CONFIG;

pub type ExporterFuture = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'static>>;

/// `endpoint` label used for requests that matched no route. A single shared
/// label keeps the metric registry's cardinality bounded by the route table
/// rather than by whatever paths clients happen to ask for.
pub const UNMATCHED_ENDPOINT_LABEL: &str = "unmatched";

/// The `endpoint` label policy applied to every HTTP metric.
///
/// Requests that match no route reach the router's fallback, which never gets a
/// [`axum::extract::MatchedPath`] extension. `axum-prometheus`' default
/// [`EndpointLabel::MatchedPath`] then falls back to the *raw* URI, so every
/// distinct unknown path becomes 16 permanent series (a counter, a pending gauge
/// and an 11-bucket histogram). `PrometheusBuilder` sets no `idle_timeout`, so
/// nothing is ever reclaimed — measured at ~1.9 `KiB` of live heap retained for
/// the process's remaining life, per distinct path. Because the auth middleware
/// also wraps the fallback, unauthenticated probing counts: ~9 junk paths per
/// second grows a replica by ~60 `MiB`/h, and a client that malforms a table URL
/// (missing route prefix, trailing slash, doubled slash) leaks one series per
/// table forever.
///
/// Collapsing every unmatched path onto [`UNMATCHED_ENDPOINT_LABEL`] bounds the
/// registry by the route table. The requested paths are still recorded by the
/// request-tracing layer, so nothing observable is lost.
#[must_use]
pub fn endpoint_label_policy() -> EndpointLabel {
    EndpointLabel::MatchedPathWithFallbackFn(|_| UNMATCHED_ENDPOINT_LABEL.to_string())
}

/// Builds the HTTP metric layer with the [`endpoint_label_policy`] applied.
///
/// Production and `tests/metrics_endpoint_cardinality.rs` both go through here,
/// so the test exercises the real wiring and not a copy of it.
#[must_use]
pub fn build_metric_layer(handle: PrometheusHandle) -> PrometheusMetricLayer<'static> {
    let (layer, _) = PrometheusMetricLayerBuilder::new()
        .with_metrics_from_fn(move || handle)
        .with_endpoint_label_type(endpoint_label_policy())
        .build_pair();
    layer
}

/// How often histograms are drained into their buckets.
const UPKEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// The metrics port: bound, not yet serving.
#[derive(Debug)]
#[must_use = "the metrics port serves nothing until `serve` runs"]
pub struct MetricsServer {
    handle: PrometheusHandle,
    listener: std::net::TcpListener,
    port: u16,
}

impl MetricsServer {
    /// Serves the metrics on `/metrics` and on every path `routes` does not match, and `OK` on
    /// `/health`. Also drains histograms and reports Tokio runtime metrics until cancelled.
    ///
    /// # Errors
    /// The future fails if the listener cannot be used by Tokio or the server stops on an error.
    #[must_use]
    pub fn serve(
        self,
        routes: axum::Router,
        cancellation_token: crate::CancellationToken,
    ) -> ExporterFuture {
        let Self {
            handle,
            listener,
            port,
        } = self;
        Box::pin(async move {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            let upkeep_handle = handle.clone();
            let upkeep = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(UPKEEP_INTERVAL).await;
                    upkeep_handle.run_upkeep();
                }
            });
            let runtime_metrics_reporter = tokio::spawn(
                tokio_metrics::RuntimeMetricsReporterBuilder::default()
                    .with_interval(CONFIG.metrics.tokio.report_interval)
                    .describe_and_run(),
            );
            let token = cancellation_token.clone();
            let result = axum::serve(listener, metrics_router(handle, routes))
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
                .map_err(|e| anyhow::anyhow!("Metrics exporter failed: {e:?}"));
            if cancellation_token.is_cancelled() {
                tracing::info!(port, "Metrics exporter cancelled");
            }
            upkeep.abort();
            runtime_metrics_reporter.abort();
            let _ = runtime_metrics_reporter.await;
            result
        })
    }
}

/// `routes` plus the Prometheus text on `/metrics` and as fallback, and `OK` on `/health`.
pub(crate) fn metrics_router(handle: PrometheusHandle, routes: axum::Router) -> axum::Router {
    let render = move || {
        let handle = handle.clone();
        async move {
            match tokio::task::spawn_blocking(move || handle.render()).await {
                Ok(text) => ([(http::header::CONTENT_TYPE, "text/plain")], text).into_response(),
                Err(e) => {
                    tracing::error!(?e, "Failed to render metrics");
                    http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
    };
    routes
        .route("/health", axum::routing::any(|| async { "OK" }))
        .route("/metrics", axum::routing::any(render.clone()))
        .fallback(render)
}

/// Creates `PrometheusRecorder` and installs it as the global metrics recorder. Also creates a
/// `PrometheusMetricLayer` (which captures axum requests) and binds the metrics port.
///
/// # Errors
/// Fails if the `PrometheusBuilder` fails to build or the port cannot be bound.
pub fn get_axum_layer_and_install_recorder(
    metrics_port: u16,
) -> anyhow::Result<(PrometheusMetricLayer<'static>, MetricsServer)> {
    let listener = std::net::TcpListener::bind((CONFIG.bind_ip, metrics_port))?;
    listener.set_nonblocking(true)?;
    let recorder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(
                PREFIXED_HTTP_REQUESTS_DURATION_SECONDS
                    .get()
                    .map_or(AXUM_HTTP_REQUESTS_DURATION_SECONDS, |s| s.as_str())
                    .to_string(),
            ),
            utils::SECONDS_DURATION_BUCKETS,
        )?
        // Histograms without explicit buckets are rendered as Prometheus
        // *summaries* (quantile series, no `_bucket`), which cannot be
        // aggregated across replicas or fed to `histogram_quantile`. Give the
        // admission-gate histograms real buckets; they measure sub-second
        // request-path latency, so the HTTP duration buckets fit.
        .set_buckets_for_metric(
            Matcher::Prefix("lakekeeper_admission".to_string()),
            utils::SECONDS_DURATION_BUCKETS,
        )?
        .build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)?;

    Ok((
        build_metric_layer(handle.clone()),
        MetricsServer {
            handle,
            listener,
            port: metrics_port,
        },
    ))
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    async fn get(router: &axum::Router, uri: &str) -> (http::StatusCode, String) {
        let response = router
            .clone()
            .oneshot(
                http::Request::get(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn every_path_but_health_and_own_routes_serves_the_metrics() {
        let recorder = PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("demo_requests_total").increment(3);
        });
        let own_routes =
            axum::Router::new().route("/own", axum::routing::get(|| async { "own route" }));
        let router = metrics_router(recorder.handle(), own_routes);

        let metrics_text = "# TYPE demo_requests_total counter\ndemo_requests_total 3\n\n";
        for (uri, expected) in [
            ("/metrics", metrics_text),
            ("/", metrics_text),
            ("/anything/else", metrics_text),
            ("/health", "OK"),
            ("/own", "own route"),
        ] {
            assert_eq!(
                get(&router, uri).await,
                (http::StatusCode::OK, expected.to_string()),
                "{uri}"
            );
        }
    }
}
