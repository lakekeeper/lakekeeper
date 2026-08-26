use std::{future::Future, pin::Pin};

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

/// Creates `PrometheusRecorder` and installs it as the global metrics recorder. Also creates a
/// `PrometheusMetricLayer` (which captures axum requests), a Tokio Runtime Metrics recorder (which captures tokio runtime metrics),
/// and an `ExporterFuture` that serves metrics on a given port.
///
/// # Errors
/// Fails if the `PrometheusBuilder` fails to build.
pub fn get_axum_layer_and_install_recorder(
    metrics_port: u16,
    cancellation_token: crate::CancellationToken,
) -> anyhow::Result<(PrometheusMetricLayer<'static>, ExporterFuture)> {
    let (recorder, exporter) = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(
                PREFIXED_HTTP_REQUESTS_DURATION_SECONDS
                    .get()
                    .map_or(AXUM_HTTP_REQUESTS_DURATION_SECONDS, |s| s.as_str())
                    .to_string(),
            ),
            utils::SECONDS_DURATION_BUCKETS,
        )?
        .with_http_listener((CONFIG.bind_ip, metrics_port))
        .build()?;
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)?;

    let runtime_metrics_reporter_handle = tokio::task::spawn(
        tokio_metrics::RuntimeMetricsReporterBuilder::default()
            .with_interval(CONFIG.metrics.tokio.report_interval)
            .describe_and_run(),
    );

    let layer = build_metric_layer(handle);

    Ok((
        layer,
        Box::pin(async move {
            let result = tokio::select! {
                () = cancellation_token.cancelled() => {
                    tracing::info!(port = metrics_port, "Metrics exporter cancelled");
                    Ok(())
                },
                r = exporter => {
                    r.map_err(|e| anyhow::anyhow!("Metrics exporter failed: {e:?}"))
                }
            };
            runtime_metrics_reporter_handle.abort();
            let _ = runtime_metrics_reporter_handle.await;
            result
        }),
    ))
}
