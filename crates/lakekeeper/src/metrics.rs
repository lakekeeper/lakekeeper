use std::{future::Future, pin::Pin};

use axum_prometheus::{
    metrics,
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder},
    utils, PrometheusMetricLayer, PrometheusMetricLayerBuilder,
    AXUM_HTTP_REQUESTS_DURATION_SECONDS, PREFIXED_HTTP_REQUESTS_DURATION_SECONDS,
};

use crate::CONFIG;

pub type ExporterFuture = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'static>>;

/// Creates `PrometheusRecorder` and installs it as the global metrics recorder. Also creates a
/// `PrometheusMetricLayer` which captures axum requests and an `ExporterFuture` that serves metrics
/// on a given port.
///
/// # Errors
/// Fails if the `PrometheusBuilder` fails to build.
pub fn get_axum_layer_and_install_recorder(
    metrics_port: u16,
    cancellation_token: tokio_util::sync::CancellationToken,
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

    let (layer, _) = PrometheusMetricLayerBuilder::new()
        .with_metrics_from_fn(|| handle)
        .build_pair();

    Ok((
        layer,
        Box::pin(async move {
            tokio::select! {
                () = cancellation_token.cancelled() => {
                    tracing::info!(port = metrics_port, "Metrics exporter cancelled");
                    Ok(())
                },
                r = exporter => {
                    r.map_err(|e| anyhow::anyhow!("Metrics exporter failed: {e:?}"))
                }
            }
        }),
    ))
}
