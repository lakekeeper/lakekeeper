//! Binary-local cloud-event backend wiring.
//!
//! Each backend lives in its own crate (`lakekeeper-events-nats`,
//! `lakekeeper-events-kafka`); the binary aggregates them plus the in-core
//! tracing backend into the `Vec<Arc<dyn CloudEventBackend>>` that the
//! publisher consumes.

use std::sync::Arc;

use lakekeeper::{
    service::events::{CloudEventBackend, maybe_tracing_cloud_event_backend},
    tracing,
};

/// Builds the default set of cloud-event backends from the binary's
/// runtime configuration.
///
/// # Errors
/// If any backend fails to initialize from configuration.
pub(crate) async fn get_default_cloud_event_backends_from_config()
-> anyhow::Result<Vec<Arc<dyn CloudEventBackend + Sync + Send>>> {
    let mut sinks: Vec<Arc<dyn CloudEventBackend + Sync + Send>> = Vec::new();

    if let Some(nats) = lakekeeper_events_nats::build_nats_publisher_from_config().await? {
        sinks.push(Arc::new(nats));
    }
    if let Some(kafka) = lakekeeper_events_kafka::build_kafka_publisher_from_config()? {
        sinks.push(Arc::new(kafka));
    }
    if let Some(tracing) = maybe_tracing_cloud_event_backend() {
        sinks.push(tracing);
    }

    if sinks.is_empty() {
        tracing::info!("Running without publisher.");
    }

    Ok(sinks)
}
