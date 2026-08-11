use std::{ffi::OsString, time::Duration};

use anyhow::Context as _;
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::{
    OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, Protocol,
    WithExportConfig as _,
};
use opentelemetry_sdk::{
    Resource,
    resource::{EnvResourceDetector, ResourceDetector, TelemetryResourceDetector},
    trace::SdkTracerProvider,
};
use tracing_subscriber::{
    EnvFilter,
    filter::LevelFilter,
    layer::{Layer as _, SubscriberExt as _},
    util::SubscriberInitExt as _,
};

use crate::CONFIG_BIN;

const DEFAULT_SERVICE_NAME: &str = "lakekeeper";
const OTLP_TRACES_PATH: &str = "/v1/traces";

/// Owns the optional provider so buffered spans are flushed on every exit path.
#[derive(Debug)]
pub(crate) struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(error) = provider.shutdown_with_timeout(Duration::from_secs(5))
        {
            eprintln!("Failed to shut down OpenTelemetry tracer provider: {error}");
        }
    }
}

/// Install the process-wide tracing subscriber.
///
/// OTLP is intentionally opt-in and server-only. Other subcommands retain the
/// existing JSON tracing subscriber even if OTLP environment variables exist.
pub(crate) fn init(enable_otel_for_command: bool) -> anyhow::Result<TelemetryGuard> {
    let provider = if enable_otel_for_command && otlp_endpoint_is_configured() {
        Some(build_provider()?)
    } else {
        None
    };

    let otel_layer = provider.as_ref().map(|provider| {
        let tracer = provider.tracer(DEFAULT_SERVICE_NAME);
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_error_events_to_status(true)
            .with_error_records_to_exceptions(true)
            .with_filter(LevelFilter::INFO)
    });

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(true)
        .with_file(CONFIG_BIN.debug.extended_logs)
        .with_line_number(CONFIG_BIN.debug.extended_logs)
        .with_filter(filter);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()
        .context("Failed to initialize tracing subscriber")?;

    if let Some(provider) = &provider {
        global::set_tracer_provider(provider.clone());
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        lakekeeper::tracing::info!(
            protocol = "http/protobuf",
            "OpenTelemetry trace export enabled"
        );
    }

    Ok(TelemetryGuard { provider })
}

fn build_provider() -> anyhow::Result<SdkTracerProvider> {
    let endpoint = resolved_configured_endpoint()?
        .context("An OTLP trace endpoint must be configured before building the exporter")?;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_protocol(Protocol::HttpBinary)
        .build()
        .context("Failed to configure OTLP HTTP trace exporter")?;

    let configured_service_name = std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|name| !name.trim().is_empty());
    let resource = build_resource(
        configured_service_name,
        Box::new(EnvResourceDetector::new()),
    );

    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build())
}

fn build_resource(
    configured_service_name: Option<String>,
    environment_detector: Box<dyn ResourceDetector>,
) -> Resource {
    let resource_builder = Resource::builder_empty()
        .with_service_name(DEFAULT_SERVICE_NAME)
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_detector(environment_detector);
    match configured_service_name {
        Some(service_name) => resource_builder.with_service_name(service_name),
        None => resource_builder,
    }
    .build()
}

fn parse_endpoint(name: &str, value: &str) -> anyhow::Result<url::Url> {
    let endpoint =
        url::Url::parse(value).with_context(|| format!("{name} must be a valid absolute URL"))?;
    anyhow::ensure!(
        matches!(endpoint.scheme(), "http" | "https") && endpoint.host().is_some(),
        "{name} must be an absolute HTTP(S) URL"
    );
    Ok(endpoint)
}

fn resolved_configured_endpoint() -> anyhow::Result<Option<String>> {
    configured_endpoint()
        .map(resolve_selected_endpoint)
        .transpose()
}

fn resolve_selected_endpoint((name, value): (&str, OsString)) -> anyhow::Result<String> {
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{name} must contain valid UTF-8"))?;
    let mut endpoint = parse_endpoint(name, &value)?;

    if name == OTEL_EXPORTER_OTLP_ENDPOINT {
        let path = format!(
            "{}{}",
            endpoint.path(),
            if endpoint.path().ends_with('/') {
                &OTLP_TRACES_PATH[1..]
            } else {
                OTLP_TRACES_PATH
            }
        );
        endpoint.set_path(&path);
        Ok(endpoint.into())
    } else {
        Ok(value)
    }
}

fn otlp_endpoint_is_configured() -> bool {
    configured_endpoint().is_some()
}

fn configured_endpoint() -> Option<(&'static str, OsString)> {
    select_configured_endpoint(
        std::env::var_os(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT),
        std::env::var_os(OTEL_EXPORTER_OTLP_ENDPOINT),
    )
}

fn select_configured_endpoint(
    traces_endpoint: Option<OsString>,
    generic_endpoint: Option<OsString>,
) -> Option<(&'static str, OsString)> {
    [
        (OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, traces_endpoint),
        (OTEL_EXPORTER_OTLP_ENDPOINT, generic_endpoint),
    ]
    .into_iter()
    .find_map(|(name, value)| {
        value
            .filter(|value| !value.is_empty())
            .map(|value| (name, value))
    })
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read as _, Write as _},
        net::TcpListener,
        sync::mpsc,
        thread,
    };

    use opentelemetry::trace::{Span as _, Tracer as _, TracerProvider as _};

    use super::*;

    #[derive(Debug)]
    struct TestResourceDetector(Option<&'static str>);

    impl ResourceDetector for TestResourceDetector {
        fn detect(&self) -> Resource {
            match self.0 {
                Some(service_name) => Resource::builder_empty()
                    .with_service_name(service_name)
                    .build(),
                None => Resource::builder_empty().build(),
            }
        }
    }

    #[test]
    fn service_name_uses_standard_precedence_and_lakekeeper_default() {
        let service_name = opentelemetry::Key::new("service.name");

        let default = build_resource(None, Box::new(TestResourceDetector(None)));
        assert_eq!(
            default.get(&service_name),
            Some(opentelemetry::Value::from(DEFAULT_SERVICE_NAME))
        );

        let resource_attribute = build_resource(
            None,
            Box::new(TestResourceDetector(Some("resource-service"))),
        );
        assert_eq!(
            resource_attribute.get(&service_name),
            Some(opentelemetry::Value::from("resource-service"))
        );

        let explicit = build_resource(
            Some("explicit-service".to_string()),
            Box::new(TestResourceDetector(Some("resource-service"))),
        );
        assert_eq!(
            explicit.get(&service_name),
            Some(opentelemetry::Value::from("explicit-service"))
        );
    }

    #[test]
    fn endpoint_selection_uses_standard_precedence_and_ignores_empty_values() {
        assert_eq!(select_configured_endpoint(None, None), None);
        assert_eq!(
            select_configured_endpoint(Some(OsString::new()), Some(OsString::new())),
            None
        );
        assert_eq!(
            select_configured_endpoint(
                Some(OsString::from("http://traces:4318/v1/traces")),
                Some(OsString::from("http://generic:4318")),
            ),
            Some((
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                OsString::from("http://traces:4318/v1/traces")
            ))
        );
        assert_eq!(
            select_configured_endpoint(
                Some(OsString::new()),
                Some(OsString::from("http://generic:4318")),
            ),
            Some((
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                OsString::from("http://generic:4318")
            ))
        );
    }

    #[test]
    fn selected_endpoint_is_passed_to_exporter_with_the_expected_trace_path() {
        let generic = select_configured_endpoint(
            Some(OsString::new()),
            Some(OsString::from("http://generic:4318")),
        )
        .unwrap();
        assert_eq!(
            resolve_selected_endpoint(generic).unwrap(),
            "http://generic:4318/v1/traces"
        );

        let generic_with_trailing_slash = (
            OTEL_EXPORTER_OTLP_ENDPOINT,
            OsString::from("http://generic:4318/"),
        );
        assert_eq!(
            resolve_selected_endpoint(generic_with_trailing_slash).unwrap(),
            "http://generic:4318/v1/traces"
        );

        let generic_with_query = (
            OTEL_EXPORTER_OTLP_ENDPOINT,
            OsString::from("https://collector/otlp?token=x"),
        );
        assert_eq!(
            resolve_selected_endpoint(generic_with_query).unwrap(),
            "https://collector/otlp/v1/traces?token=x"
        );

        let traces = (
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
            OsString::from("http://traces:4318/custom/path"),
        );
        assert_eq!(
            resolve_selected_endpoint(traces).unwrap(),
            "http://traces:4318/custom/path"
        );
    }

    #[test]
    fn endpoint_must_be_an_absolute_http_url() {
        assert!(parse_endpoint("ENDPOINT", "http://collector:4318").is_ok());
        assert!(parse_endpoint("ENDPOINT", "https://collector.example/v1/traces").is_ok());
        assert!(parse_endpoint("ENDPOINT", "://invalid").is_err());
        assert!(parse_endpoint("ENDPOINT", "file:///tmp/traces").is_err());
    }

    #[test]
    fn batch_exporter_works_without_a_tokio_reactor_on_its_worker_thread() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (request_received_tx, request_received_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 64 * 1024];
            let bytes_read = stream.read(&mut request).unwrap();
            assert!(bytes_read > 0);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 2\r\n\r\n\x0a\x00",
                )
                .unwrap();
            request_received_tx.send(()).unwrap();
        });

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpBinary)
            .build()
            .unwrap();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build();
        let tracer = provider.tracer("batch-exporter-regression-test");
        let mut span = tracer.start("test-span");
        span.end();

        provider.force_flush().unwrap();
        request_received_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        provider.shutdown().unwrap();
        server.join().unwrap();
    }
}
