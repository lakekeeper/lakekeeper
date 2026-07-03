use http::{HeaderMap, Request};
use opentelemetry::{
    global,
    propagation::{Extractor, TextMapPropagator},
    trace::TraceContextExt as _,
};
use tower_http::{
    request_id::{MakeRequestId, RequestId},
    trace::MakeSpan,
};
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use uuid::Uuid;

use crate::{
    X_FORWARDED_HOST_HEADER, X_FORWARDED_PORT_HEADER, X_FORWARDED_PREFIX_HEADER,
    X_FORWARDED_PROTO_HEADER, api::X_REQUEST_ID_HEADER,
};

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

fn set_remote_parent(span: &Span, headers: &HeaderMap) {
    global::get_text_map_propagator(|propagator| {
        set_remote_parent_with_propagator(span, headers, propagator);
    });
}

fn set_remote_parent_with_propagator(
    span: &Span,
    headers: &HeaderMap,
    propagator: &dyn TextMapPropagator,
) {
    let context = propagator.extract(&HeaderExtractor(headers));
    if context.span().span_context().is_valid() {
        // This runs immediately after span creation, before TraceLayer enters
        // it, which is the only point at which its parent may be set.
        let _ = span.set_parent(context);
    }
}

/// A `MakeSpan` implementation that attaches the `request_id` to the span.
#[derive(Debug, Clone)]
pub struct RestMakeSpan {
    level: Level,
    log_authorization_header: bool,
}

impl RestMakeSpan {
    /// Create a [tracing span] with a certain [`Level`].
    ///
    /// [tracing span]: https://docs.rs/tracing/latest/tracing/#spans
    #[must_use]
    pub fn new(level: Level) -> Self {
        Self {
            level,
            log_authorization_header: false,
        }
    }

    /// If enabled, the `Authorization` header will be included in request spans.
    /// This exposes sensitive credentials and should never be enabled in production.
    #[must_use]
    pub fn with_log_authorization_header(mut self, enabled: bool) -> Self {
        self.log_authorization_header = enabled;
        self
    }
}

/// tower-http's `MakeSpan` implementation does not attach a `request_id` to the span. The impl below
/// does.
impl<B> MakeSpan<B> for RestMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        // This ugly macro is needed, unfortunately, because `tracing::span!`
        // required the level argument to be static. Meaning we can't just pass
        // `self.level`.
        macro_rules! make_full_span {
            ($level:expr) => {
                tracing::span!(
                    $level,
                    "request",
                    otel.kind = "server",
                    method = %request.method(),
                    host = %request.headers().get("host").and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-host" = %request.headers().get(X_FORWARDED_HOST_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-proto" = %request.headers().get(X_FORWARDED_PROTO_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-port" = %request.headers().get(X_FORWARDED_PORT_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-prefix" = %request.headers().get(X_FORWARDED_PREFIX_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    uri = %request.uri(),
                    version = ?request.version(),
                    request_id = %request
                                .headers()
                                .get(X_REQUEST_ID_HEADER)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("MISSING-REQUEST-ID"),
                )
            }
        }
        macro_rules! make_full_span_with_auth {
            ($level:expr, $auth:expr) => {
                tracing::span!(
                    $level,
                    "request",
                    otel.kind = "server",
                    method = %request.method(),
                    host = %request.headers().get("host").and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-host" = %request.headers().get(X_FORWARDED_HOST_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-proto" = %request.headers().get(X_FORWARDED_PROTO_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-port" = %request.headers().get(X_FORWARDED_PORT_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    "x-forwarded-prefix" = %request.headers().get(X_FORWARDED_PREFIX_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                    uri = %request.uri(),
                    version = ?request.version(),
                    request_id = %request
                                .headers()
                                .get(X_REQUEST_ID_HEADER)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("MISSING-REQUEST-ID"),
                    authorization = %$auth,
                )
            }
        }
        macro_rules! make_reduced_span {
            ($level:expr) => {
                tracing::span!(
                    $level,
                    "request",
                    otel.kind = "server",
                    method = %request.method(),
                    uri = %request.uri(),
                    version = ?request.version(),
                    request_id = %request
                                .headers()
                                .get(X_REQUEST_ID_HEADER)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("MISSING-REQUEST-ID"),
                )
            }
        }
        let path = request.uri().path();
        let is_info_endpoint = request.method() == http::Method::GET
            && (path.ends_with("/v1/config") || path.ends_with("/management/v1/info"));

        let span = if self.log_authorization_header && is_info_endpoint {
            let authorization = request
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("not set");

            match self.level {
                Level::TRACE => make_full_span_with_auth!(tracing::Level::TRACE, authorization),
                Level::DEBUG => make_full_span_with_auth!(tracing::Level::DEBUG, authorization),
                Level::INFO => make_full_span_with_auth!(tracing::Level::INFO, authorization),
                Level::WARN => make_full_span_with_auth!(tracing::Level::WARN, authorization),
                Level::ERROR => make_full_span_with_auth!(tracing::Level::ERROR, authorization),
            }
        } else if is_info_endpoint {
            match self.level {
                Level::TRACE => make_full_span!(tracing::Level::TRACE),
                Level::DEBUG => make_full_span!(tracing::Level::DEBUG),
                Level::INFO => make_full_span!(tracing::Level::INFO),
                Level::WARN => make_full_span!(tracing::Level::WARN),
                Level::ERROR => make_full_span!(tracing::Level::ERROR),
            }
        } else {
            match self.level {
                Level::TRACE => make_reduced_span!(tracing::Level::TRACE),
                Level::DEBUG => make_reduced_span!(tracing::Level::DEBUG),
                Level::INFO => make_reduced_span!(tracing::Level::INFO),
                Level::WARN => make_reduced_span!(tracing::Level::WARN),
                Level::ERROR => make_reduced_span!(tracing::Level::ERROR),
            }
        };

        set_remote_parent(&span, request.headers());
        span
    }
}

/// A [`MakeRequestId`] that generates `UUIDv7`s.
#[derive(Debug, Clone, Copy, Default)]
pub struct MakeRequestUuid7;

impl MakeRequestId for MakeRequestUuid7 {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let request_id = Uuid::now_v7().to_string().parse().unwrap();
        Some(RequestId::new(request_id))
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    const REMOTE_TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";

    fn request_with_traceparent(value: &str) -> Request<()> {
        Request::builder()
            .uri("/catalog/v1/config")
            .header("traceparent", value)
            .body(())
            .unwrap()
    }

    #[test]
    fn request_span_continues_valid_remote_trace() {
        let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("request-tracing-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let request =
                request_with_traceparent(&format!("00-{REMOTE_TRACE_ID}-b7ad6b7169203331-01"));
            let span = RestMakeSpan::new(Level::INFO).make_span(&request);
            set_remote_parent_with_propagator(&span, request.headers(), &propagator);
            let context = span.context();
            let span_context = context.span();
            assert_eq!(
                span_context.span_context().trace_id().to_string(),
                REMOTE_TRACE_ID
            );
            assert_ne!(
                span_context.span_context().span_id().to_string(),
                "b7ad6b7169203331"
            );
        });
    }

    #[test]
    fn malformed_traceparent_starts_new_trace() {
        let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("request-tracing-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let request = request_with_traceparent("not-a-valid-traceparent");
            let span = RestMakeSpan::new(Level::INFO).make_span(&request);
            set_remote_parent_with_propagator(&span, request.headers(), &propagator);
            let context = span.context();
            assert!(context.span().span_context().is_valid());
            assert_ne!(
                context.span().span_context().trace_id().to_string(),
                REMOTE_TRACE_ID
            );
        });
    }
}
