use http::Request;
use tower_http::{
    request_id::{MakeRequestId, RequestId},
    trace::MakeSpan,
};
use tracing::{Level, Span};
use uuid::Uuid;

use crate::{
    X_FORWARDED_HOST_HEADER, X_FORWARDED_PORT_HEADER, X_FORWARDED_PREFIX_HEADER,
    X_FORWARDED_PROTO_HEADER, api::X_REQUEST_ID_HEADER,
};

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

        if self.log_authorization_header && is_info_endpoint {
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
        }
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
