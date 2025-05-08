use http::Request;
use tower_http::{
    request_id::{MakeRequestId, RequestId},
    trace::MakeSpan,
};
use tracing::{Level, Span};
use uuid::Uuid;

use crate::{
    api::X_REQUEST_ID_HEADER, X_FORWARDED_FOR_HEADER, X_FORWARDED_PORT_HEADER,
    X_FORWARDED_PROTO_HEADER,
};

/// A `MakeSpan` implementation that attaches the `request_id` to the span.
#[derive(Debug, Clone)]
pub(crate) struct RestMakeSpan {
    level: Level,
}

impl RestMakeSpan {
    /// Create a [tracing span] with a certain [`Level`].
    ///
    /// [tracing span]: https://docs.rs/tracing/latest/tracing/#spans
    pub(crate) fn new(level: Level) -> Self {
        Self { level }
    }
}

/// tower-http's `MakeSpan` implementation does not attach a `request_id` to the span. The impl below
/// does.
impl<B> MakeSpan<B> for RestMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        // This ugly macro is needed, unfortunately, because `tracing::span!`
        // required the level argument to be static. Meaning we can't just pass
        // `self.level`.
        macro_rules! make_span {
            ($level:expr) => {
                    tracing::span!(
                        $level,
                        "request",
                        method = %request.method(),
                        host = %request.headers().get("host").and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                        "x-forwarded-for" = %request.headers().get(X_FORWARDED_FOR_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                        "x-forwarded-proto" = %request.headers().get(X_FORWARDED_PROTO_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                        "x-forwarded-port" = %request.headers().get(X_FORWARDED_PORT_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("not set"),
                        uri = %request.uri(),
                        version = ?request.version(),
                        request_id = %request
                                    .headers()
                                    .get(X_REQUEST_ID_HEADER)
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("MISSING-REQUEST-ID")
                    )
            }
        }
        match self.level {
            Level::TRACE => make_span!(tracing::Level::TRACE),
            Level::DEBUG => make_span!(tracing::Level::DEBUG),
            Level::INFO => make_span!(tracing::Level::INFO),
            Level::WARN => make_span!(tracing::Level::WARN),
            Level::ERROR => make_span!(tracing::Level::ERROR),
        }
    }
}

/// A [`MakeRequestId`] that generates `UUIDv7`s.
#[derive(Clone, Copy, Default)]
pub(crate) struct MakeRequestUuid7;

impl MakeRequestId for MakeRequestUuid7 {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let request_id = Uuid::now_v7().to_string().parse().unwrap();
        Some(RequestId::new(request_id))
    }
}
