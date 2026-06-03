use crate::{ErrorKind, IOError};

/// Convert an `object_store::Error` into an `IOError` carrying the relevant
/// `ErrorKind` for downstream classification.
///
/// `object_store` already maps HTTP-layer failures into structured variants
/// (`NotFound`, `Precondition`, `PermissionDenied`, ...), so this is mostly a
/// 1-1 enum mapping. The `Generic` fallback is the only place where we still
/// have to look at the wrapped source — and even there, we keep the textual
/// 429 / `Server Busy` heuristic from the old `adls_error.rs` since the
/// upstream classification of throttling responses is not exhaustive.
pub(crate) fn parse_error(err: object_store::Error, location: &str) -> IOError {
    let kind = error_kind(&err);
    let msg = err.to_string();
    IOError::new(kind, msg, location.to_string()).set_source(err)
}

fn error_kind(err: &object_store::Error) -> ErrorKind {
    match err {
        object_store::Error::NotFound { .. } => ErrorKind::NotFound,
        object_store::Error::AlreadyExists { .. }
        | object_store::Error::Precondition { .. }
        | object_store::Error::NotModified { .. }
        | object_store::Error::InvalidPath { .. } => ErrorKind::ConditionNotMatch,
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => ErrorKind::PermissionDenied,
        object_store::Error::Generic { source, .. } => classify_generic(source.as_ref()),
        // Known-but-uninteresting variants today (`NotSupported`,
        // `NotImplemented`, `UnknownConfigurationKey`, the optional
        // `JoinError`). These already map cleanly to `Unexpected`.
        object_store::Error::NotSupported { .. }
        | object_store::Error::NotImplemented { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => ErrorKind::Unexpected,
        // `object_store::Error` is `#[non_exhaustive]`. Anything unknown lands
        // here. Emit a warn so a new upstream variant after a dependency bump
        // is observable in production logs rather than silently degraded to
        // `Unexpected`. Bump `error_kind` to map the new variant when this
        // fires.
        other => {
            tracing::warn!(
                error = ?other,
                "Unmapped `object_store::Error` variant; check `adls_error::error_kind` after \
                 the latest `object_store` upgrade. Falling back to ErrorKind::Unexpected.",
            );
            ErrorKind::Unexpected
        }
    }
}

/// Classifier for the `Generic` fallback variant.
///
/// Strategy (structural first, heuristic last):
/// 1. Walk the error chain via [`std::error::Error::source`] for a
///    [`reqwest::Error`] and use its `.status()`. This is the only
///    structural status accessor publicly exposed in this dep graph —
///    `object_store::client::retry::RetryError` would be the better target
///    but it sits behind `pub(crate)`.
/// 2. Fall back to substring matching on the concatenated error-chain
///    Display strings for Azure-specific throttling markers (`Server Busy`,
///    `Operation Timeout`) and status codes that appear in
///    `RequestError::Status`'s rendering.
//
// TODO(adls-error): drop the substring fallback once `object_store` exposes
// `RetryError::status()` publicly.
fn classify_generic(source: &(dyn std::error::Error + Send + Sync + 'static)) -> ErrorKind {
    if let Some(status) = walk_for_reqwest_status(source)
        && let Some(kind) = http_status_to_error_kind(status.as_u16())
    {
        return kind;
    }

    // Substring fallback for the Azure-specific *body text* markers that the
    // structural path can't see. Bare-number patterns (`"429"`, `"503"`, ...)
    // are intentionally absent — they false-positive on path content and
    // would already be caught by the structural path if they came from an
    // HTTP status.
    let msg = error_chain_to_string(source);
    if msg.contains("Server Busy") || msg.contains("Operation Timeout") {
        return ErrorKind::RateLimited;
    }
    if msg.contains("TooManyRequests") {
        return ErrorKind::RateLimited;
    }
    if msg.contains("ServiceUnavailable") {
        return ErrorKind::ServiceUnavailable;
    }
    ErrorKind::Unexpected
}

/// Walk the `err.source()` chain looking for a `reqwest::Error` and return
/// its HTTP status if it carries one.
fn walk_for_reqwest_status(err: &(dyn std::error::Error + 'static)) -> Option<reqwest::StatusCode> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(re) = e.downcast_ref::<reqwest::Error>()
            && let Some(status) = re.status()
        {
            return Some(status);
        }
        current = e.source();
    }
    None
}

/// Walk and concatenate the error-chain Display strings — gives the
/// substring heuristic something to match against the deeper
/// `RequestError::Status { status: 429 ... }` rendering, not just the
/// outermost `Generic store error: ...` wrapper.
fn error_chain_to_string(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut current = err.source();
    while let Some(e) = current {
        out.push_str(" :: ");
        out.push_str(&e.to_string());
        current = e.source();
    }
    out
}

/// HTTP status → `ErrorKind`, used by [`classify_generic`] after
/// downcasting to `reqwest::Error::status`. Returning `Option` lets callers
/// decide the default for unmapped statuses (`classify_generic` falls back
/// to substring heuristics).
pub(super) fn http_status_to_error_kind(status: u16) -> Option<ErrorKind> {
    match status {
        404 => Some(ErrorKind::NotFound),
        401 | 403 => Some(ErrorKind::PermissionDenied),
        408 | 504 => Some(ErrorKind::RequestTimeout),
        409 | 412 => Some(ErrorKind::ConditionNotMatch),
        429 => Some(ErrorKind::RateLimited),
        500 | 503 => Some(ErrorKind::ServiceUnavailable),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper for the substring-fallback tests: build a hand-rolled error
    /// chain whose Display text contains the desired marker.
    #[derive(Debug)]
    struct StringErr(&'static str);
    impl std::fmt::Display for StringErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for StringErr {}

    fn kind_for_msg(msg: &'static str) -> ErrorKind {
        classify_generic(&StringErr(msg))
    }

    // ---- substring-fallback classification ----

    #[test]
    fn classify_generic_server_busy_is_rate_limited() {
        assert_eq!(kind_for_msg("Server Busy"), ErrorKind::RateLimited);
    }

    #[test]
    fn classify_generic_operation_timeout_is_rate_limited() {
        // Azure surfaces this in 5xx bodies for backend timeouts.
        assert_eq!(kind_for_msg("Operation Timeout"), ErrorKind::RateLimited);
    }

    #[test]
    fn classify_generic_too_many_requests_is_rate_limited() {
        assert_eq!(
            kind_for_msg("Body says TooManyRequests"),
            ErrorKind::RateLimited
        );
    }

    #[test]
    fn classify_generic_service_unavailable_is_service_unavailable() {
        assert_eq!(
            kind_for_msg("Body says ServiceUnavailable"),
            ErrorKind::ServiceUnavailable
        );
    }

    #[test]
    fn classify_generic_unrelated_is_unexpected() {
        // Bare-number patterns are deliberately absent — a path
        // containing "429" must NOT be misclassified as RateLimited.
        assert_eq!(kind_for_msg("data/queue-429/file"), ErrorKind::Unexpected);
        assert_eq!(kind_for_msg("path with 503 in name"), ErrorKind::Unexpected);
    }

    // ---- structural classification via reqwest::Error::status() ----

    /// Drive `classify_generic` through the structural downcast path with
    /// a real `reqwest::Error` whose `.status()` is `429`. This is the
    /// path that *should* take priority over the substring heuristic.
    #[tokio::test]
    async fn classify_generic_structural_path_handles_reqwest_status_429() {
        use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        // `error_for_status()` converts a non-2xx Response into a
        // reqwest::Error with `.status()` populated. This is what
        // `object_store`'s `Generic` source is typically built from.
        let resp = reqwest::get(server.uri()).await.expect("wiremock GET");
        let req_err = resp
            .error_for_status()
            .expect_err("429 must become reqwest::Error");
        assert_eq!(
            req_err.status(),
            Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
        );

        // Pass straight to classify_generic. The walk_for_reqwest_status
        // path should find the status and skip the substring fallback.
        assert_eq!(classify_generic(&req_err), ErrorKind::RateLimited);
    }

    /// 503 takes the same structural path → `ServiceUnavailable`.
    #[tokio::test]
    async fn classify_generic_structural_path_handles_reqwest_status_503() {
        use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let resp = reqwest::get(server.uri()).await.expect("wiremock GET");
        let req_err = resp.error_for_status().expect_err("503 must error");
        assert_eq!(classify_generic(&req_err), ErrorKind::ServiceUnavailable);
    }

    // ---- structural per-variant mapping pin ----

    /// Pin the current mapping from each named `object_store::Error` variant
    /// to its `ErrorKind`. The match arms in [`error_kind`] are the source
    /// of truth; this test catches accidental edits.
    ///
    /// Not a drift detector for *new* upstream variants — `object_store::Error`
    /// is `#[non_exhaustive]` and Rust on stable cannot enforce exhaustiveness
    /// across crates. Instead, [`error_kind`]'s catch-all `tracing::warn!`s on
    /// every unknown variant so the bump is observable in production logs.
    /// Check that warning after every `object_store` upgrade.
    #[test]
    fn error_kind_pinned_mapping_per_named_variant() {
        use object_store::path::Path;
        let path = Path::from("foo");

        // `Generic` is exercised by the `classify_generic_*` tests above —
        // its source has to be a real error chain, so we don't include it
        // in this enum-shape mapping list.
        let cases: &[(object_store::Error, ErrorKind)] = &[
            (
                object_store::Error::NotFound {
                    path: path.to_string(),
                    source: "x".into(),
                },
                ErrorKind::NotFound,
            ),
            (
                object_store::Error::AlreadyExists {
                    path: path.to_string(),
                    source: "x".into(),
                },
                ErrorKind::ConditionNotMatch,
            ),
            (
                object_store::Error::Precondition {
                    path: path.to_string(),
                    source: "x".into(),
                },
                ErrorKind::ConditionNotMatch,
            ),
            (
                object_store::Error::NotModified {
                    path: path.to_string(),
                    source: "x".into(),
                },
                ErrorKind::ConditionNotMatch,
            ),
            (
                object_store::Error::PermissionDenied {
                    path: path.to_string(),
                    source: "x".into(),
                },
                ErrorKind::PermissionDenied,
            ),
            (
                object_store::Error::Unauthenticated {
                    path: path.to_string(),
                    source: "x".into(),
                },
                ErrorKind::PermissionDenied,
            ),
            (
                object_store::Error::InvalidPath {
                    source: object_store::path::Error::EmptySegment {
                        path: String::new(),
                    },
                },
                ErrorKind::ConditionNotMatch,
            ),
        ];

        for (err, expected) in cases {
            let actual = error_kind(err);
            assert_eq!(
                actual, *expected,
                "mapping drift for {err:?}: expected {expected:?}, got {actual:?}",
            );
        }
    }

    // ---- error-chain walk: depth > 0 ----

    /// Verify `walk_for_reqwest_status` actually walks past depth 0. The
    /// `classify_generic_structural_path_*` tests above pass a `reqwest::Error`
    /// directly, so they only exercise depth 0 — a "simplification" that
    /// replaced the walk with a single `downcast_ref` would still pass them.
    /// Wrap a real `reqwest::Error` inside another error one level deep so
    /// the walker has to step at least once.
    #[tokio::test]
    async fn classify_generic_walks_chain_past_depth_zero() {
        use std::{error::Error, fmt};

        use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

        #[derive(Debug)]
        struct Wrap(Box<dyn Error + Send + Sync + 'static>);
        impl fmt::Display for Wrap {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("wrap")
            }
        }
        impl Error for Wrap {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                Some(self.0.as_ref())
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;
        let resp = reqwest::get(server.uri()).await.expect("wiremock GET");
        let req_err = resp.error_for_status().expect_err("429 must error");

        let wrapped = Wrap(Box::new(req_err));
        assert_eq!(classify_generic(&wrapped), ErrorKind::RateLimited);
    }
}
