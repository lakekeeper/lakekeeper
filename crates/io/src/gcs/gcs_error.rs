use std::error::Error as _;

use google_cloud_gax::error::{CredentialsError, rpc::Code};
use google_cloud_storage::Error;

use crate::{ErrorKind, IOError};

/// Maps an error of the GCS SDK to an [`IOError`].
///
/// Data requests (HTTP) report an HTTP status code, metadata requests (gRPC) a status code.
/// The message holds the status only: an HTTP error body can be a page of a proxy.
pub(crate) fn parse_error(err: Error, location: &str) -> IOError {
    let kind = error_kind(&err);
    let reason = match (err.status(), err.http_status_code()) {
        (Some(status), _) => format!("GCS request failed: {:?}: {}", status.code, status.message),
        (None, Some(status_code)) => format!("GCS request failed with HTTP status {status_code}"),
        (None, None) => "GCS request failed".to_string(),
    };
    let io_error = IOError::new(kind, reason, location.to_string());
    let io_error = match err.http_status_code() {
        Some(status_code) => io_error.with_http_status(status_code),
        None => io_error,
    };
    io_error.set_source(Box::new(err))
}

fn error_kind(err: &Error) -> ErrorKind {
    if let Some(status_code) = err.http_status_code() {
        return match status_code {
            404 => ErrorKind::NotFound,
            401 | 403 => ErrorKind::PermissionDenied,
            408 | 504 => ErrorKind::RequestTimeout,
            409 | 412 => ErrorKind::ConditionNotMatch, // Conflict | Precondition Failed
            429 => ErrorKind::RateLimited,
            503 => ErrorKind::ServiceUnavailable,
            _ => ErrorKind::Unexpected,
        };
    }
    if let Some(status) = err.status() {
        return match status.code {
            Code::NotFound => ErrorKind::NotFound,
            Code::PermissionDenied | Code::Unauthenticated => ErrorKind::PermissionDenied,
            Code::DeadlineExceeded => ErrorKind::RequestTimeout,
            Code::FailedPrecondition | Code::AlreadyExists => ErrorKind::ConditionNotMatch,
            Code::ResourceExhausted => ErrorKind::RateLimited,
            Code::Unavailable => ErrorKind::ServiceUnavailable,
            _ => ErrorKind::Unexpected,
        };
    }
    if err.is_timeout() {
        return ErrorKind::RequestTimeout;
    }
    if let Some(credentials_error) = err
        .source()
        .and_then(|source| source.downcast_ref::<CredentialsError>())
    {
        return if credentials_error.is_transient() {
            ErrorKind::ServiceUnavailable
        } else {
            ErrorKind::ConfigInvalid
        };
    }
    // Resumable uploads report failures of a request as an io error wrapping the SDK error.
    if let Some(inner) = err
        .source()
        .and_then(|source| source.downcast_ref::<Error>())
    {
        return error_kind(inner);
    }
    ErrorKind::Unexpected
}

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::rpc::Status;
    use http::HeaderMap;

    use super::*;

    fn http_error(status_code: u16) -> Error {
        Error::http(status_code, HeaderMap::new(), bytes::Bytes::new())
    }

    fn grpc_error(code: Code) -> Error {
        Error::service(Status::default().set_code(code).set_message("test"))
    }

    #[test]
    fn http_status_maps_to_kind() {
        for (status_code, kind) in [
            (404, ErrorKind::NotFound),
            (401, ErrorKind::PermissionDenied),
            (403, ErrorKind::PermissionDenied),
            (408, ErrorKind::RequestTimeout),
            (504, ErrorKind::RequestTimeout),
            (409, ErrorKind::ConditionNotMatch),
            (412, ErrorKind::ConditionNotMatch),
            (429, ErrorKind::RateLimited),
            (503, ErrorKind::ServiceUnavailable),
            (500, ErrorKind::Unexpected),
        ] {
            assert_eq!(error_kind(&http_error(status_code)), kind, "{status_code}");
        }
    }

    #[test]
    fn grpc_not_found_maps_to_not_found() {
        let err = grpc_error(Code::NotFound);
        assert_eq!(err.http_status_code(), None);
        assert_eq!(
            parse_error(err, "gs://bucket/key").kind(),
            ErrorKind::NotFound
        );
    }

    #[test]
    fn grpc_code_maps_to_kind() {
        for (code, kind) in [
            (Code::PermissionDenied, ErrorKind::PermissionDenied),
            (Code::Unauthenticated, ErrorKind::PermissionDenied),
            (Code::DeadlineExceeded, ErrorKind::RequestTimeout),
            (Code::FailedPrecondition, ErrorKind::ConditionNotMatch),
            (Code::Aborted, ErrorKind::Unexpected),
            (Code::AlreadyExists, ErrorKind::ConditionNotMatch),
            (Code::ResourceExhausted, ErrorKind::RateLimited),
            (Code::Unavailable, ErrorKind::ServiceUnavailable),
            (Code::Internal, ErrorKind::Unexpected),
        ] {
            assert_eq!(error_kind(&grpc_error(code)), kind, "{code:?}");
        }
    }

    #[test]
    fn timeout_maps_to_request_timeout() {
        let err = Error::timeout(std::io::Error::other("deadline"));
        assert_eq!(error_kind(&err), ErrorKind::RequestTimeout);
    }

    #[test]
    fn io_wrapped_error_keeps_its_kind() {
        let err = Error::io(Error::authentication(CredentialsError::from_msg(
            false, "bad key",
        )));
        assert_eq!(error_kind(&err), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn message_excludes_http_payload() {
        let err = Error::http(
            502,
            HeaderMap::new(),
            bytes::Bytes::from_static(b"<html>proxy error page</html>"),
        );
        let io_error = parse_error(err, "gs://bucket/key");
        assert_eq!(io_error.reason(), "GCS request failed with HTTP status 502");
        assert_eq!(io_error.http_status(), Some(502));
        assert_eq!(io_error.kind(), ErrorKind::Unexpected);
    }

    #[test]
    fn message_includes_grpc_status() {
        let io_error = parse_error(grpc_error(Code::NotFound), "gs://bucket/key");
        assert_eq!(io_error.reason(), "GCS request failed: NotFound: test");
    }

    #[test]
    fn credentials_error_maps_by_transience() {
        let permanent = Error::authentication(CredentialsError::from_msg(false, "bad key"));
        assert_eq!(error_kind(&permanent), ErrorKind::ConfigInvalid);
        let transient = Error::authentication(CredentialsError::from_msg(true, "no route"));
        assert_eq!(error_kind(&transient), ErrorKind::ServiceUnavailable);
    }
}
