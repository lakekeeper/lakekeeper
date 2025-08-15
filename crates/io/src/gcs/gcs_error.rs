use google_cloud_storage::http::Error;

use crate::{error::ErrorKind, IOError};

pub(crate) fn parse_error(err: Error, location: &str) -> IOError {
    let err = Box::new(err);

    match err.as_ref() {
        Error::Response(error_response) => {
            let error_kind = match error_response.code {
                404 => ErrorKind::NotFound,
                401 | 403 => ErrorKind::PermissionDenied,
                408 | 504 => ErrorKind::RequestTimeout,
                409 | 412 => ErrorKind::ConditionNotMatch, // Conflict | Precondition Failed
                429 => ErrorKind::RateLimited,
                503 => ErrorKind::ServiceUnavailable,
                _ => ErrorKind::Unexpected,
            };

            IOError::new(
                error_kind,
                format!(
                    "GCS HTTP {} - {}",
                    error_response.code, error_response.message
                ),
                location.to_string(),
            )
            .with_context(format!("GCS Error: {}", error_response.message))
            .set_source(err)
        }
        Error::HttpClient(error) => IOError::new(
            ErrorKind::Unexpected,
            format!("GCS HTTP client error: {error}"),
            location.to_string(),
        )
        .set_source(err),
        Error::HttpMiddleware(error) => IOError::new(
            ErrorKind::Unexpected,
            format!("GCS HTTP middleware error: {error}"),
            location.to_string(),
        )
        .set_source(err),
        Error::TokenSource(error) => IOError::new(
            ErrorKind::ConfigInvalid,
            format!("GCS token source error: {error}"),
            location.to_string(),
        )
        .set_source(err),
        Error::InvalidRangeHeader(message) => IOError::new(
            ErrorKind::Unexpected,
            format!("GCS invalid range header: {message}"),
            location.to_string(),
        )
        .set_source(err),
        Error::RawResponse(error, _) => IOError::new(
            ErrorKind::Unexpected,
            format!("GCS raw response error: {error}"),
            location.to_string(),
        )
        .set_source(err),
    }
}
