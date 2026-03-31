use azure_core::{error::ErrorKind as AzureErrorKind, http::StatusCode};

use crate::{IOError, error::ErrorKind};

pub(crate) fn parse_error(err: azure_core::Error, location: &str) -> IOError {
    let err = Box::new(err);

    let Some(status) = err.http_status() else {
        return IOError::new(
            ErrorKind::Unexpected,
            format!("Non-HTTP error occurred while accessing ADLS: {err}"),
            location.to_string(),
        )
        .set_source(err);
    };

    // Extract an optional error code string from the error kind
    let error_code: Option<String> = match err.kind() {
        AzureErrorKind::HttpResponse { error_code, .. } => error_code.clone(),
        _ => None,
    };
    let error_code_str = error_code.as_deref().unwrap_or_default();

    // Rate-limit / server-busy detection (matches old "Server Busy" / "Operation Timeout" logic)
    if [
        StatusCode::ServiceUnavailable,
        StatusCode::InternalServerError,
    ]
    .contains(&status)
        && (error_code_str.contains("Server Busy")
            || error_code_str.contains("Operation Timeout")
            || err.to_string().contains("Server Busy")
            || err.to_string().contains("Operation Timeout"))
    {
        return IOError::new(
            ErrorKind::RateLimited,
            format!("{} - {err}", status.canonical_reason()),
            location.to_string(),
        )
        .with_context(format!("HTTP error code: {error_code_str}"))
        .set_source(err);
    }

    let error_kind = match status {
        StatusCode::NotFound => ErrorKind::NotFound,
        StatusCode::Forbidden | StatusCode::Unauthorized => ErrorKind::PermissionDenied,
        StatusCode::RequestTimeout | StatusCode::GatewayTimeout => ErrorKind::RequestTimeout,
        StatusCode::ServiceUnavailable => ErrorKind::ServiceUnavailable,
        StatusCode::PreconditionFailed | StatusCode::Conflict => ErrorKind::ConditionNotMatch,
        StatusCode::TooManyRequests => ErrorKind::RateLimited,
        s if s.is_server_error() => ErrorKind::Unexpected,
        _ => ErrorKind::Unexpected,
    };

    IOError::new(
        error_kind,
        format!("{} - {err}", status.canonical_reason()),
        location.to_string(),
    )
    .with_context(format!("HTTP error code: {error_code_str}"))
    .set_source(err)
}
