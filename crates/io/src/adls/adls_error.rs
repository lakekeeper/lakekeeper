use azure_core::StatusCode;

use crate::{error::ErrorKind, IOError};

pub(crate) fn parse_error(err: azure_core::Error, location: &str) -> IOError {
    let err = Box::new(err);
    let Some(http_err) = err.as_http_error() else {
        return IOError::new(
            ErrorKind::Unexpected,
            format!("Non-HTTP error occurred while reading from ADLS: {err}"),
            location.to_string(),
        )
        .set_source(err);
    };

    if [
        StatusCode::ServiceUnavailable,
        StatusCode::InternalServerError,
    ]
    .contains(&http_err.status())
        && (http_err
            .error_message()
            .unwrap_or_default()
            .contains("Server Busy")
            || http_err
                .error_message()
                .unwrap_or_default()
                .contains("Operation Timeout"))
    {
        return IOError::new(
            ErrorKind::RateLimited,
            format!("{} - {err}", http_err.status().canonical_reason()),
            location.to_string(),
        )
        .with_context(format!(
            "HTTP Error Message: {}",
            http_err.error_message().unwrap_or_default()
        ))
        .set_source(err);
    }

    let error_kind = match http_err.status() {
        StatusCode::NotFound => ErrorKind::NotFound,
        StatusCode::Forbidden | StatusCode::Unauthorized => ErrorKind::PermissionDenied,
        StatusCode::RequestTimeout | StatusCode::GatewayTimeout => ErrorKind::RequestTimeout,
        StatusCode::ServiceUnavailable => ErrorKind::ServiceUnavailable,
        StatusCode::PreconditionFailed | StatusCode::Conflict => ErrorKind::ConditionNotMatch,
        StatusCode::TooManyRequests => ErrorKind::RateLimited,
        status if status.is_server_error() => ErrorKind::Unexpected,
        _ => ErrorKind::Unexpected,
    };

    IOError::new(
        error_kind,
        format!("{} - {err}", http_err.status().canonical_reason()),
        location.to_string(),
    )
    .with_context(format!(
        "HTTP Error Message: {}",
        http_err.error_message().unwrap_or_default()
    ))
    .set_source(err)
}
