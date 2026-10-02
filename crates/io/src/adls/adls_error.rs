use azure_core::StatusCode;

use crate::{ErrorKind, IOError};

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

    let http_status = u16::from(http_err.status());

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
        .with_http_status(http_status)
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
    .with_http_status(http_status)
    .with_context(format!(
        "HTTP Error Message: {}",
        http_err.error_message().unwrap_or_default()
    ))
    .set_source(err)
}

#[cfg(test)]
mod tests {
    use azure_core::{Response, StatusCode, error::HttpError, headers::Headers};

    use super::*;

    async fn http_error(status: StatusCode) -> azure_core::Error {
        let body = futures::stream::once(async { Ok(bytes::Bytes::new()) });
        let response = Response::new(status, Headers::new(), Box::pin(body));
        let http_error = HttpError::new(response).await;
        azure_core::Error::new(
            azure_core::error::ErrorKind::http_response(status, None),
            http_error,
        )
    }

    #[tokio::test]
    async fn test_parse_error_keeps_the_http_status() {
        // 401 and 403 share a kind; only the status tells them apart.
        for status in [StatusCode::Unauthorized, StatusCode::Forbidden] {
            let error = parse_error(
                http_error(status).await,
                "abfss://filesystem@account.dfs.core.windows.net/key",
            );
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            assert_eq!(error.http_status(), Some(u16::from(status)));
        }
    }
}
