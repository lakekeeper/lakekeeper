use azure_core::StatusCode;

use crate::{ErrorKind, IOError};

pub(crate) fn parse_error(err: azure_core::Error, location: &str) -> IOError {
    let err = Box::new(err);
    let Some(http_err) = err.as_http_error() else {
        return IOError::new(
            transport_error_kind(&err),
            format!("ADLS request failed: {err}"),
            location.to_string(),
        )
        .set_source(err);
    };

    let http_status = u16::from(http_err.status());
    let error_kind = error_code_kind(http_err.error_code()).unwrap_or(match http_err.status() {
        StatusCode::NotFound => ErrorKind::NotFound,
        StatusCode::Forbidden | StatusCode::Unauthorized => ErrorKind::PermissionDenied,
        StatusCode::RequestTimeout | StatusCode::GatewayTimeout => ErrorKind::RequestTimeout,
        StatusCode::PreconditionFailed | StatusCode::Conflict => ErrorKind::ConditionNotMatch,
        StatusCode::TooManyRequests => ErrorKind::RateLimited,
        StatusCode::InternalServerError
        | StatusCode::BadGateway
        | StatusCode::ServiceUnavailable => ErrorKind::ServiceUnavailable,
        _ => ErrorKind::Unexpected,
    });

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

/// Kind of the error codes that Azure Storage returns in `x-ms-error-code` for one HTTP
/// status with several meanings.
fn error_code_kind(code: Option<&str>) -> Option<ErrorKind> {
    match code? {
        "ServerBusy" => Some(ErrorKind::RateLimited),
        "OperationTimedOut" => Some(ErrorKind::RequestTimeout),
        "InternalError" => Some(ErrorKind::ServiceUnavailable),
        _ => None,
    }
}

/// Kind of an error without an HTTP response. An I/O error of the HTTP client is a timeout or a
/// failed connection; every other error is unexpected.
fn transport_error_kind(err: &azure_core::Error) -> ErrorKind {
    if *err.kind() != azure_core::error::ErrorKind::Io {
        return ErrorKind::Unexpected;
    }
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<reqwest::Error>() {
            return if error.is_timeout() {
                ErrorKind::RequestTimeout
            } else {
                ErrorKind::ServiceUnavailable
            };
        }
        source = error.source();
    }
    ErrorKind::ServiceUnavailable
}

#[cfg(test)]
mod tests {
    use azure_core::{Response, StatusCode, error::HttpError, headers::Headers};

    use super::*;

    async fn http_error(status: StatusCode) -> azure_core::Error {
        http_error_with_code(status, None).await
    }

    async fn http_error_with_code(status: StatusCode, code: Option<&str>) -> azure_core::Error {
        let body = futures::stream::once(async { Ok(bytes::Bytes::new()) });
        let mut headers = Headers::new();
        if let Some(code) = code {
            headers.insert("x-ms-error-code", code.to_string());
        }
        let response = Response::new(status, headers, Box::pin(body));
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

    #[tokio::test]
    async fn service_error_codes_decide_the_kind() {
        for (status, code, kind) in [
            (
                StatusCode::ServiceUnavailable,
                Some("ServerBusy"),
                ErrorKind::RateLimited,
            ),
            (
                StatusCode::InternalServerError,
                Some("OperationTimedOut"),
                ErrorKind::RequestTimeout,
            ),
            (
                StatusCode::InternalServerError,
                Some("InternalError"),
                ErrorKind::ServiceUnavailable,
            ),
            (StatusCode::BadGateway, None, ErrorKind::ServiceUnavailable),
            (
                StatusCode::ServiceUnavailable,
                None,
                ErrorKind::ServiceUnavailable,
            ),
            (
                StatusCode::InternalServerError,
                None,
                ErrorKind::ServiceUnavailable,
            ),
            (StatusCode::NotImplemented, None, ErrorKind::Unexpected),
            (
                StatusCode::HttpVersionNotSupported,
                None,
                ErrorKind::Unexpected,
            ),
            (StatusCode::BadRequest, None, ErrorKind::Unexpected),
        ] {
            let error = parse_error(http_error_with_code(status, code).await, "location");
            assert_eq!(error.kind(), kind, "{status} {code:?}");
        }
    }

    /// The error the SDK returns when the HTTP client fails, as its reqwest transport builds it.
    fn transport_error(error: reqwest::Error) -> azure_core::Error {
        azure_core::Error::full(
            azure_core::error::ErrorKind::Io,
            error,
            "failed to execute `reqwest` request",
        )
    }

    #[tokio::test]
    async fn client_timeouts_are_request_timeouts() {
        // The listener accepts connections but never answers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let client = reqwest::Client::builder()
            .read_timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();
        let error = client.get(url).send().await.unwrap_err();
        assert!(error.is_timeout());
        assert_eq!(
            parse_error(transport_error(error), "location").kind(),
            ErrorKind::RequestTimeout
        );
    }

    #[tokio::test]
    async fn connection_failures_are_service_unavailable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let error = reqwest::get(url).await.unwrap_err();
        assert!(error.is_connect());
        assert_eq!(
            parse_error(transport_error(error), "location").kind(),
            ErrorKind::ServiceUnavailable
        );
    }
}
