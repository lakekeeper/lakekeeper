// macro to implement IntoResponse
use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
};

use http::StatusCode;
pub use iceberg::Error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use valuable::Valuable;

#[cfg(feature = "axum")]
macro_rules! impl_into_response {
    ($type:ty) => {
        impl axum::response::IntoResponse for $type {
            fn into_response(self) -> axum::http::Response<axum::body::Body> {
                axum::Json(self).into_response()
            }
        }
    };
    () => {};
}

#[cfg(feature = "axum")]
pub(crate) use impl_into_response;
use typed_builder::TypedBuilder;

impl From<IcebergErrorResponse> for iceberg::Error {
    fn from(resp: IcebergErrorResponse) -> iceberg::Error {
        resp.error.into()
    }
}

impl std::fmt::Display for IcebergErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl From<ErrorModel> for iceberg::Error {
    fn from(value: ErrorModel) -> Self {
        let mut error = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, &value.message)
            .with_context("type", &value.r#type)
            .with_context("code", format!("{}", value.code));
        error = error.with_context("stack", value.to_string());

        error
    }
}

fn error_chain_fmt(e: impl std::error::Error, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    writeln!(f, "{e}\n")?;
    let mut current = e.source();
    while let Some(cause) = current {
        writeln!(f, "Caused by:\n\t{cause}")?;
        current = cause.source();
    }
    Ok(())
}

fn error_chain_vec(e: &(dyn std::error::Error + Send + Sync + 'static)) -> Vec<String> {
    let mut details = Vec::new();
    let mut current = Some(e as &(dyn std::error::Error + 'static));
    while let Some(cause) = current {
        details.push(format!("{cause}"));
        current = cause.source();
    }
    details
}

impl From<ErrorModel> for IcebergErrorResponse {
    fn from(value: ErrorModel) -> Self {
        IcebergErrorResponse { error: value }
    }
}

impl From<IcebergErrorResponse> for ErrorModel {
    fn from(value: IcebergErrorResponse) -> Self {
        value.error
    }
}

/// JSON wrapper for all error responses (non-2xx)
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IcebergErrorResponse {
    pub error: ErrorModel,
}

/// JSON error payload returned in a response with further details on the error
#[derive(Default, Debug, TypedBuilder, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorModel {
    /// Human-readable error message
    #[builder(setter(into))]
    pub message: String,
    /// Internal type definition of the error
    #[builder(setter(into))]
    pub r#type: String,
    /// HTTP response code
    pub code: u16,
    #[serde(skip)]
    #[builder(default)]
    pub source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    #[builder(default)]
    pub stack: Vec<String>,
    #[serde(skip)]
    #[builder(default)]
    pub skip_log: bool,
    #[serde(skip)]
    #[builder(default=uuid::Uuid::now_v7())]
    pub error_id: Uuid,
}

impl StdError for ErrorModel {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn StdError + 'static))
    }
}

impl Display for ErrorModel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{} ({}): {}", self.r#type, self.code, self.message)?;

        if !self.stack.is_empty() {
            writeln!(f, "Stack:")?;
            for detail in &self.stack {
                writeln!(f, "  {detail}")?;
            }
        }

        if let Some(source) = self.source.as_ref() {
            writeln!(f, "Caused by:")?;
            // Dereference `source` to get `dyn StdError` and then take a reference to pass
            error_chain_fmt(&**source, f)?;
        }

        Ok(())
    }
}

impl ErrorModel {
    pub fn bad_request(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(message, r#type, StatusCode::BAD_REQUEST.as_u16(), source)
    }

    pub fn not_implemented(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(
            message,
            r#type,
            StatusCode::NOT_IMPLEMENTED.as_u16(),
            source,
        )
    }

    pub fn precondition_failed(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(
            message,
            r#type,
            StatusCode::PRECONDITION_FAILED.as_u16(),
            source,
        )
    }

    pub fn internal(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(
            message,
            r#type,
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source,
        )
    }

    pub fn conflict(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(message, r#type, StatusCode::CONFLICT.as_u16(), source)
    }

    pub fn not_found(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(message, r#type, StatusCode::NOT_FOUND.as_u16(), source)
    }

    pub fn not_allowed(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(
            message,
            r#type,
            StatusCode::METHOD_NOT_ALLOWED.as_u16(),
            source,
        )
    }

    pub fn unauthorized(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(message, r#type, StatusCode::UNAUTHORIZED.as_u16(), source)
    }

    pub fn forbidden(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(message, r#type, StatusCode::FORBIDDEN.as_u16(), source)
    }

    pub fn failed_dependency(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(
            message,
            r#type,
            StatusCode::FAILED_DEPENDENCY.as_u16(),
            source,
        )
    }

    pub fn new(
        message: impl Into<String>,
        r#type: impl Into<String>,
        code: u16,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::builder()
            .message(message)
            .r#type(r#type)
            .code(code)
            .source(source)
            .build()
    }

    #[must_use]
    pub fn append_details(mut self, details: impl IntoIterator<Item = String>) -> Self {
        self.stack.extend(details);
        self
    }

    #[must_use]
    pub fn append_detail(mut self, detail: impl Into<String>) -> Self {
        self.stack.push(detail.into());
        self
    }

    #[must_use]
    pub fn from_io_error_with_code(
        io_error: lakekeeper_io::IOError,
        code: impl Into<u16>,
        detail: &str,
    ) -> Self {
        let message = match &io_error.location() {
            Some(location) => format!("IO error at `{location}`: {}", io_error.reason()),
            None => format!("IO error: {}", io_error.reason()),
        };

        Self::builder()
            .message(message)
            .r#type(io_error.kind().to_string())
            .code(code.into())
            .stack(
                io_error
                    .context()
                    .iter()
                    .map(ToString::to_string)
                    .chain(std::iter::once(detail.to_string()))
                    .collect(),
            )
            .source(io_error.into_source().map(Into::into))
            .build()
    }

    #[must_use]
    pub fn from_io_error(io_error: lakekeeper_io::IOError, detail: &str) -> Self {
        // Map external IO errors (e.g., from S3) to appropriate HTTP status codes.
        // We use PRECONDITION_FAILED (412) for most delegation/dependency errors
        // to avoid leaking internal architecture details or confusing clients about
        // where auth/permission issues occurred.
        let code = match io_error.kind() {
            lakekeeper_io::ErrorKind::Unexpected => StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            lakekeeper_io::ErrorKind::NotFound => StatusCode::BAD_REQUEST.as_u16(),
            lakekeeper_io::ErrorKind::RequestTimeout
            | lakekeeper_io::ErrorKind::ServiceUnavailable
            | lakekeeper_io::ErrorKind::ConfigInvalid
            | lakekeeper_io::ErrorKind::PermissionDenied
            | lakekeeper_io::ErrorKind::RateLimited
            | lakekeeper_io::ErrorKind::ConditionNotMatch
            | lakekeeper_io::ErrorKind::CredentialsExpired => {
                StatusCode::PRECONDITION_FAILED.as_u16()
            }
        };

        Self::from_io_error_with_code(io_error, code, detail)
    }
}

impl IcebergErrorResponse {
    #[must_use]
    pub fn append_details(mut self, details: impl IntoIterator<Item = String>) -> Self {
        self.error.stack.extend(details);
        self
    }

    #[must_use]
    pub fn append_detail(mut self, detail: impl Into<String>) -> Self {
        self.error.stack.push(detail.into());
        self
    }
}

#[derive(Debug)]
struct TracedResponseError<'a> {
    r#type: &'a str,
    code: u16,
    message: &'a str,
    stack: &'a [String],
    error_id: String,
    source: &'a [String],
}

impl valuable::Valuable for TracedResponseError<'_> {
    fn as_value(&self) -> valuable::Value<'_> {
        valuable::Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn valuable::Visit) {
        visit.visit_entry(
            valuable::Value::String("type"),
            valuable::Value::String(self.r#type),
        );
        visit.visit_entry(
            valuable::Value::String("code"),
            valuable::Value::U16(self.code),
        );
        visit.visit_entry(
            valuable::Value::String("message"),
            valuable::Value::String(self.message),
        );
        if !self.stack.is_empty() {
            visit.visit_entry(valuable::Value::String("stack"), self.stack.as_value());
        }
        visit.visit_entry(
            valuable::Value::String("error_id"),
            valuable::Value::String(&self.error_id),
        );
        if !self.source.is_empty() {
            visit.visit_entry(valuable::Value::String("source"), self.source.as_value());
        }
    }
}

impl valuable::Mappable for TracedResponseError<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut len = 4; // type, code, message, error_id
        if !self.stack.is_empty() {
            len += 1;
        }
        if !self.source.is_empty() {
            len += 1;
        }
        (len, Some(len))
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for ErrorModel {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        IcebergErrorResponse { error: self }.into_response()
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for IcebergErrorResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let Self { error } = self;
        let ErrorModel {
            message,
            r#type,
            code,
            source,
            stack,
            error_id,
            skip_log: skip_trace,
        } = error;
        let source = source.map(|e| error_chain_vec(&*e)).unwrap_or_default();

        let traced_error = TracedResponseError {
            r#type: &r#type,
            code,
            message: &message,
            stack: &stack,
            error_id: error_id.to_string(),
            source: &source,
        };

        // Hide stack from user for 5xx errors, only log internally.
        // Log at error level for 5xx errors
        let mut response = if code >= 500 {
            if !skip_trace {
                tracing::error!(
                    event_source = "error_response",
                    error = tracing::field::valuable(&traced_error.as_value()),
                    "Internal server error response"
                );
            }
            axum::Json(IcebergErrorResponse {
                error: ErrorModel {
                    message,
                    r#type,
                    code,
                    source: None,
                    stack: vec![format!("Error ID: {error_id}")],
                    error_id,
                    skip_log: skip_trace,
                },
            })
            .into_response()
        } else {
            // Log at info level for 4xx errors
            if !skip_trace {
                tracing::info!(
                    event_source = "error_response",
                    error = tracing::field::valuable(&traced_error.as_value()),
                    "Error response"
                );
            }
            let mut stack = stack;
            stack.push(format!("Error ID: {error_id}"));

            axum::Json(IcebergErrorResponse {
                error: ErrorModel {
                    message,
                    r#type,
                    code,
                    source: None,
                    stack,
                    error_id,
                    skip_log: skip_trace,
                },
            })
            .into_response()
        };

        *response.status_mut() = axum::http::StatusCode::from_u16(code)
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        response
    }
}

#[cfg(all(test, feature = "axum"))]
mod tests {
    use futures_util::stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_iceberg_error_response_serialization() {
        let val = IcebergErrorResponse {
            error: ErrorModel::builder()
                .message("The server does not support this operation")
                .r#type("UnsupportedOperationException")
                .code(StatusCode::NOT_ACCEPTABLE.as_u16())
                .build(),
        };
        let resp = axum::response::IntoResponse::into_response(val);
        assert_eq!(resp.status(), StatusCode::NOT_ACCEPTABLE);

        // Not sure how we'd get the body otherwise
        let mut b = resp.into_body().into_data_stream();
        let mut buf = Vec::with_capacity(1024);
        while let Some(d) = b.next().await {
            buf.extend_from_slice(d.unwrap().as_ref());
        }
        let resp: IcebergErrorResponse = serde_json::from_slice(&buf).unwrap();
        assert_eq!(
            resp.error.message,
            "The server does not support this operation"
        );
        assert_eq!(resp.error.r#type, "UnsupportedOperationException");
        assert_eq!(resp.error.code, 406);

        let json = serde_json::json!({"error": {
            "message": "The server does not support this operation",
            "type": "UnsupportedOperationException",
            "code": 406
        }});

        let resp: IcebergErrorResponse = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(resp).unwrap(), json);
    }

    #[test]
    fn test_error_model_display() {
        let error = ErrorModel::builder()
            .message("Something went wrong")
            .r#type("TestError")
            .code(500)
            .build();

        let display_output = format!("{error}");
        assert!(display_output.contains("Something went wrong"));
        assert!(display_output.contains("TestError"));
        assert!(display_output.contains("500"));
        // Should not contain "Stack:" since it's empty
        assert!(!display_output.contains("Stack:"));
        // Should not contain "Caused by:" since there's no source
        assert!(!display_output.contains("Caused by:"));

        let error_with_stack = ErrorModel::builder()
            .message("Another error")
            .r#type("StackError")
            .code(400)
            .stack(vec!["detail1".to_string(), "detail2".to_string()])
            .build();

        let display_output = format!("{error_with_stack}");
        assert!(display_output.contains("Another error"));
        assert!(display_output.contains("StackError"));
        assert!(display_output.contains("400"));
        assert!(display_output.contains("Stack:"));
        assert!(display_output.contains("  detail1"));
        assert!(display_output.contains("  detail2"));

        // Test error with source
        let source_error = Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found",
        )) as Box<dyn std::error::Error + Send + Sync + 'static>;

        let error_with_source = ErrorModel::builder()
            .message("IO operation failed")
            .r#type("IOError")
            .code(404)
            .source(Some(source_error))
            .stack(vec!["io_stack".to_string()])
            .build();

        let display_output = format!("{error_with_source}");
        assert!(display_output.contains("IO operation failed"));
        assert!(display_output.contains("IOError"));
        assert!(display_output.contains("404"));
        assert!(display_output.contains("Stack:"));
        assert!(display_output.contains("  io_stack"));
        assert!(display_output.contains("Caused by:"));
        assert_eq!(display_output.matches("Caused by:").count(), 1);
        assert!(display_output.contains("File not found"));
    }

    #[tokio::test]
    async fn test_into_response_server_error_redacts_stack_and_adds_error_id() {
        let val = IcebergErrorResponse {
            error: ErrorModel::builder()
                .message("internal error")
                .r#type("Internal")
                .code(500)
                .stack(vec!["secret detail".into()])
                .build(),
        };
        let resp = axum::response::IntoResponse::into_response(val);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = resp
            .into_body()
            .into_data_stream()
            .collect::<Vec<_>>()
            .await;
        let buf = body
            .into_iter()
            .flat_map(|r| r.unwrap())
            .collect::<bytes::Bytes>();
        let parsed: IcebergErrorResponse = serde_json::from_slice(&buf).unwrap();

        // Stack should contain only the error id, not the original detail
        assert!(parsed.error.stack.len() == 1);
        assert!(parsed.error.stack[0].starts_with("Error ID: "));
    }

    #[tokio::test]
    async fn test_into_response_client_error_preserves_stack_and_adds_error_id() {
        let val = IcebergErrorResponse {
            error: ErrorModel::builder()
                .message("bad input")
                .r#type("BadRequest")
                .code(400)
                .stack(vec!["user detail".into()])
                .build(),
        };
        let resp = axum::response::IntoResponse::into_response(val);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = resp
            .into_body()
            .into_data_stream()
            .collect::<Vec<_>>()
            .await;
        let buf = body
            .into_iter()
            .flat_map(|r| r.unwrap())
            .collect::<bytes::Bytes>();
        let parsed: IcebergErrorResponse = serde_json::from_slice(&buf).unwrap();

        // Stack should preserve original and append error id
        assert_eq!(parsed.error.stack.len(), 2);
        assert!(parsed.error.stack[0] == "user detail");
        assert!(parsed.error.stack[1].starts_with("Error ID: "));
    }
}
