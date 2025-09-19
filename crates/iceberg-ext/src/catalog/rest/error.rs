// macro to implement IntoResponse
use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
};

use http::StatusCode;
pub use iceberg::Error;
use serde::{Deserialize, Serialize};

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
}

impl StdError for ErrorModel {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|s| &**s as &(dyn StdError + 'static))
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

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for IcebergErrorResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let Self { error } = self;
        let stack_s = error.to_string();
        let ErrorModel {
            message,
            r#type,
            code,
            source: _,
            stack: details,
        } = error;
        let error_id = uuid::Uuid::now_v7();
        let mut response = if code >= 500 || [401, 403, 424].contains(&code) {
            tracing::error!(%error_id, %stack_s, ?details, %message, %r#type, %code, "Error response");
            axum::Json(IcebergErrorResponse {
                error: ErrorModel {
                    message,
                    r#type,
                    code,
                    source: None,
                    stack: vec![format!("Error ID: {error_id}")],
                },
            })
            .into_response()
        } else {
            // Log at info level for 4xx errors
            tracing::info!(%error_id, %stack_s, ?details, %message, %r#type, %code, "Error response");

            let mut details = details;
            details.push(format!("Error ID: {error_id}"));

            axum::Json(IcebergErrorResponse {
                error: ErrorModel {
                    message,
                    r#type,
                    code,
                    source: None,
                    stack: details,
                },
            })
            .into_response()
        };

        *response.status_mut() = axum::http::StatusCode::from_u16(code)
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        response
    }
}

#[cfg(test)]
mod tests {
    use futures_util::stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_iceberg_error_response_serialization() {
        let val = IcebergErrorResponse {
            error: ErrorModel {
                message: "The server does not support this operation".to_string(),
                r#type: "UnsupportedOperationException".to_string(),
                code: 406,
                source: None,
                stack: vec![],
            },
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
        // Test basic error without source or stack
        let error = ErrorModel {
            message: "Something went wrong".to_string(),
            r#type: "TestError".to_string(),
            code: 500,
            source: None,
            stack: vec![],
        };

        let display_output = format!("{error}");
        assert!(display_output.contains("Something went wrong"));
        assert!(display_output.contains("TestError"));
        assert!(display_output.contains("500"));
        // Should not contain "Stack:" since it's empty
        assert!(!display_output.contains("Stack:"));
        // Should not contain "Caused by:" since there's no source
        assert!(!display_output.contains("Caused by:"));

        // Test error with stack details
        let error_with_stack = ErrorModel {
            message: "Another error".to_string(),
            r#type: "StackError".to_string(),
            code: 400,
            source: None,
            stack: vec!["detail1".to_string(), "detail2".to_string()],
        };

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

        let error_with_source = ErrorModel {
            message: "IO operation failed".to_string(),
            r#type: "IOError".to_string(),
            code: 404,
            source: Some(source_error),
            stack: vec!["io_stack".to_string()],
        };

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
            error: ErrorModel {
                message: "internal error".into(),
                r#type: "Internal".into(),
                code: 500,
                source: None,
                stack: vec!["secret detail".into()],
            },
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
            error: ErrorModel {
                message: "bad input".into(),
                r#type: "BadRequest".into(),
                code: 400,
                source: None,
                stack: vec!["user detail".into()],
            },
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
