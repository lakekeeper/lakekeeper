use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
// macro to implement IntoResponse
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

/// JSON wrapper for all error responses (non-2xx)
#[derive(Default, Debug)]
pub struct IcebergErrorResponse {
    pub error: ErrorModel,
}

impl From<IcebergErrorResponse> for iceberg::Error {
    fn from(resp: IcebergErrorResponse) -> iceberg::Error {
        resp.error.into()
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

/// JSON error payload returned in a response with further details on the error
#[allow(clippy::module_name_repetitions)]
#[derive(Deserialize, Serialize, Debug)]
pub struct ApiError {
    /// Human-readable error message
    pub message: String,
    /// Internal type definition of the error
    pub r#type: String,
    /// HTTP response code
    pub code: u16,
}

/// JSON error payload returned in a response with further details on the error
#[allow(clippy::module_name_repetitions)]
#[derive(Default, Debug, TypedBuilder)]
pub struct ErrorModel {
    /// Human-readable error message
    #[builder(setter(into))]
    pub message: String,
    /// Internal type definition of the error
    #[builder(setter(into))]
    pub r#type: String,
    /// HTTP response code
    pub code: u16,
    #[builder(default)]
    pub source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
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
        if let Some(source) = self.source.as_ref() {
            writeln!(f, "{}", self.message)?;
            // Dereference `source` to get `dyn StdError` and then take a reference to pass
            error_chain_fmt(&**source, f)?;
        } else {
            write!(f, "{}", self.message)?;
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

    pub fn forbidden(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::new(message, r#type, StatusCode::FORBIDDEN.as_u16(), source)
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
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for IcebergErrorResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let Self { error } = self;
        let stack = error.to_string();
        let ErrorModel {
            message,
            r#type,
            code,
            source: _,
        } = error;

        tracing::info!(%stack, %message, %r#type, %code, "Error response");

        let mut response = axum::Json(ApiError {
            message,
            r#type,
            code,
        })
        .into_response();

        *response.status_mut() = axum::http::StatusCode::from_u16(code)
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream::StreamExt;

    #[tokio::test]
    async fn test_iceberg_error_response_serialization() {
        let val = IcebergErrorResponse {
            error: ErrorModel {
                message: "The server does not support this operation".to_string(),
                r#type: "UnsupportedOperationException".to_string(),
                code: 406,
                source: None,
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
        let resp: ApiError = serde_json::from_slice(&buf).unwrap();
        assert_eq!(resp.message, "The server does not support this operation");
        assert_eq!(resp.r#type, "UnsupportedOperationException");
        assert_eq!(resp.code, 406);

        let json = serde_json::json!({
            "message": "The server does not support this operation",
            "type": "UnsupportedOperationException",
            "code": 406
        });

        let resp: ApiError = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(resp).unwrap(), json);
    }
}
