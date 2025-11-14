use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

#[derive(Debug, thiserror::Error)]
pub(crate) enum SignError {
    #[error("Expected location {expected_location} but request signing for {actual_location}.")]
    RequestUriMismatch {
        request_uri: String,
        expected_location: String,
        actual_location: String,
    },
}

impl From<SignError> for IcebergErrorResponse {
    fn from(value: SignError) -> Self {
        let message = value.to_string();
        match value {
            SignError::RequestUriMismatch {
                request_uri,
                expected_location: _,
                actual_location: _,
            } => ErrorModel::bad_request(message, "RequestUriMismatch", None)
                .append_detail(format!("Request URI: {request_uri}"))
                .into(),
        }
    }
}
