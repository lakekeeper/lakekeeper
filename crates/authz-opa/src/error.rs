use lakekeeper::service::authz::{
    AuthorizationBackendUnavailable, AuthzBackendErrorOrBadRequest, IsAllowedActionError,
};

pub type OpaResult<T> = Result<T, OpaError>;

/// Everything that can go wrong talking to OPA.
///
/// The variants collapse into [`AuthorizationBackendUnavailable`] when converted
/// into the Lakekeeper-facing error hierarchy — we treat any OPA problem as a
/// backend-unavailable condition rather than a permission denial.
#[derive(Debug, thiserror::Error)]
pub enum OpaError {
    #[error("OPA HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("OPA request body could not be serialized: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("OPA response body could not be deserialized: {0}")]
    Deserialize(#[source] serde_json::Error),
    #[error("OPA returned a non-boolean `result` for {policy_path}: {value}")]
    UnexpectedResult {
        policy_path: String,
        value: serde_json::Value,
    },
    #[error("OPA response missing `result` for {policy_path}")]
    MissingResult { policy_path: String },
    #[error("OPA URL construction failed: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("invalid OPA bearer_token: {0}")]
    InvalidBearerToken(#[from] http::header::InvalidHeaderValue),
}

impl From<OpaError> for AuthorizationBackendUnavailable {
    fn from(err: OpaError) -> Self {
        AuthorizationBackendUnavailable::new(err)
    }
}

impl From<OpaError> for AuthzBackendErrorOrBadRequest {
    fn from(err: OpaError) -> Self {
        AuthorizationBackendUnavailable::from(err).into()
    }
}

impl From<OpaError> for IsAllowedActionError {
    fn from(err: OpaError) -> Self {
        IsAllowedActionError::AuthorizationBackendUnavailable(err.into())
    }
}
