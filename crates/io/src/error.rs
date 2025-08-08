// use lakekeeper::{api::ErrorModel, service::storage::error::InvalidLocationError};

// use crate::OperationType;

#[derive(Debug, thiserror::Error)]
#[error("Invalid location `{location}`: {reason}")]
pub struct InvalidLocationError {
    pub reason: String,
    pub location: String,
}

// #[derive(Debug, thiserror::Error)]
// #[error("Failed to refresh credentials Storage Credentials: {message}")]
// pub(crate) struct CredentialRefreshError {
//     pub message: String,
//     pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
// }

// #[derive(thiserror::Error, Debug)]
// pub enum StorageError {
//     #[error("{0}")]
//     InvalidLocation(#[from] InvalidLocationError),
//     #[error("{0}")]
//     StorageOperationError(#[from] StorageOperationError),
// }

pub trait RetryableError {
    /// Get the retryable error kind for this error.
    fn retryable_error_kind(&self) -> RetryableErrorKind;

    /// Check if the error should be retried.
    fn should_retry(&self) -> bool {
        self.retryable_error_kind().should_retry()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("{0}")]
    InvalidLocation(#[from] InvalidLocationError),
    #[error("Failed to write object: {0}")]
    IOError(#[from] IOError),
}

impl RetryableError for WriteError {
    fn retryable_error_kind(&self) -> RetryableErrorKind {
        match self {
            WriteError::InvalidLocation(_) => RetryableErrorKind::Permanent,
            WriteError::IOError(io_error) => io_error.kind().retryable_error_kind(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeleteError {
    #[error("{0}")]
    InvalidLocation(#[from] InvalidLocationError),
    #[error("Failed to delete object: {0}")]
    IOError(#[from] IOError),
}

impl RetryableError for DeleteError {
    fn retryable_error_kind(&self) -> RetryableErrorKind {
        match self {
            DeleteError::InvalidLocation(_) => RetryableErrorKind::Permanent,
            DeleteError::IOError(io_error) => io_error.kind().retryable_error_kind(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error("{0}")]
    InvalidLocation(#[from] InvalidLocationError),
    #[error("Failed to delete object: {0}")]
    IOError(#[from] IOError),
}

impl RetryableError for ReadError {
    fn retryable_error_kind(&self) -> RetryableErrorKind {
        match self {
            ReadError::InvalidLocation(_) => RetryableErrorKind::Permanent,
            ReadError::IOError(io_error) => io_error.kind().retryable_error_kind(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeleteBatchFatalError {
    #[error("{0}")]
    InvalidLocation(#[from] InvalidLocationError),
    #[error("Failed to batch delete objects: {0}")]
    IOError(#[from] IOError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchDeleteError {
    /// The original path that was requested for deletion, if available
    pub path: Option<String>,
    /// The storage-specific key (e.g., S3 key) that failed, if available
    pub storage_key: Option<String>,
    /// Error code from the storage service, if available
    pub error_code: Option<String>,
    /// Error message from the storage service, if available
    pub error_message: Option<String>,
}

impl BatchDeleteError {
    #[must_use]
    pub fn new(
        path: Option<String>,
        storage_key: Option<String>,
        error_code: Option<String>,
        error_message: Option<String>,
    ) -> Self {
        Self {
            path,
            storage_key,
            error_code,
            error_message,
        }
    }
}

impl std::fmt::Display for BatchDeleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.path, &self.storage_key) {
            (Some(path), _) => write!(f, "Failed to delete path '{path}': ")?,
            (None, Some(key)) => write!(f, "Failed to delete storage key '{key}': ")?,
            (None, None) => write!(f, "Failed to delete unknown item: ")?,
        }

        match (&self.error_code, &self.error_message) {
            (Some(code), Some(message)) => write!(f, "{code} - {message}"),
            (Some(code), None) => write!(f, "{code}"),
            (None, Some(message)) => write!(f, "{message}"),
            (None, None) => write!(f, "Unknown error"),
        }
    }
}

impl std::error::Error for BatchDeleteError {}

#[derive(thiserror::Error, Debug)]
#[error("IO operation failed ({kind}): {message}{}. Source: {}", location.as_ref().map_or(String::new(), |l| format!(" at {l}")), source.as_ref().map_or(String::new(), |s| format!("{s:#}")))]
pub struct IOError {
    kind: ErrorKind,
    message: String,
    location: Option<String>,
    context: Vec<String>,
    source: Option<anyhow::Error>,
}

impl IOError {
    /// New deletion error
    pub fn new(kind: ErrorKind, reason: impl Into<String>, location: String) -> Self {
        Self {
            message: reason.into(),
            location: Some(location),
            source: None,
            kind,
            context: Vec::new(),
        }
    }

    /// New without a location
    pub fn new_without_location(kind: ErrorKind, reason: impl Into<String>) -> Self {
        Self {
            message: reason.into(),
            location: None,
            source: None,
            kind,
            context: Vec::new(),
        }
    }

    /// Add location information to the error
    #[must_use]
    pub fn set_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Add source error to the error
    #[must_use]
    pub fn set_source(mut self, source: impl Into<anyhow::Error>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Add context to the error
    #[must_use]
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context.push(context.into());
        self
    }

    /// Get the location if available
    #[must_use]
    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    /// Get the reason for the error
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.message
    }

    /// Get the kind of the error
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Get the context of the error
    #[must_use]
    pub fn context(&self) -> &[String] {
        &self.context
    }
}

impl RetryableError for IOError {
    fn retryable_error_kind(&self) -> RetryableErrorKind {
        self.kind.retryable_error_kind()
    }
}

/// `ErrorKind` is all kinds of Error of opendal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, strum_macros::Display)]
#[non_exhaustive]
pub enum ErrorKind {
    Unexpected,
    RequestTimeout,
    ServiceUnavailable,
    ConfigInvalid,
    NotFound,
    PermissionDenied,
    RateLimited,
    ConditionNotMatch,
    CredentialsExpired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, strum_macros::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum RetryableErrorKind {
    Temporary,
    Permanent,
}

impl RetryableErrorKind {
    #[must_use]
    pub fn should_retry(&self) -> bool {
        matches!(self, RetryableErrorKind::Temporary)
    }
}

impl ErrorKind {
    #[must_use]
    pub fn retryable_error_kind(&self) -> RetryableErrorKind {
        match self {
            ErrorKind::RateLimited
            | ErrorKind::Unexpected
            | ErrorKind::RequestTimeout
            | ErrorKind::ServiceUnavailable
            | ErrorKind::CredentialsExpired => RetryableErrorKind::Temporary,
            _ => RetryableErrorKind::Permanent,
        }
    }
}
