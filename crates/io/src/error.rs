// use lakekeeper::{api::ErrorModel, service::storage::error::InvalidLocationError};

// use crate::OperationType;

#[derive(Debug, thiserror::Error)]
#[error("Invalid location `{location}`: {reason}")]
pub struct InvalidLocationError {
    pub reason: String,
    pub location: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Could not initialize storage client: {reason}")]
pub struct InitializeClientError {
    pub source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    pub reason: String,
}

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
    #[error("Invalid Location during write - {0}")]
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
    #[error("Invalid Location during delete - {0}")]
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
    #[error("Invalid Location during read - {0}")]
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
    #[error("Invalid Location during batch deletion - {0}")]
    InvalidLocation(#[from] InvalidLocationError),
    #[error("Failed to batch delete objects: {0}")]
    IOError(#[from] IOError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchDeleteError {
    /// The path that was failed for deletion, if available
    pub path: String,
    /// Error code from the storage service, if available
    pub error_code: Option<String>,
    /// Error message from the storage service
    pub error_message: String,
}

impl BatchDeleteError {
    #[must_use]
    pub fn new(path: String, error_code: Option<String>, error_message: String) -> Self {
        Self {
            path,
            error_code,
            error_message,
        }
    }
}

impl std::fmt::Display for BatchDeleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to delete path '{}': ", self.path)?;

        match (&self.error_code, &self.error_message) {
            (Some(code), message) => write!(f, "{code} - {message}"),
            (None, message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for BatchDeleteError {}

#[derive(thiserror::Error, Debug)]
#[error("IO operation failed ({kind}): {message}{}. Source: {}", location.as_ref().map_or(String::new(), |l| format!(" at `{l}`")), source.as_ref().map_or(String::new(), |s| format!("{s:#}")))]
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
