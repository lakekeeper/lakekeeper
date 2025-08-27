// use lakekeeper::{api::ErrorModel, service::storage::error::InvalidLocationError};

// use crate::OperationType;

#[derive(Debug, thiserror::Error)]
#[error("Invalid location `{location}`. Context: {context:?}. Source: {reason}")]
pub struct InvalidLocationError {
    pub reason: String,
    pub location: String,
    pub context: Vec<String>,
}

impl InvalidLocationError {
    pub fn new(location: String, reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            location,
            context: Vec::new(),
        }
    }

    /// Add context to the error
    #[must_use]
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context.push(context.into());
        self
    }
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

impl RetryableError for InvalidLocationError {
    fn retryable_error_kind(&self) -> RetryableErrorKind {
        RetryableErrorKind::Permanent
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("{0}")]
    InvalidLocation(InvalidLocationError),
    #[error("{0}")]
    IOError(IOError),
}

impl From<InvalidLocationError> for WriteError {
    fn from(err: InvalidLocationError) -> Self {
        WriteError::InvalidLocation(err.with_context("Write operation failed"))
    }
}

impl From<IOError> for WriteError {
    fn from(err: IOError) -> Self {
        WriteError::IOError(err.with_context("Write operation failed"))
    }
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
    InvalidLocation(InvalidLocationError),
    #[error("{0}")]
    IOError(IOError),
}

impl From<InvalidLocationError> for DeleteError {
    fn from(err: InvalidLocationError) -> Self {
        DeleteError::InvalidLocation(err.with_context("Delete operation failed"))
    }
}

impl From<IOError> for DeleteError {
    fn from(err: IOError) -> Self {
        DeleteError::IOError(err.with_context("Delete operation failed"))
    }
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
    InvalidLocation(InvalidLocationError),
    #[error("{0}")]
    IOError(IOError),
}

impl From<InvalidLocationError> for ReadError {
    fn from(err: InvalidLocationError) -> Self {
        ReadError::InvalidLocation(err.with_context("Read operation failed"))
    }
}

impl From<IOError> for ReadError {
    fn from(err: IOError) -> Self {
        ReadError::IOError(err.with_context("Read operation failed"))
    }
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
pub enum DeleteBatchError {
    #[error("{0}")]
    InvalidLocation(InvalidLocationError),
    #[error("{0}")]
    IOError(IOError),
}

impl From<InvalidLocationError> for DeleteBatchError {
    fn from(err: InvalidLocationError) -> Self {
        DeleteBatchError::InvalidLocation(err.with_context("Batch delete operation failed"))
    }
}

impl From<IOError> for DeleteBatchError {
    fn from(err: IOError) -> Self {
        DeleteBatchError::IOError(err.with_context("Batch delete operation failed"))
    }
}

impl From<DeleteError> for DeleteBatchError {
    fn from(err: DeleteError) -> Self {
        match err {
            DeleteError::InvalidLocation(e) => DeleteBatchError::InvalidLocation(e),
            DeleteError::IOError(e) => DeleteBatchError::IOError(e),
        }
    }
}

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct BatchDeleteError {
//     /// The path that was failed for deletion, if available
//     pub path: String,
//     /// Error code from the storage service, if available
//     pub error_code: Option<String>,
//     /// Error message from the storage service
//     pub error_message: String,
// }

// impl BatchDeleteError {
//     #[must_use]
//     pub fn new(path: String, error_code: Option<String>, error_message: String) -> Self {
//         Self {
//             path,
//             error_code,
//             error_message,
//         }
//     }
// }

// impl std::fmt::Display for BatchDeleteError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "Failed to delete path '{}': ", self.path)?;

//         match (&self.error_code, &self.error_message) {
//             (Some(code), message) => write!(f, "{code} - {message}"),
//             (None, message) => write!(f, "{message}"),
//         }
//     }
// }

// impl std::error::Error for BatchDeleteError {}

#[derive(thiserror::Error, Debug)]
#[error("IO operation failed ({kind}): {message}{}. Context: {context:?} Source: {}", location.as_ref().map_or(String::new(), |l| format!(" at `{l}`")), source.as_ref().map_or(String::new(), |s| format!("{s:#}")))]
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

#[derive(thiserror::Error, Debug)]
#[error("Internal Error (retryable: {retryable}). Context: {context:?}. Reason: {reason}")]
pub struct InternalError {
    pub reason: String,
    pub retryable: RetryableErrorKind,
    pub context: Vec<String>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl InternalError {
    #[must_use]
    pub fn new(reason: String, retryable: RetryableErrorKind) -> Self {
        Self {
            reason,
            retryable,
            context: Vec::new(),
            source: None,
        }
    }

    #[must_use]
    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        self.source = Some(source.into());
        self
    }

    #[must_use]
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context.push(context.into());
        self
    }
}
