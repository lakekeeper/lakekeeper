use lakekeeper_io::ReadError as IoReadError;
pub use lakekeeper_io::{
    DeleteBatchError, DeleteError, IOError, InternalError, InvalidLocationError, RetryableError,
    RetryableErrorKind,
};

#[derive(thiserror::Error, Debug)]
#[error("Failed to parse content of file `{path}` as {target_type} - {reason}")]
pub struct ParseError {
    pub target_type: String,
    pub reason: String,
    pub path: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error("{0}")]
    InvalidLocationError(#[from] InvalidLocationError),
    #[error("{0}")]
    IOError(#[from] IOError),
    #[error("{0}")]
    ParseError(#[from] ParseError),
    #[error("{0}")]
    InternalError(#[from] InternalError),
}

impl From<IoReadError> for ReadError {
    fn from(error: IoReadError) -> Self {
        match error {
            IoReadError::InvalidLocation(e) => e.into(),
            IoReadError::IOError(e) => e.into(),
        }
    }
}

impl RetryableError for ReadError {
    fn retryable_error_kind(&self) -> RetryableErrorKind {
        match self {
            ReadError::InvalidLocationError(e) => e.retryable_error_kind(),
            ReadError::IOError(e) => e.kind().retryable_error_kind(),
            ReadError::ParseError(_) => RetryableErrorKind::Permanent,
            ReadError::InternalError(e) => e.retryable,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Task was cancelled before completion.")]
pub struct CancelledError {}
