use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::{
    DeleteError, IOError, InitializeClientError, InternalError, InvalidLocationError,
    RetryableErrorKind,
    adls::{InvalidADLSAccountName, InvalidADLSFilesystemName, InvalidADLSHost},
    gcs::InvalidGCSBucketName,
};

use crate::server::{compression_codec::UnsupportedCompressionCodec, io::IOErrorExt};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("{0}")]
    IoOperationFailed(#[from] Box<IOError>),
    #[error(transparent)]
    Credentials(#[from] Box<CredentialsError>),
    #[error(transparent)]
    InvalidProfile(#[from] Box<InvalidProfileError>),
    #[error(transparent)]
    UnsupportedCompressionCodec(#[from] Box<UnsupportedCompressionCodec>),
    #[error("{0}")]
    InvalidLocation(#[from] Box<InvalidLocationError>),
    #[error(transparent)]
    Internal(#[from] Box<InternalError>),
    #[error("Failed to deserialize table metadata: {0}")]
    Deserialization(#[source] Box<serde_json::Error>),
    #[error("Failed to finish decompressing file: {0}")]
    FileDecompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error(transparent)]
    TableConfig(Box<TableConfigError>),
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid Storage Profile entry for `{entity}`: {reason}")]
pub struct InvalidProfileError {
    pub source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    pub reason: String,
    pub entity: String,
}

impl From<InvalidADLSAccountName> for ValidationError {
    fn from(value: InvalidADLSAccountName) -> Self {
        ValidationError::InvalidProfile(Box::new(InvalidProfileError {
            source: None,
            reason: value.to_string(),
            entity: "account_name".to_string(),
        }))
    }
}

impl From<InvalidADLSFilesystemName> for ValidationError {
    fn from(value: InvalidADLSFilesystemName) -> Self {
        ValidationError::InvalidProfile(Box::new(InvalidProfileError {
            source: None,
            reason: value.to_string(),
            entity: "filesystem".to_string(),
        }))
    }
}

impl From<InvalidADLSHost> for ValidationError {
    fn from(value: InvalidADLSHost) -> Self {
        ValidationError::InvalidProfile(Box::new(InvalidProfileError {
            source: None,
            reason: value.to_string(),
            entity: "host".to_string(),
        }))
    }
}

impl From<InvalidGCSBucketName> for ValidationError {
    fn from(value: InvalidGCSBucketName) -> Self {
        ValidationError::InvalidProfile(Box::new(InvalidProfileError {
            source: None,
            reason: value.to_string(),
            entity: "bucket_name".to_string(),
        }))
    }
}

impl From<InvalidLocationError> for ValidationError {
    fn from(value: InvalidLocationError) -> Self {
        ValidationError::InvalidLocation(Box::new(value))
    }
}

impl From<InvalidProfileError> for ValidationError {
    fn from(value: InvalidProfileError) -> Self {
        ValidationError::InvalidProfile(Box::new(value))
    }
}

impl From<IOErrorExt> for ValidationError {
    fn from(value: IOErrorExt) -> Self {
        match value {
            IOErrorExt::InvalidLocation(e) => Box::new(e).into(),
            IOErrorExt::IOError(e) => Box::new(e).into(),
            IOErrorExt::Serialization(e) => ValidationError::Internal(Box::new(
                InternalError::new(
                    format!("Serialization failed: {e}"),
                    RetryableErrorKind::Permanent,
                )
                .with_source(e),
            )),
            IOErrorExt::Deserialization(e) => ValidationError::Deserialization(Box::new(e)),
            IOErrorExt::FileCompression(e) => ValidationError::Internal(Box::new(
                InternalError::new(
                    format!("File compression failed: {e}"),
                    RetryableErrorKind::Permanent,
                )
                .with_source(e),
            )),
            IOErrorExt::FileDecompression(e) => ValidationError::FileDecompression(e),
        }
    }
}

impl From<InitializeClientError> for ValidationError {
    fn from(value: InitializeClientError) -> Self {
        ValidationError::Credentials(Box::new(value.into()))
    }
}

impl From<CredentialsError> for ValidationError {
    fn from(value: CredentialsError) -> Self {
        ValidationError::Credentials(Box::new(value))
    }
}

impl From<TableConfigError> for ValidationError {
    fn from(value: TableConfigError) -> Self {
        match value {
            TableConfigError::Credentials(e) => Box::new(e).into(),
            TableConfigError::ExplainedMisconfiguration { .. } => {
                ValidationError::TableConfig(Box::new(value))
            }
            TableConfigError::FailedDependency(_) | TableConfigError::Misconfiguration(_) => {
                let reason = value.to_string();
                ValidationError::InvalidProfile(Box::new(InvalidProfileError {
                    source: Some(Box::new(value)),
                    reason,
                    entity: "TableConfig".to_string(),
                }))
            }
            TableConfigError::Internal(_, _) => ValidationError::Internal(Box::new(
                InternalError::new(value.to_string(), RetryableErrorKind::Permanent)
                    .with_source(value),
            )),
        }
    }
}

impl From<DeleteError> for ValidationError {
    fn from(value: DeleteError) -> Self {
        match value {
            DeleteError::IOError(e) => Box::new(e).into(),
            DeleteError::InvalidLocation(e) => Box::new(e).into(),
        }
    }
}

impl From<ValidationError> for ErrorModel {
    fn from(value: ValidationError) -> Self {
        let msg = value.to_string();
        match value {
            ValidationError::IoOperationFailed(e) => ErrorModel::from_io_error_with_code(
                *e,
                http::StatusCode::BAD_REQUEST,
                "IO Operation Failed during validation.",
            ),
            ValidationError::Credentials(e) => {
                if matches!(e.as_ref(), CredentialsError::UnexpectedStorageType(_)) {
                    ErrorModel::bad_request(e.to_string(), "UnexpectedStorageProfileType", Some(e))
                } else {
                    (*e).into()
                }
            }
            ValidationError::InvalidProfile(profile_err) => {
                let InvalidProfileError {
                    source,
                    reason,
                    entity,
                } = *profile_err;
                ErrorModel::bad_request(reason, format!("Invalid{entity}"), source)
            }
            ValidationError::UnsupportedCompressionCodec(e) => ErrorModel::bad_request(
                e.to_string(),
                "UnsupportedCompressionCodec",
                Some(Box::new(e)),
            ),
            ValidationError::InvalidLocation(e) => {
                ErrorModel::bad_request(e.to_string(), "InvalidLocation", None)
            }
            e @ ValidationError::Internal { .. } => {
                ErrorModel::internal(e.to_string(), "ValidationFailedError", Some(Box::new(e)))
            }
            ValidationError::Deserialization(e) => {
                ErrorModel::bad_request(msg, "DeserializationError", Some(Box::new(e)))
            }
            ValidationError::FileDecompression(e) => ErrorModel::bad_request(
                format!("Failed to decompress file: {e}"),
                "FileDecompressionError",
                Some(e),
            ),
            ValidationError::TableConfig(e) => (*e).into(),
        }
    }
}

impl From<ValidationError> for IcebergErrorResponse {
    fn from(value: ValidationError) -> Self {
        ErrorModel::from(value).into()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TableConfigError {
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
    #[error("Failed Dependency: '{0}', please check your storage provider configuration.")]
    FailedDependency(String),
    #[error("Misconfiguration: {0}")]
    Misconfiguration(String),
    /// A misconfiguration reported by the storage, explained as what to change.
    #[error("Misconfiguration: {message}")]
    ExplainedMisconfiguration {
        /// What is wrong and what to change.
        message: String,
        /// The error's `type`, naming the specific misconfiguration.
        error_type: &'static str,
        /// What the storage answered, for the error's `stack`.
        details: Vec<String>,
        #[source]
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    },
    #[error("Internal error: {0}")]
    Internal(
        String,
        #[source] Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    ),
}

impl From<TableConfigError> for ErrorModel {
    fn from(value: TableConfigError) -> Self {
        match value {
            TableConfigError::Credentials(e) => e.into(),
            e @ TableConfigError::FailedDependency(_) => {
                ErrorModel::failed_dependency(e.to_string(), "FailedDependency", Some(Box::new(e)))
            }
            e @ TableConfigError::Misconfiguration(_) => {
                ErrorModel::bad_request(e.to_string(), "Misconfiguration", Some(Box::new(e)))
            }
            TableConfigError::ExplainedMisconfiguration {
                message,
                error_type,
                details,
                source,
            } => {
                ErrorModel::bad_request(format!("Misconfiguration: {message}"), error_type, source)
                    .append_details(details)
            }
            e @ TableConfigError::Internal(_, _) => {
                ErrorModel::internal(e.to_string(), "StsError", Some(Box::new(e)))
            }
        }
    }
}

impl From<TableConfigError> for IcebergErrorResponse {
    fn from(value: TableConfigError) -> Self {
        ErrorModel::from(value).into()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UpdateError {
    #[error("Field `{0}` cannot be updated to prevent loss of data.")]
    ImmutableField(String),
    #[error("Incompatible profiles: {0} cannot be updated with {0}")]
    IncompatibleProfiles(String, String),
}

impl From<UpdateError> for ErrorModel {
    fn from(value: UpdateError) -> Self {
        ErrorModel::bad_request(value.to_string(), "UpdateError", None)
    }
}

impl From<UpdateError> for IcebergErrorResponse {
    fn from(value: UpdateError) -> Self {
        ErrorModel::from(value).into()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CredentialsError {
    #[error("Credential is missing, a credential is required for storage `{0}`")]
    MissingCredential(String),
    #[error("Credential not supported: {0}")]
    UnsupportedCredential(String),
    #[error("Failed to create short-term credential: {reason}")]
    ShortTermCredential {
        reason: String,
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    },
    #[error("Failed to create short-term credential: STS request failed: {}", rejection.summary())]
    StsRejected {
        rejection: StsRejection,
        source: Box<dyn std::error::Error + 'static + Send + Sync>,
    },
    #[error("{0}")]
    UnexpectedStorageType(#[from] UnexpectedStorageType),
    #[error("Failed to serialize credential.")]
    SerializationError(#[from] serde_json::Error),
    #[error("Credentials misconfigured: {0}")]
    Misconfiguration(String),
    #[error("{0}")]
    InitializeClientError(#[from] InitializeClientError),
}

impl From<CredentialsError> for ErrorModel {
    fn from(value: CredentialsError) -> Self {
        let boxed = Box::new(value);
        let message = boxed.to_string();
        match boxed.as_ref() {
            CredentialsError::ShortTermCredential { .. } => {
                ErrorModel::precondition_failed(message, "ShortTermCredentialError", Some(boxed))
            }
            CredentialsError::StsRejected { rejection, .. } => {
                let request_id = rejection.request_id_line();
                ErrorModel::precondition_failed(message, "ShortTermCredentialError", Some(boxed))
                    .append_details(request_id)
            }
            CredentialsError::UnexpectedStorageType(_) => {
                ErrorModel::internal(message, "UnexpectedStorageProfileType", Some(boxed))
            }
            CredentialsError::MissingCredential(_) => {
                ErrorModel::bad_request(message, "MissingCredentialError", Some(boxed))
            }
            CredentialsError::UnsupportedCredential(_) => {
                ErrorModel::not_implemented(message, "UnsupportedCredentialError", Some(boxed))
            }
            CredentialsError::SerializationError(_) => {
                ErrorModel::internal(message, "SerializationError", Some(boxed))
            }
            CredentialsError::Misconfiguration(_) => {
                ErrorModel::bad_request(message, "Misconfiguration", Some(boxed))
            }
            CredentialsError::InitializeClientError(_) => {
                ErrorModel::precondition_failed(message, "InitializeClientError", Some(boxed))
            }
        }
    }
}

impl From<CredentialsError> for IcebergErrorResponse {
    fn from(value: CredentialsError) -> Self {
        ErrorModel::from(value).into()
    }
}

/// What a failed STS request returned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StsRejection {
    http_status: Option<u16>,
    /// The error code, e.g. `AccessDenied`.
    code: Option<String>,
    /// The endpoint's own error message, or a fixed description of the failure.
    message: Option<String>,
    request_id: Option<String>,
}

impl StsRejection {
    /// Longest `code`, `message` or `request_id` kept, in characters.
    pub const MAX_FIELD_CHARS: usize = 256;

    /// The fields come from the endpoint, so each is cut to [`Self::MAX_FIELD_CHARS`] and
    /// its control characters are replaced by spaces.
    #[must_use]
    pub fn new(
        http_status: Option<u16>,
        code: Option<String>,
        message: Option<String>,
        request_id: Option<String>,
    ) -> Self {
        let clean = |value: Option<String>| {
            value.map(|v| {
                v.chars()
                    .take(Self::MAX_FIELD_CHARS)
                    .map(|c| if c.is_control() { ' ' } else { c })
                    .collect()
            })
        };
        Self {
            http_status,
            code: clean(code),
            message: clean(message),
            request_id: clean(request_id),
        }
    }

    #[must_use]
    pub fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    /// The error code, e.g. `AccessDenied`.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// The endpoint's own error message, or a fixed description of the failure.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// The answer on one line, e.g. `AccessDenied (HTTP 403): User ... is not authorized`.
    #[must_use]
    pub fn summary(&self) -> String {
        let status = self.http_status.map(|s| format!("HTTP {s}"));
        let head = match (&self.code, status) {
            (Some(code), Some(status)) => format!("{code} ({status})"),
            (Some(code), None) => code.clone(),
            (None, Some(status)) => status,
            (None, None) => {
                return self
                    .message
                    .clone()
                    .unwrap_or_else(|| "no response".to_string());
            }
        };
        match &self.message {
            Some(message) => format!("{head}: {message}"),
            None => head,
        }
    }

    /// Lines for an error's `stack`: the answer and the request id.
    #[must_use]
    pub fn details(&self) -> Vec<String> {
        let mut lines = vec![format!("STS answered {}", self.summary())];
        lines.extend(self.request_id_line());
        lines
    }

    fn request_id_line(&self) -> Option<String> {
        self.request_id
            .as_ref()
            .map(|id| format!("STS request ID: {id}"))
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Expected storage of type {to}, is: {is}")]
pub struct UnexpectedStorageType {
    pub is: &'static str,
    pub to: &'static str,
}

impl From<UnexpectedStorageType> for IcebergErrorResponse {
    fn from(value: UnexpectedStorageType) -> Self {
        ErrorModel::internal(
            format!(
                "Failed to convert '{is}' to '{to}'",
                to = value.to,
                is = value.is
            ),
            "ConversionError",
            Some(Box::new(value)),
        )
        .into()
    }
}
