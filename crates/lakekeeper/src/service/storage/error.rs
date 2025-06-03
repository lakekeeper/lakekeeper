use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    catalog::{compression_codec::UnsupportedCompressionCodec, io::IoError},
    service::storage::{StorageProfile, StorageType},
};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("{0}")]
    IoOperationFailed(#[source] IoError, Box<StorageProfile>),
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
    #[error("{reason}")]
    InvalidProfile {
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
        reason: String,
        entity: String,
    },
    #[error(transparent)]
    FileIoError(#[from] FileIoError),
    #[error(transparent)]
    UnsupportedCompressionCodec(#[from] UnsupportedCompressionCodec),
    #[error("{reason}")]
    InvalidLocation {
        reason: String,
        location: String,
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
        storage_type: StorageType,
    },
    #[error("{reason}")]
    Internal {
        reason: String,
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    },
}

impl From<TableConfigError> for ValidationError {
    fn from(value: TableConfigError) -> Self {
        match value {
            TableConfigError::Credentials(e) => e.into(),
            TableConfigError::FailedDependency(_) | TableConfigError::Misconfiguration(_) => {
                let reason = value.to_string();
                ValidationError::InvalidProfile {
                    source: Some(Box::new(value)),
                    reason,
                    entity: "TableConfig".to_string(),
                }
            }
            TableConfigError::Internal(_, _) => ValidationError::Internal {
                reason: value.to_string(),
                source: Some(Box::new(value)),
            },
        }
    }
}

impl From<ValidationError> for IcebergErrorResponse {
    fn from(value: ValidationError) -> Self {
        match value {
            ValidationError::IoOperationFailed(e, _) => e.into(),
            ValidationError::Credentials(e) => {
                if let CredentialsError::Mismatch(_) = e {
                    ErrorModel::bad_request(
                        e.to_string(),
                        "CredentialMismatchError",
                        Some(Box::new(e)),
                    )
                    .into()
                } else {
                    e.into()
                }
            }
            ValidationError::InvalidProfile {
                source,
                reason,
                entity,
            } => ErrorModel::bad_request(reason, format!("Invalid{entity}"), source).into(),
            ValidationError::FileIoError(e) => e.into(),
            ValidationError::UnsupportedCompressionCodec(e) => ErrorModel::bad_request(
                e.to_string(),
                "UnsupportedCompressionCodec",
                Some(Box::new(e)),
            )
            .into(),
            ValidationError::InvalidLocation {
                reason,
                location,
                source,
                storage_type,
            } => ErrorModel::bad_request(
                reason,
                format!("Invalid{}Location", storage_type.to_string().to_uppercase()),
                source,
            )
            .append_detail(location)
            .into(),
            e @ ValidationError::Internal { .. } => {
                ErrorModel::internal(e.to_string(), "ValidationFailedError", Some(Box::new(e)))
                    .into()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FileIoError {
    #[error("{0} not supported")]
    UnsupportedAction(String),
    #[error(transparent)]
    FileIoCreationFailed(#[from] iceberg::Error),
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
}

impl From<FileIoError> for IcebergErrorResponse {
    fn from(err: FileIoError) -> Self {
        match err {
            FileIoError::UnsupportedAction(ref action) => ErrorModel::not_implemented(
                err.to_string(),
                format!("{action}NotSupported"),
                Some(Box::new(err)),
            )
            .into(),
            FileIoError::FileIoCreationFailed(_) => ErrorModel::precondition_failed(
                "Error creating file io",
                "FileIoCreationFailed",
                Some(Box::new(err)),
            )
            .into(),
            FileIoError::Credentials(cred_e) => cred_e.into(),
        }
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
    #[error("Internal error: {0}")]
    Internal(
        String,
        #[source] Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    ),
}

impl From<TableConfigError> for IcebergErrorResponse {
    fn from(value: TableConfigError) -> Self {
        match value {
            TableConfigError::Credentials(e) => e.into(),
            e @ TableConfigError::FailedDependency(_) => {
                ErrorModel::failed_dependency(e.to_string(), "FailedDependency", Some(Box::new(e)))
                    .into()
            }
            e @ TableConfigError::Misconfiguration(_) => {
                ErrorModel::bad_request(e.to_string(), "Misconfiguration", Some(Box::new(e))).into()
            }
            e @ TableConfigError::Internal(_, _) => {
                ErrorModel::internal(e.to_string(), "StsError", Some(Box::new(e))).into()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UpdateError {
    #[error("Field `{0}` cannot be updated to prevent loss of data.")]
    ImmutableField(String),
    #[error("Incompatible profiles: {0} cannot be updated with {0}")]
    IncompatibleProfiles(String, String),
}

impl From<UpdateError> for IcebergErrorResponse {
    fn from(value: UpdateError) -> Self {
        ErrorModel::bad_request(value.to_string(), "UpdateError", None).into()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CredentialsError {
    #[error("Credential is missing, a credential is required for: {0}")]
    MissingCredential(StorageType),
    #[error("Credential not supported: {0}")]
    UnsupportedCredential(String),
    #[error("Failed to create short-term credential: {reason}")]
    ShortTermCredential {
        reason: String,
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    },
    #[error("Failed to convert credential: {0}")]
    Mismatch(#[from] ConversionError),
    #[error("Failed to serialize credential.")]
    SerializationError(#[from] serde_json::Error),
    #[error("Credentials misconfigured: {0}")]
    Misconfiguration(String),
}

impl From<CredentialsError> for IcebergErrorResponse {
    fn from(value: CredentialsError) -> Self {
        let boxed = Box::new(value);
        let message = boxed.to_string();
        match boxed.as_ref() {
            CredentialsError::ShortTermCredential { .. } => {
                ErrorModel::precondition_failed(message, "ShortTermCredentialError", Some(boxed))
                    .into()
            }
            CredentialsError::Mismatch(_) => {
                ErrorModel::internal(message, "CredentialMismatchError", Some(boxed)).into()
            }
            CredentialsError::MissingCredential(_) => {
                ErrorModel::bad_request(message, "MissingCredentialError", Some(boxed)).into()
            }
            CredentialsError::UnsupportedCredential(_) => {
                ErrorModel::not_implemented(message, "UnsupportedCredentialError", Some(boxed))
                    .into()
            }
            CredentialsError::SerializationError(_) => {
                ErrorModel::internal(message, "SerializationError", Some(boxed)).into()
            }
            CredentialsError::Misconfiguration(_) => {
                ErrorModel::bad_request(message, "Misconfiguration", Some(boxed)).into()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to convert to {to}, is: {is}")]
pub struct ConversionError {
    pub is: StorageType,
    pub to: StorageType,
}

impl From<ConversionError> for IcebergErrorResponse {
    fn from(value: ConversionError) -> Self {
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
