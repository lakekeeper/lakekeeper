use std::error::Error;

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::{
    adls::{InvalidADLSAccountName, InvalidADLSFilesystemName, InvalidADLSHost},
    gcs::InvalidGCSBucketName,
    DeleteError, IOError, InitializeClientError, InternalError, InvalidLocationError,
    RetryableErrorKind,
};

use crate::catalog::{compression_codec::UnsupportedCompressionCodec, io::IOErrorExt};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("{0}")]
    IoOperationFailed(#[from] Box<IOError>),
    #[error("Validation of STS credentials with Iceberg FileIO failed: {0}")]
    IcebergFileIoError(#[from] Box<IcebergFileIoError>),
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

impl From<IcebergFileIoError> for ValidationError {
    fn from(value: IcebergFileIoError) -> Self {
        ValidationError::IcebergFileIoError(Box::new(value))
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
            ValidationError::IoOperationFailed(e) => {
                tracing::debug!(
                    "Validation failed with IO error: {e}. Source: {:?}",
                    e.source()
                );
                ErrorModel::bad_request(
                    format!("IO Operation failed during Validation: {e}"),
                    "IoOperationFailed",
                    None,
                )
                .append_details(e.context().iter().map(ToString::to_string))
            }
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
            ValidationError::IcebergFileIoError(e) => ErrorModel::from(*e)
                .append_detail("Validating Vended Credentials Access with Iceberg FileIO failed."),
        }
    }
}

impl From<ValidationError> for IcebergErrorResponse {
    fn from(value: ValidationError) -> Self {
        ErrorModel::from(value).into()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum IcebergFileIoError {
    #[error("Action not supported: {0}")]
    UnsupportedAction(String),
    #[error(transparent)]
    IcebergError(#[from] iceberg::Error),
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
}

impl From<IcebergFileIoError> for ErrorModel {
    fn from(err: IcebergFileIoError) -> Self {
        match err {
            IcebergFileIoError::UnsupportedAction(ref action) => ErrorModel::not_implemented(
                err.to_string(),
                format!("{action}NotSupported"),
                Some(Box::new(err)),
            ),
            IcebergFileIoError::IcebergError(e) => ErrorModel::precondition_failed(
                format!("Iceberg FileIO returned an error: {e}"),
                "IcebergFileIOError",
                Some(Box::new(e)),
            ),
            IcebergFileIoError::Credentials(cred_e) => cred_e.into(),
        }
    }
}

// #[derive(thiserror::Error, Debug)]
// pub enum IOCreationError {
//     #[error(transparent)]
//     Credentials(#[from] CredentialsError),
// }

// impl From<IOCreationError> for ErrorModel {
//     fn from(value: IOCreationError) -> Self {
//         match value {
//             IOCreationError::Credentials(e) => e.into(),
//         }
//     }
// }

// impl From<IOCreationError> for IcebergErrorResponse {
//     fn from(value: IOCreationError) -> Self {
//         ErrorModel::from(value).into()
//     }
// }

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
    #[error("Credential is missing, a credential is required for storage `{0}`")]
    MissingCredential(String),
    #[error("Credential not supported: {0}")]
    UnsupportedCredential(String),
    #[error("Failed to create short-term credential: {reason}")]
    ShortTermCredential {
        reason: String,
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
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
