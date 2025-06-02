use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use openfga_client::error::Error as OpenFGAClientError;

use crate::service::authz::implementations::FgaType;

pub(crate) type OpenFGAResult<T> = Result<T, OpenFGAError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenFGAError {
    #[error("OpenFGA client error: {0}")]
    ClientError(#[source] OpenFGAClientError),
    #[error("Active authorization model with version {0} not found in OpenFGA. Make sure to run migration first!")]
    ActiveAuthModelNotFound(String),
    #[error("OpenFGA Store not found: {0}. Make sure to run migration first!")]
    StoreNotFound(String),
    #[error("Unexpected entity for type {type:?}: {value}. {reason}")]
    UnexpectedEntity {
        r#type: Vec<FgaType>,
        value: String,
        reason: String,
    },
    #[error("Unknown OpenFGA type: {0}")]
    UnknownType(String),
    #[error("Invalid OpenFGA entity string: `{0}`")]
    InvalidEntity(String),
    #[error("Project ID could not be inferred from request. Please the x-project-id header.")]
    NoProjectId,
    #[error("Authentication required")]
    AuthenticationRequired,
    #[error("Unauthorized for action `{relation}` on `{object}` for `{user}`")]
    Unauthorized {
        user: String,
        relation: String,
        object: String,
    },
    #[error("Cannot assign {0} to itself")]
    SelfAssignment(String),
}

impl OpenFGAError {
    pub(crate) fn unexpected_entity(r#type: Vec<FgaType>, value: String, reason: String) -> Self {
        OpenFGAError::UnexpectedEntity {
            r#type,
            value,
            reason,
        }
    }
}

impl From<OpenFGAClientError> for OpenFGAError {
    fn from(err: OpenFGAClientError) -> Self {
        OpenFGAError::ClientError(err)
    }
}

impl From<OpenFGAError> for ErrorModel {
    fn from(err: OpenFGAError) -> Self {
        let err_msg = err.to_string();
        match err {
            e @ OpenFGAError::NoProjectId => {
                ErrorModel::bad_request(err_msg, "NoProjectId", Some(Box::new(e)))
            }
            e @ OpenFGAError::AuthenticationRequired => {
                ErrorModel::unauthorized(err_msg, "AuthenticationRequired", Some(Box::new(e)))
            }
            e @ OpenFGAError::Unauthorized { .. } => {
                ErrorModel::unauthorized(err_msg, "Unauthorized", Some(Box::new(e)))
            }
            e @ OpenFGAError::SelfAssignment { .. } => {
                ErrorModel::bad_request(err_msg, "SelfAssignment", Some(Box::new(e)))
            }
            OpenFGAError::ClientError(client_error) => {
                let tonic_msg = match &client_error {
                    OpenFGAClientError::RequestFailed(status) => Some(status.message().to_string()),
                    _ => None,
                };
                if let Some(tonic_msg) = tonic_msg {
                    if tonic_msg.starts_with("cannot write a tuple which already exists") {
                        ErrorModel::conflict(
                            tonic_msg,
                            "TupleAlreadyExistsError",
                            Some(Box::new(client_error)),
                        )
                    } else if tonic_msg.starts_with("cannot delete a tuple which does not exist") {
                        ErrorModel::not_found(
                            tonic_msg,
                            "TupleNotFoundError",
                            Some(Box::new(client_error)),
                        )
                    } else {
                        ErrorModel::new(
                            err_msg,
                            "AuthorizationError",
                            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                            Some(Box::new(client_error)),
                        )
                    }
                } else {
                    ErrorModel::new(
                        err_msg,
                        "AuthorizationError",
                        StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                        Some(Box::new(client_error)),
                    )
                }
            }
            e @ OpenFGAError::UnexpectedEntity { .. } => {
                ErrorModel::internal(err_msg, "UnexpectedEntity", Some(Box::new(e)))
            }
            _ => ErrorModel::new(
                err.to_string(),
                "AuthorizationError",
                StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                Some(Box::new(err)),
            ),
        }
    }
}

impl From<OpenFGAError> for IcebergErrorResponse {
    fn from(err: OpenFGAError) -> Self {
        let err_model = ErrorModel::from(err);
        err_model.into()
    }
}
