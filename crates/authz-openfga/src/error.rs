use lakekeeper::{
    api::ErrorModel,
    service::{
        authz::{AuthorizationBackendUnavailable, IsAllowedActionError},
        events::{AuthorizationFailureReason, AuthorizationFailureSource},
    },
};
use openfga_client::{
    client::{CheckError, check_error::Code},
    error::Error as OpenFGAClientError,
};

use crate::FgaType;

pub type OpenFGAResult<T> = Result<T, OpenFGAError>;

#[derive(Debug, thiserror::Error)]
pub enum OpenFGABackendUnavailable {
    #[error("OpenFGA client error: {0}")]
    InternalClientError(#[from] Box<OpenFGAClientError>),
    #[error(transparent)]
    UnexpectedCorrelationId(#[from] UnexpectedCorrelationId),
    #[error(transparent)]
    BatchCheckError(#[from] BatchCheckError),
    #[error(transparent)]
    MissingItemInBatchCheck(#[from] MissingItemInBatchCheck),
}
impl From<OpenFGABackendUnavailable> for IsAllowedActionError {
    fn from(err: OpenFGABackendUnavailable) -> Self {
        IsAllowedActionError::AuthorizationBackendUnavailable(
            AuthorizationBackendUnavailable::from(err),
        )
    }
}
impl From<OpenFGAClientError> for OpenFGABackendUnavailable {
    fn from(err: OpenFGAClientError) -> Self {
        OpenFGABackendUnavailable::InternalClientError(Box::new(err))
    }
}

impl From<OpenFGABackendUnavailable> for OpenFGAError {
    fn from(err: OpenFGABackendUnavailable) -> Self {
        match err {
            OpenFGABackendUnavailable::InternalClientError(e) => (*e).into(),
            OpenFGABackendUnavailable::UnexpectedCorrelationId(e) => e.into(),
            OpenFGABackendUnavailable::BatchCheckError(e) => e.into(),
            OpenFGABackendUnavailable::MissingItemInBatchCheck(e) => e.into(),
        }
    }
}

impl From<OpenFGABackendUnavailable> for AuthorizationBackendUnavailable {
    fn from(err: OpenFGABackendUnavailable) -> Self {
        match err {
            OpenFGABackendUnavailable::InternalClientError(e) => {
                AuthorizationBackendUnavailable::new(e).append_detail("OpenFGA client error")
            }
            OpenFGABackendUnavailable::UnexpectedCorrelationId(e) => {
                AuthorizationBackendUnavailable::new(e)
            }
            OpenFGABackendUnavailable::BatchCheckError(e) => {
                AuthorizationBackendUnavailable::new(e)
            }
            OpenFGABackendUnavailable::MissingItemInBatchCheck(e) => {
                AuthorizationBackendUnavailable::new(e)
            }
        }
    }
}
impl AuthorizationFailureSource for OpenFGABackendUnavailable {
    fn into_error_model(self) -> ErrorModel {
        AuthorizationBackendUnavailable::from(self).into_error_model()
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InternalAuthorizationError
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OpenFGAError {
    #[error("OpenFGA client error: {0}")]
    InternalClientError(Box<OpenFGAClientError>),
    #[error(transparent)]
    UnexpectedCorrelationId(#[from] UnexpectedCorrelationId),
    #[error(transparent)]
    BatchCheckError(#[from] BatchCheckError),
    #[error(transparent)]
    MissingItemInBatchCheck(#[from] MissingItemInBatchCheck),
    #[error(transparent)]
    CannotWriteTupleAlreadyExists(#[from] CannotWriteTupleAlreadyExists),
    #[error(transparent)]
    CannotDeleteTupleNotFound(#[from] CannotDeleteTupleNotFound),
    #[error(
        "Active authorization model with version {0} not found in OpenFGA. Make sure to run migration first!"
    )]
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
    #[error("Unauthorized for action `{relation}` on `{object}`")]
    Unauthorized { relation: String, object: String },
    #[error("Cannot assign {0} to itself")]
    SelfAssignment(String),
    #[error("Invalid OpenFGA query: {0}")]
    InvalidQuery(String),
    #[error("Cannot grant permissions while role is assumed in OpenFGA Authorizer")]
    GrantRoleWithAssumedRole,
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
        let tonic_msg = match &err {
            OpenFGAClientError::RequestFailed(status) => Some(status.message().to_string()),
            _ => None,
        };
        if let Some(tonic_msg) = tonic_msg {
            if tonic_msg.starts_with("cannot write a tuple which already exists") {
                CannotWriteTupleAlreadyExists::new(err).into()
            } else if tonic_msg.starts_with("cannot delete a tuple which does not exist") {
                CannotDeleteTupleNotFound::new(err).into()
            } else {
                OpenFGAError::InternalClientError(Box::new(err))
            }
        } else {
            OpenFGAError::InternalClientError(Box::new(err))
        }
    }
}

impl AuthorizationFailureSource for OpenFGAError {
    fn into_error_model(self) -> ErrorModel {
        let err_msg = self.to_string();
        match self {
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
            OpenFGAError::CannotWriteTupleAlreadyExists(e) => {
                ErrorModel::conflict(err_msg, "TupleAlreadyExistsError", Some(Box::new(e)))
            }
            OpenFGAError::CannotDeleteTupleNotFound(e) => {
                ErrorModel::not_found(err_msg, "TupleNotFoundError", Some(Box::new(e)))
            }
            OpenFGAError::InternalClientError(client_error) => {
                OpenFGABackendUnavailable::from(client_error).into_error_model()
            }
            e @ OpenFGAError::UnexpectedEntity { .. } => {
                ErrorModel::internal(err_msg, "UnexpectedEntity", Some(Box::new(e)))
            }
            OpenFGAError::UnexpectedCorrelationId(e) => {
                OpenFGABackendUnavailable::from(e).into_error_model()
            }
            OpenFGAError::BatchCheckError(e) => {
                OpenFGABackendUnavailable::from(e).into_error_model()
            }
            OpenFGAError::MissingItemInBatchCheck(e) => {
                OpenFGABackendUnavailable::from(e).into_error_model()
            }
            OpenFGAError::UnknownType(_) => {
                ErrorModel::bad_request(err_msg, "UnknownOpenFGAType", None)
            }
            _ => ErrorModel::internal(err_msg, "OpenFGAError", Some(Box::new(self))),
        }
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        match self {
            OpenFGAError::NoProjectId => AuthorizationFailureReason::InvalidRequestData,
            OpenFGAError::AuthenticationRequired => AuthorizationFailureReason::InvalidRequestData,
            OpenFGAError::Unauthorized { .. } => AuthorizationFailureReason::ActionForbidden,
            OpenFGAError::SelfAssignment { .. } => AuthorizationFailureReason::InvalidRequestData,
            OpenFGAError::CannotWriteTupleAlreadyExists(_) => {
                AuthorizationFailureReason::InvalidRequestData
            }
            OpenFGAError::CannotDeleteTupleNotFound(_) => {
                AuthorizationFailureReason::ResourceNotFound
            }
            OpenFGAError::UnexpectedEntity { .. } => AuthorizationFailureReason::InvalidRequestData,
            OpenFGAError::UnknownType(_) => AuthorizationFailureReason::InvalidRequestData,
            OpenFGAError::InternalClientError(_)
            | OpenFGAError::UnexpectedCorrelationId(_)
            | OpenFGAError::BatchCheckError(_)
            | OpenFGAError::MissingItemInBatchCheck(_)
            | OpenFGAError::ActiveAuthModelNotFound(_)
            | OpenFGAError::StoreNotFound(_) => {
                AuthorizationFailureReason::InternalAuthorizationError
            }
            OpenFGAError::InvalidEntity(_)
            | OpenFGAError::InvalidQuery(_)
            | OpenFGAError::GrantRoleWithAssumedRole => {
                AuthorizationFailureReason::InvalidRequestData
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unexpected correlation ID returned by server in batch check: `{found}`. Expected usize.")]
pub struct UnexpectedCorrelationId {
    found: String,
}
impl UnexpectedCorrelationId {
    #[must_use]
    pub fn new(found: String) -> Self {
        Self { found }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("One of the checks in a batch returned {} error with code {}: {message}", 
    error_type.as_deref().unwrap_or("unknown"), 
    code.map(|c| c.to_string()).unwrap_or_else(|| "unknown".to_string())
)]
pub struct BatchCheckError {
    message: String,
    error_type: Option<String>,
    code: Option<i32>,
}
impl From<CheckError> for BatchCheckError {
    fn from(err: CheckError) -> Self {
        let CheckError { message, code } = err;

        if let Some(code) = code {
            match code {
                Code::InputError(code) => Self {
                    message,
                    error_type: Some("InputError".to_string()),
                    code: Some(code),
                },
                Code::InternalError(code) => Self {
                    message,
                    error_type: Some("InternalError".to_string()),
                    code: Some(code),
                },
            }
        } else {
            Self {
                message,
                error_type: None,
                code: None,
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Batch check did not return results for {} items. First missing items: {}", missing_indexes.len(), 
    missing_indexes.iter().take(5).map(std::string::ToString::to_string).collect::<Vec<_>>().join(", ")
)]
pub struct MissingItemInBatchCheck {
    pub(crate) missing_indexes: Vec<usize>,
}

#[derive(Debug, thiserror::Error)]
#[error("Cannot write a tuple which already exists")]
pub struct CannotWriteTupleAlreadyExists {
    source: Box<OpenFGAClientError>,
}
impl CannotWriteTupleAlreadyExists {
    #[must_use]
    pub fn new(source: OpenFGAClientError) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Cannot delete a tuple which does not exist")]
pub struct CannotDeleteTupleNotFound {
    source: Box<OpenFGAClientError>,
}
impl CannotDeleteTupleNotFound {
    #[must_use]
    pub fn new(source: OpenFGAClientError) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {

    // Name is important for test profile
    mod openfga_integration_tests {
        use http::StatusCode;
        use lakekeeper::{ProjectId, tokio};
        use openfga_client::client::{TupleKey, TupleKeyWithoutCondition};

        use super::super::*;
        use crate::{
            authorizer::tests::openfga_integration_tests::new_authorizer_in_empty_store,
            entities::OpenFgaEntity as _, relations::WarehouseRelation,
        };

        #[tokio::test]
        async fn test_delete_non_existing_tuple_err_parsed_correctly() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            let err = authorizer
                .write(
                    None,
                    Some(vec![TupleKeyWithoutCondition {
                        user: project_id.to_openfga(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: "warehouse:my_warehouse".to_string(),
                    }]),
                )
                .await
                .unwrap_err();

            assert!(matches!(err, OpenFGAError::CannotDeleteTupleNotFound(_)));
            let err_model = err.into_error_model();
            assert_eq!(err_model.code, StatusCode::NOT_FOUND.as_u16());
            assert_eq!(err_model.r#type, "TupleNotFoundError");
        }

        #[tokio::test]
        async fn test_write_existing_tuple_err_parsed_correctly() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            let tuple = TupleKey {
                user: project_id.to_openfga(),
                relation: WarehouseRelation::Project.to_string(),
                object: "warehouse:my_warehouse".to_string(),
                condition: None,
            };
            // First write should succeed
            authorizer
                .write(Some(vec![tuple.clone()]), None)
                .await
                .unwrap();
            // Second write should fail with tuple already exists
            let err = authorizer
                .write(Some(vec![tuple.clone()]), None)
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                OpenFGAError::CannotWriteTupleAlreadyExists(_)
            ));
            let err_model = err.into_error_model();
            assert_eq!(err_model.code, StatusCode::CONFLICT.as_u16());
            assert_eq!(err_model.r#type, "TupleAlreadyExistsError");
        }
    }
}
