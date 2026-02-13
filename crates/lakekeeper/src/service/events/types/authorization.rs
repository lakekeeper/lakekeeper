use std::{collections::HashMap, sync::Arc};

use iceberg_ext::catalog::rest::ErrorModel;
use valuable::Valuable;

use crate::api::RequestMetadata;

/// Trait for extracting failure reason from authorization errors
pub trait AuthorizationFailureSource: Send + Sized {
    fn to_failure_reason(&self) -> AuthorizationFailureReason;

    fn into_error_model(self) -> ErrorModel;
}

#[derive(Clone, Debug)]
pub struct AuthorizationError {
    pub r#type: String,
    pub message: String,
    pub stack: Vec<String>,
    pub code: u16,
}

impl AuthorizationError {
    pub fn clone_from_error_model(error_model: &ErrorModel) -> Self {
        Self {
            r#type: error_model.r#type.clone(),
            message: error_model.message.clone(),
            stack: error_model.stack.clone(),
            code: error_model.code,
        }
    }
}

// ===== Authorization Events =====

pub trait ThreadSafeAuthorizationEvent : Valuable + Send + Sync + std::fmt::Debug {}

/// Event emitted when an authorization check fails during request processing.
///
/// This event enables audit trails for security monitoring and compliance,
/// capturing who attempted what action and why it was denied.
#[derive(Clone, Debug)]
pub struct AuthorizationFailedEvent {
    /// Request metadata including the actor who attempted the action
    pub request_metadata: Arc<RequestMetadata>,

    /// The user-provided entity that was being accessed
    pub entity: Arc<dyn ThreadSafeAuthorizationEvent>,

    /// The action that was attempted, serialized from CatalogAction
    pub action: String,

    /// Why the authorization failed
    pub failure_reason: AuthorizationFailureReason,

    /// Authorization Error
    pub error: Arc<AuthorizationError>,

    /// Any additional context that may be useful for debugging or auditing
    pub extra_context: Arc<HashMap<String, String>>,
}

/// Event emitted when an authorization check succeeds during request processing.
///
/// This event can be used for audit trails, security monitoring, and compliance,
/// capturing who performed what action successfully.
#[derive(Clone, Debug)]
pub struct AuthorizationSucceededEvent {
    /// Request metadata including the actor who attempted the action
    pub request_metadata: Arc<RequestMetadata>,

    /// The user-provided entity that was being accessed
    pub entity: Arc<dyn ThreadSafeAuthorizationEvent>,

    /// The action that was attempted, serialized from CatalogAction
    pub action: String,

    /// Any additional context that may be useful for debugging or auditing
    pub extra_context: Arc<HashMap<String, String>>,
}

// ===== Resource-Specific Authorization Failed Events =====

/// Reason why an authorization check failed
///
/// Note: HTTP responses may be deliberately ambiguous (e.g., 404 for both ResourceNotFound
/// and CannotSeeResource), but audit logs are concrete for debugging and compliance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizationFailureReason {
    /// Action is not allowed for the user
    ActionForbidden,

    /// Resource does not exist
    ResourceNotFound,

    /// Resource exists but user lacks permission to see it
    CannotSeeResource,

    /// Authorization backend service is unavailable
    InternalAuthorizationError,

    /// An internal Catalog error occured before authorization check could be completed
    InternalCatalogError,

    /// Invalid data provided by the client that caused authorization to fail (e.g. malformed resource identifier)
    InvalidRequestData,
}

/// Delegates `AuthorizationFailureSource` to inner types of an enum.
/// All variants must be newtype variants wrapping a type that implements `AuthorizationFailureSource`.
macro_rules! delegate_authorization_failure_source {
    ($enum_type:ty => { $($variant:ident),* $(,)? }) => {
        impl $crate::service::events::AuthorizationFailureSource for $enum_type {
            fn into_error_model(self) -> iceberg_ext::catalog::rest::ErrorModel {
                match self {
                    $(Self::$variant(e) => e.into_error_model(),)*
                }
            }
            fn to_failure_reason(&self) -> $crate::service::events::AuthorizationFailureReason {
                match self {
                    $(Self::$variant(e) => e.to_failure_reason(),)*
                }
            }
        }
    };
}
/// Implements `AuthorizationFailureSource` for types that implement `Into<ErrorModel>`
/// with a fixed `AuthorizationFailureReason`.
macro_rules! impl_authorization_failure_source {
    ($error_type:ty => $reason:ident) => {
        impl $crate::service::events::AuthorizationFailureSource for $error_type {
            fn to_failure_reason(&self) -> $crate::service::events::AuthorizationFailureReason {
                $crate::service::events::AuthorizationFailureReason::$reason
            }
            fn into_error_model(self) -> iceberg_ext::catalog::rest::ErrorModel {
                iceberg_ext::catalog::rest::ErrorModel::from(self)
            }
        }
    };
}

pub(crate) use delegate_authorization_failure_source;
pub(crate) use impl_authorization_failure_source;
