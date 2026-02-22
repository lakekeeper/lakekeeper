use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
};

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    request_metadata::ProjectIdMissing,
    service::{
        CreateRoleError, DeleteRoleError, GetRoleAcrossProjectsError, GetTaskDetailsError,
        InternalErrorMessage, ListRolesError, NoWarehouseTaskError, ResolveTasksError,
        SearchRolesError, TaskNotFoundError, UpdateRoleError,
        authz::{
            AuthZCannotSeeAnonymousNamespace, AuthZCannotSeeNamespace, AuthZCannotSeeTable,
            AuthZCannotSeeTableLocation, AuthZCannotSeeView, AuthZCannotUseWarehouseId,
            AuthZTableActionForbidden, AuthZUserActionForbidden, AuthZWarehouseActionForbidden,
            RequireNamespaceActionError, RequireProjectActionError, RequireRoleActionError,
            RequireTableActionError, RequireTabularActionsError, RequireViewActionError,
            RequireWarehouseActionError,
        },
        error_chain_fmt,
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource,
            delegate_authorization_failure_source,
        },
        impl_error_stack_methods,
    },
};

#[derive(Debug, PartialEq, derive_more::From)]
pub enum BackendUnavailableOrCountMismatch {
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
}
impl From<IsAllowedActionError> for BackendUnavailableOrCountMismatch {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(BackendUnavailableOrCountMismatch => {
    AuthorizationCountMismatch,
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
});

#[derive(Debug, PartialEq)]
pub struct AuthorizationCountMismatch {
    pub expected_authorizations: usize,
    pub actual_authorizations: usize,
    pub type_name: String,
}

impl AuthorizationCountMismatch {
    #[must_use]
    pub fn new(
        expected_authorizations: usize,
        actual_authorizations: usize,
        type_name: &str,
    ) -> Self {
        Self {
            expected_authorizations,
            actual_authorizations,
            type_name: type_name.to_string(),
        }
    }
}
impl AuthorizationFailureSource for AuthorizationCountMismatch {
    fn into_error_model(self) -> ErrorModel {
        let AuthorizationCountMismatch {
            expected_authorizations,
            actual_authorizations,
            type_name,
        } = self;

        ErrorModel::builder()
            .r#type("AuthorizationCountMismatch")
            .code(StatusCode::INTERNAL_SERVER_ERROR.as_u16())
            .message("Authorization service returned invalid response")
            .source(Some(Box::new(InternalErrorMessage(format!(
                "Authorization count mismatch for {type_name} batch check: expected {expected_authorizations}, got {actual_authorizations}."
            )))))
            .build()
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InternalAuthorizationError
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("Not allowed to inspect permissions for object {object}")]
pub struct CannotInspectPermissions {
    object: String,
}
impl CannotInspectPermissions {
    #[must_use]
    pub fn new(object: &impl ToString) -> Self {
        Self {
            object: object.to_string(),
        }
    }
}
impl AuthorizationFailureSource for CannotInspectPermissions {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::forbidden(self.to_string(), "CannotInspectPermissions", None)
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

#[derive(Debug, derive_more::From)]
pub enum IsAllowedActionError {
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
}
delegate_authorization_failure_source!(IsAllowedActionError => {
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
});

#[derive(Debug)]
pub struct AuthorizationBackendUnavailable {
    pub stack: Vec<String>,
    pub source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl_error_stack_methods!(AuthorizationBackendUnavailable);

impl PartialEq for AuthorizationBackendUnavailable {
    fn eq(&self, other: &Self) -> bool {
        self.stack == other.stack && self.source.to_string() == other.source.to_string()
    }
}

impl AuthorizationBackendUnavailable {
    pub fn new<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            stack: Vec::new(),
            source: Box::new(source),
        }
    }
}

impl StdError for AuthorizationBackendUnavailable {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source as &(dyn StdError + 'static))
    }
}

impl Display for AuthorizationBackendUnavailable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "AuthorizationBackendError: {}", self.source)?;

        if !self.stack.is_empty() {
            writeln!(f, "Stack:")?;
            for detail in &self.stack {
                writeln!(f, "  {detail}")?;
            }
        }

        if let Some(source) = self.source.source() {
            writeln!(f, "Caused by:")?;
            // Dereference `source` to get `dyn StdError` and then take a reference to pass
            error_chain_fmt(source, f)?;
        }

        Ok(())
    }
}

impl AuthorizationFailureSource for AuthorizationBackendUnavailable {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::builder()
            .r#type("AuthorizationBackendError")
            .code(StatusCode::SERVICE_UNAVAILABLE.as_u16())
            .message("Authorization service is unavailable")
            .stack(self.stack)
            .source(Some(self.source))
            .build()
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InternalAuthorizationError
    }
}

#[derive(Debug, derive_more::From)]
pub enum AuthZError {
    RequireWarehouseActionError(RequireWarehouseActionError),
    RequireTableActionError(RequireTableActionError),
    RequireNamespaceActionError(RequireNamespaceActionError),
    AuthZCannotSeeTable(AuthZCannotSeeTable),
    RequireViewActionError(RequireViewActionError),
    AuthZCannotSeeView(AuthZCannotSeeView),
    AuthZCannotSeeTableLocation(AuthZCannotSeeTableLocation),
    ProjectIdMissing(ProjectIdMissing),
    TaskNotFoundError(TaskNotFoundError),
    NoWarehouseTaskError(NoWarehouseTaskError),
    RequireProjectActionError(RequireProjectActionError),
    RequireRoleActionError(RequireRoleActionError),
    CreateRoleError(CreateRoleError),
    ListRolesError(ListRolesError),
    GetRoleAcrossProjectsError(GetRoleAcrossProjectsError),
    DeleteRoleError(DeleteRoleError),
    UpdateRoleError(UpdateRoleError),
    SearchRolesError(SearchRolesError),
    AuthZUserActionForbidden(AuthZUserActionForbidden),
    BackendUnavailableOrCountMismatch(BackendUnavailableOrCountMismatch),
}
impl From<ResolveTasksError> for AuthZError {
    fn from(err: ResolveTasksError) -> Self {
        match err {
            ResolveTasksError::TaskNotFoundError(e) => e.into(),
            ResolveTasksError::DatabaseIntegrityError(e) => {
                RequireWarehouseActionError::from(e).into()
            }
            ResolveTasksError::CatalogBackendError(e) => {
                RequireWarehouseActionError::from(e).into()
            }
        }
    }
}
impl From<GetTaskDetailsError> for AuthZError {
    fn from(value: GetTaskDetailsError) -> Self {
        match value {
            GetTaskDetailsError::TaskNotFoundError(e) => e.into(),
            GetTaskDetailsError::DatabaseIntegrityError(e) => {
                RequireWarehouseActionError::from(e).into()
            }
            GetTaskDetailsError::CatalogBackendError(e) => {
                RequireWarehouseActionError::from(e).into()
            }
        }
    }
}
impl From<AuthorizationCountMismatch> for AuthZError {
    fn from(err: AuthorizationCountMismatch) -> Self {
        RequireWarehouseActionError::AuthorizationCountMismatch(err).into()
    }
}
impl From<AuthZCannotUseWarehouseId> for AuthZError {
    fn from(err: AuthZCannotUseWarehouseId) -> Self {
        RequireWarehouseActionError::from(err).into()
    }
}
impl From<AuthZWarehouseActionForbidden> for AuthZError {
    fn from(err: AuthZWarehouseActionForbidden) -> Self {
        RequireWarehouseActionError::from(err).into()
    }
}
impl From<AuthZTableActionForbidden> for AuthZError {
    fn from(err: AuthZTableActionForbidden) -> Self {
        RequireTableActionError::AuthZTableActionForbidden(err).into()
    }
}
impl From<RequireTabularActionsError> for AuthZError {
    fn from(err: RequireTabularActionsError) -> Self {
        match err {
            RequireTabularActionsError::AuthorizationBackendUnavailable(e) => {
                RequireWarehouseActionError::AuthorizationBackendUnavailable(e).into()
            }
            RequireTabularActionsError::AuthZViewActionForbidden(e) => {
                RequireViewActionError::from(e).into()
            }
            RequireTabularActionsError::AuthZTableActionForbidden(e) => {
                RequireTableActionError::from(e).into()
            }
            RequireTabularActionsError::AuthorizationCountMismatch(e) => {
                RequireWarehouseActionError::AuthorizationCountMismatch(e).into()
            }
            RequireTabularActionsError::CannotInspectPermissions(e) => {
                RequireWarehouseActionError::CannotInspectPermissions(e).into()
            }
        }
    }
}
impl From<AuthZCannotSeeNamespace> for AuthZError {
    fn from(err: AuthZCannotSeeNamespace) -> Self {
        Self::RequireNamespaceActionError(err.into())
    }
}
impl From<AuthZCannotSeeAnonymousNamespace> for AuthZError {
    fn from(err: AuthZCannotSeeAnonymousNamespace) -> Self {
        Self::RequireNamespaceActionError(err.into())
    }
}
delegate_authorization_failure_source!(AuthZError => {
    RequireWarehouseActionError,
    RequireTableActionError,
    RequireNamespaceActionError,
    AuthZCannotSeeTable,
    RequireViewActionError,
    AuthZCannotSeeView,
    AuthZCannotSeeTableLocation,
    ProjectIdMissing,
    TaskNotFoundError,
    NoWarehouseTaskError,
    RequireProjectActionError,
    RequireRoleActionError,
    CreateRoleError,
    ListRolesError,
    GetRoleAcrossProjectsError,
    DeleteRoleError,
    UpdateRoleError,
    SearchRolesError,
    AuthZUserActionForbidden,
    BackendUnavailableOrCountMismatch
});
