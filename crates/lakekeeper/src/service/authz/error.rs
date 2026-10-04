use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
};

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    request_metadata::ProjectIdMissing,
    service::{
        ApplyTagError, ColumnNotFound, CreateRoleError, CreateTagDefinitionError, DeleteRoleError,
        DeleteTagDefinitionError, GetRoleAcrossProjectsError, GetRoleInProjectError,
        GetTaskDetailsError, InternalErrorMessage, ListRolesError, ListTagAttachmentsError,
        ListTagDefinitionsError, ManagedRoleImmutable, NoWarehouseTaskError, RemoveTagError,
        ResolveTasksError, RoleMembershipCycle, RoleProviderIdReserved, RoleProviderNotApiManaged,
        SearchRolesError, SystemRoleImmutable, SystemRoleMemberRolesNotSupported,
        SystemRoleMembershipRequiresInstanceAdmin, TagDefinitionReserved, TagNameNotFound,
        TagTargetNotFound, TaskNotFoundError, UpdateRoleError, UpdateTagDefinitionError,
        authz::{
            AuthZCannotSeeAnonymousNamespace, AuthZCannotSeeGenericTable, AuthZCannotSeeNamespace,
            AuthZCannotSeeTable, AuthZCannotSeeTableLocation, AuthZCannotSeeView,
            AuthZCannotUseWarehouseId, AuthZTableActionForbidden, AuthZUserActionForbidden,
            AuthZWarehouseActionForbidden, RequireGenericTableActionError,
            RequireNamespaceActionError, RequireProjectActionError, RequireRoleActionError,
            RequireServerActionError, RequireTableActionError, RequireTabularActionsError,
            RequireTagActionError, RequireViewActionError, RequireWarehouseActionError,
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
}
delegate_authorization_failure_source!(BackendUnavailableOrCountMismatch => {
    AuthorizationCountMismatch,
    AuthorizationBackendUnavailable,
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

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("{reason}")]
pub struct AuthzBadRequest {
    reason: String,
}
impl AuthzBadRequest {
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}
impl AuthorizationFailureSource for AuthzBadRequest {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::forbidden(self.to_string(), "AuthzBadRequest", None)
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InvalidRequestData
    }
}

#[derive(Debug, derive_more::From)]
pub enum IsAllowedActionError {
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    BadRequest(AuthzBadRequest),
    CountMismatch(AuthorizationCountMismatch),
    InternalError(AuthorizationInternalError),
}
delegate_authorization_failure_source!(IsAllowedActionError => {
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
    BadRequest,
    CountMismatch,
    InternalError
});

impl From<BackendUnavailableOrCountMismatch> for IsAllowedActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => {
                IsAllowedActionError::AuthorizationBackendUnavailable(e)
            }
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => {
                IsAllowedActionError::CountMismatch(e)
            }
        }
    }
}

/// Error from authorizer checks that admit a bad request: the backend is
/// unavailable (503), the request is invalid, or the authorizer hit an internal
/// error (500).
#[derive(Debug, PartialEq, derive_more::From)]
pub enum AuthzBackendErrorOrBadRequest {
    BackendUnavailable(AuthorizationBackendUnavailable),
    BadRequest(AuthzBadRequest),
    InternalError(AuthorizationInternalError),
}
delegate_authorization_failure_source!(AuthzBackendErrorOrBadRequest => {
    BackendUnavailable,
    BadRequest,
    InternalError,
});

impl From<AuthzBackendErrorOrBadRequest> for IsAllowedActionError {
    fn from(err: AuthzBackendErrorOrBadRequest) -> Self {
        match err {
            AuthzBackendErrorOrBadRequest::BackendUnavailable(e) => e.into(),
            AuthzBackendErrorOrBadRequest::BadRequest(e) => e.into(),
            AuthzBackendErrorOrBadRequest::InternalError(e) => e.into(),
        }
    }
}
impl From<AuthzBackendErrorOrBadRequest> for AuthZError {
    fn from(err: AuthzBackendErrorOrBadRequest) -> Self {
        IsAllowedActionError::from(err).into()
    }
}

/// Error from [`ManagesRoleAssignments::add_role_assignments`](super::ManagesRoleAssignments::add_role_assignments).
///
/// OpenFGA only fails with `BackendUnavailable` (it tolerates cycles). Authorizers
/// that enforce assignment integrity may also reject a cycle, so the trait admits
/// that variant.
#[derive(Debug, derive_more::From)]
pub enum AddRoleAssignmentsError {
    BackendUnavailable(AuthorizationBackendUnavailable),
    Cycle(RoleMembershipCycle),
}
impl From<AddRoleAssignmentsError> for ErrorModel {
    fn from(err: AddRoleAssignmentsError) -> Self {
        match err {
            AddRoleAssignmentsError::BackendUnavailable(e) => e.into_error_model(),
            AddRoleAssignmentsError::Cycle(e) => e.into(),
        }
    }
}

/// The authorization backend returned a role assignment Lakekeeper cannot parse.
/// Lakekeeper wrote these records, so this is an internal invariant violation
/// (HTTP 500), not the backend being *unavailable* (503).
///
/// `reason` is returned to the client; `source` (the authorizer's typed parse
/// error) is only logged — the same split as [`ErrorModel`]'s message vs. its
/// `#[serde(skip)]` source.
#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct MalformedRoleAssignment {
    reason: String,
    #[source]
    source: Box<dyn StdError + Send + Sync + 'static>,
}
impl MalformedRoleAssignment {
    pub fn new(reason: impl Into<String>, source: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            reason: reason.into(),
            source: Box::new(source),
        }
    }
}
impl AuthorizationFailureSource for MalformedRoleAssignment {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::internal(self.reason, "MalformedRoleAssignment", Some(self.source))
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InternalAuthorizationError
    }
}

/// Error from [`ManagesRoleAssignments::list_role_assignments`](super::ManagesRoleAssignments::list_role_assignments):
/// the backend is unavailable (503), or it returned a tuple we cannot interpret
/// (500). The two are deliberately distinct — a parse failure is not a transient
/// availability problem.
#[derive(Debug, derive_more::From)]
pub enum ListRoleAssignmentsError {
    BackendUnavailable(AuthorizationBackendUnavailable),
    Malformed(MalformedRoleAssignment),
}
impl From<ListRoleAssignmentsError> for ErrorModel {
    fn from(err: ListRoleAssignmentsError) -> Self {
        match err {
            ListRoleAssignmentsError::BackendUnavailable(e) => e.into_error_model(),
            ListRoleAssignmentsError::Malformed(e) => e.into_error_model(),
        }
    }
}

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

/// The authorizer hit a deterministic internal error, such as a request it
/// cannot build. Retrying yields the same failure, so this is HTTP 500 and
/// distinct from [`AuthorizationBackendUnavailable`] (503), which clients and
/// load balancers retry.
///
/// The client receives a generic message; `stack` and `source` are only logged.
#[derive(Debug, thiserror::Error)]
#[error("Internal authorization error: {source}")]
pub struct AuthorizationInternalError {
    pub stack: Vec<String>,
    #[source]
    pub source: Box<dyn StdError + Send + Sync + 'static>,
}

impl_error_stack_methods!(AuthorizationInternalError);

impl PartialEq for AuthorizationInternalError {
    fn eq(&self, other: &Self) -> bool {
        self.stack == other.stack && self.source.to_string() == other.source.to_string()
    }
}

impl AuthorizationInternalError {
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

impl AuthorizationFailureSource for AuthorizationInternalError {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::internal(
            "Authorization failed due to an internal error",
            "AuthorizationInternalError",
            Some(self.source),
        )
        .append_details(self.stack)
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
    AuthZCannotSeeGenericTable(AuthZCannotSeeGenericTable),
    RequireGenericTableActionError(RequireGenericTableActionError),
    AuthZCannotSeeTableLocation(AuthZCannotSeeTableLocation),
    ProjectIdMissing(ProjectIdMissing),
    TaskNotFoundError(TaskNotFoundError),
    NoWarehouseTaskError(NoWarehouseTaskError),
    RequireProjectActionError(RequireProjectActionError),
    RequireServerActionError(RequireServerActionError),
    RequireRoleActionError(RequireRoleActionError),
    SystemRoleImmutable(SystemRoleImmutable),
    SystemRoleMembershipRequiresInstanceAdmin(SystemRoleMembershipRequiresInstanceAdmin),
    SystemRoleMemberRolesNotSupported(SystemRoleMemberRolesNotSupported),
    ManagedRoleImmutable(ManagedRoleImmutable),
    RoleProviderIdReserved(RoleProviderIdReserved),
    RoleProviderNotApiManaged(RoleProviderNotApiManaged),
    CreateRoleError(CreateRoleError),
    ListRolesError(ListRolesError),
    GetRoleAcrossProjectsError(GetRoleAcrossProjectsError),
    GetRoleInProjectError(GetRoleInProjectError),
    DeleteRoleError(DeleteRoleError),
    UpdateRoleError(UpdateRoleError),
    SearchRolesError(SearchRolesError),
    RequireTagActionError(RequireTagActionError),
    TagDefinitionReserved(TagDefinitionReserved),
    CreateTagDefinitionError(CreateTagDefinitionError),
    ListTagDefinitionsError(ListTagDefinitionsError),
    ListTagAttachmentsError(ListTagAttachmentsError),
    UpdateTagDefinitionError(UpdateTagDefinitionError),
    DeleteTagDefinitionError(DeleteTagDefinitionError),
    ApplyTagError(ApplyTagError),
    RemoveTagError(RemoveTagError),
    TagNameNotFound(TagNameNotFound),
    ColumnNotFound(ColumnNotFound),
    TagTargetNotFound(TagTargetNotFound),
    AuthZUserActionForbidden(AuthZUserActionForbidden),
    BackendUnavailableOrCountMismatch(BackendUnavailableOrCountMismatch),
    BadRequest(AuthzBadRequest),
    IsAllowedActionError(IsAllowedActionError),
    GrantActionForbidden(super::grant::AuthZGrantActionForbidden),
    ApplyGrantsStoreError(crate::service::ApplyGrantsStoreError),
    ListGrantsStoreError(crate::service::ListGrantsStoreError),
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
            RequireTabularActionsError::AuthorizationInternalError(e) => {
                RequireWarehouseActionError::AuthorizationInternalError(e).into()
            }
            RequireTabularActionsError::CannotInspectPermissions(e) => {
                RequireWarehouseActionError::CannotInspectPermissions(e).into()
            }
            RequireTabularActionsError::AuthorizerValidationFailed(e) => {
                RequireTableActionError::AuthorizerValidationFailed(e).into()
            }
            RequireTabularActionsError::AuthZGenericTableActionForbidden(e) => {
                RequireGenericTableActionError::from(e).into()
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
    AuthZCannotSeeGenericTable,
    RequireGenericTableActionError,
    AuthZCannotSeeTableLocation,
    ProjectIdMissing,
    TaskNotFoundError,
    NoWarehouseTaskError,
    RequireProjectActionError,
    RequireServerActionError,
    RequireRoleActionError,
    SystemRoleImmutable,
    SystemRoleMembershipRequiresInstanceAdmin,
    SystemRoleMemberRolesNotSupported,
    ManagedRoleImmutable,
    RoleProviderIdReserved,
    RoleProviderNotApiManaged,
    CreateRoleError,
    ListRolesError,
    GetRoleAcrossProjectsError,
    GetRoleInProjectError,
    DeleteRoleError,
    UpdateRoleError,
    SearchRolesError,
    RequireTagActionError,
    TagDefinitionReserved,
    CreateTagDefinitionError,
    ListTagDefinitionsError,
    ListTagAttachmentsError,
    UpdateTagDefinitionError,
    DeleteTagDefinitionError,
    ApplyTagError,
    RemoveTagError,
    TagNameNotFound,
    ColumnNotFound,
    TagTargetNotFound,
    AuthZUserActionForbidden,
    GrantActionForbidden,
    BackendUnavailableOrCountMismatch,
    BadRequest,
    IsAllowedActionError,
    ApplyGrantsStoreError,
    ListGrantsStoreError
});

#[cfg(test)]
mod tests {
    use super::*;

    fn internal_error() -> AuthorizationInternalError {
        AuthorizationInternalError::new(std::io::Error::other("secret internal detail"))
            .append_detail("action Lakekeeper::Action::\"CreateNamespace\"")
    }

    #[test]
    fn internal_error_is_a_500_with_a_generic_message() {
        let err = internal_error();
        assert_eq!(
            err.to_failure_reason(),
            AuthorizationFailureReason::InternalAuthorizationError
        );

        let model = err.into_error_model();
        assert_eq!(model.code, StatusCode::INTERNAL_SERVER_ERROR.as_u16());
        assert_eq!(model.r#type, "AuthorizationInternalError");
        assert_eq!(
            model.message,
            "Authorization failed due to an internal error"
        );
        assert_eq!(
            model.stack,
            vec!["action Lakekeeper::Action::\"CreateNamespace\"".to_string()]
        );
        assert_eq!(
            model.source.map(|s| s.to_string()).as_deref(),
            Some("secret internal detail")
        );
    }

    #[test]
    fn internal_error_stays_a_500_through_authorizer_error_conversions() {
        let from_bad_request_enum: IsAllowedActionError =
            AuthzBackendErrorOrBadRequest::from(internal_error()).into();
        assert!(matches!(
            from_bad_request_enum,
            IsAllowedActionError::InternalError(_)
        ));

        let errors = [
            AuthZError::from(AuthzBackendErrorOrBadRequest::from(internal_error())),
            AuthZError::from(IsAllowedActionError::from(internal_error())),
            AuthZError::from(RequireWarehouseActionError::from(
                IsAllowedActionError::from(internal_error()),
            )),
            AuthZError::from(RequireTabularActionsError::from(
                IsAllowedActionError::from(internal_error()),
            )),
        ];
        for err in errors {
            assert_eq!(
                err.to_failure_reason(),
                AuthorizationFailureReason::InternalAuthorizationError
            );
            let model = err.into_error_model();
            assert_eq!(model.code, StatusCode::INTERNAL_SERVER_ERROR.as_u16());
            assert_eq!(model.r#type, "AuthorizationInternalError");
        }
    }

    #[test]
    fn backend_unavailable_stays_a_503() {
        let err = IsAllowedActionError::from(AuthorizationBackendUnavailable::new(
            std::io::Error::other("connection refused"),
        ));
        assert_eq!(
            err.into_error_model().code,
            StatusCode::SERVICE_UNAVAILABLE.as_u16()
        );
    }
}
