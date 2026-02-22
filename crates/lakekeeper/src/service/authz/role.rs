use std::sync::Arc;

use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::RequestMetadata,
    service::{
        ArcProjectId, CatalogBackendError, GetRoleInProjectError, InvalidPaginationToken, Role,
        RoleId, RoleIdNotFoundInProject,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            AuthorizerValidationFailed, BackendUnavailableOrCountMismatch,
            CannotInspectPermissions, CatalogRoleAction, IsAllowedActionError, MustUse, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource,
            delegate_authorization_failure_source,
        },
    },
};

pub trait RoleAction
where
    Self: std::fmt::Display + Send + Sync + Copy + From<CatalogRoleAction> + PartialEq,
{
}

impl RoleAction for CatalogRoleAction {}

// --------------------------- Errors ---------------------------

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeRole {
    project_id: ArcProjectId,
    role_id: RoleId,
    /// Whether the resource was confirmed not to exist (for audit logging)
    /// HTTP response is deliberately ambiguous, but audit log should be concrete
    internal_resource_not_found: bool,
    internal_server_stack: Vec<String>,
}
impl AuthZCannotSeeRole {
    #[must_use]
    pub fn new(
        project_id: ArcProjectId,
        role_id: RoleId,
        resource_not_found: bool,
        error_stack: Vec<String>,
    ) -> Self {
        Self {
            project_id,
            role_id,
            internal_resource_not_found: resource_not_found,
            internal_server_stack: error_stack,
        }
    }
}
impl From<RoleIdNotFoundInProject> for AuthZCannotSeeRole {
    fn from(err: RoleIdNotFoundInProject) -> Self {
        let RoleIdNotFoundInProject {
            project_id,
            role_id,
            stack,
        } = err;
        AuthZCannotSeeRole {
            project_id,
            role_id,
            internal_resource_not_found: true,
            internal_server_stack: stack,
        }
    }
}
impl AuthorizationFailureSource for AuthZCannotSeeRole {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeRole {
            project_id,
            role_id,
            internal_resource_not_found: _,
            internal_server_stack,
        } = self;
        RoleIdNotFoundInProject::new(role_id, project_id)
            .append_detail("Role not found or access denied")
            .append_details(internal_server_stack)
            .into()
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.internal_resource_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::CannotSeeResource
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZRoleActionForbidden {
    role_id: RoleId,
    action: String,
}
impl AuthZRoleActionForbidden {
    #[must_use]
    pub fn new(role_id: RoleId, action: impl RoleAction) -> Self {
        Self {
            role_id,
            action: action.to_string(),
        }
    }
}
impl AuthorizationFailureSource for AuthZRoleActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZRoleActionForbidden { role_id, action } = self;
        ErrorModel::forbidden(
            format!("Role action `{action}` forbidden on role `{role_id}`",),
            "RoleActionForbidden",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireRoleActionError {
    AuthZRoleActionForbidden(AuthZRoleActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    AuthorizerValidationFailed(AuthorizerValidationFailed),
    // Hide the existence of the role
    AuthZCannotSeeRole(AuthZCannotSeeRole),
    // Propagated directly
    CatalogBackendError(CatalogBackendError),
    InvalidPaginationToken(InvalidPaginationToken),
}
impl From<BackendUnavailableOrCountMismatch> for RequireRoleActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<GetRoleInProjectError> for RequireRoleActionError {
    fn from(err: GetRoleInProjectError) -> Self {
        match err {
            GetRoleInProjectError::CatalogBackendError(e) => e.into(),
            GetRoleInProjectError::RoleIdNotFoundInProject(e) => AuthZCannotSeeRole::from(e).into(),
            GetRoleInProjectError::InvalidPaginationToken(e) => e.into(),
        }
    }
}
impl From<IsAllowedActionError> for RequireRoleActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::AuthorizerValidationFailed(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireRoleActionError => {
    AuthZRoleActionForbidden,
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
    AuthorizationCountMismatch,
    AuthZCannotSeeRole,
    CatalogBackendError,
    InvalidPaginationToken,
    AuthorizerValidationFailed
});

#[async_trait::async_trait]
pub trait AuthZRoleOps: Authorizer {
    fn require_role_presence(
        &self,
        role: Result<Arc<Role>, GetRoleInProjectError>,
    ) -> Result<Arc<Role>, RequireRoleActionError> {
        let role = role?;
        Ok(role)
    }

    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        role: &Role,
        action: impl Into<Self::RoleAction> + Send + Copy + Sync,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        let [decision] = self
            .are_allowed_role_actions_arr(metadata, for_user, &[(role, action)])
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_role_actions_vec<A: Into<Self::RoleAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        roles_with_actions: &[(&Role, A)],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }
        if metadata.has_admin_privileges() && for_user.is_none() {
            Ok(vec![true; roles_with_actions.len()])
        } else {
            let converted = roles_with_actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect::<Vec<_>>();
            let decisions = self
                .are_allowed_role_actions_impl(metadata, for_user, &converted)
                .await?;

            debug_assert!(
                decisions.len() == roles_with_actions.len(),
                "Mismatched role decision lengths",
            );

            Ok(decisions)
        }
        .map(MustUse::from)
    }

    async fn are_allowed_role_actions_arr<
        const N: usize,
        A: Into<Self::RoleAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        roles_with_actions: &[(&Role, A); N],
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
        let result = self
            .are_allowed_role_actions_vec(metadata, for_user, roles_with_actions)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "role"))?;
        Ok(MustUse::from(arr))
    }

    async fn require_role_action(
        &self,
        metadata: &RequestMetadata,
        role: Result<Arc<Role>, GetRoleInProjectError>,
        action: impl Into<Self::RoleAction> + Send,
    ) -> Result<Arc<Role>, RequireRoleActionError> {
        let role = self.require_role_presence(role)?;

        let action = action.into();
        if self
            .is_allowed_role_action(metadata, None, &role, action)
            .await?
            .into_inner()
        {
            Ok(role)
        } else {
            Err(AuthZRoleActionForbidden::new(role.id, action).into())
        }
    }
}

impl<T> AuthZRoleOps for T where T: Authorizer {}
