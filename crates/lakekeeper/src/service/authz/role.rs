use std::sync::Arc;

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    ProjectId,
    api::{RequestMetadata, management::v1::role::Role},
    service::{
        Actor, CatalogBackendError, GetRoleError, InvalidPaginationToken, RoleId, RoleIdNotFound,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CannotInspectPermissions, CatalogRoleAction,
            MustUse, UserOrRole,
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
    project_id: ProjectId,
    role_id: RoleId,
}
impl AuthZCannotSeeRole {
    #[must_use]
    pub fn new(project_id: ProjectId, role_id: RoleId) -> Self {
        Self {
            project_id,
            role_id,
        }
    }
}
impl From<RoleIdNotFound> for AuthZCannotSeeRole {
    fn from(err: RoleIdNotFound) -> Self {
        // Deliberately discard the stack trace to avoid leaking
        // information about the existence of the role.
        AuthZCannotSeeRole {
            project_id: err.project_id,
            role_id: err.role_id,
        }
    }
}
impl From<AuthZCannotSeeRole> for ErrorModel {
    fn from(err: AuthZCannotSeeRole) -> Self {
        let AuthZCannotSeeRole {
            project_id,
            role_id,
        } = err;
        RoleIdNotFound::new(role_id, project_id)
            .append_detail("Role not found or access denied")
            .into()
    }
}
impl From<AuthZCannotSeeRole> for IcebergErrorResponse {
    fn from(err: AuthZCannotSeeRole) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZRoleActionForbidden {
    role_id: RoleId,
    action: String,
    actor: Actor,
}
impl AuthZRoleActionForbidden {
    #[must_use]
    pub fn new(role_id: RoleId, action: impl RoleAction, actor: Actor) -> Self {
        Self {
            role_id,
            action: action.to_string(),
            actor,
        }
    }
}
impl From<AuthZRoleActionForbidden> for ErrorModel {
    fn from(err: AuthZRoleActionForbidden) -> Self {
        let AuthZRoleActionForbidden {
            role_id,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!("Role action `{action}` forbidden for {actor} on role `{role_id}`",),
            "RoleActionForbidden",
            None,
        )
    }
}
impl From<AuthZRoleActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZRoleActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireRoleActionError {
    AuthZRoleActionForbidden(AuthZRoleActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
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
            BackendUnavailableOrCountMismatch::CannotInspectPermissions(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<RequireRoleActionError> for ErrorModel {
    fn from(err: RequireRoleActionError) -> Self {
        match err {
            RequireRoleActionError::AuthZRoleActionForbidden(e) => e.into(),
            RequireRoleActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireRoleActionError::CannotInspectPermissions(e) => e.into(),
            RequireRoleActionError::AuthorizationCountMismatch(e) => e.into(),
            RequireRoleActionError::AuthZCannotSeeRole(e) => e.into(),
            RequireRoleActionError::CatalogBackendError(e) => e.into(),
            RequireRoleActionError::InvalidPaginationToken(e) => e.into(),
        }
    }
}
impl From<RequireRoleActionError> for IcebergErrorResponse {
    fn from(err: RequireRoleActionError) -> Self {
        ErrorModel::from(err).into()
    }
}
impl From<GetRoleError> for RequireRoleActionError {
    fn from(err: GetRoleError) -> Self {
        match err {
            GetRoleError::CatalogBackendError(e) => e.into(),
            GetRoleError::RoleIdNotFound(e) => AuthZCannotSeeRole::from(e).into(),
            GetRoleError::InvalidPaginationToken(e) => e.into(),
        }
    }
}

#[async_trait::async_trait]
pub trait AuthZRoleOps: Authorizer {
    fn require_role_presence(
        &self,
        role: Result<Arc<Role>, GetRoleError>,
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
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
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
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
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
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
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
        role: Result<Arc<Role>, GetRoleError>,
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
            Err(AuthZRoleActionForbidden::new(role.id, action, metadata.actor().clone()).into())
        }
    }
}

impl<T> AuthZRoleOps for T where T: Authorizer {}
