use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CannotInspectPermissions, CatalogRoleAction,
            MustUse, UserOrRole,
        },
        Actor, RoleId,
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
        }
    }
}
impl From<RequireRoleActionError> for IcebergErrorResponse {
    fn from(err: RequireRoleActionError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[async_trait::async_trait]
pub trait AuthZRoleOps: Authorizer {
    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        role_id: RoleId,
        action: impl Into<Self::RoleAction> + Send + Copy + Sync,
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            let [decision] = self
                .are_allowed_role_actions_arr(metadata, for_user, &[(role_id, action)])
                .await?
                .into_inner();
            Ok(decision)
        }
        .map(MustUse::from)
    }

    async fn are_allowed_role_actions_vec<A: Into<Self::RoleAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        roles_with_actions: &[(RoleId, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; roles_with_actions.len()])
        } else {
            let converted: Vec<(RoleId, Self::RoleAction)> = roles_with_actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect();
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
        roles_with_actions: &[(RoleId, A); N],
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
        role_id: RoleId,
        action: impl Into<Self::RoleAction> + Send,
    ) -> Result<(), RequireRoleActionError> {
        let action = action.into();
        if self
            .is_allowed_role_action(metadata, None, role_id, action)
            .await?
            .into_inner()
        {
            Ok(())
        } else {
            Err(AuthZRoleActionForbidden::new(role_id, action, metadata.actor().clone()).into())
        }
    }
}

impl<T> AuthZRoleOps for T where T: Authorizer {}
