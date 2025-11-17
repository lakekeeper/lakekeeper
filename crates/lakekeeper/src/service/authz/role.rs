use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{AuthorizationBackendUnavailable, Authorizer, CatalogRoleAction, MustUse},
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
}
impl From<RequireRoleActionError> for ErrorModel {
    fn from(err: RequireRoleActionError) -> Self {
        match err {
            RequireRoleActionError::AuthZRoleActionForbidden(e) => e.into(),
            RequireRoleActionError::AuthorizationBackendUnavailable(e) => e.into(),
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
        role_id: RoleId,
        action: impl Into<Self::RoleAction> + Send,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_role_action_impl(metadata, role_id, action.into())
                .await
        }
        .map(MustUse::from)
    }

    async fn are_allowed_role_actions_vec<A: Into<Self::RoleAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        roles_with_actions: &[(RoleId, A)],
    ) -> Result<MustUse<Vec<bool>>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; roles_with_actions.len()])
        } else {
            let converted: Vec<(RoleId, Self::RoleAction)> = roles_with_actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect();
            let decisions = self
                .are_allowed_role_actions_impl(metadata, &converted)
                .await?;

            debug_assert!(
                decisions.len() == roles_with_actions.len(),
                "Mismatched role decision lengths",
            );

            Ok(decisions)
        }
        .map(MustUse::from)
    }

    async fn require_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: impl Into<Self::RoleAction> + Send,
    ) -> Result<(), RequireRoleActionError> {
        let action = action.into();
        if self
            .is_allowed_role_action(metadata, role_id, action)
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
