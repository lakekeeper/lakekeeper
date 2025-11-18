use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            AuthorizationBackendUnavailable, Authorizer, CannotInspectPermissions,
            CatalogUserAction, IsAllowedActionError, MustUse, UserOrRole,
        },
        Actor, UserId,
    },
};

pub trait UserAction
where
    Self: std::fmt::Display + Send + Sync + Copy + From<CatalogUserAction> + PartialEq,
{
}

impl UserAction for CatalogUserAction {}

// --------------------------- Errors ---------------------------

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZUserActionForbidden {
    user_id: UserId,
    action: String,
    actor: Actor,
}
impl AuthZUserActionForbidden {
    #[must_use]
    pub fn new(user_id: UserId, action: impl UserAction, actor: Actor) -> Self {
        Self {
            user_id,
            action: action.to_string(),
            actor,
        }
    }
}
impl From<AuthZUserActionForbidden> for ErrorModel {
    fn from(err: AuthZUserActionForbidden) -> Self {
        let AuthZUserActionForbidden {
            user_id,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!("User action `{action}` forbidden for {actor} on user `{user_id}`",),
            "UserActionForbidden",
            None,
        )
    }
}
impl From<AuthZUserActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZUserActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireUserActionError {
    AuthZUserActionForbidden(AuthZUserActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
}
impl From<IsAllowedActionError> for RequireUserActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<RequireUserActionError> for ErrorModel {
    fn from(err: RequireUserActionError) -> Self {
        match err {
            RequireUserActionError::AuthZUserActionForbidden(e) => e.into(),
            RequireUserActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireUserActionError::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<RequireUserActionError> for IcebergErrorResponse {
    fn from(err: RequireUserActionError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[async_trait::async_trait]
pub trait AuthZUserOps: Authorizer {
    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        user_id: &UserId,
        action: impl Into<Self::UserAction> + Send,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_user_action_impl(metadata, for_user, user_id, action.into())
                .await
        }
        .map(MustUse::from)
    }

    async fn are_allowed_user_actions_vec<A: Into<Self::UserAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        users_with_actions: &[(&UserId, A)],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; users_with_actions.len()])
        } else {
            let converted = users_with_actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect::<Vec<_>>();
            let decisions = self
                .are_allowed_user_actions_impl(metadata, for_user, &converted)
                .await?;

            debug_assert!(
                decisions.len() == users_with_actions.len(),
                "Mismatched user decision lengths",
            );

            Ok(decisions)
        }
        .map(MustUse::from)
    }

    async fn require_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: impl Into<Self::UserAction> + Send,
    ) -> Result<(), RequireUserActionError> {
        let action = action.into();
        if self
            .is_allowed_user_action(metadata, None, user_id, action)
            .await?
            .into_inner()
        {
            Ok(())
        } else {
            Err(
                AuthZUserActionForbidden::new(user_id.clone(), action, metadata.actor().clone())
                    .into(),
            )
        }
    }
}

impl<T> AuthZUserOps for T where T: Authorizer {}
