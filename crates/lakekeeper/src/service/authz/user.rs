use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        Actor, UserId,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CannotInspectPermissions, CatalogUserAction,
            MustUse, UserOrRole,
        },
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
    AuthorizationCountMismatch(AuthorizationCountMismatch),
}
impl From<BackendUnavailableOrCountMismatch> for RequireUserActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::CannotInspectPermissions(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<RequireUserActionError> for ErrorModel {
    fn from(err: RequireUserActionError) -> Self {
        match err {
            RequireUserActionError::AuthZUserActionForbidden(e) => e.into(),
            RequireUserActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireUserActionError::CannotInspectPermissions(e) => e.into(),
            RequireUserActionError::AuthorizationCountMismatch(e) => e.into(),
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
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
        let [decision] = self
            .are_allowed_user_actions_arr(metadata, for_user, &[(user_id, action.into())])
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_user_actions_vec<A: Into<Self::UserAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        users_with_actions: &[(&UserId, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }

        if metadata.has_admin_privileges() && for_user.is_none() {
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

    async fn are_allowed_user_actions_arr<
        const N: usize,
        A: Into<Self::UserAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        users_with_actions: &[(&UserId, A)],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let result = self
            .are_allowed_user_actions_vec(metadata, for_user, users_with_actions)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "user"))?;
        Ok(MustUse::from(arr))
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
