use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::RequestMetadata,
    service::{
        UserId,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            AuthzBadRequest, BackendUnavailableOrCountMismatch, CannotInspectPermissions,
            CatalogUserAction, IsAllowedActionError, MustUse, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource,
            delegate_authorization_failure_source,
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
    action: String,
}
impl AuthZUserActionForbidden {
    #[must_use]
    pub fn new(action: impl UserAction) -> Self {
        Self {
            action: action.to_string(),
        }
    }
}
impl AuthorizationFailureSource for AuthZUserActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZUserActionForbidden { action } = self;
        ErrorModel::forbidden(
            format!("Action `{action}` forbidden",),
            "UserActionForbidden",
            None,
        )
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireUserActionError {
    AuthZUserActionForbidden(AuthZUserActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    AuthorizerValidationFailed(AuthzBadRequest),
}
impl From<BackendUnavailableOrCountMismatch> for RequireUserActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<IsAllowedActionError> for RequireUserActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireUserActionError => {
    AuthZUserActionForbidden,
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
    AuthorizationCountMismatch,
    AuthorizerValidationFailed,
});

#[async_trait::async_trait]
pub trait AuthZUserOps: Authorizer {
    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        user_id: &UserId,
        action: impl Into<Self::UserAction> + Send,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
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
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
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
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
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
            Err(AuthZUserActionForbidden::new(action).into())
        }
    }
}

impl<T> AuthZUserOps for T where T: Authorizer {}
