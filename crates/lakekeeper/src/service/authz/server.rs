use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::RequestMetadata,
    service::{
        Actor, ServerId,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            AuthorizerValidationFailed, BackendUnavailableOrCountMismatch,
            CannotInspectPermissions, CatalogServerAction, IsAllowedActionError, MustUse,
            UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource, context::UserProvidedRole,
            delegate_authorization_failure_source,
        },
    },
};
pub trait ServerAction
where
    Self: std::fmt::Display + Send + Sync + Copy + From<CatalogServerAction> + PartialEq,
{
}

impl ServerAction for CatalogServerAction {}

// --------------------------- Errors ---------------------------

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZServerActionForbidden {
    server_id: ServerId,
    action: String,
}
impl AuthZServerActionForbidden {
    #[must_use]
    pub fn new(server_id: ServerId, action: impl ServerAction) -> Self {
        Self {
            server_id,
            action: action.to_string(),
        }
    }
}
impl AuthorizationFailureSource for AuthZServerActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZServerActionForbidden { server_id, action } = self;
        ErrorModel::forbidden(
            format!("Server action `{action}` forbidden on server `{server_id}`",),
            "ServerActionForbidden",
            None,
        )
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

// --------------------------- Assume Role Errors ---------------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AssumeRoleForbidden {
    pub(crate) role: UserProvidedRole,
}
impl AssumeRoleForbidden {
    #[must_use]
    pub fn new(role: UserProvidedRole) -> Self {
        Self { role }
    }
}
impl AuthorizationFailureSource for AssumeRoleForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AssumeRoleForbidden { role } = self;
        ErrorModel::forbidden(
            format!("Assume {role} forbidden",),
            "AssumeRoleForbidden",
            None,
        )
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}
#[derive(Debug, derive_more::From)]
pub enum CheckActorError {
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AssumeRoleForbidden(AssumeRoleForbidden),
}

delegate_authorization_failure_source!(CheckActorError => {
    AuthorizationBackendUnavailable,
    AssumeRoleForbidden,
});

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireServerActionError {
    AuthZServerActionForbidden(AuthZServerActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    AuthorizerValidationFailed(AuthorizerValidationFailed),
}
impl From<BackendUnavailableOrCountMismatch> for RequireServerActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<IsAllowedActionError> for RequireServerActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::AuthorizerValidationFailed(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireServerActionError => {
    AuthZServerActionForbidden,
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
    AuthorizationCountMismatch,
    AuthorizerValidationFailed
});

// --------------------------- Server Ops ---------------------------

#[async_trait::async_trait]
pub trait AuthZServerOps: Authorizer {
    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        action: impl Into<Self::ServerAction> + Send + Sync + Copy,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        let [decision] = self
            .are_allowed_server_actions_arr(metadata, for_user, &[action])
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_server_actions_vec<A: Into<Self::ServerAction> + Send + Sync + Copy>(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        actions: &[A],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }

        if metadata.has_admin_privileges() && for_user.is_none() {
            Ok(vec![true; actions.len()])
        } else {
            let converted = actions.iter().map(|a| (*a).into()).collect::<Vec<_>>();
            let decisions = self
                .are_allowed_server_actions_impl(metadata, for_user, &converted)
                .await?;

            if decisions.len() != actions.len() {
                return Err(AuthorizationCountMismatch::new(
                    actions.len(),
                    decisions.len(),
                    "server",
                )
                .into());
            }

            Ok(decisions)
        }
        .map(MustUse::from)
    }

    async fn are_allowed_server_actions_arr<
        const N: usize,
        A: Into<Self::ServerAction> + Send + Sync + Copy,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
        let result = self
            .are_allowed_server_actions_vec(metadata, for_user, actions)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "server"))?;
        Ok(MustUse::from(arr))
    }

    async fn require_server_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        action: impl Into<Self::ServerAction> + Send + Sync + Copy,
    ) -> Result<(), RequireServerActionError> {
        let action = action.into();
        if self
            .is_allowed_server_action(metadata, for_user, action)
            .await?
            .into_inner()
        {
            Ok(())
        } else {
            Err(AuthZServerActionForbidden::new(self.server_id(), action).into())
        }
    }

    async fn check_actor(
        &self,
        actor: &Actor,
        request_metadata: &RequestMetadata,
    ) -> Result<(), CheckActorError> {
        match actor {
            Actor::Principal(_user_id) => Ok(()),
            Actor::Anonymous => Ok(()),
            Actor::Role {
                principal,
                assumed_role,
            } => {
                let assume_role_allowed = self
                    .check_assume_role_impl(principal, assumed_role, request_metadata)
                    .await?;

                if assume_role_allowed {
                    Ok(())
                } else {
                    Err(AssumeRoleForbidden::new(UserProvidedRole::Ident {
                        project_id: assumed_role.project_id().clone(),
                        ident: assumed_role.ident_arc(),
                    })
                    .into())
                }
            }
        }
    }

    async fn can_search_users(
        &self,
        metadata: &RequestMetadata,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.can_search_users_impl(metadata).await
        }
        .map(MustUse::from)
    }

    async fn require_search_users(
        &self,
        metadata: &RequestMetadata,
    ) -> Result<(), RequireServerActionError> {
        let can_search = self.can_search_users(metadata).await?;

        if can_search.into_inner() {
            Ok(())
        } else {
            Err(AuthZServerActionForbidden {
                server_id: self.server_id(),
                action: "search_users".to_string(),
            }
            .into())
        }
    }
}

impl<T> AuthZServerOps for T where T: Authorizer {}
