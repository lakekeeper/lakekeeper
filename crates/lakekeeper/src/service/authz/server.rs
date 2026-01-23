use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        Actor, ServerId,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CannotInspectPermissions, CatalogServerAction,
            MustUse, UserOrRole,
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
    actor: Actor,
}
impl AuthZServerActionForbidden {
    #[must_use]
    pub fn new(server_id: ServerId, action: impl ServerAction, actor: Actor) -> Self {
        Self {
            server_id,
            action: action.to_string(),
            actor,
        }
    }
}
impl From<AuthZServerActionForbidden> for ErrorModel {
    fn from(err: AuthZServerActionForbidden) -> Self {
        let AuthZServerActionForbidden {
            server_id,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!("Server action `{action}` forbidden for {actor} on server `{server_id}`",),
            "ServerActionForbidden",
            None,
        )
    }
}
impl From<AuthZServerActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZServerActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireServerActionError {
    AuthZServerActionForbidden(AuthZServerActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
}
impl From<BackendUnavailableOrCountMismatch> for RequireServerActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
            BackendUnavailableOrCountMismatch::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<RequireServerActionError> for ErrorModel {
    fn from(err: RequireServerActionError) -> Self {
        match err {
            RequireServerActionError::AuthZServerActionForbidden(e) => e.into(),
            RequireServerActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireServerActionError::CannotInspectPermissions(e) => e.into(),
            RequireServerActionError::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<RequireServerActionError> for IcebergErrorResponse {
    fn from(err: RequireServerActionError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Server Ops ---------------------------

#[async_trait::async_trait]
pub trait AuthZServerOps: Authorizer {
    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        action: impl Into<Self::ServerAction> + Send + Sync + Copy,
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
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
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
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
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
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
            Err(
                AuthZServerActionForbidden::new(self.server_id(), action, metadata.actor().clone())
                    .into(),
            )
        }
    }

    async fn check_actor(
        &self,
        actor: &Actor,
        request_metadata: &RequestMetadata,
    ) -> Result<(), ErrorModel> {
        match actor {
            Actor::Principal(_user_id) => Ok(()),
            Actor::Anonymous => Ok(()),
            Actor::Role {
                principal,
                assumed_role,
            } => {
                let assume_role_allowed = self
                    .check_assume_role_impl(principal, *assumed_role, request_metadata)
                    .await?;

                if assume_role_allowed {
                    Ok(())
                } else {
                    Err(ErrorModel::forbidden(
                        format!(
                            "Actor `{principal}` is not allowed to assume role `{assumed_role}`"
                        ),
                        "RoleAssumptionNotAllowed",
                        None,
                    ))
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
                actor: metadata.actor().clone(),
            }
            .into())
        }
    }
}

impl<T> AuthZServerOps for T where T: Authorizer {}
