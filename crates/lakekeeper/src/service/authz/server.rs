use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{AuthorizationBackendUnavailable, Authorizer, CatalogServerAction, MustUse},
        Actor, ServerId,
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
}
impl From<RequireServerActionError> for ErrorModel {
    fn from(err: RequireServerActionError) -> Self {
        match err {
            RequireServerActionError::AuthZServerActionForbidden(e) => e.into(),
            RequireServerActionError::AuthorizationBackendUnavailable(e) => e.into(),
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
        action: impl Into<Self::ServerAction> + Send,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_server_action_impl(metadata, action.into())
                .await
        }
        .map(MustUse::from)
    }

    async fn require_server_action(
        &self,
        metadata: &RequestMetadata,
        action: impl Into<Self::ServerAction> + Send,
    ) -> Result<(), RequireServerActionError> {
        let action = action.into();
        if self
            .is_allowed_server_action(metadata, action)
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

    async fn check_actor(&self, actor: &Actor) -> Result<(), ErrorModel> {
        match actor {
            Actor::Principal(_user_id) => Ok(()),
            Actor::Anonymous => Ok(()),
            Actor::Role {
                principal,
                assumed_role,
            } => {
                let assume_role_allowed = self
                    .check_assume_role_impl(principal, *assumed_role)
                    .await?;

                if assume_role_allowed {
                    Ok(())
                } else {
                    Err(ErrorModel::forbidden(
                        format!("Principal is not allowed to assume role with id {assumed_role}"),
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
