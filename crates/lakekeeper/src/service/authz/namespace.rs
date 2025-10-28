use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CatalogNamespaceAction, MustUse,
        },
        Actor, CatalogBackendError, CatalogGetNamespaceError, InvalidNamespaceIdentifier,
        Namespace, NamespaceIdentOrId, NamespaceNotFound,
    },
    WarehouseId,
};

const CAN_SEE_PERMISSION: CatalogNamespaceAction = CatalogNamespaceAction::CanGetMetadata;

pub trait NamespaceAction
where
    Self: std::fmt::Display + Send + Sync + Copy + PartialEq + Eq + From<CatalogNamespaceAction>,
{
}

impl NamespaceAction for CatalogNamespaceAction {}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeNamespace {
    warehouse_id: WarehouseId,
    namespace: NamespaceIdentOrId,
}
impl AuthZCannotSeeNamespace {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, namespace: impl Into<NamespaceIdentOrId>) -> Self {
        Self {
            warehouse_id,
            namespace: namespace.into(),
        }
    }
}
impl From<NamespaceNotFound> for AuthZCannotSeeNamespace {
    fn from(err: NamespaceNotFound) -> Self {
        // Deliberately discard the stack trace to avoid leaking
        // information about the existence of the namespace.
        AuthZCannotSeeNamespace {
            warehouse_id: err.warehouse_id,
            namespace: err.namespace,
        }
    }
}
impl From<AuthZCannotSeeNamespace> for ErrorModel {
    fn from(err: AuthZCannotSeeNamespace) -> Self {
        let AuthZCannotSeeNamespace {
            warehouse_id,
            namespace,
        } = err;
        NamespaceNotFound::new(warehouse_id, namespace)
            .append_detail("Namespace not found or access denied")
            .into()
    }
}
impl From<AuthZCannotSeeNamespace> for IcebergErrorResponse {
    fn from(err: AuthZCannotSeeNamespace) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZNamespaceActionForbidden {
    warehouse_id: WarehouseId,
    namespace: NamespaceIdentOrId,
    action: String,
    actor: Actor,
}
impl AuthZNamespaceActionForbidden {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId>,
        action: impl NamespaceAction,
        actor: Actor,
    ) -> Self {
        Self {
            warehouse_id,
            namespace: namespace.into(),
            action: action.to_string(),
            actor,
        }
    }
}
impl From<AuthZNamespaceActionForbidden> for ErrorModel {
    fn from(err: AuthZNamespaceActionForbidden) -> Self {
        let AuthZNamespaceActionForbidden {
            warehouse_id,
            namespace,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!(
                "Namespace action `{action}` forbidden for {actor} on namespace with `{namespace}` in warehouse `{warehouse_id}`"
            ),
            "NamespaceActionForbidden",
            None,
        )
    }
}
impl From<AuthZNamespaceActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZNamespaceActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, derive_more::From)]
pub enum RequireNamespaceActionError {
    AuthZNamespaceActionForbidden(AuthZNamespaceActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    // Hide the existence of the namespace
    AuthZCannotSeeNamespace(AuthZCannotSeeNamespace),
    // Propagated directly
    CatalogBackendError(CatalogBackendError),
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
}
impl From<BackendUnavailableOrCountMismatch> for RequireNamespaceActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<CatalogGetNamespaceError> for RequireNamespaceActionError {
    fn from(err: CatalogGetNamespaceError) -> Self {
        match err {
            CatalogGetNamespaceError::CatalogBackendError(e) => e.into(),
            CatalogGetNamespaceError::InvalidNamespaceIdentifier(e) => e.into(),
        }
    }
}
impl From<RequireNamespaceActionError> for ErrorModel {
    fn from(err: RequireNamespaceActionError) -> Self {
        match err {
            RequireNamespaceActionError::AuthZCannotSeeNamespace(e) => e.into(),
            RequireNamespaceActionError::CatalogBackendError(e) => e.into(),
            RequireNamespaceActionError::InvalidNamespaceIdentifier(e) => e.into(),
            RequireNamespaceActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireNamespaceActionError::AuthZNamespaceActionForbidden(e) => e.into(),
            RequireNamespaceActionError::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<RequireNamespaceActionError> for IcebergErrorResponse {
    fn from(err: RequireNamespaceActionError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[async_trait::async_trait]
pub trait AuthzNamespaceOps: Authorizer {
    async fn require_namespace_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        user_provided_namespace: impl Into<NamespaceIdentOrId> + Send,
        namespace: Result<Option<Namespace>, CatalogGetNamespaceError>,
        action: impl Into<Self::NamespaceAction> + Send,
    ) -> Result<Namespace, RequireNamespaceActionError> {
        let actor = metadata.actor();
        // OK to return because this goes via the Into method
        // of RequireNamespaceActionError
        let namespace = namespace?;
        let Some(namespace) = namespace else {
            return Err(
                AuthZCannotSeeNamespace::new(warehouse_id, user_provided_namespace.into()).into(),
            );
        };
        let namespace_name = namespace.namespace_ident.clone();
        let user_provided_namespace = user_provided_namespace.into();
        let cant_see_err =
            AuthZCannotSeeNamespace::new(warehouse_id, user_provided_namespace.clone()).into();
        let action = action.into();

        #[cfg(debug_assertions)]
        {
            match &user_provided_namespace {
                NamespaceIdentOrId::Id(id) => {
                    assert_eq!(
                        *id, namespace.namespace_id,
                        "Mismatched namespace ID: user provided {id}, got {namespace:?}"
                    );
                }
                NamespaceIdentOrId::Name(ident) => {
                    assert_eq!(
                        ident, &namespace.namespace_ident,
                        "Mismatched namespace ident: user provided {ident}, got {namespace:?}"
                    );
                }
            }
        }

        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_namespace_action(metadata, &namespace, action)
                .await?
                .into_inner();
            is_allowed.then_some(namespace).ok_or(cant_see_err)
        } else {
            let [can_see_namespace, is_allowed] = self
                .are_allowed_namespace_actions_arr(
                    metadata,
                    &namespace,
                    &[CAN_SEE_PERMISSION.into(), action],
                )
                .await?
                .into_inner();
            if can_see_namespace {
                is_allowed.then_some(namespace).ok_or_else(|| {
                    AuthZNamespaceActionForbidden::new(
                        warehouse_id,
                        namespace_name.clone(),
                        action,
                        actor.clone(),
                    )
                    .into()
                })
            } else {
                return Err(cant_see_err);
            }
        }
    }

    async fn is_allowed_namespace_action(
        &self,
        metadata: &RequestMetadata,
        namespace: &Namespace,
        action: impl Into<Self::NamespaceAction> + Send,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_namespace_action_impl(metadata, namespace, action.into())
                .await
        }
        .map(MustUse::from)
    }

    async fn are_allowed_namespace_actions_arr<
        const N: usize,
        A: Into<Self::NamespaceAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        namespace: &Namespace,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let actions = actions
            .iter()
            .map(|a| (namespace, (*a).into()))
            .collect::<Vec<_>>();
        let result = self
            .are_allowed_namespace_actions_vec(metadata, &actions)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "namespace"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_namespace_actions_vec<
        A: Into<Self::NamespaceAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        actions: &[(&Namespace, A)],
    ) -> Result<MustUse<Vec<bool>>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; actions.len()])
        } else {
            let converted = actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect::<Vec<_>>();
            self.are_allowed_namespace_actions_impl(metadata, &converted)
                .await
        }
        .map(MustUse::from)
    }
}

impl<T> AuthzNamespaceOps for T where T: Authorizer {}
