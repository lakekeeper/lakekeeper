use std::sync::Arc;

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            AuthzWarehouseOps as _, BackendUnavailableOrCountMismatch, CannotInspectPermissions,
            CatalogNamespaceAction, IsAllowedActionError, MustUse, UserOrRole,
        },
        Actor, CachePolicy, CatalogBackendError, CatalogGetNamespaceError, CatalogNamespaceOps,
        CatalogStore, CatalogWarehouseOps, InvalidNamespaceIdentifier, NamespaceHierarchy,
        NamespaceIdentOrId, NamespaceNotFound, ResolvedWarehouse, SerializationError,
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
    actor: Box<Actor>,
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
            actor: Box::new(actor),
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
    CannotInspectPermissions(CannotInspectPermissions),
    // Hide the existence of the namespace
    AuthZCannotSeeNamespace(AuthZCannotSeeNamespace),
    // Propagated directly
    CatalogBackendError(CatalogBackendError),
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
    SerializationError(SerializationError),
}
impl From<IsAllowedActionError> for RequireNamespaceActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<BackendUnavailableOrCountMismatch> for RequireNamespaceActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
            BackendUnavailableOrCountMismatch::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<CatalogGetNamespaceError> for RequireNamespaceActionError {
    fn from(err: CatalogGetNamespaceError) -> Self {
        match err {
            CatalogGetNamespaceError::CatalogBackendError(e) => e.into(),
            CatalogGetNamespaceError::InvalidNamespaceIdentifier(e) => e.into(),
            CatalogGetNamespaceError::SerializationError(e) => e.into(),
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
            RequireNamespaceActionError::SerializationError(e) => e.into(),
            RequireNamespaceActionError::CannotInspectPermissions(e) => e.into(),
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
    fn require_namespace_presence(
        &self,
        warehouse_id: WarehouseId,
        user_provided_namespace: impl Into<NamespaceIdentOrId> + Send,
        namespace: Result<Option<NamespaceHierarchy>, CatalogGetNamespaceError>,
    ) -> Result<NamespaceHierarchy, RequireNamespaceActionError> {
        let namespace = namespace?;
        let user_provided_namespace = user_provided_namespace.into();
        let cant_see_err =
            AuthZCannotSeeNamespace::new(warehouse_id, user_provided_namespace).into();
        let Some(namespace) = namespace else {
            return Err(cant_see_err);
        };
        Ok(namespace)
    }

    async fn load_and_authorize_namespace_action<C: CatalogStore>(
        &self,
        request_metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId> + Send,
        action: impl Into<Self::NamespaceAction> + Send,
        cache_policy: CachePolicy,
        catalog_state: C::State,
    ) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy), ErrorModel> {
        let provided_namespace = namespace.into();
        let (warehouse, namespace) = tokio::join!(
            C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
            C::get_namespace_cache_aware(
                warehouse_id,
                provided_namespace.clone(),
                cache_policy,
                catalog_state.clone()
            )
        );
        let warehouse = self.require_warehouse_presence(warehouse_id, warehouse)?;

        let namespace = self
            .require_namespace_action(
                request_metadata,
                &warehouse,
                provided_namespace,
                namespace,
                action.into(),
            )
            .await?;

        Ok((warehouse, namespace))
    }

    async fn require_namespace_action(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        user_provided_namespace: impl Into<NamespaceIdentOrId> + Send,
        namespace: Result<Option<NamespaceHierarchy>, CatalogGetNamespaceError>,
        action: impl Into<Self::NamespaceAction> + Send,
    ) -> Result<NamespaceHierarchy, RequireNamespaceActionError> {
        let actor = metadata.actor();
        // OK to return because this goes via the Into method
        // of RequireNamespaceActionError
        let user_provided_namespace = user_provided_namespace.into();
        let namespace = self.require_namespace_presence(
            warehouse.warehouse_id,
            user_provided_namespace.clone(),
            namespace,
        )?;
        let cant_see_err =
            AuthZCannotSeeNamespace::new(warehouse.warehouse_id, user_provided_namespace.clone())
                .into();

        let namespace_name = namespace.namespace_ident().clone();

        let action = action.into();

        #[cfg(debug_assertions)]
        {
            match &user_provided_namespace {
                NamespaceIdentOrId::Id(id) => {
                    assert_eq!(
                        *id,
                        namespace.namespace_id(),
                        "Mismatched namespace ID: user provided {id}, got {namespace:?}"
                    );
                }
                NamespaceIdentOrId::Name(ident) => {
                    assert_eq!(
                        ident,
                        namespace.namespace_ident(),
                        "Mismatched namespace ident: user provided {ident}, got {namespace:?}"
                    );
                }
            }
        }

        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_namespace_action(metadata, None, warehouse, &namespace, action)
                .await?
                .into_inner();
            is_allowed.then_some(namespace).ok_or(cant_see_err)
        } else {
            let [can_see_namespace, is_allowed] = self
                .are_allowed_namespace_actions_arr(
                    metadata,
                    None,
                    warehouse,
                    &namespace,
                    &[CAN_SEE_PERMISSION.into(), action],
                )
                .await?
                .into_inner();
            if can_see_namespace {
                is_allowed.then_some(namespace).ok_or_else(|| {
                    AuthZNamespaceActionForbidden::new(
                        warehouse.warehouse_id,
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
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        action: impl Into<Self::NamespaceAction> + Send + Sync + Copy,
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
        if namespace.warehouse_id() != warehouse.warehouse_id {
            tracing::debug!(
                "Namespace warehouse_id `{}` does not match provided warehouse_id `{}`. Denying access.",
                namespace.warehouse_id(),
                warehouse.warehouse_id
            );
            return Ok(MustUse::from(false));
        }

        let [decision] = self
            .are_allowed_namespace_actions_arr(metadata, for_user, warehouse, namespace, &[action])
            .await?
            .into_inner();

        Ok(MustUse::from(decision))
    }

    async fn are_allowed_namespace_actions_arr<
        const N: usize,
        A: Into<Self::NamespaceAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let actions = actions
            .iter()
            .map(|a| (namespace, (*a).into()))
            .collect::<Vec<_>>();
        let result = self
            .are_allowed_namespace_actions_vec(metadata, for_user, warehouse, &actions)
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
        mut for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        actions: &[(&NamespaceHierarchy, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }
        // First check warehouse_id for all namespaces
        let warehouse_matches: Vec<bool> = actions
            .iter()
            .map(|(ns, _)| {
                let same_warehouse = ns.warehouse_id() == warehouse.warehouse_id;
                if !same_warehouse {
                    tracing::warn!(
                        "Namespace warehouse_id `{}` does not match provided warehouse_id `{}`. Denying access.",
                        ns.warehouse_id(),
                        warehouse.warehouse_id
                    );
                }
            same_warehouse
        })
            .collect();

        if metadata.has_admin_privileges() && for_user.is_none() {
            Ok(warehouse_matches)
        } else {
            let converted = actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect::<Vec<_>>();
            let authz_results = self
                .are_allowed_namespace_actions_impl(metadata, for_user, warehouse, &converted)
                .await?;

            if warehouse_matches.len() != actions.len() {
                return Err(AuthorizationCountMismatch::new(
                    actions.len(),
                    warehouse_matches.len(),
                    "namespace",
                )
                .into());
            }

            // Combine warehouse check with authorization check (both must be true)
            let results = warehouse_matches
                .iter()
                .zip(authz_results.iter())
                .map(|(warehouse_match, authz_allowed)| *warehouse_match && *authz_allowed)
                .collect::<Vec<_>>();

            Ok(results)
        }
        .map(MustUse::from)
    }
}

impl<T> AuthzNamespaceOps for T where T: Authorizer {}
