use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        AuthZNamespaceInfo, CachePolicy, CatalogBackendError, CatalogGetNamespaceError,
        CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, InvalidNamespaceIdentifier,
        NamespaceHierarchy, NamespaceId, NamespaceIdentOrId, NamespaceNotFound,
        NamespaceWithParent, ResolvedWarehouse, SerializationError,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            AuthzBadRequest, AuthzWarehouseOps as _, BackendUnavailableOrCountMismatch,
            CannotInspectPermissions, CatalogAction, CatalogNamespaceAction, IsAllowedActionError,
            MustUse, RequireWarehouseActionError, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource, context::UserProvidedNamespace,
            delegate_authorization_failure_source,
        },
    },
};

const CAN_SEE_PERMISSION: CatalogNamespaceAction = CatalogNamespaceAction::GetMetadata;

pub trait NamespaceAction
where
    Self: CatalogAction + Clone + PartialEq + Eq + From<CatalogNamespaceAction>,
{
}

impl NamespaceAction for CatalogNamespaceAction {}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeNamespace {
    warehouse_id: WarehouseId,
    namespace: NamespaceIdentOrId,
    /// Whether the resource was confirmed not to exist (for audit logging)
    /// HTTP response is deliberately ambiguous, but audit log should be concrete
    internal_resource_not_found: bool,
    internal_error_stack: Vec<String>,
}
impl AuthZCannotSeeNamespace {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId>,
        resource_not_found: bool,
        error_stack: Vec<String>,
    ) -> Self {
        Self {
            warehouse_id,
            namespace: namespace.into(),
            internal_resource_not_found: resource_not_found,
            internal_error_stack: error_stack,
        }
    }
    #[must_use]
    pub fn new_not_found(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId>,
    ) -> Self {
        Self::new(warehouse_id, namespace, true, vec![])
    }

    #[must_use]
    pub fn new_forbidden(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId>,
    ) -> Self {
        Self::new(warehouse_id, namespace, false, vec![])
    }
}
impl From<NamespaceNotFound> for AuthZCannotSeeNamespace {
    fn from(err: NamespaceNotFound) -> Self {
        let NamespaceNotFound {
            warehouse_id,
            namespace,
            stack,
        } = err;
        AuthZCannotSeeNamespace {
            warehouse_id,
            namespace,
            internal_resource_not_found: true, // Resource confirmed not to exist
            internal_error_stack: stack,
        }
    }
}
impl AuthorizationFailureSource for AuthZCannotSeeNamespace {
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.internal_resource_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::CannotSeeResource
        }
    }

    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeNamespace {
            warehouse_id,
            namespace,
            internal_resource_not_found: _,
            internal_error_stack: internal_server_stack,
        } = self;
        NamespaceNotFound::new(warehouse_id, namespace)
            .append_detail("Namespace not found or access denied")
            .append_details(internal_server_stack)
            .into()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeAnonymousNamespace {
    warehouse_id: WarehouseId,
    /// Whether the resource was confirmed not to exist (for audit logging)
    /// HTTP response is deliberately ambiguous, but audit log should be concrete
    internal_resource_not_found: bool,
    internal_error_stack: Vec<String>,
}
impl AuthZCannotSeeAnonymousNamespace {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        resource_not_found: bool,
        error_stack: Vec<String>,
    ) -> Self {
        Self {
            warehouse_id,
            internal_resource_not_found: resource_not_found,
            internal_error_stack: error_stack,
        }
    }
    #[must_use]
    pub fn new_not_found(warehouse_id: WarehouseId) -> Self {
        Self::new(warehouse_id, true, vec![])
    }

    #[must_use]
    pub fn new_forbidden(warehouse_id: WarehouseId) -> Self {
        Self::new(warehouse_id, false, vec![])
    }
}
impl AuthorizationFailureSource for AuthZCannotSeeAnonymousNamespace {
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.internal_resource_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::CannotSeeResource
        }
    }

    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeAnonymousNamespace {
            internal_resource_not_found: _,
            internal_error_stack: internal_server_stack,
            ..
        } = self;
        ErrorModel::builder()
            .r#type("NoSuchNamespaceException")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message("Namespace not found or access denied")
            .stack(internal_server_stack)
            .build()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZNamespaceActionForbidden {
    warehouse_id: WarehouseId,
    namespace: NamespaceIdentOrId,
    action: String,
}
impl AuthZNamespaceActionForbidden {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId>,
        action: &impl NamespaceAction,
    ) -> Self {
        Self {
            warehouse_id,
            namespace: namespace.into(),
            action: action.as_log_str(),
        }
    }
}

impl AuthorizationFailureSource for AuthZNamespaceActionForbidden {
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
    fn into_error_model(self) -> ErrorModel {
        let AuthZNamespaceActionForbidden {
            warehouse_id,
            namespace,
            action,
        } = self;
        ErrorModel::forbidden(
            format!(
                "Namespace action `{action}` forbidden on namespace with {namespace} in warehouse `{warehouse_id}`"
            ),
            "NamespaceActionForbidden",
            None,
        )
    }
}

#[derive(Debug, derive_more::From)]
pub enum RequireNamespaceActionError {
    AuthZNamespaceActionForbidden(AuthZNamespaceActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizerValidationFailed(AuthzBadRequest),
    // Hide the existence of the namespace
    AuthZCannotSeeNamespace(AuthZCannotSeeNamespace),
    AuthZCannotSeeAnonymousNamespace(AuthZCannotSeeAnonymousNamespace),
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
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
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
            CatalogGetNamespaceError::SerializationError(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireNamespaceActionError => {
    AuthZCannotSeeNamespace,
    AuthZCannotSeeAnonymousNamespace,
    AuthZNamespaceActionForbidden,
    AuthorizationBackendUnavailable,
    AuthorizerValidationFailed,
    CannotInspectPermissions,
    AuthorizationCountMismatch,
    CatalogBackendError,
    InvalidNamespaceIdentifier,
    SerializationError,
});

#[derive(Debug, derive_more::From)]
pub enum LoadAndAuthorizeNamespaceError {
    RequireWarehouseActionError(RequireWarehouseActionError),
    RequireNamespaceActionError(RequireNamespaceActionError),
}
delegate_authorization_failure_source!(LoadAndAuthorizeNamespaceError => {
    RequireWarehouseActionError,
    RequireNamespaceActionError,
});

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
            AuthZCannotSeeNamespace::new_not_found(warehouse_id, user_provided_namespace).into();
        let Some(namespace) = namespace else {
            return Err(cant_see_err);
        };
        Ok(namespace)
    }

    async fn load_and_authorize_namespace_action<C: CatalogStore>(
        &self,
        request_metadata: &RequestMetadata,
        namespace: UserProvidedNamespace,
        action: impl Into<Self::NamespaceAction> + Send,
        cache_policy: CachePolicy,
        catalog_state: C::State,
    ) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy), LoadAndAuthorizeNamespaceError> {
        let warehouse_id = namespace.warehouse_id;
        let namespace_ident_or_id = namespace.namespace.clone();
        let action = action.into();

        let (warehouse, namespace) = tokio::join!(
            C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
            C::get_namespace_cache_aware(
                warehouse_id,
                namespace_ident_or_id.clone(),
                cache_policy,
                catalog_state.clone()
            )
        );
        let warehouse = self.require_warehouse_presence(warehouse_id, warehouse)?;

        let namespace = self
            .require_namespace_action(
                request_metadata,
                &warehouse,
                namespace_ident_or_id,
                namespace,
                action,
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
        // OK to return because this goes via the Into method
        // of RequireNamespaceActionError
        let user_provided_namespace = user_provided_namespace.into();
        let namespace = self.require_namespace_presence(
            warehouse.warehouse_id,
            user_provided_namespace.clone(),
            namespace,
        )?;
        let cant_see_err = AuthZCannotSeeNamespace::new_forbidden(
            warehouse.warehouse_id,
            user_provided_namespace.clone(),
        )
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

        let namespace_authz_context = namespace.namespace.clone();
        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_namespace_action(
                    metadata,
                    None,
                    warehouse,
                    &namespace.parents,
                    &namespace_authz_context,
                    action,
                )
                .await?
                .into_inner();
            is_allowed.then_some(namespace).ok_or(cant_see_err)
        } else {
            let parents_map = namespace
                .parents
                .iter()
                .map(|ns| (ns.namespace_id(), ns.clone()))
                .collect();
            let [can_see_namespace, is_allowed] = self
                .are_allowed_namespace_actions_arr(
                    metadata,
                    None,
                    warehouse,
                    &parents_map,
                    &[
                        (&namespace_authz_context, CAN_SEE_PERMISSION.into()),
                        (&namespace_authz_context, action.clone()),
                    ],
                )
                .await?
                .into_inner();
            if can_see_namespace {
                is_allowed.then_some(namespace).ok_or_else(|| {
                    AuthZNamespaceActionForbidden::new(
                        warehouse.warehouse_id,
                        namespace_name.clone(),
                        &action,
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
        parent_namespaces: &[NamespaceWithParent],
        namespace: &impl AuthZNamespaceInfo,
        action: impl Into<Self::NamespaceAction> + Send + Sync + Clone,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        if namespace.warehouse_id() != warehouse.warehouse_id {
            tracing::debug!(
                "Namespace warehouse_id `{}` does not match provided warehouse_id `{}`. Denying access.",
                namespace.warehouse_id(),
                warehouse.warehouse_id
            );
            return Ok(MustUse::from(false));
        }
        let namespace_parents_map = parent_namespaces
            .iter()
            .map(|ns| (ns.namespace_id(), ns.clone()))
            .collect();
        let [decision] = self
            .are_allowed_namespace_actions_arr(
                metadata,
                for_user,
                warehouse,
                &namespace_parents_map,
                &[(namespace, action)],
            )
            .await?
            .into_inner();

        Ok(MustUse::from(decision))
    }

    async fn are_allowed_namespace_actions_arr<
        const N: usize,
        A: Into<Self::NamespaceAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, A); N],
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
        let result = self
            .are_allowed_namespace_actions_vec(
                metadata,
                for_user,
                warehouse,
                parent_namespaces,
                actions,
            )
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "namespace"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_namespace_actions_vec<
        A: Into<Self::NamespaceAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, A)],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
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
                .map(|(id, action)| (*id, action.clone().into()))
                .collect::<Vec<_>>();
            let authz_results = self
                .are_allowed_namespace_actions_impl(
                    metadata,
                    for_user,
                    warehouse,
                    parent_namespaces,
                    &converted,
                )
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
