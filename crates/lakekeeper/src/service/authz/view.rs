use std::{collections::HashMap, sync::Arc};

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            refresh_warehouse_and_namespace_if_needed, AuthorizationBackendUnavailable,
            AuthorizationCountMismatch, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            BackendUnavailableOrCountMismatch, CannotInspectPermissions, CatalogViewAction,
            MustUse, UserOrRole,
        },
        catalog_store::{
            CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps,
            TabularListFlags,
        },
        Actor, AuthZViewInfo, CatalogBackendError, GetTabularInfoError, InternalParseLocationError,
        InvalidNamespaceIdentifier, NamespaceHierarchy, NamespaceId, NamespaceWithParent,
        ResolvedWarehouse, SerializationError, TabularNotFound, UnexpectedTabularInResponse,
        ViewId, ViewIdentOrId, ViewInfo,
    },
    WarehouseId,
};

const CAN_SEE_PERMISSION: CatalogViewAction = CatalogViewAction::CanGetMetadata;

pub trait ViewAction
where
    Self: std::fmt::Display + Send + Sync + Copy + PartialEq + Eq + From<CatalogViewAction>,
{
}

impl ViewAction for CatalogViewAction {}

// ------------------ Cannot See Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeView {
    warehouse_id: WarehouseId,
    view: ViewIdentOrId,
}
impl AuthZCannotSeeView {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, view: impl Into<ViewIdentOrId>) -> Self {
        Self {
            warehouse_id,
            view: view.into(),
        }
    }
}
impl From<AuthZCannotSeeView> for ErrorModel {
    fn from(err: AuthZCannotSeeView) -> Self {
        let AuthZCannotSeeView { warehouse_id, view } = err;
        TabularNotFound::new(warehouse_id, view)
            .append_detail("View not found or access denied")
            .into()
    }
}
impl From<AuthZCannotSeeView> for IcebergErrorResponse {
    fn from(err: AuthZCannotSeeView) -> Self {
        ErrorModel::from(err).into()
    }
}
// ------------------ Action Forbidden Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZViewActionForbidden {
    warehouse_id: WarehouseId,
    view: ViewIdentOrId,
    action: String,
    actor: Box<Actor>,
}
impl AuthZViewActionForbidden {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        view: impl Into<ViewIdentOrId>,
        action: impl ViewAction,
        actor: Actor,
    ) -> Self {
        Self {
            warehouse_id,
            view: view.into(),
            action: action.to_string(),
            actor: Box::new(actor),
        }
    }
}
impl From<AuthZViewActionForbidden> for ErrorModel {
    fn from(err: AuthZViewActionForbidden) -> Self {
        let AuthZViewActionForbidden {
            warehouse_id,
            view,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!(
                "View action `{action}` forbidden for `{actor}` on view {view} in warehouse `{warehouse_id}`"
            ),
            "ViewActionForbidden",
            None,
        )
    }
}
impl From<AuthZViewActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZViewActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, derive_more::From)]
pub enum RequireViewActionError {
    AuthZViewActionForbidden(AuthZViewActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
    // Hide the existence of the view
    AuthZCannotSeeView(AuthZCannotSeeView),
    // Propagated directly
    CatalogBackendError(CatalogBackendError),
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
    SerializationError(SerializationError),
    UnexpectedTabularInResponse(UnexpectedTabularInResponse),
    InternalParseLocationError(InternalParseLocationError),
}

impl From<BackendUnavailableOrCountMismatch> for RequireViewActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
            BackendUnavailableOrCountMismatch::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<GetTabularInfoError> for RequireViewActionError {
    fn from(err: GetTabularInfoError) -> Self {
        match err {
            GetTabularInfoError::CatalogBackendError(e) => e.into(),
            GetTabularInfoError::InvalidNamespaceIdentifier(e) => e.into(),
            GetTabularInfoError::SerializationError(e) => e.into(),
            GetTabularInfoError::UnexpectedTabularInResponse(e) => e.into(),
            GetTabularInfoError::InternalParseLocationError(e) => e.into(),
        }
    }
}
impl From<RequireViewActionError> for ErrorModel {
    fn from(err: RequireViewActionError) -> Self {
        match err {
            RequireViewActionError::AuthZViewActionForbidden(e) => e.into(),
            RequireViewActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireViewActionError::AuthorizationCountMismatch(e) => e.into(),
            RequireViewActionError::AuthZCannotSeeView(e) => e.into(),
            RequireViewActionError::CatalogBackendError(e) => e.into(),
            RequireViewActionError::InvalidNamespaceIdentifier(e) => e.into(),
            RequireViewActionError::SerializationError(e) => e.into(),
            RequireViewActionError::UnexpectedTabularInResponse(e) => e.into(),
            RequireViewActionError::InternalParseLocationError(e) => e.into(),
            RequireViewActionError::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<RequireViewActionError> for IcebergErrorResponse {
    fn from(err: RequireViewActionError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[async_trait::async_trait]
pub trait AuthZViewOps: Authorizer {
    fn require_view_presence<T: AuthZViewInfo>(
        &self,
        warehouse_id: WarehouseId,
        user_provided_view: impl Into<ViewIdentOrId> + Send,
        view: Result<Option<T>, impl Into<RequireViewActionError> + Send>,
    ) -> Result<T, RequireViewActionError> {
        let view = view.map_err(Into::into)?;
        let Some(view) = view else {
            return Err(AuthZCannotSeeView::new(warehouse_id, user_provided_view).into());
        };
        Ok(view)
    }

    async fn require_view_action<T: AuthZViewInfo>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        user_provided_view: impl Into<ViewIdentOrId> + Send,
        view: Result<Option<T>, impl Into<RequireViewActionError> + Send>,
        action: impl Into<Self::ViewAction> + Send,
    ) -> Result<T, RequireViewActionError> {
        let actor = metadata.actor();
        let warehouse_id = warehouse.warehouse_id;
        // OK to return because this goes via the Into method
        // of RequireViewActionError
        let user_provided_view = user_provided_view.into();
        let view = self.require_view_presence(warehouse_id, user_provided_view.clone(), view)?;
        let view_ident = view.view_ident().clone();

        let cant_see_err = AuthZCannotSeeView::new(warehouse_id, user_provided_view.clone()).into();
        let action = action.into();

        #[cfg(debug_assertions)]
        {
            match &user_provided_view {
                ViewIdentOrId::Id(user_id) => {
                    debug_assert_eq!(
                        *user_id,
                        view.view_id(),
                        "View ID in request ({user_id}) does not match the resolved view ID ({})",
                        view.view_id()
                    );
                }
                ViewIdentOrId::Ident(user_ident) => {
                    debug_assert_eq!(
                        user_ident, view.view_ident(),
                        "View identifier in request ({user_ident}) does not match the resolved view identifier ({})",
                        view.view_ident()
                    );
                }
            }
        }

        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_view_action(metadata, None, warehouse, namespace, &view, action)
                .await?
                .into_inner();
            is_allowed.then_some(view).ok_or(cant_see_err)
        } else {
            let [can_see_view, is_allowed] = self
                .are_allowed_view_actions_arr(
                    metadata,
                    None,
                    warehouse,
                    namespace,
                    &view,
                    &[CAN_SEE_PERMISSION.into(), action],
                )
                .await?
                .into_inner();
            if can_see_view {
                is_allowed.then_some(view).ok_or_else(|| {
                    AuthZViewActionForbidden::new(
                        warehouse_id,
                        view_ident.clone(),
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

    /// Fetches and authorizes a view operation in one call.
    ///
    /// This is a convenience method that combines:
    /// 1. Parallel fetching of warehouse, namespace, and view
    /// 2. Validation of warehouse and namespace presence
    /// 3. Namespace ID consistency check (with TOCTOU protection)
    /// 4. Authorization of the specified action
    ///
    /// # Arguments
    /// * `request_metadata` - The request metadata containing actor information
    /// * `warehouse_id` - The warehouse ID
    /// * `view` - Either a `TableIdent` (name-based) or `ViewId` (UUID-based)
    /// * `view_flags` - Flags to control which views to include (active, staged, deleted)
    /// * `action` - The action to authorize (e.g., `CanDrop`, `CanReadData`, etc.)
    /// * `catalog_state` - The catalog state for database operations
    ///
    /// # Returns
    /// A tuple of `(warehouse, namespace, view)` if all checks pass
    ///
    /// # Errors
    /// Returns `ErrorModel` if:
    /// - Warehouse, namespace, or view not found
    /// - Namespace ID mismatch (TOCTOU race condition)
    /// - User not authorized for the action
    /// - Database or authorization backend errors
    async fn load_and_authorize_view_operation<C>(
        &self,
        request_metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        user_provided_view: impl Into<ViewIdentOrId> + Send,
        view_flags: TabularListFlags,
        action: impl Into<Self::ViewAction> + Send,
        catalog_state: C::State,
    ) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, ViewInfo), ErrorModel>
    where
        C: CatalogStore,
    {
        let user_provided_view = user_provided_view.into();

        // Determine the fetch strategy based on whether we have a ViewId or ViewIdent
        let (warehouse, namespace, view_info) = match &user_provided_view {
            ViewIdentOrId::Id(view_id) => {
                fetch_warehouse_namespace_view_by_id::<C, _>(
                    self,
                    warehouse_id,
                    *view_id,
                    view_flags,
                    catalog_state.clone(),
                )
                .await?
            }
            ViewIdentOrId::Ident(view_ident) => {
                // For ViewIdent: fetch all three in parallel
                let (warehouse_result, namespace_result, view_result) = tokio::join!(
                    C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
                    C::get_namespace(
                        warehouse_id,
                        view_ident.namespace.clone(),
                        catalog_state.clone()
                    ),
                    C::get_view_info(
                        warehouse_id,
                        view_ident.clone(),
                        view_flags,
                        catalog_state.clone()
                    )
                );

                // Validate presence
                let warehouse = self.require_warehouse_presence(warehouse_id, warehouse_result)?;
                let namespace = self.require_namespace_presence(
                    warehouse_id,
                    view_ident.namespace.clone(),
                    namespace_result,
                )?;
                let view_info =
                    self.require_view_presence(warehouse_id, view_ident.clone(), view_result)?;

                (warehouse, namespace, view_info)
            }
        };

        // Validate namespace ID consistency and version (with TOCTOU protection)
        let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _, _>(
            self,
            &warehouse,
            &view_info,
            namespace,
            catalog_state,
            AuthZCannotSeeView::new(warehouse_id, user_provided_view.clone()),
        )
        .await?;

        // Perform authorization check
        let view_info = self
            .require_view_action(
                request_metadata,
                &warehouse,
                &namespace,
                user_provided_view,
                Ok::<_, RequireViewActionError>(Some(view_info)),
                action,
            )
            .await?;

        Ok((warehouse, namespace, view_info))
    }

    async fn is_allowed_view_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        view: &impl AuthZViewInfo,
        action: impl Into<Self::ViewAction> + Send,
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
        let [decision] = self
            .are_allowed_view_actions_arr(
                metadata,
                for_user,
                warehouse,
                namespace,
                view,
                &[action.into()],
            )
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_view_actions_arr<
        const N: usize,
        A: Into<Self::ViewAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace_hierarchy: &NamespaceHierarchy,
        view: &impl AuthZViewInfo,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let actions = actions
            .iter()
            .map(|a| (&namespace_hierarchy.namespace, view, (*a).into()))
            .collect::<Vec<_>>();
        let result = self
            .are_allowed_view_actions_vec(
                metadata,
                for_user,
                warehouse,
                &namespace_hierarchy
                    .parents
                    .iter()
                    .map(|ns| (ns.namespace_id(), ns.clone()))
                    .collect(),
                &actions,
            )
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "view"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_view_actions_vec<A: Into<Self::ViewAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&NamespaceWithParent, &impl AuthZViewInfo, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        #[cfg(debug_assertions)]
        {
            let namespaces: Vec<&NamespaceWithParent> =
                actions.iter().map(|(ns, _, _)| *ns).collect();
            super::table::validate_namespace_hierarchy(&namespaces, parent_namespaces);
        }

        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }

        let warehouse_matches = actions
            .iter()
            .map(|(_, view, _)| {
                let same_warehouse = view.warehouse_id() == warehouse.warehouse_id;
                if !same_warehouse {
                    tracing::warn!(
                        "View warehouse_id `{}` does not match provided warehouse_id `{}`. Denying access.",
                        view.warehouse_id(),
                        warehouse.warehouse_id
                    );
                }
                same_warehouse
            })
            .collect::<Vec<_>>();

        if metadata.has_admin_privileges() && for_user.is_none() {
            Ok(warehouse_matches)
        } else {
            let converted = actions
                .iter()
                .map(|(ns, id, action)| (*ns, *id, (*action).into()))
                .collect::<Vec<_>>();
            let decisions = self
                .are_allowed_view_actions_impl(
                    metadata,
                    for_user,
                    warehouse,
                    parent_namespaces,
                    &converted,
                )
                .await?;

            if decisions.len() != actions.len() {
                return Err(AuthorizationCountMismatch::new(
                    actions.len(),
                    decisions.len(),
                    "view",
                )
                .into());
            }

            let decisions = warehouse_matches
                .iter()
                .zip(decisions.iter())
                .map(|(warehouse_match, authz_allowed)| *warehouse_match && *authz_allowed)
                .collect::<Vec<_>>();

            Ok(decisions)
        }
        .map(MustUse::from)
    }
}

impl<T> AuthZViewOps for T where T: Authorizer {}

pub(crate) async fn fetch_warehouse_namespace_view_by_id<C, A>(
    authorizer: &A,
    warehouse_id: WarehouseId,
    user_provided_view: ViewId,
    table_flags: TabularListFlags,
    catalog_state: C::State,
) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, ViewInfo), ErrorModel>
where
    C: CatalogStore,
    A: AuthzWarehouseOps + AuthzNamespaceOps,
{
    // For TableId: fetch warehouse and table in parallel first
    let (warehouse_result, table_result) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_view_info(
            warehouse_id,
            user_provided_view,
            table_flags,
            catalog_state.clone()
        )
    );

    // Validate warehouse and table presence
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse_result)?;
    let view_info =
        authorizer.require_view_presence(warehouse_id, user_provided_view, table_result)?;

    // Fetch namespace with cache policy to ensure it's at least as fresh as the table
    let namespace_result = C::get_namespace_cache_aware(
        warehouse_id,
        view_info.view_ident().namespace.clone(), // Must fetch via name to ensure consistency. Id is checked later
        CachePolicy::RequireMinimumVersion(*view_info.namespace_version),
        catalog_state.clone(),
    )
    .await;

    let namespace = authorizer.require_namespace_presence(
        warehouse_id,
        view_info.namespace_id,
        namespace_result,
    )?;

    Ok((warehouse, namespace, view_info))
}
