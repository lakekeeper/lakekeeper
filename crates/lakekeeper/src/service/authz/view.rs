use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CatalogViewAction, MustUse,
        },
        Actor, AuthZViewInfo, CatalogBackendError, GetTabularInfoError, InternalParseLocationError,
        InvalidNamespaceIdentifier, SerializationError, TabularNotFound,
        UnexpectedTabularInResponse, ViewIdentOrId,
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
    actor: Actor,
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
            actor,
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
                "View action `{action}` forbidden for `{actor}` on namespace with {view} in warehouse `{warehouse_id}`"
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
    async fn require_view_action<T: AuthZViewInfo>(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        user_provided_view: impl Into<ViewIdentOrId> + Send,
        view: Result<Option<T>, impl Into<RequireViewActionError> + Send>,
        action: impl Into<Self::ViewAction> + Send,
    ) -> Result<T, RequireViewActionError> {
        let actor = metadata.actor();
        // OK to return because this goes via the Into method
        // of RequireViewActionError
        let view = view.map_err(Into::into)?;
        let Some(view) = view else {
            return Err(AuthZCannotSeeView::new(warehouse_id, user_provided_view).into());
        };
        let view_ident = view.view_ident().clone();
        let user_provided_view = user_provided_view.into();
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
                .is_allowed_view_action(metadata, &view, action)
                .await?
                .into_inner();
            is_allowed.then_some(view).ok_or(cant_see_err)
        } else {
            let [can_see_view, is_allowed] = self
                .are_allowed_view_actions_arr(metadata, &view, &[CAN_SEE_PERMISSION.into(), action])
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

    async fn is_allowed_view_action(
        &self,
        metadata: &RequestMetadata,
        view: &impl AuthZViewInfo,
        action: impl Into<Self::ViewAction> + Send,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_view_action_impl(metadata, view, action.into())
                .await
        }
        .map(MustUse::from)
    }

    async fn are_allowed_view_actions_arr<
        const N: usize,
        A: Into<Self::ViewAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        view: &impl AuthZViewInfo,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let actions = actions
            .iter()
            .map(|a| (view, (*a).into()))
            .collect::<Vec<_>>();
        let result = self
            .are_allowed_view_actions_vec(metadata, &actions)
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
        actions: &[(&impl AuthZViewInfo, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; actions.len()])
        } else {
            let converted = actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect::<Vec<_>>();
            let decisions = self
                .are_allowed_view_actions_impl(metadata, &converted)
                .await?;

            if decisions.len() != actions.len() {
                return Err(AuthorizationCountMismatch::new(
                    actions.len(),
                    decisions.len(),
                    "view",
                )
                .into());
            }

            Ok(decisions)
        }
        .map(MustUse::from)
    }
}

impl<T> AuthZViewOps for T where T: Authorizer {}
