use std::sync::Arc;

use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        CatalogBackendError, CatalogGetWarehouseByIdError, DatabaseIntegrityError,
        ResolvedWarehouse, WarehouseIdNotFound,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            AuthzBadRequest, BackendUnavailableOrCountMismatch, CannotInspectPermissions,
            CatalogAction, CatalogWarehouseAction, IsAllowedActionError, MustUse, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource,
            delegate_authorization_failure_source,
        },
    },
};

const CAN_SEE_PERMISSION: CatalogWarehouseAction = CatalogWarehouseAction::Use;

pub trait WarehouseAction
where
    Self: CatalogAction + Clone + From<CatalogWarehouseAction> + Eq + PartialEq,
{
}

impl WarehouseAction for CatalogWarehouseAction {}

// --------------------------- Errors ---------------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotListAllTasks {
    warehouse_id: WarehouseId,
}
impl AuthZCannotListAllTasks {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId) -> Self {
        Self { warehouse_id }
    }
}
impl AuthorizationFailureSource for AuthZCannotListAllTasks {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotListAllTasks { warehouse_id } = self;
        ErrorModel::forbidden(
            format!(
                "Not authorized to see all tasks in Warehouse with id {warehouse_id}. Add the `entity` filter to query tasks for specific entities."
            ),
            "WarehouseListTasksForbidden",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotUseWarehouseId {
    warehouse_id: WarehouseId,
    resource_not_found: bool,
}
impl AuthZCannotUseWarehouseId {
    #[must_use]
    fn new(warehouse_id: WarehouseId, resource_not_found: bool) -> Self {
        Self {
            warehouse_id,
            resource_not_found,
        }
    }

    #[must_use]
    pub fn new_not_found(warehouse_id: WarehouseId) -> Self {
        Self {
            warehouse_id,
            resource_not_found: true,
        }
    }

    #[must_use]
    pub fn new_access_denied(warehouse_id: WarehouseId) -> Self {
        Self {
            warehouse_id,
            resource_not_found: false,
        }
    }
}
impl AuthorizationFailureSource for AuthZCannotUseWarehouseId {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotUseWarehouseId {
            warehouse_id,
            resource_not_found: _, // Hidden in ErrorModel, present in FailureReason
        } = self;
        WarehouseIdNotFound::new(warehouse_id)
            .append_detail("Warehouse not found or access denied")
            .into()
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.resource_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::ActionForbidden
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZWarehouseActionForbidden {
    warehouse_id: WarehouseId,
    action: String,
}
impl AuthZWarehouseActionForbidden {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, action: &impl WarehouseAction) -> Self {
        Self {
            warehouse_id,
            action: action.as_log_str(),
        }
    }
}
impl AuthorizationFailureSource for AuthZWarehouseActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZWarehouseActionForbidden {
            warehouse_id,
            action,
        } = self;
        ErrorModel::forbidden(
            format!("Warehouse action `{action}` forbidden on warehouse `{warehouse_id}`"),
            "WarehouseActionForbidden",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

#[derive(Debug, PartialEq, derive_more::From)]
pub enum AuthZRequireWarehouseUseError {
    CannotUseWarehouseId(AuthZCannotUseWarehouseId),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
}
delegate_authorization_failure_source!(AuthZRequireWarehouseUseError => {
    CannotUseWarehouseId,
    AuthorizationBackendUnavailable,
});

#[derive(Debug, derive_more::From)]
pub enum RequireWarehouseActionError {
    AuthZWarehouseActionForbidden(AuthZWarehouseActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthZCannotListAllTasks(AuthZCannotListAllTasks),
    AuthorizerValidationFailed(AuthzBadRequest),
    // Hide the existence of the namespace
    AuthZCannotUseWarehouseId(AuthZCannotUseWarehouseId),
    // Propagated directly
    CatalogBackendError(CatalogBackendError),
    DatabaseIntegrityError(DatabaseIntegrityError),
}

impl From<BackendUnavailableOrCountMismatch> for RequireWarehouseActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<IsAllowedActionError> for RequireWarehouseActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireWarehouseActionError => {
    AuthZWarehouseActionForbidden,
    AuthorizationBackendUnavailable,
    AuthorizationCountMismatch,
    CannotInspectPermissions,
    AuthZCannotUseWarehouseId,
    CatalogBackendError,
    DatabaseIntegrityError,
    AuthZCannotListAllTasks,
    AuthorizerValidationFailed
});

impl From<CatalogGetWarehouseByIdError> for RequireWarehouseActionError {
    fn from(err: CatalogGetWarehouseByIdError) -> Self {
        match err {
            CatalogGetWarehouseByIdError::CatalogBackendError(e) => e.into(),
            CatalogGetWarehouseByIdError::DatabaseIntegrityError(e) => e.into(),
        }
    }
}

#[async_trait::async_trait]
pub trait AuthzWarehouseOps: Authorizer {
    fn require_warehouse_presence(
        &self,
        user_provided_warehouse: WarehouseId,
        warehouse: Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByIdError>,
    ) -> Result<Arc<ResolvedWarehouse>, RequireWarehouseActionError> {
        let warehouse = warehouse?;
        let warehouse_not_found = warehouse.is_none();
        warehouse.ok_or_else(|| {
            AuthZCannotUseWarehouseId::new(user_provided_warehouse, warehouse_not_found).into()
        })
    }

    async fn require_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        user_provided_warehouse: WarehouseId,
        warehouse: Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByIdError>,
        action: impl Into<Self::WarehouseAction> + Send,
    ) -> Result<Arc<ResolvedWarehouse>, RequireWarehouseActionError> {
        let action = action.into();
        let warehouse = warehouse?;
        let cant_see_err =
            AuthZCannotUseWarehouseId::new(user_provided_warehouse, warehouse.is_none()).into();
        let Some(warehouse) = warehouse else {
            return Err(cant_see_err);
        };
        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_warehouse_action(metadata, None, &warehouse, action)
                .await?
                .into_inner();
            is_allowed.then_some(warehouse).ok_or(cant_see_err)
        } else {
            let [can_see, is_allowed] = self
                .are_allowed_warehouse_actions_arr(
                    metadata,
                    None,
                    &[
                        (&warehouse, CAN_SEE_PERMISSION.into()),
                        (&warehouse, action.clone()),
                    ],
                )
                .await?
                .into_inner();
            if can_see {
                is_allowed.then_some(warehouse).ok_or_else(|| {
                    AuthZWarehouseActionForbidden::new(user_provided_warehouse, &action).into()
                })
            } else {
                return Err(cant_see_err);
            }
        }
    }

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        action: impl Into<Self::WarehouseAction> + Clone + Send + Sync,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        let [decision] = self
            .are_allowed_warehouse_actions_arr(metadata, for_user, &[(warehouse, action)])
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_warehouse_actions_arr<
        const N: usize,
        A: Into<Self::WarehouseAction> + Clone + Send + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, A); N],
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
        let result = self
            .are_allowed_warehouse_actions_vec(metadata, for_user, warehouses_with_actions)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "warehouse"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_warehouse_actions_vec<
        A: Into<Self::WarehouseAction> + Clone + Send + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, A)],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }

        if metadata.has_admin_privileges() && for_user.is_none() {
            Ok(vec![true; warehouses_with_actions.len()])
        } else {
            let converted: Vec<(&ResolvedWarehouse, Self::WarehouseAction)> =
                warehouses_with_actions
                    .iter()
                    .map(|(id, action)| (*id, action.clone().into()))
                    .collect();
            let decisions = self
                .are_allowed_warehouse_actions_impl(metadata, for_user, &converted)
                .await?;

            if decisions.len() != warehouses_with_actions.len() {
                return Err(AuthorizationCountMismatch::new(
                    warehouses_with_actions.len(),
                    decisions.len(),
                    "warehouse",
                )
                .into());
            }

            Ok(decisions)
        }
        .map(MustUse::from)
    }
}

impl<T> AuthzWarehouseOps for T where T: Authorizer {}
