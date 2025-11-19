use std::sync::Arc;

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CannotInspectPermissions, CatalogWarehouseAction,
            MustUse, UserOrRole,
        },
        Actor, CatalogBackendError, CatalogGetWarehouseByIdError, DatabaseIntegrityError,
        ResolvedWarehouse, WarehouseIdNotFound,
    },
    WarehouseId,
};

const CAN_SEE_PERMISSION: CatalogWarehouseAction = CatalogWarehouseAction::CanUse;

pub trait WarehouseAction
where
    Self: std::fmt::Display + Send + Sync + Copy + From<CatalogWarehouseAction> + PartialEq,
{
}

impl WarehouseAction for CatalogWarehouseAction {}

// --------------------------- Errors ---------------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotUseWarehouseId {
    warehouse_id: WarehouseId,
}
impl AuthZCannotUseWarehouseId {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId) -> Self {
        Self { warehouse_id }
    }
}
impl From<AuthZCannotUseWarehouseId> for ErrorModel {
    fn from(err: AuthZCannotUseWarehouseId) -> Self {
        let AuthZCannotUseWarehouseId { warehouse_id } = err;
        WarehouseIdNotFound::new(warehouse_id)
            .append_detail("Warehouse not found or access denied")
            .into()
    }
}
impl From<AuthZCannotUseWarehouseId> for IcebergErrorResponse {
    fn from(err: AuthZCannotUseWarehouseId) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZWarehouseActionForbidden {
    warehouse_id: WarehouseId,
    action: String,
    actor: Actor,
}
impl AuthZWarehouseActionForbidden {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, action: impl WarehouseAction, actor: Actor) -> Self {
        Self {
            warehouse_id,
            action: action.to_string(),
            actor,
        }
    }
}
impl From<AuthZWarehouseActionForbidden> for ErrorModel {
    fn from(err: AuthZWarehouseActionForbidden) -> Self {
        let AuthZWarehouseActionForbidden {
            warehouse_id,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!(
                "Warehouse action `{action}` forbidden for `{actor}` on warehouse `{warehouse_id}`"
            ),
            "WarehouseActionForbidden",
            None,
        )
    }
}
impl From<AuthZWarehouseActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZWarehouseActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotListNamespaces {
    warehouse_id: WarehouseId,
}
impl AuthZCannotListNamespaces {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId) -> Self {
        Self { warehouse_id }
    }
}

impl From<AuthZCannotListNamespaces> for ErrorModel {
    fn from(err: AuthZCannotListNamespaces) -> Self {
        let AuthZCannotListNamespaces { warehouse_id } = err;
        ErrorModel::builder()
            .r#type("ListNamespacesForbidden".to_string())
            .code(403)
            .message(format!(
                "User is forbidden to list Namespaces in Warehouse with id '{warehouse_id}'"
            ))
            .stack(vec![])
            .build()
    }
}
impl From<AuthZCannotListNamespaces> for IcebergErrorResponse {
    fn from(err: AuthZCannotListNamespaces) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, PartialEq, derive_more::From)]
pub enum AuthZRequireWarehouseUseError {
    CannotUseWarehouseId(AuthZCannotUseWarehouseId),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
}
impl From<AuthZRequireWarehouseUseError> for ErrorModel {
    fn from(err: AuthZRequireWarehouseUseError) -> Self {
        match err {
            AuthZRequireWarehouseUseError::CannotUseWarehouseId(e) => e.into(),
            AuthZRequireWarehouseUseError::AuthorizationBackendUnavailable(e) => e.into(),
        }
    }
}
impl From<AuthZRequireWarehouseUseError> for IcebergErrorResponse {
    fn from(err: AuthZRequireWarehouseUseError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, derive_more::From)]
pub enum RequireWarehouseActionError {
    AuthZWarehouseActionForbidden(AuthZWarehouseActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
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
            BackendUnavailableOrCountMismatch::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<RequireWarehouseActionError> for ErrorModel {
    fn from(err: RequireWarehouseActionError) -> Self {
        match err {
            RequireWarehouseActionError::AuthZWarehouseActionForbidden(e) => e.into(),
            RequireWarehouseActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireWarehouseActionError::AuthorizationCountMismatch(e) => e.into(),
            RequireWarehouseActionError::AuthZCannotUseWarehouseId(e) => e.into(),
            RequireWarehouseActionError::CatalogBackendError(e) => e.into(),
            RequireWarehouseActionError::DatabaseIntegrityError(e) => e.into(),
            RequireWarehouseActionError::CannotInspectPermissions(e) => e.into(),
        }
    }
}
impl From<RequireWarehouseActionError> for IcebergErrorResponse {
    fn from(err: RequireWarehouseActionError) -> Self {
        ErrorModel::from(err).into()
    }
}
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
        warehouse.ok_or_else(|| AuthZCannotUseWarehouseId::new(user_provided_warehouse).into())
    }

    async fn require_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        user_provided_warehouse: WarehouseId,
        warehouse: Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByIdError>,
        action: impl Into<Self::WarehouseAction> + Send,
    ) -> Result<Arc<ResolvedWarehouse>, RequireWarehouseActionError> {
        let action = action.into();
        let actor = metadata.actor();
        let warehouse = warehouse?;
        let cant_see_err = AuthZCannotUseWarehouseId::new(user_provided_warehouse).into();
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
                        (&warehouse, action),
                    ],
                )
                .await?
                .into_inner();
            if can_see {
                is_allowed.then_some(warehouse).ok_or_else(|| {
                    AuthZWarehouseActionForbidden::new(
                        user_provided_warehouse,
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

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        action: impl Into<Self::WarehouseAction> + Send + Sync + Copy,
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
        let [decision] = self
            .are_allowed_warehouse_actions_arr(metadata, for_user, &[(warehouse, action)])
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_warehouse_actions_arr<
        const N: usize,
        A: Into<Self::WarehouseAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, A); N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
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
        A: Into<Self::WarehouseAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }

        if metadata.has_admin_privileges() && for_user.is_none() {
            Ok(vec![true; warehouses_with_actions.len()])
        } else {
            let converted: Vec<(&ResolvedWarehouse, Self::WarehouseAction)> =
                warehouses_with_actions
                    .iter()
                    .map(|(id, action)| (*id, (*action).into()))
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
