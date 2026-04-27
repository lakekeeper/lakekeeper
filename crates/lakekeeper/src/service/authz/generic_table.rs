use std::collections::HashMap;

use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        AuthZGenericTableInfo, CatalogBackendError, GenericTableIdentOrId, GetTabularInfoError,
        InternalParseLocationError, InvalidNamespaceIdentifier, NamespaceHierarchy, NamespaceId,
        NamespaceWithParent, ResolvedWarehouse, SerializationError, TabularNotFound,
        UnexpectedTabularInResponse,
        authz::{
            ActionOnGenericTable, AuthorizationBackendUnavailable, AuthorizationCountMismatch,
            Authorizer, AuthzBadRequest, BackendUnavailableOrCountMismatch,
            CannotInspectPermissions, CatalogAction, CatalogGenericTableAction,
            IsAllowedActionError, MustUse, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource,
            delegate_authorization_failure_source,
        },
    },
};

const CAN_SEE_PERMISSION: CatalogGenericTableAction = CatalogGenericTableAction::GetMetadata;

pub trait GenericTableAction
where
    Self: CatalogAction + Clone + PartialEq + Eq + From<CatalogGenericTableAction>,
{
    /// Whether this action reads or writes generic-table row data (as opposed
    /// to metadata or catalog operations). Used to exclude data-plane actions
    /// from the instance-admin bypass.
    fn is_data_plane(&self) -> bool;
}

impl GenericTableAction for CatalogGenericTableAction {
    fn is_data_plane(&self) -> bool {
        matches!(self, Self::ReadData | Self::WriteData)
    }
}

// ------------------ Cannot See Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeGenericTable {
    warehouse_id: WarehouseId,
    generic_table: GenericTableIdentOrId,
    internal_resource_not_found: bool,
    /// Set when the generic table was accessed via a DEFINER referenced-by chain
    is_delegated_execution: Option<bool>,
}
impl AuthZCannotSeeGenericTable {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        generic_table: impl Into<GenericTableIdentOrId>,
        resource_not_found: bool,
    ) -> Self {
        Self {
            warehouse_id,
            generic_table: generic_table.into(),
            internal_resource_not_found: resource_not_found,
            is_delegated_execution: None,
        }
    }

    #[must_use]
    pub fn new_not_found(
        warehouse_id: WarehouseId,
        generic_table: impl Into<GenericTableIdentOrId>,
    ) -> Self {
        Self::new(warehouse_id, generic_table, true)
    }

    #[must_use]
    pub fn new_forbidden(
        warehouse_id: WarehouseId,
        generic_table: impl Into<GenericTableIdentOrId>,
    ) -> Self {
        Self::new(warehouse_id, generic_table, false)
    }

    #[must_use]
    pub fn with_delegated_execution(mut self, is_delegated: bool) -> Self {
        self.is_delegated_execution = Some(is_delegated);
        self
    }
}
impl AuthorizationFailureSource for AuthZCannotSeeGenericTable {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeGenericTable {
            warehouse_id,
            generic_table,
            internal_resource_not_found: _,
            is_delegated_execution,
        } = self;
        let mut err = TabularNotFound::new(warehouse_id, generic_table);
        if is_delegated_execution == Some(true) {
            err = err
                .append_detail("Access denied during delegated execution via DEFINER view chain");
        }
        err.into()
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.internal_resource_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::CannotSeeResource
        }
    }
}
// ------------------ Action Forbidden Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZGenericTableActionForbidden {
    warehouse_id: WarehouseId,
    generic_table: GenericTableIdentOrId,
    action: String,
}
impl AuthZGenericTableActionForbidden {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        generic_table: impl Into<GenericTableIdentOrId>,
        action: &impl GenericTableAction,
    ) -> Self {
        Self {
            warehouse_id,
            generic_table: generic_table.into(),
            action: action.as_log_str(),
        }
    }
}
impl AuthorizationFailureSource for AuthZGenericTableActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZGenericTableActionForbidden {
            warehouse_id,
            generic_table,
            action,
        } = self;
        ErrorModel::forbidden(
            format!(
                "Generic table action `{action}` forbidden on generic table {generic_table} in warehouse `{warehouse_id}`"
            ),
            "GenericTableActionForbidden",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

#[derive(Debug, derive_more::From)]
pub enum RequireGenericTableActionError {
    AuthZGenericTableActionForbidden(AuthZGenericTableActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizerValidationFailed(AuthzBadRequest),
    AuthZCannotSeeGenericTable(AuthZCannotSeeGenericTable),
    CatalogBackendError(CatalogBackendError),
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
    SerializationError(SerializationError),
    UnexpectedTabularInResponse(UnexpectedTabularInResponse),
    InternalParseLocationError(InternalParseLocationError),
}

impl From<BackendUnavailableOrCountMismatch> for RequireGenericTableActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<IsAllowedActionError> for RequireGenericTableActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}
impl From<GetTabularInfoError> for RequireGenericTableActionError {
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
delegate_authorization_failure_source!(RequireGenericTableActionError => {
    AuthZGenericTableActionForbidden,
    AuthorizationBackendUnavailable,
    AuthorizationCountMismatch,
    CannotInspectPermissions,
    AuthZCannotSeeGenericTable,
    CatalogBackendError,
    InvalidNamespaceIdentifier,
    SerializationError,
    UnexpectedTabularInResponse,
    InternalParseLocationError,
    AuthorizerValidationFailed
});

#[async_trait::async_trait]
pub trait AuthZGenericTableOps: Authorizer {
    fn require_generic_table_presence<T: AuthZGenericTableInfo>(
        &self,
        warehouse_id: WarehouseId,
        user_provided: impl Into<GenericTableIdentOrId> + Send,
        result: Result<Option<T>, impl Into<RequireGenericTableActionError> + Send>,
    ) -> Result<T, RequireGenericTableActionError> {
        let info = result.map_err(Into::into)?;
        let Some(info) = info else {
            return Err(
                AuthZCannotSeeGenericTable::new_not_found(warehouse_id, user_provided).into(),
            );
        };
        Ok(info)
    }

    async fn require_generic_table_action<T: AuthZGenericTableInfo>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        user_provided: impl Into<GenericTableIdentOrId> + Send,
        result: Result<Option<T>, impl Into<RequireGenericTableActionError> + Send>,
        action: impl Into<Self::GenericTableAction> + Send,
    ) -> Result<T, RequireGenericTableActionError> {
        let warehouse_id = warehouse.warehouse_id;
        let user_provided = user_provided.into();
        let info =
            self.require_generic_table_presence(warehouse_id, user_provided.clone(), result)?;
        let ident = info.generic_table_ident().clone();

        #[cfg(debug_assertions)]
        {
            match &user_provided {
                GenericTableIdentOrId::Id(user_id) => {
                    debug_assert_eq!(
                        *user_id,
                        info.generic_table_id(),
                        "Generic table ID in request ({user_id}) does not match the resolved generic table ID ({})",
                        info.generic_table_id()
                    );
                }
                GenericTableIdentOrId::Ident(user_ident) => {
                    debug_assert_eq!(
                        user_ident,
                        info.generic_table_ident(),
                        "Generic table identifier in request ({user_ident}) does not match the resolved generic table identifier ({})",
                        info.generic_table_ident()
                    );
                }
            }
        }

        let cant_see_err =
            AuthZCannotSeeGenericTable::new_forbidden(warehouse_id, user_provided).into();
        let action = action.into();

        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_generic_table_action(
                    metadata, None, warehouse, namespace, &info, action,
                )
                .await?
                .into_inner();
            is_allowed.then_some(info).ok_or(cant_see_err)
        } else {
            let [can_see, is_allowed] = self
                .are_allowed_generic_table_actions_arr(
                    metadata,
                    None,
                    warehouse,
                    namespace,
                    &info,
                    &[CAN_SEE_PERMISSION.into(), action.clone()],
                )
                .await?
                .into_inner();
            if can_see {
                is_allowed.then_some(info).ok_or_else(|| {
                    AuthZGenericTableActionForbidden::new(warehouse_id, ident.clone(), &action)
                        .into()
                })
            } else {
                Err(cant_see_err)
            }
        }
    }

    async fn is_allowed_generic_table_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        info: &impl AuthZGenericTableInfo,
        action: impl Into<Self::GenericTableAction> + Send,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        let [decision] = self
            .are_allowed_generic_table_actions_arr(
                metadata,
                for_user,
                warehouse,
                namespace,
                info,
                &[action.into()],
            )
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_generic_table_actions_arr<
        const N: usize,
        A: GenericTableAction + Into<Self::GenericTableAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace_hierarchy: &NamespaceHierarchy,
        info: &impl AuthZGenericTableInfo,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
        let wrapped = actions
            .iter()
            .map(|a| {
                (
                    &namespace_hierarchy.namespace,
                    ActionOnGenericTable {
                        info,
                        action: a.clone(),
                        user: for_user,
                        is_delegated_execution: false,
                    },
                )
            })
            .collect::<Vec<_>>();
        let result = self
            .are_allowed_generic_table_actions_vec(
                metadata,
                warehouse,
                &namespace_hierarchy
                    .parents
                    .iter()
                    .map(|ns| (ns.namespace_id(), ns.clone()))
                    .collect(),
                &wrapped,
            )
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "generic_table"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_generic_table_actions_vec<
        A: GenericTableAction + Into<Self::GenericTableAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnGenericTable<'_, '_, impl AuthZGenericTableInfo, A>,
        )],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
        #[cfg(debug_assertions)]
        {
            let namespaces: Vec<&NamespaceWithParent> = actions.iter().map(|(ns, _)| *ns).collect();
            super::table::validate_namespace_hierarchy(&namespaces, parent_namespaces);
        }

        let internal = metadata.is_lakekeeper_internal();

        let mut auto_approved: Vec<Option<bool>> = Vec::with_capacity(actions.len());
        let mut actions_to_check = Vec::new();

        for (ns, action) in actions {
            let same_warehouse = action.info.warehouse_id() == warehouse.warehouse_id;
            if !same_warehouse {
                tracing::warn!(
                    "Generic table warehouse_id `{}` does not match provided warehouse_id `{}`. Denying access.",
                    action.info.warehouse_id(),
                    warehouse.warehouse_id
                );
                auto_approved.push(Some(false));
                continue;
            }

            // Normalize user: if it's the actor itself, treat as None (acting as self).
            let normalized_user = if metadata.actor().to_user_or_role().as_ref() == action.user {
                None
            } else {
                action.user
            };

            // `LakekeeperInternal` bypasses all actions including data-plane.
            // Instance admins bypass only non-data-plane actions.
            let bypass = metadata.bypasses_control_plane_authz(normalized_user)
                && (internal || !action.action.is_data_plane());
            if bypass {
                auto_approved.push(Some(true));
            } else {
                auto_approved.push(None);
                let mut normalized_action = action.clone();
                normalized_action.user = normalized_user;
                actions_to_check.push((*ns, normalized_action));
            }
        }

        if actions_to_check.is_empty() {
            Ok(auto_approved.into_iter().map(|v| v.unwrap()).collect())
        } else {
            let decisions = self
                .are_allowed_generic_table_actions_impl(
                    metadata,
                    warehouse,
                    parent_namespaces,
                    &actions_to_check,
                )
                .await?;

            if decisions.len() != actions_to_check.len() {
                return Err(AuthorizationCountMismatch::new(
                    actions_to_check.len(),
                    decisions.len(),
                    "generic_table",
                )
                .into());
            }

            let mut decision_iter = decisions.into_iter();
            let final_decisions: Vec<bool> = auto_approved
                .into_iter()
                .map(|auto| auto.unwrap_or_else(|| decision_iter.next().unwrap()))
                .collect();

            Ok(final_decisions)
        }
        .map(MustUse::from)
    }
}

impl<T> AuthZGenericTableOps for T where T: Authorizer {}
