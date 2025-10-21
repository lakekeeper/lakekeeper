use std::collections::{HashMap, HashSet};

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools as _;

use crate::{
    api::RequestMetadata,
    service::{
        authz::{
            AuthZViewActionForbidden, AuthZViewOps, AuthorizationBackendUnavailable,
            AuthorizationCountMismatch, Authorizer, BackendUnavailableOrCountMismatch,
            CatalogTableAction, MustUse,
        },
        Actor, AuthZTableInfo, AuthZViewInfo, CatalogBackendError, GetTabularInfoByLocationError,
        GetTabularInfoError, InternalParseLocationError, InvalidNamespaceIdentifier,
        SerializationError, TableId, TableIdentOrId, TabularNotFound, UnexpectedTabularInResponse,
    },
    WarehouseId,
};

const CAN_SEE_PERMISSION: CatalogTableAction = CatalogTableAction::CanGetMetadata;

pub trait TableAction
where
    Self: std::hash::Hash
        + std::fmt::Display
        + Send
        + Sync
        + Copy
        + PartialEq
        + Eq
        + From<CatalogTableAction>,
{
}

impl TableAction for CatalogTableAction {}

// ------------------ Cannot See Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeTable {
    warehouse_id: WarehouseId,
    table: TableIdentOrId,
}
impl AuthZCannotSeeTable {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, table: impl Into<TableIdentOrId>) -> Self {
        Self {
            warehouse_id,
            table: table.into(),
        }
    }
}
impl From<AuthZCannotSeeTable> for ErrorModel {
    fn from(err: AuthZCannotSeeTable) -> Self {
        let AuthZCannotSeeTable {
            warehouse_id,
            table,
        } = err;
        TabularNotFound::new(warehouse_id, table)
            .append_detail("Table not found or access denied")
            .into()
    }
}
impl From<AuthZCannotSeeTable> for IcebergErrorResponse {
    fn from(err: AuthZCannotSeeTable) -> Self {
        ErrorModel::from(err).into()
    }
}
// ------------------ Action Forbidden Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZTableActionForbidden {
    warehouse_id: WarehouseId,
    table: TableIdentOrId,
    action: String,
    actor: Actor,
}
impl AuthZTableActionForbidden {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        table: impl Into<TableIdentOrId>,
        action: impl TableAction,
        actor: Actor,
    ) -> Self {
        Self {
            warehouse_id,
            table: table.into(),
            action: action.to_string(),
            actor,
        }
    }
}
impl From<AuthZTableActionForbidden> for ErrorModel {
    fn from(err: AuthZTableActionForbidden) -> Self {
        let AuthZTableActionForbidden {
            warehouse_id,
            table,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!(
                "able action `{action}` forbidden for `{actor}` on table {table} in warehouse `{warehouse_id}`"
            ),
            "TableActionForbidden",
            None,
        )
    }
}
impl From<AuthZTableActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZTableActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, derive_more::From)]
pub enum RequireTableActionError {
    AuthZTableActionForbidden(AuthZTableActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    // Hide the existence of the table
    AuthZCannotSeeTable(AuthZCannotSeeTable),
    // Propagated directly
    CatalogBackendError(CatalogBackendError),
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
    SerializationError(SerializationError),
    UnexpectedTabularInResponse(UnexpectedTabularInResponse),
    InternalParseLocationError(InternalParseLocationError),
}

impl From<BackendUnavailableOrCountMismatch> for RequireTableActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<GetTabularInfoError> for RequireTableActionError {
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
impl From<GetTabularInfoByLocationError> for RequireTableActionError {
    fn from(err: GetTabularInfoByLocationError) -> Self {
        match err {
            GetTabularInfoByLocationError::CatalogBackendError(e) => e.into(),
            GetTabularInfoByLocationError::InvalidNamespaceIdentifier(e) => e.into(),
            GetTabularInfoByLocationError::SerializationError(e) => e.into(),
            GetTabularInfoByLocationError::UnexpectedTabularInResponse(e) => e.into(),
            GetTabularInfoByLocationError::InternalParseLocationError(e) => e.into(),
        }
    }
}
impl From<RequireTableActionError> for ErrorModel {
    fn from(err: RequireTableActionError) -> Self {
        match err {
            RequireTableActionError::AuthZTableActionForbidden(e) => e.into(),
            RequireTableActionError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireTableActionError::AuthorizationCountMismatch(e) => e.into(),
            RequireTableActionError::AuthZCannotSeeTable(e) => e.into(),
            RequireTableActionError::CatalogBackendError(e) => e.into(),
            RequireTableActionError::InvalidNamespaceIdentifier(e) => e.into(),
            RequireTableActionError::SerializationError(e) => e.into(),
            RequireTableActionError::UnexpectedTabularInResponse(e) => e.into(),
            RequireTableActionError::InternalParseLocationError(e) => e.into(),
        }
    }
}
impl From<RequireTableActionError> for IcebergErrorResponse {
    fn from(err: RequireTableActionError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, PartialEq, derive_more::From)]
pub enum RequireTabularActionsError {
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthZViewActionForbidden(AuthZViewActionForbidden),
    AuthZTableActionForbidden(AuthZTableActionForbidden),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
}
impl From<RequireTabularActionsError> for ErrorModel {
    fn from(err: RequireTabularActionsError) -> Self {
        match err {
            RequireTabularActionsError::AuthorizationBackendUnavailable(e) => e.into(),
            RequireTabularActionsError::AuthZViewActionForbidden(e) => e.into(),
            RequireTabularActionsError::AuthZTableActionForbidden(e) => e.into(),
            RequireTabularActionsError::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<RequireTabularActionsError> for IcebergErrorResponse {
    fn from(err: RequireTabularActionsError) -> Self {
        ErrorModel::from(err).into()
    }
}
impl From<BackendUnavailableOrCountMismatch> for RequireTabularActionsError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}

#[async_trait::async_trait]
pub trait AuthZTableOps: Authorizer {
    async fn require_table_action<T: AuthZTableInfo>(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        user_provided_table: impl Into<TableIdentOrId> + Send,
        table: Result<Option<T>, impl Into<RequireTableActionError> + Send>,
        action: impl Into<Self::TableAction> + Send,
    ) -> Result<T, RequireTableActionError> {
        let actor = metadata.actor();
        // OK to return because this goes via the Into method
        // of RequireTableActionError
        let table = table.map_err(Into::into)?;
        let Some(table) = table else {
            return Err(AuthZCannotSeeTable::new(warehouse_id, user_provided_table).into());
        };
        let table_ident = table.table_ident().clone();
        let user_provided_table = user_provided_table.into();
        let cant_see_err =
            AuthZCannotSeeTable::new(warehouse_id, user_provided_table.clone()).into();
        let action = action.into();

        #[cfg(debug_assertions)]
        {
            match &user_provided_table {
                TableIdentOrId::Id(user_id) => {
                    debug_assert_eq!(
                        *user_id,
                        table.table_id(),
                        "Table ID in request ({user_id}) does not match the resolved table ID ({})",
                        table.table_id()
                    );
                }
                TableIdentOrId::Ident(user_ident) => {
                    debug_assert_eq!(
                        user_ident, table.table_ident(),
                        "Table identifier in request ({user_ident}) does not match the resolved table identifier ({})",
                        table.table_ident()
                    );
                }
            }
        }

        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_table_action(metadata, &table, action)
                .await?
                .into_inner();
            is_allowed.then_some(table).ok_or(cant_see_err)
        } else {
            let [can_see_table, is_allowed] = self
                .are_allowed_table_actions_arr(
                    metadata,
                    &table,
                    &[CAN_SEE_PERMISSION.into(), action],
                )
                .await?
                .into_inner();
            if can_see_table {
                is_allowed.then_some(table).ok_or_else(|| {
                    AuthZTableActionForbidden::new(
                        warehouse_id,
                        table_ident.clone(),
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

    async fn require_table_actions<T: AuthZTableInfo>(
        &self,
        metadata: &RequestMetadata,
        tables_with_actions: &[(&T, impl Into<Self::TableAction> + Send + Sync + Copy)],
        // OK Output is a sideproduct that caller may use
    ) -> Result<(), RequireTableActionError> {
        let actor = metadata.actor();

        let tables_with_actions: HashMap<(WarehouseId, TableId), (&T, HashSet<Self::TableAction>)> =
            tables_with_actions
                .iter()
                .fold(HashMap::new(), |mut acc, (table, action)| {
                    acc.entry((table.warehouse_id(), table.table_id()))
                        .or_insert_with(|| (table, HashSet::new()))
                        .1
                        .insert((*action).into());
                    acc
                });

        // Prepare batch authorization requests.
        // Make sure CAN_SEE_PERMISSION comes first for each table.
        let batch_requests = tables_with_actions
            .into_iter()
            .flat_map(|(_id, (table, mut actions))| {
                actions.remove(&CAN_SEE_PERMISSION.into());
                itertools::chain(std::iter::once(CAN_SEE_PERMISSION.into()), actions)
                    .map(move |action| (table, action))
            })
            .collect_vec();
        // Perform batch authorization
        let decisions = self
            .are_allowed_table_actions_vec(metadata, &batch_requests)
            .await?
            .into_inner();

        // Check authorization results.
        // Due to ordering above, CAN_SEE_PERMISSION is always first for each table.
        for ((table, action), &is_allowed) in batch_requests.iter().zip(decisions.iter()) {
            if !is_allowed {
                if *action == CAN_SEE_PERMISSION.into() {
                    return Err(
                        AuthZCannotSeeTable::new(table.warehouse_id(), table.table_id()).into(),
                    );
                }
                return Err(AuthZTableActionForbidden::new(
                    table.warehouse_id(),
                    table.table_ident().clone(),
                    *action,
                    actor.clone(),
                )
                .into());
            }
        }

        Ok(())
    }

    async fn is_allowed_table_action(
        &self,
        metadata: &RequestMetadata,
        table: &impl AuthZTableInfo,
        action: impl Into<Self::TableAction> + Send,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_table_action_impl(metadata, table, action.into())
                .await
        }
        .map(MustUse::from)
    }

    async fn are_allowed_table_actions_arr<
        const N: usize,
        A: Into<Self::TableAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        table: &impl AuthZTableInfo,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let actions = actions
            .iter()
            .map(|a| (table, (*a).into()))
            .collect::<Vec<_>>();
        let result = self
            .are_allowed_table_actions_vec(metadata, &actions)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "table"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_table_actions_vec<A: Into<Self::TableAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        actions: &[(&impl AuthZTableInfo, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; actions.len()])
        } else {
            let converted = actions
                .iter()
                .map(|(id, action)| (*id, (*action).into()))
                .collect::<Vec<_>>();
            let decisions = self
                .are_allowed_table_actions_impl(metadata, &converted)
                .await?;

            if decisions.len() != actions.len() {
                return Err(AuthorizationCountMismatch::new(
                    actions.len(),
                    decisions.len(),
                    "table",
                )
                .into());
            }

            Ok(decisions)
        }
        .map(MustUse::from)
    }

    async fn are_allowed_tabular_actions_vec<
        AT: Into<Self::TableAction> + Send + Copy + Sync,
        AV: Into<Self::ViewAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        actions: &[ActionOnTableOrView<'_, impl AuthZTableInfo, impl AuthZViewInfo, AT, AV>],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; actions.len()])
        } else {
            let (tables, views): (Vec<_>, Vec<_>) = actions.iter().partition_map(|a| match a {
                ActionOnTableOrView::Table((t, a)) => itertools::Either::Left((*t, (*a).into())),
                ActionOnTableOrView::View((v, a)) => itertools::Either::Right((*v, (*a).into())),
            });

            let table_results = if tables.is_empty() {
                Vec::new()
            } else {
                self.are_allowed_table_actions_vec(metadata, &tables)
                    .await?
                    .into_inner()
            };

            let view_results = if views.is_empty() {
                Vec::new()
            } else {
                self.are_allowed_view_actions_vec(metadata, &views)
                    .await?
                    .into_inner()
            };

            if table_results.len() != tables.len() {
                return Err(AuthorizationCountMismatch::new(
                    tables.len(),
                    table_results.len(),
                    "table",
                )
                .into());
            }
            if view_results.len() != views.len() {
                return Err(AuthorizationCountMismatch::new(
                    views.len(),
                    view_results.len(),
                    "view",
                )
                .into());
            }

            // Reorder results to match the original order of actions
            let mut table_idx = 0;
            let mut view_idx = 0;
            let ordered_results: Vec<bool> = actions
                .iter()
                .map(|action| match action {
                    ActionOnTableOrView::Table(_) => {
                        let result = table_results[table_idx];
                        table_idx += 1;
                        result
                    }
                    ActionOnTableOrView::View(_) => {
                        let result = view_results[view_idx];
                        view_idx += 1;
                        result
                    }
                })
                .collect();

            #[cfg(debug_assertions)]
            {
                debug_assert_eq!(
                    ordered_results.len(),
                    actions.len(),
                    "Final result length {} does not match input actions length {}",
                    ordered_results.len(),
                    actions.len()
                );
            }

            Ok(ordered_results)
        }
        .map(MustUse::from)
    }

    async fn require_tabular_actions<
        AT: Into<Self::TableAction> + Send + Copy + Sync,
        AV: Into<Self::ViewAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        tabulars: &[ActionOnTableOrView<'_, impl AuthZTableInfo, impl AuthZViewInfo, AT, AV>],
    ) -> Result<(), RequireTabularActionsError> {
        let decisions = self
            .are_allowed_tabular_actions_vec(metadata, tabulars)
            .await?
            .into_inner();

        for (t, &allowed) in tabulars.iter().zip(decisions.iter()) {
            if !allowed {
                match t {
                    ActionOnTableOrView::View((info, action)) => {
                        return Err(AuthZViewActionForbidden::new(
                            info.warehouse_id(),
                            info.view_id(),
                            (*action).into(),
                            metadata.actor().clone(),
                        )
                        .into());
                    }
                    ActionOnTableOrView::Table((info, action)) => {
                        return Err(AuthZTableActionForbidden::new(
                            info.warehouse_id(),
                            info.table_id(),
                            (*action).into(),
                            metadata.actor().clone(),
                        )
                        .into());
                    }
                }
            }
        }

        Ok(())
    }
}

impl<T> AuthZTableOps for T where T: Authorizer {}

#[derive(Debug)]
pub enum ActionOnTableOrView<'a, IT: AuthZTableInfo, IV: AuthZViewInfo, AT, AV> {
    Table((&'a IT, AT)),
    View((&'a IV, AV)),
}
