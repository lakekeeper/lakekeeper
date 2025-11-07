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
        NamespaceHierarchy, NamespaceId, NamespaceWithParent, ResolvedWarehouse,
        SerializationError, TableId, TableIdentOrId, TabularNotFound, UnexpectedTabularInResponse,
    },
    WarehouseId,
};

const CAN_SEE_PERMISSION: CatalogTableAction = CatalogTableAction::CanGetMetadata;

/// Validates that the full namespace hierarchy is provided for all namespaces.
/// This is a debug assertion to ensure data consistency.
#[cfg(debug_assertions)]
pub(super) fn validate_namespace_hierarchy(
    namespaces: &[&NamespaceWithParent],
    parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
) {
    for ns in namespaces {
        let mut this_ns = *ns;
        while let Some((parent_id, parent_version)) = this_ns.parent {
            assert!(
                parent_namespaces.contains_key(&parent_id),
                "Parent namespace ID {} of namespace ID {} not found in provided parent namespaces",
                parent_id,
                this_ns.namespace_id()
            );

            let parent_ns = parent_namespaces
                .get(&parent_id)
                .expect("Parent namespace must exist");
            assert!(
                parent_ns.version() == parent_version,
                "Parent namespace version mismatch for namespace ID {}: expected {}, found {}",
                this_ns.namespace_id(),
                parent_version,
                parent_ns.version()
            );
            let expected_parent_ns = this_ns.namespace_ident().clone().parent().unwrap();

            assert!(
                &expected_parent_ns == parent_ns.namespace_ident(),
                "Parent namespace identifier mismatch for namespace ID {}: expected {:?}, found {:?}",
                this_ns.namespace_id(),
                expected_parent_ns,
                parent_ns.namespace_ident()
            );

            this_ns = parent_ns;
        }
    }
}

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
    actor: Box<Actor>,
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
            actor: Box::new(actor),
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
    fn require_table_presence<T: AuthZTableInfo>(
        &self,
        warehouse_id: WarehouseId,
        user_provided_table: impl Into<TableIdentOrId> + Send,
        table: Result<Option<T>, impl Into<RequireTableActionError> + Send>,
    ) -> Result<T, RequireTableActionError> {
        let table = table.map_err(Into::into)?;
        let Some(table) = table else {
            return Err(AuthZCannotSeeTable::new(warehouse_id, user_provided_table).into());
        };
        Ok(table)
    }

    async fn require_table_action<T: AuthZTableInfo>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        user_provided_table: impl Into<TableIdentOrId> + Send,
        table: Result<Option<T>, impl Into<RequireTableActionError> + Send>,
        action: impl Into<Self::TableAction> + Send,
    ) -> Result<T, RequireTableActionError> {
        let actor = metadata.actor();
        let warehouse_id = warehouse.warehouse_id;
        // OK to return because this goes via the Into method
        // of RequireTableActionError
        let user_provided_table = user_provided_table.into();
        let table =
            self.require_table_presence(warehouse_id, user_provided_table.clone(), table)?;
        let table_ident = table.table_ident().clone();
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
                .is_allowed_table_action(metadata, warehouse, namespace, &table, action)
                .await?
                .into_inner();
            is_allowed.then_some(table).ok_or(cant_see_err)
        } else {
            let [can_see_table, is_allowed] = self
                .are_allowed_table_actions_arr(
                    metadata,
                    warehouse,
                    namespace,
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
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        tables_with_actions: &[(
            &NamespaceWithParent,
            &T,
            impl Into<Self::TableAction> + Send + Sync + Copy,
        )],
        // OK Output is a sideproduct that caller may use
    ) -> Result<(), RequireTableActionError> {
        let actor = metadata.actor();

        let tables_with_actions: HashMap<
            (WarehouseId, TableId),
            (&NamespaceWithParent, &T, HashSet<Self::TableAction>),
        > = tables_with_actions
            .iter()
            .fold(HashMap::new(), |mut acc, (ns, table, action)| {
                acc.entry((table.warehouse_id(), table.table_id()))
                    .or_insert_with(|| (ns, table, HashSet::new()))
                    .2
                    .insert((*action).into());
                acc
            });

        // Prepare batch authorization requests.
        // Make sure CAN_SEE_PERMISSION comes first for each table.
        let batch_requests = tables_with_actions
            .into_iter()
            .flat_map(|(_id, (ns, table, mut actions))| {
                actions.remove(&CAN_SEE_PERMISSION.into());
                itertools::chain(std::iter::once(CAN_SEE_PERMISSION.into()), actions)
                    .map(move |action| (ns, table, action))
            })
            .collect_vec();
        // Perform batch authorization
        let decisions = self
            .are_allowed_table_actions_vec(metadata, warehouse, parent_namespaces, &batch_requests)
            .await?
            .into_inner();

        // Check authorization results.
        // Due to ordering above, CAN_SEE_PERMISSION is always first for each table.
        for ((_ns, table, action), &is_allowed) in batch_requests.iter().zip(decisions.iter()) {
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
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        table: &impl AuthZTableInfo,
        action: impl Into<Self::TableAction> + Send,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_table_action_impl(metadata, warehouse, namespace, table, action.into())
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
        warehouse: &ResolvedWarehouse,
        namespace_hierarchy: &NamespaceHierarchy,
        table: &impl AuthZTableInfo,
        actions: &[A; N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let actions = actions
            .iter()
            .map(|a| (&namespace_hierarchy.namespace, table, (*a).into()))
            .collect::<Vec<_>>();
        let result = self
            .are_allowed_table_actions_vec(
                metadata,
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
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "table"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_table_actions_vec<A: Into<Self::TableAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&NamespaceWithParent, &impl AuthZTableInfo, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        #[cfg(debug_assertions)]
        {
            let namespaces: Vec<&NamespaceWithParent> =
                actions.iter().map(|(ns, _, _)| *ns).collect();
            validate_namespace_hierarchy(&namespaces, parent_namespaces);
        }

        if metadata.has_admin_privileges() {
            Ok(vec![true; actions.len()])
        } else {
            let converted = actions
                .iter()
                .map(|(ns, id, action)| (*ns, *id, (*action).into()))
                .collect::<Vec<_>>();
            let decisions = self
                .are_allowed_table_actions_impl(metadata, warehouse, parent_namespaces, &converted)
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
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnTableOrView<'_, impl AuthZTableInfo, impl AuthZViewInfo, AT, AV>,
        )],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.has_admin_privileges() {
            Ok(vec![true; actions.len()])
        } else {
            let (tables, views): (Vec<_>, Vec<_>) =
                actions.iter().partition_map(|(ns, a)| match a {
                    ActionOnTableOrView::Table((t, a)) => {
                        itertools::Either::Left((*ns, *t, (*a).into()))
                    }
                    ActionOnTableOrView::View((v, a)) => {
                        itertools::Either::Right((*ns, *v, (*a).into()))
                    }
                });

            let table_results = if tables.is_empty() {
                Vec::new()
            } else {
                self.are_allowed_table_actions_vec(metadata, warehouse, parent_namespaces, &tables)
                    .await?
                    .into_inner()
            };

            let view_results = if views.is_empty() {
                Vec::new()
            } else {
                self.are_allowed_view_actions_vec(metadata, warehouse, parent_namespaces, &views)
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
                .map(|(_ns, action)| match action {
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
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        tabulars: &[(
            &NamespaceWithParent,
            ActionOnTableOrView<'_, impl AuthZTableInfo, impl AuthZViewInfo, AT, AV>,
        )],
    ) -> Result<(), RequireTabularActionsError> {
        let decisions = self
            .are_allowed_tabular_actions_vec(metadata, warehouse, parent_namespaces, tabulars)
            .await?
            .into_inner();

        for ((_ns, t), &allowed) in tabulars.iter().zip(decisions.iter()) {
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

#[cfg(test)]
mod tests {
    use iceberg::{NamespaceIdent, TableIdent};
    use sqlx::PgPool;

    use super::*;
    use crate::{
        implementations::postgres::PostgresBackend,
        service::{
            authz::{tests::HidingAuthorizer, CatalogTableAction, CatalogViewAction},
            catalog_store::TabularListFlags,
            CatalogNamespaceOps as _, CatalogTabularOps, CatalogWarehouseOps, TabularIdentBorrowed,
        },
        tests::{create_ns, create_table, create_view, memory_io_profile, SetupTestCatalog},
    };

    #[sqlx::test]
    async fn test_are_allowed_tabular_actions_vec_all_allowed(pool: PgPool) {
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse_resp) = SetupTestCatalog::builder()
            .pool(pool)
            .authorizer(authz.clone())
            .storage_profile(memory_io_profile())
            .build()
            .setup()
            .await;

        // Create a namespace, table, and view
        let prefix = warehouse_resp.warehouse_id.to_string();
        let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
        let _table = create_table(ctx.clone(), &prefix, "test_ns", "t1", false)
            .await
            .unwrap();
        let _view = create_view(ctx.clone(), &prefix, "test_ns", "v1", None)
            .await
            .unwrap();

        // Construct table identifiers
        let table_ident =
            TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t1".to_string());
        let view_ident =
            TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "v1".to_string());

        // Load tabular info
        let tabulars = vec![
            TabularIdentBorrowed::Table(&table_ident),
            TabularIdentBorrowed::View(&view_ident),
        ];
        let infos = PostgresBackend::get_tabular_infos_by_ident(
            warehouse_resp.warehouse_id,
            &tabulars,
            TabularListFlags::active(),
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();

        let table_info = infos[0].clone().into_table_info().unwrap();
        let view_info = infos[1].clone().into_view_info().unwrap();

        // Get namespace hierarchy
        let warehouse = PostgresBackend::get_active_warehouse_by_id(
            warehouse_resp.warehouse_id,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let ns_hierarchy = PostgresBackend::get_namespace(
            warehouse_resp.warehouse_id,
            &ns.namespace,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap();

        // Test with both table and view actions
        let actions = vec![
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::Table((&table_info, CatalogTableAction::CanGetMetadata)),
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View((&view_info, CatalogViewAction::CanGetMetadata)),
            ),
        ];

        let parents = ns_hierarchy
            .parents
            .iter()
            .map(|ns| (ns.namespace_id(), ns.clone()))
            .collect();
        let result = authz
            .are_allowed_tabular_actions_vec(
                &crate::tests::random_request_metadata(),
                &warehouse,
                &parents,
                &actions,
            )
            .await
            .unwrap()
            .into_inner();

        assert_eq!(result, vec![true, true]);
    }

    #[sqlx::test]
    async fn test_are_allowed_tabular_actions_vec_hidden_table(pool: PgPool) {
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse_resp) = SetupTestCatalog::builder()
            .pool(pool)
            .authorizer(authz.clone())
            .storage_profile(memory_io_profile())
            .build()
            .setup()
            .await;

        let prefix = warehouse_resp.warehouse_id.to_string();
        let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
        let _table = create_table(ctx.clone(), &prefix, "test_ns", "t1", false)
            .await
            .unwrap();
        let _view = create_view(ctx.clone(), &prefix, "test_ns", "v1", None)
            .await
            .unwrap();

        let table_ident =
            TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t1".to_string());
        let view_ident =
            TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "v1".to_string());

        let tabulars = vec![
            TabularIdentBorrowed::Table(&table_ident),
            TabularIdentBorrowed::View(&view_ident),
        ];
        let infos = PostgresBackend::get_tabular_infos_by_ident(
            warehouse_resp.warehouse_id,
            &tabulars,
            TabularListFlags::active(),
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();

        let table_info = infos[0].clone().into_table_info().unwrap();
        let view_info = infos[1].clone().into_view_info().unwrap();

        // Hide the table
        authz.hide(&format!(
            "table:{}/{}",
            warehouse_resp.warehouse_id, table_info.tabular_id
        ));

        let warehouse = PostgresBackend::get_active_warehouse_by_id(
            warehouse_resp.warehouse_id,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let ns_hierarchy = PostgresBackend::get_namespace(
            warehouse_resp.warehouse_id,
            &ns.namespace,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let parent_namespaces = ns_hierarchy
            .parents
            .iter()
            .map(|ns| (ns.namespace_id(), ns.clone()))
            .collect();

        let actions = vec![
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::Table((&table_info, CatalogTableAction::CanGetMetadata)),
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View((&view_info, CatalogViewAction::CanGetMetadata)),
            ),
        ];

        let result = authz
            .are_allowed_tabular_actions_vec(
                &crate::tests::random_request_metadata(),
                &warehouse,
                &parent_namespaces,
                &actions,
            )
            .await
            .unwrap()
            .into_inner();

        // Table is hidden, view is visible
        assert_eq!(result, vec![false, true]);
    }

    #[sqlx::test]
    async fn test_are_allowed_tabular_actions_vec_mixed_order(pool: PgPool) {
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse_resp) = SetupTestCatalog::builder()
            .pool(pool)
            .authorizer(authz.clone())
            .storage_profile(memory_io_profile())
            .build()
            .setup()
            .await;

        let prefix = warehouse_resp.warehouse_id.to_string();
        let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
        let _table1 = create_table(ctx.clone(), &prefix, "test_ns", "t1", false)
            .await
            .unwrap();
        let _view1 = create_view(ctx.clone(), &prefix, "test_ns", "v1", None)
            .await
            .unwrap();
        let _table2 = create_table(ctx.clone(), &prefix, "test_ns", "t2", false)
            .await
            .unwrap();

        let table1_ident =
            TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t1".to_string());
        let view1_ident =
            TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "v1".to_string());
        let table2_ident =
            TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t2".to_string());

        let tabulars = vec![
            TabularIdentBorrowed::Table(&table1_ident),
            TabularIdentBorrowed::View(&view1_ident),
            TabularIdentBorrowed::Table(&table2_ident),
        ];
        let infos = PostgresBackend::get_tabular_infos_by_ident(
            warehouse_resp.warehouse_id,
            &tabulars,
            TabularListFlags::active(),
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();

        let table1_info = infos[0].clone().into_table_info().unwrap();
        let view1_info = infos[1].clone().into_view_info().unwrap();
        let table2_info = infos[2].clone().into_table_info().unwrap();

        // Hide table2 and block view action
        authz.hide(&format!(
            "table:{}/{}",
            warehouse_resp.warehouse_id, table2_info.tabular_id
        ));
        authz.block_action(&format!("view:{}", CatalogViewAction::CanDrop));

        let warehouse = PostgresBackend::get_active_warehouse_by_id(
            warehouse_resp.warehouse_id,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let ns_hierarchy = PostgresBackend::get_namespace(
            warehouse_resp.warehouse_id,
            &ns.namespace,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let parent_namespaces = ns_hierarchy
            .parents
            .iter()
            .map(|ns| (ns.namespace_id(), ns.clone()))
            .collect();

        // Mix tables and views in different order
        let actions = vec![
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::Table((&table1_info, CatalogTableAction::CanGetMetadata)),
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View((&view1_info, CatalogViewAction::CanDrop)), // Blocked
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::Table((&table2_info, CatalogTableAction::CanReadData)), // Hidden
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View((&view1_info, CatalogViewAction::CanGetMetadata)), // Allowed
            ),
        ];

        let result = authz
            .are_allowed_tabular_actions_vec(
                &crate::tests::random_request_metadata(),
                &warehouse,
                &parent_namespaces,
                &actions,
            )
            .await
            .unwrap()
            .into_inner();

        // Expected: table1 allowed, view1 drop blocked, table2 hidden, view1 get allowed
        assert_eq!(result, vec![true, false, false, true]);
    }
}
