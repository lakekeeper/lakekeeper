use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools as _;
use lakekeeper_io::s3::S3Location;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        AuthZTableInfo, AuthZViewInfo, CatalogBackendError, CatalogGetNamespaceError,
        GetTabularInfoByLocationError, GetTabularInfoError, InternalParseLocationError,
        InvalidNamespaceIdentifier, NamespaceHierarchy, NamespaceId, NamespaceWithParent,
        ResolvedWarehouse, SerializationError, TableId, TableIdentOrId, TableInfo, TabularNotFound,
        TaskNotFoundError, UnexpectedTabularInResponse, WarehouseStatus,
        authz::{
            AuthZError, AuthZViewActionForbidden, AuthZViewOps, AuthorizationBackendUnavailable,
            AuthorizationCountMismatch, Authorizer, AuthzBadRequest, AuthzNamespaceOps,
            AuthzWarehouseOps, BackendUnavailableOrCountMismatch, CannotInspectPermissions,
            CatalogAction, CatalogTableAction, IsAllowedActionError, MustUse, UserOrRole,
        },
        catalog_store::{
            BasicTabularInfo, CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
            CatalogWarehouseOps, TabularListFlags,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource, context::UserProvidedTable,
            delegate_authorization_failure_source,
        },
    },
};

const CAN_SEE_PERMISSION: CatalogTableAction = CatalogTableAction::GetMetadata;

/// Refreshes warehouse and namespace if their versions are outdated.
///
/// This is a generic helper that works for both tables and views.
/// It checks warehouse and namespace ID and version consistency, refetching if necessary.
/// Warehouse and namespace refetches are performed in parallel when both are required.
#[allow(clippy::too_many_lines)]
pub(crate) async fn refresh_warehouse_and_namespace_if_needed<C, A, T>(
    warehouse: &ResolvedWarehouse,
    namespace: NamespaceHierarchy,
    tabular_info: &T,
    cannot_see_error: impl Into<AuthZError>,
    authorizer: &A,
    catalog_state: C::State,
) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy), AuthZError>
where
    C: CatalogStore,
    A: AuthzNamespaceOps + AuthzWarehouseOps,
    T: BasicTabularInfo,
{
    let warehouse_id = warehouse.warehouse_id;
    let required_warehouse_version = tabular_info.warehouse_version();
    let required_namespace_version = tabular_info.namespace_version();

    let namespace_id_matches = tabular_info.namespace_id() == namespace.namespace.namespace_id();
    let namespace_version_sufficient = namespace.namespace.version() >= required_namespace_version;
    let warehouse_version_sufficient = warehouse.version >= required_warehouse_version;

    // If all checks pass, return warehouse and namespace as-is
    if namespace_id_matches && namespace_version_sufficient && warehouse_version_sufficient {
        return Ok((Arc::new(warehouse.clone()), namespace));
    }

    // Log the reasons for refetch
    if !namespace_id_matches {
        tracing::debug!(
            warehouse_id = %warehouse_id,
            tabular_id = %tabular_info.tabular_id(),
            tabular_namespace_id = %tabular_info.namespace_id(),
            fetched_namespace_id = %namespace.namespace.namespace_id(),
            "Namespace ID mismatch after fetching namespace for table, refetching namespace"
        );
    }
    if !namespace_version_sufficient {
        tracing::debug!(
            warehouse_id = %warehouse_id,
            tabular_id = %tabular_info.tabular_id(),
            namespace_id = %tabular_info.namespace_id(),
            fetched_version = %namespace.namespace.version(),
            required_version = %required_namespace_version,
            "Namespace version too old for table requirement, refetching namespace"
        );
    }
    if !warehouse_version_sufficient {
        tracing::debug!(
            warehouse_id = %warehouse_id,
            fetched_version = %warehouse.version,
            required_version = %required_warehouse_version,
            "Warehouse version too old for table requirement, refetching warehouse"
        );
    }

    // Refetch warehouse and/or namespace in parallel when both are needed
    let warehouse_needs_refetch = !warehouse_version_sufficient;
    let namespace_needs_refetch = !namespace_id_matches || !namespace_version_sufficient;

    let (refetched_warehouse, refetched_namespace) =
        match (warehouse_needs_refetch, namespace_needs_refetch) {
            (true, true) => {
                let (warehouse_result, namespace_result) = tokio::join!(
                    C::get_warehouse_by_id_cache_aware(
                        warehouse_id,
                        WarehouseStatus::active(),
                        CachePolicy::RequireMinimumVersion(*required_warehouse_version),
                        catalog_state.clone()
                    ),
                    C::get_namespace_cache_aware(
                        warehouse_id,
                        tabular_info.tabular_ident().namespace.clone(),
                        CachePolicy::RequireMinimumVersion(*required_namespace_version),
                        catalog_state.clone()
                    )
                );

                let warehouse =
                    authorizer.require_warehouse_presence(warehouse_id, warehouse_result)?;
                let namespace = authorizer.require_namespace_presence(
                    warehouse_id,
                    tabular_info.namespace_id(),
                    namespace_result,
                )?;

                (warehouse, namespace)
            }
            (true, false) => {
                let warehouse_result = C::get_warehouse_by_id_cache_aware(
                    warehouse_id,
                    WarehouseStatus::active(),
                    CachePolicy::RequireMinimumVersion(*required_warehouse_version),
                    catalog_state,
                )
                .await;

                let warehouse =
                    authorizer.require_warehouse_presence(warehouse_id, warehouse_result)?;
                (warehouse, namespace)
            }
            (false, true) => {
                let namespace_result = C::get_namespace_cache_aware(
                    warehouse_id,
                    tabular_info.tabular_ident().namespace.clone(),
                    CachePolicy::RequireMinimumVersion(*required_namespace_version),
                    catalog_state,
                )
                .await;

                let namespace = authorizer.require_namespace_presence(
                    warehouse_id,
                    tabular_info.namespace_id(),
                    namespace_result,
                )?;

                (Arc::new(warehouse.clone()), namespace)
            }
            (false, false) => {
                // This shouldn't happen as we already returned early if all checks passed
                return Ok((Arc::new(warehouse.clone()), namespace));
            }
        };

    // Validate again after refetch
    let refetched_namespace_id_matches =
        tabular_info.namespace_id() == refetched_namespace.namespace.namespace_id();
    let refetched_namespace_version_sufficient =
        refetched_namespace.namespace.version() >= required_namespace_version;
    let refetched_warehouse_version_sufficient =
        refetched_warehouse.version >= required_warehouse_version;

    if !refetched_namespace_id_matches {
        // Namespace ID still doesn't match - serious consistency issue
        tracing::warn!(
            warehouse_id = %warehouse_id,
            tabular_id = %tabular_info.tabular_id(),
            tabular_namespace_id = %tabular_info.namespace_id(),
            refetched_namespace_id = %refetched_namespace.namespace.namespace_id(),
            "Namespace ID mismatch persists after refetch - High Replication Lag, TOCTOU race condition or data corruption"
        );

        return Err(cannot_see_error.into());
    }

    if !refetched_namespace_version_sufficient {
        // Namespace version still insufficient after refetch
        tracing::warn!(
            warehouse_id = %warehouse_id,
            tabular_id = %tabular_info.tabular_id(),
            namespace_id = %tabular_info.namespace_id(),
            refetched_version = %refetched_namespace.namespace.version(),
            required_version = %required_namespace_version,
            "Namespace version still insufficient after refetch - High Replication Lag, TOCTOU race condition or data corruption"
        );

        return Err(cannot_see_error.into());
    }

    if !refetched_warehouse_version_sufficient {
        // Warehouse version still insufficient after refetch
        tracing::warn!(
            warehouse_id = %warehouse_id,
            refetched_version = %refetched_warehouse.version,
            required_version = %required_warehouse_version,
            "Warehouse version still insufficient after refetch - High Replication Lag, TOCTOU race condition or data corruption"
        );

        return Err(cannot_see_error.into());
    }

    Ok((refetched_warehouse, refetched_namespace))
}

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
    Self: std::hash::Hash + CatalogAction + Clone + PartialEq + Eq + From<CatalogTableAction>,
{
}

impl TableAction for CatalogTableAction {}

// ------------------ Cannot See Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeTable {
    warehouse_id: WarehouseId,
    table: TableIdentOrId,
    internal_resource_not_found: bool,
    /// Set when the table was accessed via a DEFINER referenced-by chain
    is_delegated_execution: Option<bool>,
}
impl AuthZCannotSeeTable {
    #[must_use]
    fn new(
        warehouse_id: WarehouseId,
        table: impl Into<TableIdentOrId>,
        resource_not_found: bool,
    ) -> Self {
        Self {
            warehouse_id,
            table: table.into(),
            internal_resource_not_found: resource_not_found,
            is_delegated_execution: None,
        }
    }

    #[must_use]
    pub fn new_not_found(warehouse_id: WarehouseId, table: impl Into<TableIdentOrId>) -> Self {
        Self::new(warehouse_id, table, true)
    }

    #[must_use]
    pub fn new_forbidden(warehouse_id: WarehouseId, table: impl Into<TableIdentOrId>) -> Self {
        Self::new(warehouse_id, table, false)
    }

    #[must_use]
    pub fn with_delegated_execution(mut self, is_delegated: bool) -> Self {
        self.is_delegated_execution = Some(is_delegated);
        self
    }
}

impl AuthorizationFailureSource for AuthZCannotSeeTable {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeTable {
            warehouse_id,
            table,
            internal_resource_not_found: _,
            is_delegated_execution,
        } = self;
        let mut err = TabularNotFound::new(warehouse_id, table);
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

#[derive(Debug, Clone)]
pub struct AuthZCannotSeeTableLocation {
    warehouse_id: WarehouseId,
    table_location: Arc<S3Location>,
    is_not_found: bool,
}
impl AuthZCannotSeeTableLocation {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        table_location: Arc<S3Location>,
        is_not_found: bool,
    ) -> Self {
        Self {
            warehouse_id,
            table_location,
            is_not_found,
        }
    }

    #[must_use]
    pub fn new_not_found(warehouse_id: WarehouseId, table_location: Arc<S3Location>) -> Self {
        Self::new(warehouse_id, table_location, true)
    }

    #[must_use]
    pub fn new_forbidden(warehouse_id: WarehouseId, table_location: Arc<S3Location>) -> Self {
        Self::new(warehouse_id, table_location, false)
    }
}
impl AuthorizationFailureSource for AuthZCannotSeeTableLocation {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeTableLocation {
            warehouse_id,
            table_location,
            is_not_found: _,
        } = self;
        ErrorModel::bad_request(
            format!(
                "Table does not exist or user does not have permission to view it at location `{table_location}` in warehouse `{warehouse_id}`",
            ),
            "NoSuchTableLocationException",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.is_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::CannotSeeResource
        }
    }
}
// ------------------ Action Forbidden Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZTableActionForbidden {
    warehouse_id: WarehouseId,
    table: TableIdentOrId,
    action: String,
}
impl AuthZTableActionForbidden {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        table: impl Into<TableIdentOrId>,
        action: &impl TableAction,
    ) -> Self {
        Self {
            warehouse_id,
            table: table.into(),
            action: action.as_log_str(),
        }
    }
}
impl AuthorizationFailureSource for AuthZTableActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZTableActionForbidden {
            warehouse_id,
            table,
            action,
        } = self;
        ErrorModel::forbidden(
            format!(
                "Table action `{action}` forbidden on table {table} in warehouse `{warehouse_id}`"
            ),
            "TableActionForbidden",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

#[derive(Debug, derive_more::From)]
pub enum RequireTableActionError {
    AuthZTableActionForbidden(AuthZTableActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizerValidationFailed(AuthzBadRequest),
    // Hide the existence of the table
    AuthZCannotSeeTable(AuthZCannotSeeTable),
    // Propagated directly
    CatalogBackendError(CatalogBackendError),
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
    SerializationError(SerializationError),
    UnexpectedTabularInResponse(UnexpectedTabularInResponse),
    InternalParseLocationError(InternalParseLocationError),
}
impl From<CatalogGetNamespaceError> for RequireTableActionError {
    fn from(err: CatalogGetNamespaceError) -> Self {
        match err {
            CatalogGetNamespaceError::CatalogBackendError(e) => e.into(),
            CatalogGetNamespaceError::InvalidNamespaceIdentifier(e) => e.into(),
            CatalogGetNamespaceError::SerializationError(e) => e.into(),
        }
    }
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
impl From<IsAllowedActionError> for RequireTableActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireTableActionError => {
    AuthZTableActionForbidden,
    AuthorizationBackendUnavailable,
    AuthorizationCountMismatch,
    CannotInspectPermissions,
    AuthZCannotSeeTable,
    CatalogBackendError,
    InvalidNamespaceIdentifier,
    SerializationError,
    UnexpectedTabularInResponse,
    InternalParseLocationError,
    AuthorizerValidationFailed
});

#[derive(Debug, PartialEq, derive_more::From)]
pub enum RequireTabularActionsError {
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthZViewActionForbidden(AuthZViewActionForbidden),
    AuthZTableActionForbidden(AuthZTableActionForbidden),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizerValidationFailed(AuthzBadRequest),
}
delegate_authorization_failure_source!(RequireTabularActionsError => {
    AuthorizationBackendUnavailable,
    AuthZViewActionForbidden,
    AuthZTableActionForbidden,
    AuthorizationCountMismatch,
    CannotInspectPermissions,
    AuthorizerValidationFailed
});
impl From<BackendUnavailableOrCountMismatch> for RequireTabularActionsError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<IsAllowedActionError> for RequireTabularActionsError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}

impl AuthorizationFailureSource for TaskNotFoundError {
    fn into_error_model(self) -> ErrorModel {
        self.into()
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ResourceNotFound
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
            return Err(
                AuthZCannotSeeTable::new_not_found(warehouse_id, user_provided_table).into(),
            );
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
        let warehouse_id = warehouse.warehouse_id;
        // OK to return because this goes via the Into method
        // of RequireTableActionError
        let user_provided_table = user_provided_table.into();
        let table =
            self.require_table_presence(warehouse_id, user_provided_table.clone(), table)?;
        let table_ident = table.table_ident().clone();
        let cant_see_err =
            AuthZCannotSeeTable::new_forbidden(warehouse_id, user_provided_table.clone()).into();
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
                        user_ident,
                        table.table_ident(),
                        "Table identifier in request ({user_ident}) does not match the resolved table identifier ({})",
                        table.table_ident()
                    );
                }
            }
        }

        if action == CAN_SEE_PERMISSION.into() {
            let is_allowed = self
                .is_allowed_table_action(metadata, None, warehouse, namespace, &table, action)
                .await?
                .into_inner();
            is_allowed.then_some(table).ok_or(cant_see_err)
        } else {
            let parent_namespaces: HashMap<_, _> = namespace
                .parents
                .iter()
                .map(|ns| (ns.namespace_id(), ns.clone()))
                .collect();
            let [can_see_table, is_allowed] = self
                .are_allowed_table_actions_arr(
                    metadata,
                    warehouse,
                    &parent_namespaces,
                    &[
                        (
                            &namespace.namespace,
                            ActionOnTable {
                                info: &table,
                                action: CAN_SEE_PERMISSION.into(),
                                user: None,
                                is_delegated_execution: false,
                            },
                        ),
                        (
                            &namespace.namespace,
                            ActionOnTable {
                                info: &table,
                                action: action.clone(),
                                user: None,
                                is_delegated_execution: false,
                            },
                        ),
                    ],
                )
                .await?
                .into_inner();
            if can_see_table {
                is_allowed.then_some(table).ok_or_else(|| {
                    AuthZTableActionForbidden::new(warehouse_id, table_ident.clone(), &action)
                        .into()
                })
            } else {
                return Err(cant_see_err);
            }
        }
    }

    /// Fetches and authorizes a table operation in one call.
    ///
    /// This is a convenience method that combines:
    /// 1. Parallel fetching of warehouse, namespace, and table
    /// 2. Validation of warehouse and namespace presence
    /// 3. Namespace ID consistency check
    /// 4. Authorization of the specified action
    ///
    /// # Arguments
    /// * `request_metadata` - The request metadata containing actor information
    /// * `warehouse_id` - The warehouse ID
    /// * `table_ident` - Either a `TableIdent` (name-based) or `TableId` (UUID-based)
    /// * `table_flags` - Flags to control which tables to include (active, staged, deleted)
    /// * `action` - The action to authorize (e.g., `CanDrop`, `CanReadData`, etc.)
    /// * `catalog_state` - The catalog state for database operations
    ///
    /// # Returns
    /// A tuple of `(warehouse, namespace, table)` if all checks pass
    ///
    /// # Errors
    /// Returns `RequireTableActionError` if:
    /// - Warehouse, namespace, or table not found
    /// - Namespace ID mismatch
    /// - User not authorized for the action
    /// - Database or authorization backend errors
    async fn load_and_authorize_table_operation<C: CatalogStore>(
        &self,
        request_metadata: &RequestMetadata,
        user_provided_table: &UserProvidedTable,
        table_flags: TabularListFlags,
        action: impl Into<Self::TableAction> + Send,
        catalog_state: C::State,
    ) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, TableInfo), AuthZError> {
        let warehouse_id = user_provided_table.warehouse_id;
        let action = action.into();

        // Determine the fetch strategy based on whether we have a TableId or TableIdent
        let (warehouse, namespace, table_info) = match &user_provided_table.table {
            TableIdentOrId::Id(table_id) => {
                fetch_warehouse_namespace_table_by_id::<C, _>(
                    self,
                    warehouse_id,
                    *table_id,
                    table_flags,
                    catalog_state.clone(),
                )
                .await?
            }
            TableIdentOrId::Ident(table_ident) => {
                // For TableIdent: fetch all three in parallel
                let (warehouse_result, namespace_result, table_result) = tokio::join!(
                    C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
                    C::get_namespace(
                        warehouse_id,
                        table_ident.namespace.clone(),
                        catalog_state.clone()
                    ),
                    C::get_table_info(
                        warehouse_id,
                        table_ident.clone(),
                        table_flags,
                        catalog_state.clone()
                    )
                );

                // Validate presence
                let warehouse = self.require_warehouse_presence(warehouse_id, warehouse_result)?;
                let namespace = self.require_namespace_presence(
                    warehouse_id,
                    table_ident.namespace.clone(),
                    namespace_result,
                )?;
                let table =
                    self.require_table_presence(warehouse_id, table_ident.clone(), table_result)?;

                (warehouse, namespace, table)
            }
        };

        // Validate warehouse and namespace ID and version consistency (with TOCTOU protection)
        let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
            &warehouse,
            namespace,
            &table_info,
            AuthZCannotSeeTable::new_not_found(warehouse_id, user_provided_table.table.clone()),
            self,
            catalog_state,
        )
        .await?;

        // Perform authorization check
        let table_info = self
            .require_table_action(
                request_metadata,
                &warehouse,
                &namespace,
                user_provided_table.table.clone(),
                Ok::<_, RequireTableActionError>(Some(table_info)),
                action,
            )
            .await?;

        Ok((warehouse, namespace, table_info))
    }

    async fn require_table_actions<T: AuthZTableInfo>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        tables_with_actions: &[(
            &NamespaceWithParent,
            &T,
            impl Into<Self::TableAction> + Send + Sync + Clone,
        )],
        // OK Output is a sideproduct that caller may use
    ) -> Result<(), RequireTableActionError> {
        let tables_with_actions: HashMap<
            (WarehouseId, TableId),
            (&NamespaceWithParent, &T, HashSet<Self::TableAction>),
        > = tables_with_actions
            .iter()
            .fold(HashMap::new(), |mut acc, (ns, table, action)| {
                acc.entry((table.warehouse_id(), table.table_id()))
                    .or_insert_with(|| (ns, table, HashSet::new()))
                    .2
                    .insert(action.clone().into());
                acc
            });

        // Prepare batch authorization requests.
        // Make sure CAN_SEE_PERMISSION comes first for each table.
        let batch_requests = tables_with_actions
            .into_iter()
            .flat_map(|(_id, (ns, table, mut actions))| {
                actions.remove(&CAN_SEE_PERMISSION.into());
                itertools::chain(std::iter::once(CAN_SEE_PERMISSION.into()), actions).map(
                    move |action| {
                        (
                            ns,
                            ActionOnTable {
                                info: table,
                                action,
                                user: None,
                                is_delegated_execution: false,
                            },
                        )
                    },
                )
            })
            .collect_vec();
        // Perform batch authorization
        let decisions = self
            .are_allowed_table_actions_vec(metadata, warehouse, parent_namespaces, &batch_requests)
            .await?
            .into_inner();

        // Check authorization results.
        // Due to ordering above, CAN_SEE_PERMISSION is always first for each table.
        for ((_ns, action), &is_allowed) in batch_requests.iter().zip(decisions.iter()) {
            if !is_allowed {
                if action.action == CAN_SEE_PERMISSION.into() {
                    return Err(AuthZCannotSeeTable::new_forbidden(
                        action.info.warehouse_id(),
                        action.info.table_id(),
                    )
                    .into());
                }
                return Err(AuthZTableActionForbidden::new(
                    action.info.warehouse_id(),
                    action.info.table_ident().clone(),
                    &action.action,
                )
                .into());
            }
        }

        Ok(())
    }

    async fn is_allowed_table_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        table: &impl AuthZTableInfo,
        action: impl Into<Self::TableAction> + Send,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        let parent_namespaces: HashMap<_, _> = namespace
            .parents
            .iter()
            .map(|ns| (ns.namespace_id(), ns.clone()))
            .collect();
        let [decision] = self
            .are_allowed_table_actions_arr(
                metadata,
                warehouse,
                &parent_namespaces,
                &[(
                    &namespace.namespace,
                    ActionOnTable {
                        info: table,
                        action: action.into(),
                        user: for_user,
                        is_delegated_execution: false,
                    },
                )],
            )
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_table_actions_arr<
        const N: usize,
        A: Into<Self::TableAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnTable<'_, '_, impl AuthZTableInfo, A>,
        ); N],
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
        let actions_vec: Vec<_> = actions
            .iter()
            .map(|(ns, action)| (*ns, action.clone()))
            .collect();
        let result = self
            .are_allowed_table_actions_vec(metadata, warehouse, parent_namespaces, &actions_vec)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "table"))?;
        Ok(MustUse::from(arr))
    }

    async fn are_allowed_table_actions_vec<A: Into<Self::TableAction> + Send + Clone + Sync>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnTable<'_, '_, impl AuthZTableInfo, A>,
        )],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
        #[cfg(debug_assertions)]
        {
            let namespaces: Vec<&NamespaceWithParent> = actions.iter().map(|(ns, _)| *ns).collect();
            validate_namespace_hierarchy(&namespaces, parent_namespaces);
        }

        // Check warehouse matches and determine which actions can be auto-approved
        // Also collect actions that need authorization check
        let mut auto_approved: Vec<Option<bool>> = Vec::with_capacity(actions.len());
        let mut actions_to_check = Vec::new();

        for (ns, action) in actions {
            let same_warehouse = action.info.warehouse_id() == warehouse.warehouse_id;
            if !same_warehouse {
                tracing::warn!(
                    "Table warehouse_id `{}` does not match provided warehouse_id `{}`. Denying access.",
                    action.info.warehouse_id(),
                    warehouse.warehouse_id
                );
                auto_approved.push(Some(false));
                continue;
            }

            // Normalize user: if it's the actor itself, treat as None (acting as self)
            let normalized_user = if metadata.actor().to_user_or_role().as_ref() == action.user {
                None
            } else {
                action.user
            };

            // Auto-approve if admin and acting as self
            if metadata.has_admin_privileges() && normalized_user.is_none() {
                auto_approved.push(Some(true));
            } else {
                auto_approved.push(None);
                actions_to_check.push((*ns, action.clone()));
            }
        }

        // If all actions are auto-decided, return early
        if actions_to_check.is_empty() {
            Ok(auto_approved.into_iter().map(|v| v.unwrap()).collect())
        } else {
            let decisions = self
                .are_allowed_table_actions_impl(
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
                    "table",
                )
                .into());
            }

            // Merge auto-approved decisions with checked decisions
            let mut decision_iter = decisions.into_iter();
            let final_decisions: Vec<bool> = auto_approved
                .into_iter()
                .map(|auto| auto.unwrap_or_else(|| decision_iter.next().unwrap()))
                .collect();

            Ok(final_decisions)
        }
        .map(MustUse::from)
    }

    async fn are_allowed_tabular_actions_vec<
        AT: Into<Self::TableAction> + Send + Clone + Sync,
        AV: Into<Self::ViewAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnTableOrView<'_, '_, impl AuthZTableInfo, impl AuthZViewInfo, AT, AV>,
        )],
    ) -> Result<MustUse<Vec<bool>>, IsAllowedActionError> {
        let (tables, views): (Vec<_>, Vec<_>) = actions.iter().partition_map(|(ns, a)| match a {
            ActionOnTableOrView::Table(table_action) => {
                itertools::Either::Left((*ns, table_action.clone()))
            }
            ActionOnTableOrView::View(view_action) => {
                itertools::Either::Right((*ns, view_action.clone()))
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
            return Err(
                AuthorizationCountMismatch::new(views.len(), view_results.len(), "view").into(),
            );
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

        Ok(ordered_results.into())
    }

    async fn require_tabular_actions<
        AT: Into<Self::TableAction> + Send + Clone + Sync,
        AV: Into<Self::ViewAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        tabulars: &[(
            &NamespaceWithParent,
            ActionOnTableOrView<'_, '_, impl AuthZTableInfo, impl AuthZViewInfo, AT, AV>,
        )],
    ) -> Result<(), RequireTabularActionsError> {
        let decisions = self
            .are_allowed_tabular_actions_vec(metadata, warehouse, parent_namespaces, tabulars)
            .await?
            .into_inner();

        for ((_ns, t), &allowed) in tabulars.iter().zip(decisions.iter()) {
            if !allowed {
                match t {
                    ActionOnTableOrView::View(view_action) => {
                        return Err(AuthZViewActionForbidden::new(
                            view_action.info.warehouse_id(),
                            view_action.info.view_id(),
                            &view_action.action.clone().into(),
                        )
                        .into());
                    }
                    ActionOnTableOrView::Table(table_action) => {
                        return Err(AuthZTableActionForbidden::new(
                            table_action.info.warehouse_id(),
                            table_action.info.table_id(),
                            &table_action.action.clone().into(),
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

pub(crate) async fn fetch_warehouse_namespace_table_by_id<C, A>(
    authorizer: &A,
    warehouse_id: WarehouseId,
    user_provided_table: TableId,
    table_flags: TabularListFlags,
    catalog_state: C::State,
) -> Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, TableInfo), AuthZError>
where
    C: CatalogStore,
    A: AuthzWarehouseOps + AuthzNamespaceOps,
{
    // For TableId: fetch warehouse and table in parallel first
    let (warehouse_result, table_result) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_table_info(
            warehouse_id,
            user_provided_table,
            table_flags,
            catalog_state.clone()
        )
    );

    // Validate warehouse and table presence
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse_result)?;
    let table_info =
        authorizer.require_table_presence(warehouse_id, user_provided_table, table_result)?;

    // Fetch namespace with cache policy to ensure it's at least as fresh as the table
    let namespace_result = C::get_namespace_cache_aware(
        warehouse_id,
        table_info.table_ident().namespace.clone(), // Must fetch via name to ensure consistency. Id is checked later
        CachePolicy::RequireMinimumVersion(*table_info.namespace_version),
        catalog_state.clone(),
    )
    .await;

    let namespace = authorizer.require_namespace_presence(
        warehouse_id,
        table_info.namespace_id,
        namespace_result,
    )?;

    Ok((warehouse, namespace, table_info))
}

/// Represents an action to be performed on a table with authorization context.
///
/// The `is_delegated_execution` flag indicates whether this action is being performed
/// as part of a delegated execution (e.g., DEFINER view execution) where the specified
/// user's permissions are used without requiring the caller to have permission inspection rights.
pub struct ActionOnTable<'a, 'u, I: AuthZTableInfo, A> {
    pub info: &'a I,
    pub action: A,
    pub user: Option<&'u UserOrRole>,
    /// If true, skip guard checks (`CanReadAssignments`) and allow delegated execution.
    /// Use for DEFINER views where the view owner's permissions are used directly.
    pub is_delegated_execution: bool,
}

impl<I: AuthZTableInfo, A: Clone> Clone for ActionOnTable<'_, '_, I, A> {
    fn clone(&self) -> Self {
        Self {
            info: self.info,
            action: self.action.clone(),
            user: self.user,
            is_delegated_execution: self.is_delegated_execution,
        }
    }
}

impl<I: AuthZTableInfo, A> std::fmt::Debug for ActionOnTable<'_, '_, I, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionOnTable")
            .field("info", &format!("<{}>", std::any::type_name::<I>()))
            .field("action", &std::any::type_name::<A>())
            .field("user", &self.user)
            .field("is_delegated_execution", &self.is_delegated_execution)
            .finish()
    }
}

/// Represents an action to be performed on a view with authorization context.
///
/// The `is_delegated_execution` flag indicates whether this action is being performed
/// as part of a delegated execution (e.g., DEFINER view execution) where the specified
/// user's permissions are used without requiring the caller to have permission inspection rights.
pub struct ActionOnView<'a, 'u, I: AuthZViewInfo, A> {
    pub info: &'a I,
    pub action: A,
    pub user: Option<&'u UserOrRole>,
    /// If true, skip guard checks (`CanReadAssignments`) and allow delegated execution.
    /// Use for DEFINER views where the view owner's permissions are used directly.
    pub is_delegated_execution: bool,
}

impl<I: AuthZViewInfo, A: Clone> Clone for ActionOnView<'_, '_, I, A> {
    fn clone(&self) -> Self {
        Self {
            info: self.info,
            action: self.action.clone(),
            user: self.user,
            is_delegated_execution: self.is_delegated_execution,
        }
    }
}

impl<I: AuthZViewInfo, A> std::fmt::Debug for ActionOnView<'_, '_, I, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionOnView")
            .field("info", &format!("<{}>", std::any::type_name::<I>()))
            .field("action", &std::any::type_name::<A>())
            .field("user", &self.user)
            .field("is_delegated_execution", &self.is_delegated_execution)
            .finish()
    }
}

#[derive(Debug)]
pub enum ActionOnTableOrView<'a, 'u, IT: AuthZTableInfo, IV: AuthZViewInfo, AT, AV> {
    Table(ActionOnTable<'a, 'u, IT, AT>),
    View(ActionOnView<'a, 'u, IV, AV>),
}

#[cfg(test)]
mod tests {
    use iceberg::{NamespaceIdent, TableIdent};
    use sqlx::PgPool;

    use super::*;
    use crate::{
        implementations::postgres::PostgresBackend,
        service::{
            CatalogTabularOps, CatalogWarehouseOps, TabularIdentBorrowed,
            authz::{CatalogTableAction, CatalogViewAction, tests::HidingAuthorizer},
            catalog_store::TabularListFlags,
        },
        tests::{SetupTestCatalog, create_ns, create_table, create_view, memory_io_profile},
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

        let table_info = infos
            .get(&table_ident)
            .unwrap()
            .clone()
            .into_table_info()
            .unwrap();
        let view_info = infos
            .get(&view_ident)
            .unwrap()
            .clone()
            .into_view_info()
            .unwrap();

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
                ActionOnTableOrView::Table(ActionOnTable {
                    info: &table_info,
                    action: CatalogTableAction::GetMetadata,
                    user: None,
                    is_delegated_execution: false,
                }),
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View(ActionOnView {
                    info: &view_info,
                    action: CatalogViewAction::GetMetadata,
                    user: None,
                    is_delegated_execution: false,
                }),
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

        let table_info = infos
            .get(&table_ident)
            .unwrap()
            .clone()
            .into_table_info()
            .unwrap();
        let view_info = infos
            .get(&view_ident)
            .unwrap()
            .clone()
            .into_view_info()
            .unwrap();

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
                ActionOnTableOrView::Table(ActionOnTable {
                    info: &table_info,
                    action: CatalogTableAction::GetMetadata,
                    user: None,
                    is_delegated_execution: false,
                }),
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View(ActionOnView {
                    info: &view_info,
                    action: CatalogViewAction::GetMetadata,
                    user: None,
                    is_delegated_execution: false,
                }),
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

        let table1_info = infos
            .get(&table1_ident)
            .unwrap()
            .clone()
            .into_table_info()
            .unwrap();
        let view1_info = infos
            .get(&view1_ident)
            .unwrap()
            .clone()
            .into_view_info()
            .unwrap();
        let table2_info = infos
            .get(&table2_ident)
            .unwrap()
            .clone()
            .into_table_info()
            .unwrap();

        // Hide table2 and block view action
        authz.hide(&format!(
            "table:{}/{}",
            warehouse_resp.warehouse_id, table2_info.tabular_id
        ));
        authz.block_action(&format!("view:{:?}", CatalogViewAction::Drop));

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
                ActionOnTableOrView::Table(ActionOnTable {
                    info: &table1_info,
                    action: CatalogTableAction::GetMetadata,
                    user: None,
                    is_delegated_execution: false,
                }),
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View(ActionOnView {
                    info: &view1_info,
                    action: CatalogViewAction::Drop,
                    user: None,
                    is_delegated_execution: false,
                }), // Blocked
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::Table(ActionOnTable {
                    info: &table2_info,
                    action: CatalogTableAction::ReadData,
                    user: None,
                    is_delegated_execution: false,
                }), // Hidden
            ),
            (
                &ns_hierarchy.namespace,
                ActionOnTableOrView::View(ActionOnView {
                    info: &view1_info,
                    action: CatalogViewAction::GetMetadata,
                    user: None,
                    is_delegated_execution: false,
                }), // Allowed
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
