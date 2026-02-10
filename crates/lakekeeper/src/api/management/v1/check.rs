use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use unicase::UniCase;

use crate::{
    ProjectId, WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    request_metadata::ProjectIdMissing,
    service::{
        BasicTabularInfo, CachePolicy, CatalogGetNamespaceError, CatalogNamespaceOps, CatalogStore,
        CatalogTabularOps, CatalogWarehouseOps, NamespaceId, NamespaceVersion, NamespaceWithParent,
        ResolvedWarehouse, SecretStore, State, TableInfo, TabularId, TabularIdentOwned,
        TabularListFlags, TabularNotFound, ViewInfo, ViewOrTableInfo, WarehouseIdNotFound,
        WarehouseStatus, WarehouseVersion,
        authz::{
            ActionOnTableOrView, AuthZCannotSeeNamespace, AuthZCannotSeeTable, AuthZCannotSeeView,
            AuthZCannotUseWarehouseId, AuthZNamespaceActionForbidden, AuthZProjectOps,
            AuthZServerOps, AuthZTableOps, AuthorizationBackendUnavailable,
            AuthorizationCountMismatch, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogNamespaceAction, CatalogProjectAction, CatalogServerAction, CatalogTableAction,
            CatalogViewAction, CatalogWarehouseAction, FetchWarehouseNamespaceTabularError,
            MustUse, RequireNamespaceActionError, RequireTableActionError,
            RequireWarehouseActionError, UserOrRole,
        },
        namespace_cache::namespace_ident_to_cache_key,
    },
};

#[derive(Hash, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", untagged)]
/// Identifier for a namespace, either a UUID or its name and warehouse ID
pub enum NamespaceIdentOrUuid {
    #[serde(rename_all = "kebab-case")]
    Id {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        namespace_id: NamespaceId,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[cfg_attr(feature = "open-api", schema(value_type = Vec<String>))]
        namespace: NamespaceIdent,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
}

impl NamespaceIdentOrUuid {
    /// Get the warehouse ID associated with this namespace identifier
    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        match self {
            NamespaceIdentOrUuid::Id { warehouse_id, .. }
            | NamespaceIdentOrUuid::Name { warehouse_id, .. } => *warehouse_id,
        }
    }
}

#[derive(Hash, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", untagged)]
/// Identifier for a table or view, either a UUID or its name and namespace
pub enum TabularIdentOrUuid {
    #[serde(rename_all = "kebab-case")]
    IdInWarehouse {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(alias = "view_id")]
        table_id: uuid::Uuid,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[cfg_attr(feature = "open-api", schema(value_type = Vec<String>))]
        namespace: NamespaceIdent,
        /// Name of the table or view
        #[serde(alias = "view")]
        table: String,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
}

impl TabularIdentOrUuid {
    /// Get the warehouse ID associated with this table/view identifier
    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        match self {
            TabularIdentOrUuid::IdInWarehouse { warehouse_id, .. }
            | TabularIdentOrUuid::Name { warehouse_id, .. } => *warehouse_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
/// Represents an action on an object
pub(super) enum CatalogActionCheckOperation {
    Server {
        action: CatalogServerAction,
    },
    #[serde(rename_all = "kebab-case")]
    Project {
        action: CatalogProjectAction,
        #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
        project_id: Option<ProjectId>,
    },
    #[serde(rename_all = "kebab-case")]
    Warehouse {
        action: CatalogWarehouseAction,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
    Namespace {
        action: CatalogNamespaceAction,
        #[serde(flatten)]
        namespace: NamespaceIdentOrUuid,
    },
    Table {
        action: CatalogTableAction,
        #[serde(flatten)]
        table: TabularIdentOrUuid,
    },
    View {
        action: CatalogViewAction,
        #[serde(flatten)]
        view: TabularIdentOrUuid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
/// A single check item with optional identity override
pub struct CatalogActionCheckItem {
    /// Optional identifier for this check (returned in response).
    /// If not specified, the index in the request array will be used.
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    /// The user or role to check access for.
    /// If not specified, the identity of the user making the request is used.
    #[serde(skip_serializing_if = "Option::is_none")]
    identity: Option<UserOrRole>,
    /// The operation to check
    operation: CatalogActionCheckOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub(super) struct CatalogActionsBatchCheckRequest {
    /// List of checks to perform
    checks: Vec<CatalogActionCheckItem>,
    /// If true, return 404 error when resources are not found.
    /// If false, treat missing resources as denied (allowed = false).
    /// Defaults to false.
    #[serde(default)]
    error_on_not_found: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CatalogActionsBatchCheckResponse {
    results: Vec<CatalogActionsBatchCheckResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CatalogActionsBatchCheckResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    allowed: bool,
}

// Type aliases for complex grouped check types
type ServerChecksMap = HashMap<Option<UserOrRole>, Vec<(usize, CatalogServerAction)>>;
type ProjectChecksMap =
    HashMap<ProjectId, HashMap<Option<UserOrRole>, Vec<(usize, CatalogProjectAction)>>>;
type WarehouseChecksMap =
    HashMap<(WarehouseId, Option<UserOrRole>), Vec<(usize, CatalogWarehouseAction)>>;
type NamespaceChecksByIdMap = HashMap<
    (WarehouseId, Option<UserOrRole>),
    HashMap<NamespaceId, Vec<(usize, CatalogNamespaceAction)>>,
>;
type NamespaceChecksByIdentMap = HashMap<
    (WarehouseId, Option<UserOrRole>),
    HashMap<NamespaceIdent, Vec<(usize, CatalogNamespaceAction)>>,
>;
type TabularActionPair = (Option<CatalogTableAction>, Option<CatalogViewAction>);
type TabularChecksByIdMap =
    HashMap<(WarehouseId, Option<UserOrRole>), HashMap<TabularId, Vec<(usize, TabularActionPair)>>>;
type TabularChecksByIdentMap = HashMap<
    (WarehouseId, Option<UserOrRole>),
    HashMap<TabularIdentOwned, Vec<(usize, TabularActionPair)>>,
>;
type AuthzTaskJoinSet = tokio::task::JoinSet<
    Result<(Vec<usize>, MustUse<Vec<bool>>), FetchWarehouseNamespaceTabularError>,
>;

/// Grouped checks by resource type
struct GroupedChecks {
    server_checks: ServerChecksMap,
    project_checks: ProjectChecksMap,
    warehouse_checks: WarehouseChecksMap,
    namespace_checks_by_id: NamespaceChecksByIdMap,
    namespace_checks_by_ident: NamespaceChecksByIdentMap,
    tabular_checks_by_id: TabularChecksByIdMap,
    tabular_checks_by_ident: TabularChecksByIdentMap,
    seen_warehouse_ids: HashSet<WarehouseId>,
}

impl GroupedChecks {
    fn new() -> Self {
        Self {
            server_checks: HashMap::new(),
            project_checks: HashMap::new(),
            warehouse_checks: HashMap::new(),
            namespace_checks_by_id: HashMap::new(),
            namespace_checks_by_ident: HashMap::new(),
            tabular_checks_by_id: HashMap::new(),
            tabular_checks_by_ident: HashMap::new(),
            seen_warehouse_ids: HashSet::new(),
        }
    }
}

/// Group checks by resource type and prepare result slots
#[allow(clippy::too_many_lines)]
fn group_checks(
    checks: Vec<CatalogActionCheckItem>,
    metadata: &RequestMetadata,
) -> Result<(GroupedChecks, Vec<CatalogActionsBatchCheckResult>), ProjectIdMissing> {
    let mut grouped = GroupedChecks::new();
    let mut results = Vec::with_capacity(checks.len());

    for (i, check) in checks.into_iter().enumerate() {
        results.push(CatalogActionsBatchCheckResult {
            id: check.id,
            allowed: false,
        });
        let for_user = check.identity;

        match check.operation {
            CatalogActionCheckOperation::Server { action } => {
                grouped
                    .server_checks
                    .entry(for_user)
                    .or_default()
                    .push((i, action));
            }
            CatalogActionCheckOperation::Project { action, project_id } => {
                let project_id = metadata.require_project_id(project_id)?;
                grouped
                    .project_checks
                    .entry(project_id)
                    .or_default()
                    .entry(for_user)
                    .or_default()
                    .push((i, action));
            }
            CatalogActionCheckOperation::Warehouse {
                action,
                warehouse_id,
            } => {
                grouped.seen_warehouse_ids.insert(warehouse_id);
                grouped
                    .warehouse_checks
                    .entry((warehouse_id, for_user))
                    .or_default()
                    .push((i, action));
            }
            CatalogActionCheckOperation::Namespace { action, namespace } => {
                grouped.seen_warehouse_ids.insert(namespace.warehouse_id());
                match namespace {
                    NamespaceIdentOrUuid::Id {
                        namespace_id,
                        warehouse_id,
                    } => {
                        grouped
                            .namespace_checks_by_id
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(namespace_id)
                            .or_default()
                            .push((i, action));
                    }
                    NamespaceIdentOrUuid::Name {
                        namespace,
                        warehouse_id,
                    } => {
                        grouped
                            .namespace_checks_by_ident
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(namespace)
                            .or_default()
                            .push((i, action));
                    }
                }
            }
            CatalogActionCheckOperation::Table { action, table } => {
                grouped.seen_warehouse_ids.insert(table.warehouse_id());
                match table {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => {
                        let tabular_id = TabularId::Table(table_id.into());
                        grouped
                            .tabular_checks_by_id
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(tabular_id)
                            .or_default()
                            .push((i, (Some(action), None)));
                    }
                    TabularIdentOrUuid::Name {
                        namespace,
                        table: table_name,
                        warehouse_id,
                    } => {
                        let tabular_ident =
                            TabularIdentOwned::Table(TableIdent::new(namespace, table_name));
                        grouped
                            .tabular_checks_by_ident
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(tabular_ident)
                            .or_default()
                            .push((i, (Some(action), None)));
                    }
                }
            }
            CatalogActionCheckOperation::View { action, view } => {
                grouped.seen_warehouse_ids.insert(view.warehouse_id());
                match view {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => {
                        let tabular_id = TabularId::View(table_id.into());
                        grouped
                            .tabular_checks_by_id
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(tabular_id)
                            .or_default()
                            .push((i, (None, Some(action))));
                    }
                    TabularIdentOrUuid::Name {
                        namespace,
                        table: view_name,
                        warehouse_id,
                    } => {
                        let tabular_ident =
                            TabularIdentOwned::View(TableIdent::new(namespace, view_name));
                        grouped
                            .tabular_checks_by_ident
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(tabular_ident)
                            .or_default()
                            .push((i, (None, Some(action))));
                    }
                }
            }
        }
    }

    Ok((grouped, results))
}

/// Fetch tabular infos and extract minimum required versions
/// Fetches by ident and by ID IN PARALLEL
#[allow(clippy::too_many_lines)]
async fn fetch_tabulars<C: CatalogStore>(
    tabular_checks_by_id: &TabularChecksByIdMap,
    tabular_checks_by_ident: &TabularChecksByIdentMap,
    catalog_state: C::State,
) -> Result<
    (
        HashMap<(WarehouseId, TabularIdentOwned), ViewOrTableInfo>,
        HashMap<(WarehouseId, TabularId), ViewOrTableInfo>,
        HashMap<(WarehouseId, NamespaceId), NamespaceVersion>,
        HashMap<WarehouseId, WarehouseVersion>,
    ),
    FetchWarehouseNamespaceTabularError,
> {
    // Early return if nothing to fetch
    if tabular_checks_by_id.is_empty() && tabular_checks_by_ident.is_empty() {
        return Ok((
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        ));
    }

    let mut min_namespace_versions = HashMap::new();
    let mut min_warehouse_versions = HashMap::new();

    // Spawn BOTH fetch operations in parallel
    let mut tasks = tokio::task::JoinSet::new();

    // Spawn by-ident fetches
    let ident_task_count = if tabular_checks_by_ident.is_empty() {
        0
    } else {
        let mut count = 0;
        for ((warehouse_id, _for_user), tables_map) in tabular_checks_by_ident {
            let catalog_state = catalog_state.clone();
            let tabulars = tables_map.keys().cloned().collect_vec();
            let warehouse_id = *warehouse_id;
            tasks.spawn(async move {
                let tabulars = tabulars
                    .iter()
                    .map(TabularIdentOwned::as_borrowed)
                    .collect_vec();
                (
                    true,
                    C::get_tabular_infos_by_ident(
                        warehouse_id,
                        &tabulars,
                        TabularListFlags::all(),
                        catalog_state,
                    )
                    .await,
                )
            });
            count += 1;
        }
        count
    };

    // Spawn by-ID fetches
    let id_task_count = if tabular_checks_by_id.is_empty() {
        0
    } else {
        let mut count = 0;
        for ((warehouse_id, _for_user), tables_map) in tabular_checks_by_id {
            let catalog_state = catalog_state.clone();
            let tabular_ids = tables_map.keys().copied().collect_vec();
            let warehouse_id = *warehouse_id;
            tasks.spawn(async move {
                (
                    false,
                    C::get_tabular_infos_by_id(
                        warehouse_id,
                        &tabular_ids,
                        TabularListFlags::all(),
                        catalog_state,
                    )
                    .await,
                )
            });
            count += 1;
        }
        count
    };

    // Collect results from both sets of tasks
    let mut ident_results = Vec::with_capacity(ident_task_count);
    let mut id_results = Vec::with_capacity(id_task_count);

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok((is_ident, t)) => {
                let result = t.map_err(RequireTableActionError::from)?;
                if is_ident {
                    ident_results.push(result);
                } else {
                    id_results.push(result);
                }
            }
            Err(err) => {
                return Err(RequireWarehouseActionError::from(
                    AuthorizationBackendUnavailable::new(Box::new(err))
                        .append_detail("Failed to join tabular permission check task"),
                )
                .into());
            }
        }
    }

    // Process by-ident results
    let tabular_infos_by_ident = ident_results
        .into_iter()
        .flatten()
        .map(|ti| {
            min_namespace_versions
                .entry((ti.warehouse_id(), ti.namespace_id()))
                .and_modify(|v| {
                    if ti.namespace_version() < *v {
                        *v = ti.namespace_version();
                    }
                })
                .or_insert(ti.namespace_version());
            min_warehouse_versions
                .entry(ti.warehouse_id())
                .and_modify(|v| {
                    if ti.warehouse_version() < *v {
                        *v = ti.warehouse_version();
                    }
                })
                .or_insert(ti.warehouse_version());
            let tabular_ident = match &ti {
                ViewOrTableInfo::Table(info) => {
                    TabularIdentOwned::Table(info.tabular_ident().clone())
                }
                ViewOrTableInfo::View(info) => {
                    TabularIdentOwned::View(info.tabular_ident().clone())
                }
            };
            ((ti.warehouse_id(), tabular_ident), ti)
        })
        .collect::<HashMap<_, _>>();

    // Process by-ID results
    let tabular_infos_by_id = id_results
        .into_iter()
        .flatten()
        .map(|ti| {
            min_namespace_versions
                .entry((ti.warehouse_id(), ti.namespace_id()))
                .and_modify(|v| {
                    if ti.namespace_version() < *v {
                        *v = ti.namespace_version();
                    }
                })
                .or_insert(ti.namespace_version());
            min_warehouse_versions
                .entry(ti.warehouse_id())
                .and_modify(|v| {
                    if ti.warehouse_version() < *v {
                        *v = ti.warehouse_version();
                    }
                })
                .or_insert(ti.warehouse_version());
            ((ti.warehouse_id(), ti.tabular_id()), ti)
        })
        .collect::<HashMap<_, _>>();

    Ok((
        tabular_infos_by_ident,
        tabular_infos_by_id,
        min_namespace_versions,
        min_warehouse_versions,
    ))
}

/// Fetch warehouses with minimum version requirements
async fn fetch_warehouses<A: Authorizer, C: CatalogStore>(
    seen_warehouse_ids: &HashSet<WarehouseId>,
    min_warehouse_versions: &HashMap<WarehouseId, WarehouseVersion>,
    catalog_state: C::State,
    authorizer: &A,
    error_on_not_found: bool,
) -> Result<HashMap<WarehouseId, Arc<ResolvedWarehouse>>, FetchWarehouseNamespaceTabularError> {
    if seen_warehouse_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut tasks = tokio::task::JoinSet::new();
    for warehouse_id in seen_warehouse_ids {
        let catalog_state = catalog_state.clone();
        let min_warehouse_version = min_warehouse_versions.get(warehouse_id).copied();
        let warehouse_id = *warehouse_id;
        tasks.spawn(async move {
            (
                warehouse_id,
                C::get_warehouse_by_id_cache_aware(
                    warehouse_id,
                    WarehouseStatus::active_and_inactive(),
                    min_warehouse_version
                        .map_or(CachePolicy::Use, |v| CachePolicy::RequireMinimumVersion(*v)),
                    catalog_state,
                )
                .await,
            )
        });
    }

    let mut warehouses = HashMap::new();
    while let Some(res) = tasks.join_next().await {
        let (warehouse_id, warehouse) = res.map_err(|e| {
            RequireWarehouseActionError::from(
                AuthorizationBackendUnavailable::new(Box::new(e))
                    .append_detail("Failed to join warehouse permission check task"),
            )
        })?;
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse);
        match warehouse {
            Ok(warehouse) => {
                warehouses.insert(warehouse.warehouse_id, warehouse);
            }
            Err(e) if matches!(e, RequireWarehouseActionError::AuthZCannotUseWarehouseId(_)) => {
                if error_on_not_found {
                    return Err(e.into());
                }
                tracing::debug!(
                    "Warehouse {warehouse_id} not authorized or not found during fetch, excluding from all permission checks"
                );
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(warehouses)
}

/// Convert optional table/view actions into `ActionOnTableOrView`
fn convert_tabular_action(
    tabular_info: &ViewOrTableInfo,
    table_action: Option<CatalogTableAction>,
    view_action: Option<CatalogViewAction>,
) -> Option<ActionOnTableOrView<'_, TableInfo, ViewInfo, CatalogTableAction, CatalogViewAction>> {
    match tabular_info {
        ViewOrTableInfo::Table(table_info) => {
            table_action.map(|action| ActionOnTableOrView::Table((table_info, action)))
        }
        ViewOrTableInfo::View(view_info) => {
            view_action.map(|action| ActionOnTableOrView::View((view_info, action)))
        }
    }
}

/// Refetch namespaces that don't meet minimum version requirements
async fn refetch_outdated_namespaces<C: CatalogStore>(
    warehouse_id: WarehouseId,
    namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
    min_namespace_versions: &Arc<HashMap<(WarehouseId, NamespaceId), NamespaceVersion>>,
    catalog_state: C::State,
) -> Result<Vec<crate::service::NamespaceHierarchy>, CatalogGetNamespaceError> {
    let mut re_fetched_namespaces = Vec::new();
    for (namespace_id, namespace) in namespaces {
        if let Some(min_version) = min_namespace_versions.get(&(warehouse_id, *namespace_id))
            && namespace.version() < *min_version
        {
            match C::get_namespace_cache_aware(
                warehouse_id,
                *namespace_id,
                CachePolicy::RequireMinimumVersion(**min_version),
                catalog_state.clone(),
            )
            .await
            {
                Ok(Some(updated_ns)) => {
                    re_fetched_namespaces.push(updated_ns);
                }
                Ok(None) => {
                    tracing::warn!(
                        "Namespace {namespace_id} in warehouse {warehouse_id} not found when refetching with min version {min_version}"
                    );
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(re_fetched_namespaces)
}

/// Fetch namespaces by ID and ident with minimum version requirements
#[allow(clippy::too_many_lines)]
async fn fetch_namespaces<C: CatalogStore>(
    namespace_checks_by_id: &NamespaceChecksByIdMap,
    namespace_checks_by_ident: &NamespaceChecksByIdentMap,
    min_namespace_versions: &HashMap<(WarehouseId, NamespaceId), NamespaceVersion>,
    catalog_state: C::State,
) -> Result<
    (
        HashMap<WarehouseId, HashMap<NamespaceId, NamespaceWithParent>>,
        HashMap<(WarehouseId, Vec<UniCase<String>>), NamespaceId>,
    ),
    ErrorModel,
> {
    let min_namespace_versions = Arc::new(min_namespace_versions.clone());

    // Spawn by-ident fetches
    let mut tasks = tokio::task::JoinSet::new();

    if !namespace_checks_by_ident.is_empty() {
        let by_ident_grouped: HashMap<WarehouseId, Vec<NamespaceIdent>> = namespace_checks_by_ident
            .iter()
            .flat_map(|((wh_id, _), v)| v.keys().map(|ns_id| (*wh_id, ns_id.clone())))
            .into_group_map();

        for (warehouse_id, namespace_idents) in by_ident_grouped {
            let catalog_state = catalog_state.clone();
            let min_namespace_versions = min_namespace_versions.clone();
            tasks.spawn(async move {
                let namespace_idents_refs = namespace_idents.iter().collect_vec();
                let mut namespaces = C::get_namespaces_by_ident(
                    warehouse_id,
                    &namespace_idents_refs,
                    catalog_state.clone(),
                )
                .await?;

                // Refetch namespaces that don't meet minimum version requirements
                let re_fetched_namespaces = refetch_outdated_namespaces::<C>(
                    warehouse_id,
                    &namespaces,
                    &min_namespace_versions,
                    catalog_state.clone(),
                )
                .await?;

                for ns_hierarchy in re_fetched_namespaces {
                    namespaces.insert(
                        ns_hierarchy.namespace.namespace_id(),
                        ns_hierarchy.namespace,
                    );
                    for ns in ns_hierarchy.parents {
                        namespaces.insert(ns.namespace_id(), ns);
                    }
                }

                Ok::<_, ErrorModel>((true, namespaces))
            });
        }
    }

    // Spawn by-ID fetches
    if !namespace_checks_by_id.is_empty() || !min_namespace_versions.is_empty() {
        let by_id_grouped: HashMap<WarehouseId, Vec<NamespaceId>> = namespace_checks_by_id
            .iter()
            .flat_map(|((wh_id, _), v)| v.keys().map(|ns_id| (*wh_id, *ns_id)))
            .chain(
                min_namespace_versions
                    .keys()
                    .map(|(wh_id, ns_id)| (*wh_id, *ns_id)),
            )
            .into_group_map();

        for (warehouse_id, namespace_ids) in by_id_grouped {
            let catalog_state = catalog_state.clone();
            let min_namespace_versions = min_namespace_versions.clone();
            tasks.spawn(async move {
                let mut namespaces =
                    C::get_namespaces_by_id(warehouse_id, &namespace_ids, catalog_state.clone())
                        .await?;

                // Refetch namespaces that don't meet minimum version requirements
                let re_fetched_namespaces = refetch_outdated_namespaces::<C>(
                    warehouse_id,
                    &namespaces,
                    &min_namespace_versions,
                    catalog_state.clone(),
                )
                .await?;

                for ns_hierarchy in re_fetched_namespaces {
                    namespaces.insert(
                        ns_hierarchy.namespace.namespace_id(),
                        ns_hierarchy.namespace,
                    );
                    for ns in ns_hierarchy.parents {
                        namespaces.insert(ns.namespace_id(), ns);
                    }
                }

                Ok::<_, ErrorModel>((false, namespaces))
            });
        }
    }

    // Collect results
    let mut namespaces_by_id: HashMap<WarehouseId, HashMap<NamespaceId, _>> = HashMap::new();
    let mut namespace_ident_lookup = HashMap::new();

    while let Some(res) = tasks.join_next().await {
        let (is_by_ident, namespace_list) = res.map_err(|e| {
            ErrorModel::internal(
                "Failed to join fetch namespace task",
                "FailedToJoinFetchNamespaceTask",
                Some(Box::new(e)),
            )
        })??;

        for (_, namespace) in namespace_list {
            if is_by_ident {
                namespace_ident_lookup.insert(
                    (
                        namespace.warehouse_id(),
                        namespace_ident_to_cache_key(namespace.namespace_ident()),
                    ),
                    namespace.namespace_id(),
                );
            }
            namespaces_by_id
                .entry(namespace.warehouse_id())
                .or_default()
                .insert(namespace.namespace_id(), namespace);
        }
    }

    Ok((namespaces_by_id, namespace_ident_lookup))
}

/// Spawn server authorization check tasks
fn spawn_server_checks<A: Authorizer>(
    authz_tasks: &mut AuthzTaskJoinSet,
    server_checks: ServerChecksMap,
    authorizer: &A,
    metadata: &RequestMetadata,
) {
    for (for_user, actions) in server_checks {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();
        authz_tasks.spawn(async move {
            let (original_indices, actions): (Vec<_>, Vec<_>) = actions.into_iter().unzip();
            let allowed = authorizer
                .are_allowed_server_actions_vec(&metadata, for_user.as_ref(), &actions)
                .await?;
            Ok::<_, FetchWarehouseNamespaceTabularError>((original_indices, allowed))
        });
    }
}

/// Spawn project authorization check tasks
fn spawn_project_checks<A: Authorizer>(
    authz_tasks: &mut AuthzTaskJoinSet,
    project_checks: ProjectChecksMap,
    authorizer: &A,
    metadata: &RequestMetadata,
) {
    for (project_id, user_map) in project_checks {
        for (for_user, actions) in user_map {
            let authorizer = authorizer.clone();
            let metadata = metadata.clone();
            let project_id = project_id.clone();
            authz_tasks.spawn(async move {
                let (original_indices, projects_with_actions): (Vec<_>, Vec<_>) = actions
                    .into_iter()
                    .map(|(i, a)| (i, (&project_id, a)))
                    .unzip();
                let allowed = authorizer
                    .are_allowed_project_actions_vec(
                        &metadata,
                        for_user.as_ref(),
                        &projects_with_actions,
                    )
                    .await?;
                Ok::<_, FetchWarehouseNamespaceTabularError>((original_indices, allowed))
            });
        }
    }
}

/// Spawn warehouse authorization check tasks
fn spawn_warehouse_checks<A: Authorizer>(
    authz_tasks: &mut AuthzTaskJoinSet,
    warehouse_checks: WarehouseChecksMap,
    warehouses: &HashMap<WarehouseId, Arc<ResolvedWarehouse>>,
    authorizer: &A,
    metadata: &RequestMetadata,
) {
    for ((warehouse_id, for_user), actions) in warehouse_checks {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();

        if let Some(warehouse) = warehouses.get(&warehouse_id).map(Clone::clone) {
            authz_tasks.spawn(async move {
                let (original_indices, warehouses_with_actions) = actions
                    .into_iter()
                    .map(|(i, a)| (i, (&*warehouse, a)))
                    .unzip::<_, _, Vec<_>, Vec<_>>();
                let allowed = authorizer
                    .are_allowed_warehouse_actions_vec(
                        &metadata,
                        for_user.as_ref(),
                        &warehouses_with_actions,
                    )
                    .await?;
                Ok::<_, FetchWarehouseNamespaceTabularError>((original_indices, allowed))
            });
        }
    }
}

/// Spawn namespace authorization check tasks (by ID)
fn spawn_namespace_checks_by_id<A: Authorizer>(
    authz_tasks: &mut AuthzTaskJoinSet,
    namespace_checks_by_id: NamespaceChecksByIdMap,
    warehouses: &HashMap<WarehouseId, Arc<ResolvedWarehouse>>,
    namespaces_by_id: &HashMap<WarehouseId, HashMap<NamespaceId, NamespaceWithParent>>,
    authorizer: &A,
    metadata: &RequestMetadata,
    error_on_not_found: bool,
) -> Result<(), FetchWarehouseNamespaceTabularError> {
    for ((warehouse_id, for_user), actions) in namespace_checks_by_id {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();

        let warehouse = if let Some(w) = warehouses.get(&warehouse_id) {
            w.clone()
        } else {
            if error_on_not_found {
                return Err(AuthZCannotUseWarehouseId::new_not_found(warehouse_id).into());
            }
            let total_actions: usize = actions.values().map(std::vec::Vec::len).sum();
            tracing::debug!(
                "Warehouse {warehouse_id} not found for namespace-by-id checks, denying {total_actions} action(s)"
            );
            continue;
        };

        let mut checks = Vec::with_capacity(actions.len());
        for (namespace_id, actions) in actions {
            if let Some(namespace) = namespaces_by_id
                .get(&warehouse_id)
                .and_then(|m| m.get(&namespace_id))
            {
                checks.push((namespace.clone(), actions));
            } else {
                // Namespace not found
                if error_on_not_found {
                    return Err(
                        AuthZCannotSeeNamespace::new_not_found(warehouse_id, namespace_id).into(),
                    );
                }
                tracing::debug!(
                    "Namespace {namespace_id} in warehouse {warehouse_id} not found, denying {count} action(s)",
                    count = actions.len()
                );
            }
        }

        let parent_namespaces = namespaces_by_id
            .get(&warehouse_id)
            .cloned()
            .unwrap_or_default();
        authz_tasks.spawn(async move {
            let (original_indices, namespace_with_actions): (Vec<_>, Vec<_>) = checks
                .iter()
                .flat_map(|(ns, actions)| actions.iter().map(move |(i, a)| (i, (ns, a.clone()))))
                .unzip();
            let allowed = authorizer
                .are_allowed_namespace_actions_vec(
                    &metadata,
                    for_user.as_ref(),
                    &warehouse,
                    &parent_namespaces,
                    &namespace_with_actions,
                )
                .await?;
            Ok::<_, FetchWarehouseNamespaceTabularError>((original_indices, allowed))
        });
    }
    Ok(())
}

/// Parameters for namespace check spawning by ident
struct NamespaceCheckByIdentParams<'a, A: Authorizer> {
    authz_tasks: &'a mut AuthzTaskJoinSet,
    namespace_checks_by_ident: NamespaceChecksByIdentMap,
    warehouses: &'a HashMap<WarehouseId, Arc<ResolvedWarehouse>>,
    namespaces_by_id: &'a HashMap<WarehouseId, HashMap<NamespaceId, NamespaceWithParent>>,
    namespace_ident_lookup: &'a HashMap<(WarehouseId, Vec<UniCase<String>>), NamespaceId>,
    authorizer: &'a A,
    metadata: &'a RequestMetadata,
    error_on_not_found: bool,
}

/// Spawn namespace authorization check tasks (by ident)
fn spawn_namespace_checks_by_ident<A: Authorizer>(
    params: NamespaceCheckByIdentParams<'_, A>,
) -> Result<(), FetchWarehouseNamespaceTabularError> {
    let NamespaceCheckByIdentParams {
        authz_tasks,
        namespace_checks_by_ident,
        warehouses,
        namespaces_by_id,
        namespace_ident_lookup,
        authorizer,
        metadata,
        error_on_not_found,
    } = params;
    for ((warehouse_id, for_user), actions) in namespace_checks_by_ident {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();

        let warehouse = if let Some(w) = warehouses.get(&warehouse_id) {
            w.clone()
        } else {
            if error_on_not_found {
                return Err(AuthZCannotUseWarehouseId::new_not_found(warehouse_id).into());
            }
            let total_actions: usize = actions.values().map(std::vec::Vec::len).sum();
            tracing::debug!(
                "Warehouse {warehouse_id} not found for namespace-by-name checks, denying {total_actions} action(s)"
            );
            continue;
        };

        let mut checks = Vec::with_capacity(actions.len());
        for (namespace_ident, actions) in actions {
            // Look up namespace ID from ident
            let cache_key = (warehouse_id, namespace_ident_to_cache_key(&namespace_ident));
            let Some(namespace_id) = namespace_ident_lookup.get(&cache_key) else {
                // Namespace not found by ident
                if error_on_not_found {
                    return Err(AuthZCannotSeeNamespace::new_not_found(
                        warehouse_id,
                        namespace_ident,
                    )
                    .into());
                }
                tracing::debug!(
                    "Namespace '{namespace_ident}' in warehouse {warehouse_id} not found by name, denying {count} action(s)",
                    count = actions.len()
                );
                continue;
            };
            let Some(namespace) = namespaces_by_id
                .get(&warehouse_id)
                .and_then(|m| m.get(namespace_id))
            else {
                // Namespace not found by ID (shouldn't happen if lookup succeeded)
                return Err(
                    RequireNamespaceActionError::from(AuthorizationBackendUnavailable::new(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "Could not find namespace by ID {namespace_id} after successful lookup by ident '{namespace_ident}'"
                        ),
                    )))).into()
                );
            };
            checks.push((namespace.clone(), actions));
        }

        let parent_namespaces = namespaces_by_id
            .get(&warehouse_id)
            .cloned()
            .unwrap_or_default();
        authz_tasks.spawn(async move {
            let (original_indices, namespace_with_actions): (Vec<_>, Vec<_>) = checks
                .iter()
                .flat_map(|(ns_hierarchy, actions)| {
                    actions
                        .iter()
                        .map(move |(i, a)| (i, (ns_hierarchy, a.clone())))
                })
                .unzip();
            let allowed = authorizer
                .are_allowed_namespace_actions_vec(
                    &metadata,
                    for_user.as_ref(),
                    &warehouse,
                    &parent_namespaces,
                    &namespace_with_actions,
                )
                .await?;
            Ok::<_, FetchWarehouseNamespaceTabularError>((original_indices, allowed))
        });
    }
    Ok(())
}

/// Parameters for tabular check spawning by ID
struct TabularCheckByIdParams<'a, A: Authorizer> {
    authz_tasks: &'a mut AuthzTaskJoinSet,
    tabular_checks_by_id: TabularChecksByIdMap,
    warehouses: &'a HashMap<WarehouseId, Arc<ResolvedWarehouse>>,
    tabular_infos_by_id: &'a Arc<HashMap<(WarehouseId, TabularId), ViewOrTableInfo>>,
    namespaces_by_id: &'a Arc<HashMap<WarehouseId, HashMap<NamespaceId, NamespaceWithParent>>>,
    authorizer: &'a A,
    metadata: &'a RequestMetadata,
    error_on_not_found: bool,
}

/// Spawn tabular authorization check tasks (by ID)
fn spawn_tabular_checks_by_id<A: Authorizer>(
    params: TabularCheckByIdParams<'_, A>,
) -> Result<(), FetchWarehouseNamespaceTabularError> {
    let TabularCheckByIdParams {
        authz_tasks,
        tabular_checks_by_id,
        warehouses,
        tabular_infos_by_id,
        namespaces_by_id,
        authorizer,
        metadata,
        error_on_not_found,
    } = params;
    for ((warehouse_id, for_user), actions) in tabular_checks_by_id {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();
        let tabular_infos_by_id = tabular_infos_by_id.clone();
        let namespaces_by_id = namespaces_by_id.clone();

        let warehouse = if let Some(w) = warehouses.get(&warehouse_id) {
            w.clone()
        } else {
            if error_on_not_found {
                return Err(AuthZCannotUseWarehouseId::new_not_found(warehouse_id).into());
            }
            let total_actions: usize = actions.values().map(std::vec::Vec::len).sum();
            tracing::debug!(
                "Warehouse {warehouse_id} not found for tabular-by-id checks, denying {total_actions} action(s)"
            );
            continue;
        };

        authz_tasks.spawn(async move {
            let mut checks = Vec::with_capacity(actions.len());
            for (tabular_id, actions_on_tabular) in &actions {
                let Some(tabular_info) = tabular_infos_by_id.get(&(warehouse_id, *tabular_id)) else {
                    // Tabular not found
                    if error_on_not_found {
                        match tabular_id {
                            TabularId::Table(table_id) => {
                                return Err(AuthZCannotSeeTable::new_not_found(warehouse_id, *table_id).into());
                            }
                            TabularId::View(view_id) => {
                                return Err(AuthZCannotSeeView::new_not_found(warehouse_id, *view_id).into());
                            }
                        }
                    }
                    tracing::debug!(
                        "Tabular {tabular_id} in warehouse {warehouse_id} not found, denying {count} action(s)",
                        count = actions_on_tabular.len()
                    );
                    continue;
                };
                let namespace_id = tabular_info.namespace_id();
                let Some(namespace) = namespaces_by_id
                    .get(&warehouse_id)
                    .and_then(|m| m.get(&namespace_id)) else {
                    // Namespace not found
                    if error_on_not_found {
                        return Err(AuthZCannotSeeNamespace::new_not_found(warehouse_id, namespace_id).into());
                    }
                    tracing::debug!(
                        "Namespace {namespace_id} in warehouse {warehouse_id} not found for tabular {tabular_id}, denying {count} action(s)",
                        count = actions_on_tabular.len()
                    );
                    continue;
                };

                for (i, (table_action, view_action)) in actions_on_tabular {
                    if let Some(action) = convert_tabular_action(tabular_info, table_action.clone(), view_action.clone()) {
                        checks.push((i, namespace, action));
                    }
                }
            }

            let (original_indices, tabular_with_actions): (Vec<_>, Vec<_>) = checks
                .into_iter()
                .map(|(i, ns, action)| (i, (ns, action)))
                .unzip();
            let binding = HashMap::new();
            let parent_namespaces = namespaces_by_id
                .get(&warehouse_id)
                .unwrap_or(&binding);
            let allowed = authorizer
                .are_allowed_tabular_actions_vec(
                    &metadata,
                    for_user.as_ref(),
                    &warehouse,
                    parent_namespaces,
                    &tabular_with_actions,
                )
                .await?;
            Ok::<_, FetchWarehouseNamespaceTabularError>((original_indices, allowed))
        });
    }
    Ok(())
}

/// Parameters for tabular check spawning by ident
struct TabularCheckByIdentParams<'a, A: Authorizer> {
    authz_tasks: &'a mut AuthzTaskJoinSet,
    tabular_checks_by_ident: TabularChecksByIdentMap,
    warehouses: &'a HashMap<WarehouseId, Arc<ResolvedWarehouse>>,
    tabular_infos_by_ident: &'a Arc<HashMap<(WarehouseId, TabularIdentOwned), ViewOrTableInfo>>,
    namespaces_by_id: &'a Arc<HashMap<WarehouseId, HashMap<NamespaceId, NamespaceWithParent>>>,
    authorizer: &'a A,
    metadata: &'a RequestMetadata,
    error_on_not_found: bool,
}

/// Spawn tabular authorization check tasks (by ident)
fn spawn_tabular_checks_by_ident<A: Authorizer>(
    params: TabularCheckByIdentParams<'_, A>,
) -> Result<(), FetchWarehouseNamespaceTabularError> {
    let TabularCheckByIdentParams {
        authz_tasks,
        tabular_checks_by_ident,
        warehouses,
        tabular_infos_by_ident,
        namespaces_by_id,
        authorizer,
        metadata,
        error_on_not_found,
    } = params;
    for ((warehouse_id, for_user), actions) in tabular_checks_by_ident {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();
        let tabular_infos_by_ident = tabular_infos_by_ident.clone();
        let namespaces_by_id = namespaces_by_id.clone();

        let warehouse = if let Some(w) = warehouses.get(&warehouse_id) {
            w.clone()
        } else {
            if error_on_not_found {
                return Err(AuthZCannotUseWarehouseId::new_not_found(warehouse_id).into());
            }
            let total_actions: usize = actions.values().map(std::vec::Vec::len).sum();
            tracing::debug!(
                "Warehouse {warehouse_id} not found for tabular-by-name checks, denying {total_actions} action(s)"
            );
            continue;
        };

        authz_tasks.spawn(async move {
            let mut checks = Vec::with_capacity(actions.len());
            for (tabular_ident, actions_on_tabular) in &actions {
                let Some(tabular_info) = tabular_infos_by_ident.get(&(warehouse_id, tabular_ident.clone())) else {
                    // Tabular not found
                    if error_on_not_found {
                        match tabular_ident {
                            TabularIdentOwned::Table(table_ident) => {
                                return Err(AuthZCannotSeeTable::new_not_found(warehouse_id, table_ident.clone()).into());
                            }
                            TabularIdentOwned::View(view_ident) => {
                                return Err(AuthZCannotSeeView::new_not_found(warehouse_id, view_ident.clone()).into());
                            }
                        }
                    }
                    tracing::debug!(
                        "Tabular '{tabular_ident:?}' in warehouse {warehouse_id} not found by name, denying {count} action(s)",
                        count = actions_on_tabular.len()
                    );
                    continue;
                };
                let namespace_id = tabular_info.namespace_id();
                let Some(namespace) = namespaces_by_id
                    .get(&warehouse_id)
                    .and_then(|m| m.get(&namespace_id)) else {
                    // Namespace not found
                    if error_on_not_found {
                        return Err(AuthZCannotSeeNamespace::new_not_found(warehouse_id, namespace_id).into());
                    }
                    tracing::debug!(
                        "Namespace {namespace_id} in warehouse {warehouse_id} not found for tabular '{tabular_ident:?}', denying {count} action(s)",
                        count = actions_on_tabular.len()
                    );
                    continue;
                };

                for (i, (table_action, view_action)) in actions_on_tabular {
                    if let Some(action) = convert_tabular_action(tabular_info, table_action.clone(), view_action.clone()) {
                        checks.push((i, namespace, action));
                    }
                }
            }

            let (original_indices, tabular_with_actions): (Vec<_>, Vec<_>) = checks
                .into_iter()
                .map(|(i, ns, action)| (i, (ns, action)))
                .unzip();
            let binding = HashMap::new();
            let parent_namespaces = namespaces_by_id
                .get(&warehouse_id)
                .unwrap_or(&binding);
            let allowed = authorizer
                .are_allowed_tabular_actions_vec(
                    &metadata,
                    for_user.as_ref(),
                    &warehouse,
                    parent_namespaces,
                    &tabular_with_actions,
                )
                .await?;
            Ok::<_, FetchWarehouseNamespaceTabularError>((original_indices, allowed))
        });
    }
    Ok(())
}

/// Collect authorization results and update the results array
async fn collect_authz_results(
    authz_tasks: &mut AuthzTaskJoinSet,
    results: &mut [CatalogActionsBatchCheckResult],
) -> Result<(), FetchWarehouseNamespaceTabularError> {
    while let Some(res) = authz_tasks.join_next().await {
        let (original_indices, allowed) = res.map_err(|e| {
            RequireWarehouseActionError::from(
                AuthorizationBackendUnavailable::new(Box::new(e))
                    .append_detail("Failed to join authorization check task"),
            )
        })??;
        let allowed_vec = allowed.into_inner();
        if original_indices.len() != allowed_vec.len() {
            return Err(AuthorizationCountMismatch::new(
                original_indices.len(),
                allowed_vec.len(),
                "check endpoint",
            )
            .into());
        }
        for (i, is_allowed) in original_indices.into_iter().zip(allowed_vec.into_iter()) {
            results[i].allowed = is_allowed;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) async fn check_internal<A: Authorizer, C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<A, C, S>>,
    metadata: &RequestMetadata,
    request: CatalogActionsBatchCheckRequest,
) -> Result<CatalogActionsBatchCheckResponse, ErrorModel> {
    const MAX_CHECKS: usize = 1000;

    let authorizer = api_context.v1_state.authz.clone();
    let catalog_state = api_context.v1_state.catalog.clone();
    let CatalogActionsBatchCheckRequest {
        checks,
        error_on_not_found,
    } = request;

    // Limit total number of checks to prevent abuse
    if checks.len() > MAX_CHECKS {
        return Err(ErrorModel::bad_request(
            format!(
                "Too many checks requested: {}. Maximum allowed is {}",
                checks.len(),
                MAX_CHECKS
            ),
            "TooManyChecks",
            None,
        ));
    }

    // let event_ctx

    let (grouped, mut results) = group_checks(checks, metadata)?;
    let GroupedChecks {
        server_checks,
        project_checks,
        warehouse_checks,
        namespace_checks_by_id,
        namespace_checks_by_ident,
        tabular_checks_by_id,
        tabular_checks_by_ident,
        seen_warehouse_ids,
    } = grouped;

    // Load required entities
    // 1. Tabulars (which gives us min required warehouse & namespace versions)
    let (
        tabular_infos_by_ident,
        tabular_infos_by_id,
        min_namespace_versions,
        min_warehouse_versions,
    ) = fetch_tabulars::<C>(
        &tabular_checks_by_id,
        &tabular_checks_by_ident,
        catalog_state.clone(),
    )
    .await?;

    // 2. Warehouses & Namespaces, respecting min version requirements from tabulars
    let warehouses = fetch_warehouses::<A, C>(
        &seen_warehouse_ids,
        &min_warehouse_versions,
        catalog_state.clone(),
        &authorizer,
        error_on_not_found,
    )
    .await?;

    let (namespaces_by_id, namespace_ident_lookup) = fetch_namespaces::<C>(
        &namespace_checks_by_id,
        &namespace_checks_by_ident,
        &min_namespace_versions,
        catalog_state.clone(),
    )
    .await?;

    // AuthZ checks
    let namespaces_by_id = Arc::new(namespaces_by_id);
    let namespace_ident_lookup = Arc::new(namespace_ident_lookup);
    let tabular_infos_by_id = Arc::new(tabular_infos_by_id);
    let tabular_infos_by_ident = Arc::new(tabular_infos_by_ident);

    let mut authz_tasks = tokio::task::JoinSet::new();

    // Server checks
    spawn_server_checks(&mut authz_tasks, server_checks, &authorizer, metadata);

    // Project checks
    spawn_project_checks(&mut authz_tasks, project_checks, &authorizer, metadata);

    // Warehouse checks
    spawn_warehouse_checks(
        &mut authz_tasks,
        warehouse_checks,
        &warehouses,
        &authorizer,
        metadata,
    );

    // Namespace checks by ID
    spawn_namespace_checks_by_id(
        &mut authz_tasks,
        namespace_checks_by_id,
        &warehouses,
        &namespaces_by_id,
        &authorizer,
        metadata,
        error_on_not_found,
    )?;

    // Namespace checks by ident
    spawn_namespace_checks_by_ident(NamespaceCheckByIdentParams {
        authz_tasks: &mut authz_tasks,
        namespace_checks_by_ident,
        warehouses: &warehouses,
        namespaces_by_id: &namespaces_by_id,
        namespace_ident_lookup: &namespace_ident_lookup,
        authorizer: &authorizer,
        metadata,
        error_on_not_found,
    })?;

    // Tabular checks by ID
    spawn_tabular_checks_by_id(TabularCheckByIdParams {
        authz_tasks: &mut authz_tasks,
        tabular_checks_by_id,
        warehouses: &warehouses,
        tabular_infos_by_id: &tabular_infos_by_id,
        namespaces_by_id: &namespaces_by_id,
        authorizer: &authorizer,
        metadata,
        error_on_not_found,
    })?;

    // Tabular checks by ident
    spawn_tabular_checks_by_ident(TabularCheckByIdentParams {
        authz_tasks: &mut authz_tasks,
        tabular_checks_by_ident,
        warehouses: &warehouses,
        tabular_infos_by_ident: &tabular_infos_by_ident,
        namespaces_by_id: &namespaces_by_id,
        authorizer: &authorizer,
        metadata,
        error_on_not_found,
    })?;

    collect_authz_results(&mut authz_tasks, &mut results).await?;

    Ok(CatalogActionsBatchCheckResponse { results })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        api::{
            iceberg::{
                types::Prefix,
                v1::{DataAccess, NamespaceParameters, tables::TablesService},
            },
            management::v1::warehouse::TabularDeleteProfile,
        },
        implementations::{CatalogState, postgres::PostgresBackend},
        request_metadata::RequestMetadata,
        server::CatalogServer,
        service::{
            UserId,
            authz::{
                CatalogNamespaceAction, CatalogServerAction, CatalogTableAction,
                CatalogWarehouseAction, tests::HidingAuthorizer,
            },
        },
        tests::create_table_request,
    };

    #[sqlx::test]
    async fn test_check_internal_basic_permissions(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // Create a namespace
        let ns_name = "test_namespace";
        let create_ns_resp = crate::server::test::create_ns(
            api_context.clone(),
            test_warehouse.warehouse_id.to_string(),
            ns_name.to_string(),
        )
        .await;

        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(test_warehouse.warehouse_id.to_string())),
            namespace: create_ns_resp.namespace.clone(),
        };

        // Create a table
        let table_name = "test_table";
        let create_table_resp = CatalogServer::create_table(
            ns_params.clone(),
            create_table_request(Some(table_name.to_string()), None),
            DataAccess::not_specified(),
            api_context.clone(),
            metadata.clone(),
        )
        .await
        .unwrap();

        let table_id = create_table_resp.metadata.uuid();

        // Get the namespace ID from the catalog
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
        let namespace_hierarchy = PostgresBackend::get_namespace(
            test_warehouse.warehouse_id,
            create_ns_resp.namespace.clone(),
            catalog_state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let namespace_id = namespace_hierarchy.namespace_id();

        // Test 1: Check server action (should be allowed by default)
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("server-check-1".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Server {
                    action: CatalogServerAction::CreateProject,
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, Some("server-check-1".to_string()));
        assert!(response.results[0].allowed);

        // Test 2: Check warehouse action by ID
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("warehouse-check-1".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Warehouse {
                    action: CatalogWarehouseAction::Use,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].id,
            Some("warehouse-check-1".to_string())
        );
        assert!(response.results[0].allowed);

        // Test 3: Check namespace action by ID
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("namespace-check-1".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Id {
                        namespace_id,
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].id,
            Some("namespace-check-1".to_string())
        );
        assert!(response.results[0].allowed);

        // Test 4: Check namespace action by name
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("namespace-check-2".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Name {
                        namespace: create_ns_resp.namespace.clone(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].id,
            Some("namespace-check-2".to_string())
        );
        assert!(response.results[0].allowed);

        // Test 5: Check table action by ID
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("table-check-1".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id: test_warehouse.warehouse_id,
                        table_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, Some("table-check-1".to_string()));
        assert!(response.results[0].allowed);

        // Test 6: Check table action by name
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("table-check-2".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::Name {
                        namespace: create_ns_resp.namespace.clone(),
                        table: table_name.to_string(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, Some("table-check-2".to_string()));
        assert!(response.results[0].allowed);

        // Test 7: Batch check with multiple operations
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![
                CatalogActionCheckItem {
                    id: Some("batch-1".to_string()),
                    identity: None,
                    operation: CatalogActionCheckOperation::Server {
                        action: CatalogServerAction::CreateProject,
                    },
                },
                CatalogActionCheckItem {
                    id: Some("batch-2".to_string()),
                    identity: None,
                    operation: CatalogActionCheckOperation::Warehouse {
                        action: CatalogWarehouseAction::Use,
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
                CatalogActionCheckItem {
                    id: Some("batch-3".to_string()),
                    identity: None,
                    operation: CatalogActionCheckOperation::Table {
                        action: CatalogTableAction::ReadData,
                        table: TabularIdentOrUuid::IdInWarehouse {
                            warehouse_id: test_warehouse.warehouse_id,
                            table_id,
                        },
                    },
                },
            ],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 3);
        assert!(response.results.iter().all(|r| r.allowed));
        assert_eq!(response.results[0].id, Some("batch-1".to_string()));
        assert_eq!(response.results[1].id, Some("batch-2".to_string()));
        assert_eq!(response.results[2].id, Some("batch-3".to_string()));
    }

    #[sqlx::test]
    async fn test_check_internal_hidden_warehouse(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // First verify warehouse is accessible
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("visible-warehouse".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Warehouse {
                    action: CatalogWarehouseAction::Use,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].allowed); // Should be allowed initially

        // Now hide the warehouse
        authz.hide(&format!("warehouse:{}", test_warehouse.warehouse_id));

        // Check warehouse action again - should now be denied
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("hidden-warehouse".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Warehouse {
                    action: CatalogWarehouseAction::Use,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, Some("hidden-warehouse".to_string()));
        assert!(!response.results[0].allowed); // Should now be denied
    }

    #[sqlx::test]
    async fn test_check_internal_hidden_namespace(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // Create a namespace
        let create_ns_resp = crate::server::test::create_ns(
            api_context.clone(),
            test_warehouse.warehouse_id.to_string(),
            "test_namespace".to_string(),
        )
        .await;

        // Get the namespace ID
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
        let namespace_hierarchy = PostgresBackend::get_namespace(
            test_warehouse.warehouse_id,
            create_ns_resp.namespace.clone(),
            catalog_state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let namespace_id = namespace_hierarchy.namespace_id();

        // First verify namespace is accessible by ID
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("visible-namespace-id".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Id {
                        namespace_id,
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].allowed); // Should be allowed initially

        // Verify namespace is accessible by name
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("visible-namespace-name".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Name {
                        namespace: create_ns_resp.namespace.clone(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].allowed); // Should be allowed initially

        // Now hide the namespace
        authz.hide(&format!("namespace:{namespace_id}"));

        // Check namespace action by ID - should now be denied
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("hidden-namespace-id".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Id {
                        namespace_id,
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].allowed); // Should now be denied

        // Check namespace action by name - should also be denied
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("hidden-namespace-name".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Name {
                        namespace: create_ns_resp.namespace.clone(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].allowed); // Should now be denied
    }

    #[sqlx::test]
    async fn test_check_internal_hidden_table(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // Create a namespace
        let create_ns_resp = crate::server::test::create_ns(
            api_context.clone(),
            test_warehouse.warehouse_id.to_string(),
            "test_namespace".to_string(),
        )
        .await;

        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(test_warehouse.warehouse_id.to_string())),
            namespace: create_ns_resp.namespace.clone(),
        };

        // Create a table
        let table_name = "test_table";
        let create_table_resp = CatalogServer::create_table(
            ns_params.clone(),
            create_table_request(Some(table_name.to_string()), None),
            DataAccess::not_specified(),
            api_context.clone(),
            metadata.clone(),
        )
        .await
        .unwrap();

        let table_id = create_table_resp.metadata.uuid();

        // First verify table is accessible by ID
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("visible-table-id".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id: test_warehouse.warehouse_id,
                        table_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].allowed); // Should be allowed initially

        // Verify table is accessible by name
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("visible-table-name".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::Name {
                        namespace: create_ns_resp.namespace.clone(),
                        table: table_name.to_string(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].allowed); // Should be allowed initially

        // Now hide the table
        authz.hide(&format!(
            "table:{}/{}",
            test_warehouse.warehouse_id, table_id
        ));

        // Check table action by ID - should now be denied
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("hidden-table-id".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id: test_warehouse.warehouse_id,
                        table_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].allowed); // Should now be denied

        // Check table action by name - should also be denied
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("hidden-table-name".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::Name {
                        namespace: create_ns_resp.namespace.clone(),
                        table: table_name.to_string(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].allowed); // Should now be denied
    }

    #[sqlx::test]
    async fn test_check_internal_mixed_visibility(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // Create two namespaces
        let ns1_resp = crate::server::test::create_ns(
            api_context.clone(),
            test_warehouse.warehouse_id.to_string(),
            "visible_ns".to_string(),
        )
        .await;

        let ns2_resp = crate::server::test::create_ns(
            api_context.clone(),
            test_warehouse.warehouse_id.to_string(),
            "hidden_ns".to_string(),
        )
        .await;

        // Get namespace IDs
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
        let ns1_hierarchy = PostgresBackend::get_namespace(
            test_warehouse.warehouse_id,
            ns1_resp.namespace.clone(),
            catalog_state.clone(),
        )
        .await
        .unwrap()
        .unwrap();

        let ns2_hierarchy = PostgresBackend::get_namespace(
            test_warehouse.warehouse_id,
            ns2_resp.namespace.clone(),
            catalog_state.clone(),
        )
        .await
        .unwrap()
        .unwrap();

        // Hide the second namespace
        authz.hide(&format!("namespace:{}", ns2_hierarchy.namespace_id()));

        // Batch check with mixed visibility
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![
                CatalogActionCheckItem {
                    id: Some("visible".to_string()),
                    identity: None,
                    operation: CatalogActionCheckOperation::Namespace {
                        action: CatalogNamespaceAction::CreateTable {
                            properties: Arc::default(),
                        },
                        namespace: NamespaceIdentOrUuid::Id {
                            namespace_id: ns1_hierarchy.namespace_id(),
                            warehouse_id: test_warehouse.warehouse_id,
                        },
                    },
                },
                CatalogActionCheckItem {
                    id: Some("hidden".to_string()),
                    identity: None,
                    operation: CatalogActionCheckOperation::Namespace {
                        action: CatalogNamespaceAction::CreateTable {
                            properties: Arc::default(),
                        },
                        namespace: NamespaceIdentOrUuid::Id {
                            namespace_id: ns2_hierarchy.namespace_id(),
                            warehouse_id: test_warehouse.warehouse_id,
                        },
                    },
                },
            ],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].id, Some("visible".to_string()));
        assert!(response.results[0].allowed); // Visible namespace should be allowed
        assert_eq!(response.results[1].id, Some("hidden".to_string()));
        assert!(!response.results[1].allowed); // Hidden namespace should be denied
    }

    #[sqlx::test]
    async fn test_check_internal_error_on_not_found(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // Check a non-existent table with error_on_not_found = false
        let non_existent_table_id = uuid::Uuid::now_v7();
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("not-found-no-error".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id: test_warehouse.warehouse_id,
                        table_id: non_existent_table_id,
                    },
                },
            }],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].allowed); // Should be denied but not error

        // Check a non-existent table with error_on_not_found = true
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: Some("not-found-with-error".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id: test_warehouse.warehouse_id,
                        table_id: non_existent_table_id,
                    },
                },
            }],
            error_on_not_found: true,
        };

        let result = check_internal(api_context.clone(), &metadata, request).await;
        assert!(result.is_err()); // Should return an error
    }

    #[sqlx::test]
    async fn test_check_internal_no_id_defaults_to_index(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // Check without providing IDs - should use None
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![
                CatalogActionCheckItem {
                    id: None,
                    identity: None,
                    operation: CatalogActionCheckOperation::Server {
                        action: CatalogServerAction::CreateProject,
                    },
                },
                CatalogActionCheckItem {
                    id: None,
                    identity: None,
                    operation: CatalogActionCheckOperation::Warehouse {
                        action: CatalogWarehouseAction::Use,
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            ],
            error_on_not_found: false,
        };

        let response = check_internal(api_context.clone(), &metadata, request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].id, None);
        assert_eq!(response.results[1].id, None);
        assert!(response.results[0].allowed);
        assert!(response.results[1].allowed);
    }

    #[sqlx::test]
    async fn test_check_internal_max_checks_limit(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();

        let (api_context, _test_warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;

        let metadata = RequestMetadata::new_unauthenticated();

        // Create more than MAX_CHECKS (1000) checks
        let checks = (0..1001)
            .map(|i| CatalogActionCheckItem {
                id: Some(format!("check-{i}")),
                identity: None,
                operation: CatalogActionCheckOperation::Server {
                    action: CatalogServerAction::CreateProject,
                },
            })
            .collect();

        let request = CatalogActionsBatchCheckRequest {
            checks,
            error_on_not_found: false,
        };

        let result = check_internal(api_context.clone(), &metadata, request).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.r#type, "TooManyChecks");
    }

    #[test]
    fn test_table_action_update_property_serde() {
        // Trino may send non-string properties (such as int `format_version`) to OPA, which then forwards to lakekeeper.
        let expected = serde_json::json!({
            "checks":[
                {
                    "identity":{
                        "user":"oidc~9410d0bf-4487-4177-a34f-af364cac0a59"
                    },
                    "operation":{
                        "table":{
                            "action":{
                                "action":"commit",
                                "removed_properties":[],
                                "updated_properties":{"format_version":2}
                            },
                            "namespace":["test_set_properties_trino"],
                            "table":"my_table",
                            "warehouse-id":"e2c21690-dce9-11f0-9036-c3bdc0f3ba79"
                        }
                    }
                }],
                "error-on-not-found":false});
        let action = CatalogTableAction::Commit {
            removed_properties: Arc::new(vec![]),
            updated_properties: Arc::new({
                let mut map = BTreeMap::new();
                map.insert("format_version".to_string(), "2".to_string());
                map
            }),
        };
        let item = CatalogActionCheckItem {
            id: None,
            identity: Some(UserOrRole::User(UserId::new_unchecked(
                "oidc",
                "9410d0bf-4487-4177-a34f-af364cac0a59",
            ))),
            operation: CatalogActionCheckOperation::Table {
                action,
                table: TabularIdentOrUuid::Name {
                    namespace: NamespaceIdent::from_strs(vec![
                        "test_set_properties_trino".to_string(),
                    ])
                    .unwrap(),
                    table: "my_table".to_string(),
                    warehouse_id: uuid::Uuid::parse_str("e2c21690-dce9-11f0-9036-c3bdc0f3ba79")
                        .unwrap()
                        .into(),
                },
            },
        };
        let request = CatalogActionsBatchCheckRequest {
            checks: vec![item],
            error_on_not_found: false,
        };
        let deserialized: CatalogActionsBatchCheckRequest =
            serde_json::from_value(expected).unwrap();
        assert_eq!(request, deserialized);
    }
}
