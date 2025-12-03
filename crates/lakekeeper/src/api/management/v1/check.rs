use std::{collections::{HashMap, HashSet}, sync::Arc};

use iceberg::{ NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId, WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        BasicTabularInfo, CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps, NamespaceId, SecretStore, State, TabularId, TabularIdentOwned, TabularListFlags, ViewOrTableInfo, WarehouseIdNotFound, WarehouseStatus, authz::{
            ActionOnTableOrView, AuthZCannotSeeNamespace,  AuthZProjectOps, AuthZServerOps, AuthZTableOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction, CatalogProjectAction, CatalogServerAction, CatalogTableAction, CatalogViewAction, CatalogWarehouseAction, RequireTableActionError, RequireWarehouseActionError, UserOrRole
        }, build_namespace_hierarchy, namespace_cache::namespace_ident_to_cache_key
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
    pub fn warehouse_id(&self) -> WarehouseId {
        match self {
            NamespaceIdentOrUuid::Id { warehouse_id, .. } => *warehouse_id,
            NamespaceIdentOrUuid::Name { warehouse_id, .. } => *warehouse_id,
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
    pub fn warehouse_id(&self) -> WarehouseId {
        match self {
            TabularIdentOrUuid::IdInWarehouse { warehouse_id, .. } => *warehouse_id,
            TabularIdentOrUuid::Name { warehouse_id, .. } => *warehouse_id,
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

async fn check_internal<A: Authorizer, C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<A, C, S>>,
    metadata: &RequestMetadata,
    request: CatalogActionsBatchCheckRequest,
) -> Result<CatalogActionsBatchCheckResponse, ErrorModel> {
    let authorizer = api_context.v1_state.authz.clone();
    let catalog_state = api_context.v1_state.catalog.clone();
    let CatalogActionsBatchCheckRequest {
        checks,
        error_on_not_found,
    } = request;

    let mut server_checks: HashMap<Option<UserOrRole>, Vec<_>> = HashMap::new();
    let mut project_checks: HashMap<ProjectId, HashMap<Option<UserOrRole>, Vec<_>>> =
        HashMap::new();
    let mut warehouse_checks: HashMap<(WarehouseId, Option<UserOrRole>), Vec<_>> = HashMap::new();
    let mut namespace_checks_by_id: HashMap<
        (WarehouseId, Option<UserOrRole>),
        HashMap<NamespaceId, Vec<_>>,
    > = HashMap::new();
    let mut namespace_checks_by_ident: HashMap<
        (WarehouseId, Option<UserOrRole>),
        HashMap<NamespaceIdent, Vec<_>>,
    > = HashMap::new();
    let mut tabular_checks_by_id: HashMap<
        (WarehouseId, Option<UserOrRole>),
        HashMap<TabularId, Vec<_>>,
    > = HashMap::new();
    let mut tabular_checks_by_ident: HashMap<
        (WarehouseId, Option<UserOrRole>),
        HashMap<TabularIdentOwned, Vec<_>>,
    > = HashMap::new();
    let mut seen_warehouse_ids = HashSet::new();
    let mut results = Vec::with_capacity(checks.len());

    for (i, check) in checks.into_iter().enumerate() {
        results.push(CatalogActionsBatchCheckResult {
            id: check.id,
            allowed: false,
        });
        let for_user = check.identity;
        match check.operation {
            CatalogActionCheckOperation::Server { action } => {
                server_checks.entry(for_user).or_default().push((i, action));
            }
            CatalogActionCheckOperation::Project { action, project_id } => {
                let project_id = metadata.require_project_id(project_id)?;
                project_checks
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
                seen_warehouse_ids.insert(warehouse_id);
                let key = (warehouse_id, for_user);
                warehouse_checks.entry(key).or_default().push((i, action));
            }
            CatalogActionCheckOperation::Namespace { action, namespace } => {
                seen_warehouse_ids.insert(namespace.warehouse_id());
                match namespace {
                    NamespaceIdentOrUuid::Id {
                        namespace_id,
                        warehouse_id,
                    } => {
                        let key = (warehouse_id, for_user);
                        namespace_checks_by_id
                            .entry(key)
                            .or_default()
                            .entry(namespace_id)
                            .or_default()
                            .push((i, action));
                    }
                    NamespaceIdentOrUuid::Name {
                        namespace,
                        warehouse_id,
                    } => {
                        let key = (warehouse_id, for_user);
                        namespace_checks_by_ident
                            .entry(key)
                            .or_default()
                            .entry(namespace)
                            .or_default()
                            .push((i, action));
                    }
                }
            }
            CatalogActionCheckOperation::Table { action, table } => {
                seen_warehouse_ids.insert(table.warehouse_id());
                match table {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => {
                        let tabular_id = TabularId::Table(table_id.into());
                        tabular_checks_by_id
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
                        tabular_checks_by_ident
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(tabular_ident)
                            .or_default()
                            .push((i, (Some(action), None)));
                    }
                }
            }
            CatalogActionCheckOperation::View { action, view } => {
                seen_warehouse_ids.insert(view.warehouse_id());
                match view {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => {
                        let tabular_id = TabularId::View(table_id.into());
                        tabular_checks_by_id
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
                        tabular_checks_by_ident
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

    // Load required entities
    // 1. Tablulars (which gives us min required warehouse version)
    let fetch_tabular_by_ident_tasks = if !tabular_checks_by_ident.is_empty() {
        let mut tasks = tokio::task::JoinSet::new();
        for ((warehouse_id, _for_user), tables_map) in &tabular_checks_by_ident {
            let catalog_state = catalog_state.clone();
            let tabulars = tables_map.keys().cloned().collect_vec();
            let warehouse_id = warehouse_id.clone();
            tasks.spawn(async move {
                let tabulars = tabulars
                    .iter()
                    .map(TabularIdentOwned::as_borrowed)
                    .collect_vec();
                C::get_tabular_infos_by_ident(
                    warehouse_id,
                    &tabulars,
                    TabularListFlags::all(),
                    catalog_state,
                )
                .await
            });
        }
        Some(tasks)
    } else {
        None
    };

    let fetch_tabular_by_id_tasks = if !tabular_checks_by_id.is_empty() {
        let mut tasks = tokio::task::JoinSet::new();
        for ((warehouse_id, _for_user), tables_map) in &tabular_checks_by_id {
            let catalog_state = catalog_state.clone();
            let tabular_ids = tables_map.keys().cloned().collect_vec();
            let warehouse_id = warehouse_id.clone();
            tasks.spawn(async move {
                C::get_tabular_infos_by_id(
                    warehouse_id,
                    &tabular_ids,
                    TabularListFlags::all(),
                    catalog_state,
                )
                .await
            });
        }
        Some(tasks)
    } else {
        None
    };

    // Collect tabular results
    let mut min_namespace_versions = HashMap::new();
    let mut min_warehouse_versions = HashMap::new();
    let tabular_infos_by_ident = match fetch_tabular_by_ident_tasks {
        Some(mut tasks) => {
            let mut output = Vec::with_capacity(tasks.len());

            while let Some(res) = tasks.join_next().await {
                match res {
                    Ok(t) => output.push(t.map_err(RequireTableActionError::from)?),
                    Err(err) => {
                        return Err(ErrorModel::internal(
                            "Failed to fetch tabular infos",
                            "FailedToJoinFetchTabularInfosTask",
                            Some(Box::new(err)),
                        ));
                    }
                }
            }
            output
        }
        None => Vec::new(),
    }
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
        ((ti.warehouse_id(), ti.tabular_ident().clone()), ti)
    })
    .collect::<HashMap<_, _>>();

    let tabular_infos_by_id = match fetch_tabular_by_id_tasks {
        Some(mut tasks) => {
            let mut output = Vec::with_capacity(tasks.len());

            while let Some(res) = tasks.join_next().await {
                match res {
                    Ok(t) => output.push(t.map_err(RequireTableActionError::from)?),
                    Err(err) => {
                        return Err(ErrorModel::internal(
                            "Failed to fetch tabular infos",
                            "FailedToJoinFetchTabularInfosTask",
                            Some(Box::new(err)),
                        ));
                    }
                }
            }
            output
        }
        None => Vec::new(),
    }
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

        ((ti.warehouse_id(), ti.tabular_id().clone()), ti)
    })
    .collect::<HashMap<_, _>>();

    // 2. Warehouses & Namespaces
    let fetch_warehouses_tasks = if !seen_warehouse_ids.is_empty() {
        let mut tasks = tokio::task::JoinSet::new();
        for warehouse_id in seen_warehouse_ids {
            let catalog_state = catalog_state.clone();
            let min_warehouse_version = min_warehouse_versions.get(&warehouse_id).copied();
            tasks.spawn(async move {
                (
                    warehouse_id,
                    C::get_warehouse_by_id_cache_aware(
                        warehouse_id,
                        WarehouseStatus::active_and_inactive(),
                        min_warehouse_version
                            .map(|v| CachePolicy::RequireMinimumVersion(*v))
                            .unwrap_or(CachePolicy::Use),
                        catalog_state,
                    )
                    .await,
                )
            });
        }
        Some(tasks)
    } else {
        None
    };

    let min_namespace_versions = Arc::new(min_namespace_versions);
    let fetch_namespace_tasks_by_ident = if namespace_checks_by_ident.is_empty() {
        None
    } else {
        let by_ident_grouped: HashMap<WarehouseId, Vec<NamespaceIdent>> = namespace_checks_by_ident
            .iter()
            .map(|((wh_id, _), v)| v.keys().map(|ns_id| (*wh_id, ns_id.clone())))
            .flatten()
            .into_group_map();

        let mut tasks = tokio::task::JoinSet::new();

        for (warehouse_id, namespace_idents) in by_ident_grouped {
            let catalog_state = catalog_state.clone();
            let min_namespace_versions = min_namespace_versions.clone();
            tasks.spawn(async move {
                let namespace_idents_refs = namespace_idents.iter().collect_vec();
                let mut namespaces = C::get_namespaces_by_ident(warehouse_id, &namespace_idents_refs, catalog_state.clone())
                    .await?;
                
                // Refetch namespaces that don't meet minimum version requirements
                let mut re_fetched_namespaces = Vec::new();
                for (namespace_id, namespace) in &namespaces {
                    if let Some(min_version) = min_namespace_versions.get(&(warehouse_id, *namespace_id)) {
                        if namespace.version() < *min_version {
                            // Refetch with required minimum version
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
                                    // Continue with existing namespace
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to refetch namespace {namespace_id} with min version {min_version}: {e}"
                                    );
                                    // Continue with existing namespace
                                }
                            }
                        }
                    }
                }

                for ns_hierarchy in re_fetched_namespaces {
                    namespaces.insert(ns_hierarchy.namespace.namespace_id(), ns_hierarchy.namespace);
                    for ns in ns_hierarchy.parents {
                        namespaces.insert(ns.namespace_id(), ns);
                    }
                }
                
                Ok::<_, ErrorModel>(namespaces)
            });
        }
        Some(tasks)
    };

let fetch_namespace_tasks_by_id =
    if namespace_checks_by_id.is_empty() && min_namespace_versions.is_empty() {
        None
    } else {
        let by_id_grouped: HashMap<WarehouseId, Vec<NamespaceId>> = namespace_checks_by_id
            .iter()
            .map(|((wh_id, _), v)| v.keys().map(|ns_id| (*wh_id, *ns_id)))
            .flatten()
            .chain(
                min_namespace_versions
                    .keys()
                    .map(|(wh_id, ns_id)| (*wh_id, *ns_id)),
            )
            .into_group_map();

        let mut tasks = tokio::task::JoinSet::new();

        for (warehouse_id, namespace_ids) in by_id_grouped {
            let catalog_state = catalog_state.clone();
            let min_namespace_versions = min_namespace_versions.clone();
            tasks.spawn(async move {
                let mut namespaces = C::get_namespaces_by_id(warehouse_id, &namespace_ids, catalog_state.clone())
                    .await?;
                
                // Refetch namespaces that don't meet minimum version requirements
                let mut re_fetched_namespaces = Vec::new();
                for (namespace_id, namespace) in &namespaces {
                    if let Some(min_version) = min_namespace_versions.get(&(warehouse_id, *namespace_id)) {
                        if namespace.version() < *min_version {
                            // Refetch with required minimum version
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
                                    // Continue with existing namespace
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to refetch namespace {namespace_id} with min version {min_version}: {e}"
                                    );
                                    // Continue with existing namespace
                                }
                            }
                        }
                    }
                }

                for ns_hierarchy in re_fetched_namespaces {
                    namespaces.insert(ns_hierarchy.namespace.namespace_id(), ns_hierarchy.namespace);
                    for ns in ns_hierarchy.parents {
                        namespaces.insert(ns.namespace_id(), ns);
                    }
                }
                
                Ok::<_, ErrorModel>(namespaces)
            });
        }
        Some(tasks)
    };

    // Collect Warehouse results
    let warehouses = match fetch_warehouses_tasks {
        Some(mut tasks) => {
            let mut warehouses = HashMap::new();

            while let Some(res) = tasks.join_next().await {
                let (warehouse_id, warehouse) = res.map_err(|e| {
                    ErrorModel::internal(
                        "Failed to join fetch warehouse task",
                        "FailedToJoinFetchWarehouseTask",
                        Some(Box::new(e)),
                    )
                })?;
                let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse);
                match warehouse {
                    Ok(warehouse) => {
                        warehouses.insert(warehouse.warehouse_id, warehouse);
                    }
                    Err(e)
                        if matches!(
                            e,
                            RequireWarehouseActionError::AuthZCannotUseWarehouseId(_)
                        ) =>
                    {
                        if error_on_not_found {
                            return Err(e.into());
                        } else {
                            tracing::info!(
                                "Warehouse {warehouse_id} not found, treating as denied",
                            );
                            continue;
                        }
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
            warehouses
        }
        None => HashMap::new(),
    };

    // Collect Namespace results
    // First by id
    let mut namespaces_by_id = match fetch_namespace_tasks_by_id {
        Some(mut tasks) => {
            let mut namespaces: HashMap<WarehouseId, HashMap<NamespaceId, _>> = HashMap::new();

            while let Some(res) = tasks.join_next().await {
                let namespace_list = res.map_err(|e| {
                    ErrorModel::internal(
                        "Failed to join fetch namespace task",
                        "FailedToJoinFetchNamespaceTask",
                        Some(Box::new(e)),
                    )
                })??;

                for (_, namespace) in namespace_list {
                    namespaces
                        .entry(namespace.warehouse_id())
                        .or_default()
                        .insert(namespace.namespace_id(), namespace);
                }
            }
            namespaces
        }
        None => HashMap::new(),
    };
    // Then by ident
    let namespace_ident_lookup = match fetch_namespace_tasks_by_ident {
        Some(mut tasks) => {
            let mut namespce_ident_lookup = HashMap::new();
            while let Some(res) = tasks.join_next().await {
                let namespace_list = res.map_err(|e| {
                    ErrorModel::internal(
                        "Failed to join fetch namespace task",
                        "FailedToJoinFetchNamespaceTask",
                        Some(Box::new(e)),
                    )
                })??;

                for (_, namespace) in namespace_list {
                    namespce_ident_lookup.insert(
                        (
                            namespace.warehouse_id(),
                            namespace_ident_to_cache_key(namespace.namespace_ident()),
                        ),
                        namespace.namespace_id(),
                    );
                    namespaces_by_id
                        .entry(namespace.warehouse_id())
                        .or_default()
                        .insert(namespace.namespace_id(), namespace);
                }
            }
            namespce_ident_lookup
        }
        None => HashMap::new(),
    };

    // AuthZ checks
    let namespaces_by_id = Arc::new(namespaces_by_id);
    let namespace_ident_lookup = Arc::new(namespace_ident_lookup);
    let tabular_infos_by_id = Arc::new(tabular_infos_by_id);
    let tabular_infos_by_ident = Arc::new(tabular_infos_by_ident);

    let mut authz_tasks = tokio::task::JoinSet::new();

    for (for_user, actions) in server_checks {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();
        authz_tasks.spawn(async move {
            let (original_indices, actions): (Vec<_>, Vec<_>) = actions.into_iter().unzip();
            let allowed = authorizer
                .are_allowed_server_actions_vec(&metadata, for_user.as_ref(), &actions)
                .await?;
            Ok::<_, ErrorModel>((original_indices, allowed))
        });
    }

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
                Ok::<_, ErrorModel>((original_indices, allowed))
            });
        }
    }

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
                Ok::<_, ErrorModel>((original_indices, allowed))
            });
        };
    }

    for ((warehouse_id, for_user), actions) in namespace_checks_by_id {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();

        let warehouse = match warehouses.get(&warehouse_id) {
            Some(w) => w.clone(),
            None => {
                if error_on_not_found {
                    return Err(WarehouseIdNotFound::new(warehouse_id).into());
                } else {
                    tracing::info!("Warehouse {warehouse_id} not found, treating as denied",);
                    continue;
                }
            }
        };

        let mut checks = Vec::with_capacity(actions.len());
        for (namespace_id, actions) in actions {
            if let Some(namespace) = namespaces_by_id
                .get(&warehouse_id)
                .and_then(|m| m.get(&namespace_id))
            {
                let namespace_hierarchy = build_namespace_hierarchy(
                    namespace,
                    namespaces_by_id
                        .get(&warehouse_id)
                        .unwrap_or(&HashMap::new()),
                );
                checks.push((namespace_hierarchy, actions));
            } else {
                // Namespace not found
                if error_on_not_found {
                    return Err(AuthZCannotSeeNamespace::new(warehouse_id, namespace_id).into());
                } else {
                    tracing::info!(
                        "Namespace {namespace_id} in warehouse {warehouse_id} not found, treating as denied",
                    );
                }
            }
        }

        authz_tasks.spawn(async move {
            let (original_indices, namespace_with_actions): (Vec<_>, Vec<_>) = checks
                .iter()
                .map(|(ns_hierarchy, actions)| {
                    actions
                        .into_iter()
                        .map(move |(i, a)| (i, (ns_hierarchy, *a)))
                })
                .flatten()
                .unzip();
            let allowed = authorizer
                .are_allowed_namespace_actions_vec(
                    &metadata,
                    for_user.as_ref(),
                    &warehouse,
                    &namespace_with_actions,
                )
                .await?;
            Ok::<_, ErrorModel>((original_indices, allowed))
        });
    }

    for ((warehouse_id, for_user), actions) in namespace_checks_by_ident {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();

        let warehouse = match warehouses.get(&warehouse_id) {
            Some(w) => w.clone(),
            None => {
                if error_on_not_found {
                    return Err(WarehouseIdNotFound::new(warehouse_id).into());
                } else {
                    tracing::info!("Warehouse {warehouse_id} not found, treating as denied",);
                    continue;
                }
            }
        };

        let mut checks = Vec::with_capacity(actions.len());
        for (namespace_ident, actions) in actions {
            // Look up namespace ID from ident
            let cache_key = (warehouse_id, namespace_ident_to_cache_key(&namespace_ident));
            if let Some(namespace_id) = namespace_ident_lookup.get(&cache_key) {
                if let Some(namespace) = namespaces_by_id
                    .get(&warehouse_id)
                    .and_then(|m| m.get(namespace_id))
                {
                    let namespace_hierarchy = build_namespace_hierarchy(
                        namespace,
                        namespaces_by_id
                            .get(&warehouse_id)
                            .unwrap_or(&HashMap::new()),
                    );
                    checks.push((namespace_hierarchy, actions));
                } else {
                    // Namespace not found by ID (shouldn't happen if lookup succeeded)
                    return Err(ErrorModel::internal(
                        "Could not find namespace by ID after successful lookup by ident",
                        "InconsistentNamespaceLookup",
                        None,
                    ));
                }
            } else {
                // Namespace not found by ident
                if error_on_not_found {
                    return Err(AuthZCannotSeeNamespace::new(warehouse_id, namespace_ident).into());
                } else {
                    tracing::info!(
                        "Namespace {namespace_ident} in warehouse {warehouse_id} not found, treating as denied",
                    );
                }
            }
        }

        authz_tasks.spawn(async move {
            let (original_indices, namespace_with_actions): (Vec<_>, Vec<_>) = checks
                .iter()
                .map(|(ns_hierarchy, actions)| {
                    actions
                        .into_iter()
                        .map(move |(i, a)| (i, (ns_hierarchy, *a)))
                })
                .flatten()
                .unzip();
            let allowed = authorizer
                .are_allowed_namespace_actions_vec(
                    &metadata,
                    for_user.as_ref(),
                    &warehouse,
                    &namespace_with_actions,
                )
                .await?;
            Ok::<_, ErrorModel>((original_indices, allowed))
        });
    }

    for ((warehouse_id, for_user), actions) in tabular_checks_by_id {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();
        let tabular_infos_by_id = tabular_infos_by_id.clone();
        let namespaces_by_id = namespaces_by_id.clone();

        let warehouse = match warehouses.get(&warehouse_id) {
            Some(w) => w.clone(),
            None => {
                if error_on_not_found {
                    return Err(WarehouseIdNotFound::new(warehouse_id).into());
                } else {
                    tracing::info!("Warehouse {warehouse_id} not found, treating as denied");
                    continue;
                }
            }
        };

        authz_tasks.spawn(async move {
            let mut checks = Vec::with_capacity(actions.len());
            for (tabular_id, actions_on_tabular) in &actions {
                let Some(tabular_info) = tabular_infos_by_id.get(&(warehouse_id, *tabular_id)) else {
                    // Tabular not found - skip these checks
                    tracing::info!(
                        "Tabular {tabular_id} in warehouse {warehouse_id} not found, skipping checks"
                    );
                    continue;
                };
                let namespace_id = tabular_info.namespace_id();
                let Some(namespace) = namespaces_by_id
                    .get(&warehouse_id)
                    .and_then(|m| m.get(&namespace_id)) else {
                    // Namespace not found - skip these checks
                    tracing::info!(
                        "Namespace {namespace_id} in warehouse {warehouse_id} not found, skipping checks"
                    );
                    continue;
                };

                for (i, (table_action, view_action)) in actions_on_tabular {
                    let action_on_tabular = match &tabular_info {
                        ViewOrTableInfo::Table(table_info) => {
                            if let Some(action) = table_action {
                                Some(ActionOnTableOrView::Table((table_info, *action)))
                            } else {
                                None
                            }
                        }
                        ViewOrTableInfo::View(view_info) => {
                            if let Some(action) = view_action {
                                Some(ActionOnTableOrView::View((view_info, *action)))
                            } else {
                                None
                            }
                        }
                    };
                    
                    if let Some(action) = action_on_tabular {
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
            Ok::<_, ErrorModel>((original_indices, allowed))
        });
    }

    for ((warehouse_id, for_user), actions) in tabular_checks_by_ident {
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();
        let tabular_infos_by_ident = tabular_infos_by_ident.clone();
        let namespaces_by_id = namespaces_by_id.clone();

        let warehouse = match warehouses.get(&warehouse_id) {
            Some(w) => w.clone(),
            None => {
                if error_on_not_found {
                    return Err(WarehouseIdNotFound::new(warehouse_id).into());
                } else {
                    tracing::info!("Warehouse {warehouse_id} not found, treating as denied");
                    continue;
                }
            }
        };

        authz_tasks.spawn(async move {
            let mut checks = Vec::with_capacity(actions.len());
            for (tabular_ident, actions_on_tabular) in &actions {
                let Some(tabular_info) = tabular_infos_by_ident.get(&(warehouse_id, tabular_ident.as_table_ident().clone())) else {
                    // Tabular not found - skip these checks
                    tracing::info!(
                        "Tabular {:?} in warehouse {warehouse_id} not found, skipping checks",
                        tabular_ident
                    );
                    continue;
                };
                let namespace_id = tabular_info.namespace_id();
                let Some(namespace) = namespaces_by_id
                    .get(&warehouse_id)
                    .and_then(|m| m.get(&namespace_id)) else {
                    // Namespace not found - skip these checks
                    tracing::info!(
                        "Namespace {namespace_id} in warehouse {warehouse_id} not found, skipping checks"
                    );
                    continue;
                };

                for (i, (table_action, view_action)) in actions_on_tabular {
                    let action_on_tabular = match &tabular_info {
                        ViewOrTableInfo::Table(table_info) => {
                            if let Some(action) = table_action {
                                Some(ActionOnTableOrView::Table((table_info, *action)))
                            } else {
                                None
                            }
                        }
                        ViewOrTableInfo::View(view_info) => {
                            if let Some(action) = view_action {
                                Some(ActionOnTableOrView::View((view_info, *action)))
                            } else {
                                None
                            }
                        }
                    };
                    
                    if let Some(action) = action_on_tabular {
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
            Ok::<_, ErrorModel>((original_indices, allowed))
        });
    }

    while let Some(res) = authz_tasks.join_next().await {
        let (original_indices, allowed) = res.map_err(|e| {
            ErrorModel::internal(
                "Failed to join authorization task",
                "FailedToJoinAuthZTask",
                Some(Box::new(e)),
            )
        })??;
        for (i, is_allowed) in original_indices
            .into_iter()
            .zip(allowed.into_inner().into_iter())
        {
            results[i].allowed = is_allowed;
        }
    }

    Ok(CatalogActionsBatchCheckResponse { results })
}
