use std::collections::{HashMap, HashSet};

use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId, WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps, NamespaceId,
        SecretStore, State, TabularId, TabularIdentOwned, TabularListFlags, WarehouseIdNotFound,
        WarehouseStatus,
        authz::{
            AuthZProjectOps, AuthZServerOps, Authorizer, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogProjectAction, CatalogServerAction, CatalogTableAction, CatalogViewAction,
            CatalogWarehouseAction, RequireWarehouseActionError, UserOrRole,
        },
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
    let mut warehouse_checks: HashMap<WarehouseId, HashMap<Option<UserOrRole>, Vec<_>>> =
        HashMap::new();
    let mut namespace_checks: HashMap<NamespaceIdentOrUuid, HashMap<Option<UserOrRole>, Vec<_>>> =
        HashMap::new();
    let mut tabular_checks_by_id: HashMap<
        WarehouseId,
        HashMap<TabularId, HashMap<Option<UserOrRole>, Vec<_>>>,
    > = HashMap::new();
    let mut tabular_checks_by_ident: HashMap<
        WarehouseId,
        HashMap<TabularIdentOwned, HashMap<Option<UserOrRole>, Vec<_>>>,
    > = HashMap::new();
    let mut seen_warehouse_ids = HashSet::new();
    let mut seen_namespaces = HashSet::new();
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
                warehouse_checks
                    .entry(warehouse_id)
                    .or_default()
                    .entry(for_user)
                    .or_default()
                    .push((i, action));
            }
            CatalogActionCheckOperation::Namespace { action, namespace } => {
                seen_warehouse_ids.insert(namespace.warehouse_id());
                seen_namespaces.insert(namespace.clone());
                namespace_checks
                    .entry(namespace)
                    .or_default()
                    .entry(for_user)
                    .or_default()
                    .push((i, action));
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
                            .entry(warehouse_id)
                            .or_default()
                            .entry(tabular_id)
                            .or_default()
                            .entry(for_user)
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
                            .entry(warehouse_id)
                            .or_default()
                            .entry(tabular_ident)
                            .or_default()
                            .entry(for_user)
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
                            .entry(warehouse_id)
                            .or_default()
                            .entry(tabular_id)
                            .or_default()
                            .entry(for_user)
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
                            .entry(warehouse_id)
                            .or_default()
                            .entry(tabular_ident)
                            .or_default()
                            .entry(for_user)
                            .or_default()
                            .push((i, (None, Some(action))));
                    }
                }
            }
        }
    }

    // Load required entities
    let fetch_tabular_by_id_tasks = if !tabular_checks_by_ident.is_empty() {
        let mut tasks = tokio::task::JoinSet::new();
        for (warehouse_id, tables_map) in &tabular_checks_by_ident {
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

    let fetch_warehouses_tasks = if !seen_warehouse_ids.is_empty() {
        let mut tasks = tokio::task::JoinSet::new();
        for warehouse_id in seen_warehouse_ids {
            let catalog_state = catalog_state.clone();
            tasks.spawn(async move {
                (
                    warehouse_id,
                    C::get_warehouse_by_id(
                        warehouse_id,
                        WarehouseStatus::active_and_inactive(),
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

    let warehouses = match fetch_warehouses_tasks {
        Some(mut tasks) => {
            let mut warehouses = HashMap::new();
            while let Some(res) = tasks.join_next().await {
                let warehouse = res.map_err(|e| {
                    ErrorModel::internal(
                        "Failed to fetch warehouse",
                        "FailedToFetchWarehouses",
                        Some(Box::new(e)),
                    )
                })?;
                let warehouse = authorizer.require_warehouse_presence(warehouse.0, warehouse.1);
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
                            // Not found, skip
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

    for (warehouse_id, user_map) in warehouse_checks {
        for (for_user, actions) in user_map {
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
