use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId, WarehouseId,
    api::{ApiContext, RequestMetadata, Result, iceberg::v1::PaginationQuery},
    request_metadata::ProjectIdMissing,
    service::{
        ArcProjectId, ArcRole, BasicTabularInfo, CachePolicy, CatalogGetNamespaceError,
        CatalogListRolesByIdFilter, CatalogNamespaceOps, CatalogRoleOps, CatalogStore,
        CatalogTabularOps, CatalogWarehouseOps, GenericTabularInfo, GetRoleAcrossProjectsError,
        NamespaceId, NamespaceVersion, NamespaceWithParent, ResolvedWarehouse, RoleId,
        RoleIdNotFound, SecretStore, State, TableInfo, TabularId, TabularIdentOwned,
        TabularListFlags, UserId, ViewInfo, ViewOrTableInfo, WarehouseStatus, WarehouseVersion,
        authz::{
            ActionDescriptor, ActionOnGenericTable, ActionOnTable, ActionOnTableOrView,
            ActionOnView, AuthZCannotSeeGenericTable, AuthZCannotSeeNamespace, AuthZCannotSeeTable,
            AuthZCannotSeeView, AuthZCannotUseWarehouseId, AuthZError, AuthZProjectOps,
            AuthZServerOps, AuthZTableOps, AuthorizationBackendUnavailable,
            AuthorizationCountMismatch, AuthorizationDecision, Authorizer, AuthzNamespaceOps,
            AuthzWarehouseOps, CatalogAction, CatalogGenericTableAction, CatalogNamespaceAction,
            CatalogProjectAction, CatalogServerAction, CatalogTableAction, CatalogViewAction,
            CatalogWarehouseAction, DeterminingFactor, MustUse, RequireNamespaceActionError,
            RequireTableActionError, RequireWarehouseActionError,
            RoleAssignee as AuthZRoleAssignee, UserOrRole as AuthzUserOrRole, UserOrRoleId,
        },
        events::{
            APIEventContext, Authorization,
            context::{
                ENTITY_TYPE_GENERIC_TABLE, ENTITY_TYPE_NAMESPACE, ENTITY_TYPE_PROJECT,
                ENTITY_TYPE_SERVER, ENTITY_TYPE_TABLE, ENTITY_TYPE_VIEW, ENTITY_TYPE_WAREHOUSE,
                EntityDescriptor, FIELD_NAME_GENERIC_TABLE, FIELD_NAME_GENERIC_TABLE_ID,
                FIELD_NAME_NAMESPACE, FIELD_NAME_NAMESPACE_ID, FIELD_NAME_PROJECT_ID,
                FIELD_NAME_TABLE, FIELD_NAME_TABLE_ID, FIELD_NAME_VIEW, FIELD_NAME_VIEW_ID,
                FIELD_NAME_WAREHOUSE_ID, IntrospectPermissions,
            },
        },
        namespace_cache::namespace_ident_to_cache_key,
    },
};

#[derive(Hash, Eq, Debug, Clone, Serialize, Deserialize, PartialEq, derive_more::From)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
/// Identifies a user or a role
pub enum UserOrRole {
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    #[cfg_attr(feature = "open-api", schema(title = "UserOrRoleUser"))]
    /// Id of the user
    User(UserId),
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    #[cfg_attr(feature = "open-api", schema(title = "UserOrRoleRole"))]
    /// Id of the role
    Role(RoleAssignee),
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
/// Assignees to a role
pub struct RoleAssignee(RoleId);

impl RoleAssignee {
    #[must_use]
    pub fn from_role(role: RoleId) -> Self {
        RoleAssignee(role)
    }

    #[must_use]
    pub fn role_id(&self) -> RoleId {
        self.0
    }
}

impl RoleId {
    #[must_use]
    pub fn into_api_assignee(self) -> RoleAssignee {
        RoleAssignee::from_role(self)
    }
}

impl From<&AuthzUserOrRole> for UserOrRole {
    fn from(value: &AuthzUserOrRole) -> Self {
        match value {
            AuthzUserOrRole::User(user_id) => UserOrRole::User(user_id.clone()),
            AuthzUserOrRole::Role(role_assignee) => {
                UserOrRole::Role(role_assignee.role().id().into_api_assignee())
            }
        }
    }
}

impl AuthzUserOrRole {
    #[must_use]
    pub fn api_user_or_role(&self) -> UserOrRole {
        match self {
            AuthzUserOrRole::User(user_id) => UserOrRole::User(user_id.clone()),
            AuthzUserOrRole::Role(role_assignee) => {
                UserOrRole::Role(role_assignee.role().id().into_api_assignee())
            }
        }
    }
}

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
/// Identifier for a tabular (table, view, or generic table) — either a UUID
/// or its name and namespace. Wire format primary names are `table-id` and
/// `table`; `view_id` / `view` and `generic_table_id` / `generic_table` are
/// accepted as input aliases for client ergonomics.
pub enum TabularIdentOrUuid {
    #[serde(rename_all = "kebab-case")]
    IdInWarehouse {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(alias = "view_id", alias = "generic_table_id")]
        table_id: uuid::Uuid,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[cfg_attr(feature = "open-api", schema(value_type = Vec<String>))]
        namespace: NamespaceIdent,
        /// Name of the table, view, or generic table.
        #[serde(alias = "view", alias = "generic_table")]
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
pub enum CatalogActionCheckOperation {
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
    GenericTable {
        action: CatalogGenericTableAction,
        #[serde(flatten)]
        generic_table: TabularIdentOrUuid,
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
    pub id: Option<String>,
    /// The user or role to check access for.
    /// If not specified, the identity of the user making the request is used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<UserOrRole>,
    /// The operation to check
    pub operation: CatalogActionCheckOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CatalogActionsBatchCheckRequest {
    /// List of checks to perform
    pub checks: Vec<CatalogActionCheckItem>,
    /// If true, return 404 error when resources are not found.
    /// If false, treat missing resources as denied (allowed = false).
    /// Defaults to false.
    #[serde(default)]
    pub error_on_not_found: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CatalogActionsBatchCheckResponse {
    pub results: Vec<CatalogActionsBatchCheckResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CatalogActionsBatchCheckResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub allowed: bool,
    /// Policies/rules that determined this decision. Internal-only: carried
    /// to the audit event, never serialised into the API response.
    #[serde(skip)]
    pub determined_by: Vec<DeterminingFactor>,
}

/// Convert a request-side `UserOrRole` (which only carries identifiers) to
/// the service-level [`UserOrRoleId`] used in audit `Authorization` entries.
/// This avoids forcing a Role lookup just for logging.
fn principal_for_audit(identity: &UserOrRole) -> UserOrRoleId {
    match identity {
        UserOrRole::User(id) => UserOrRoleId::User(id.clone()),
        UserOrRole::Role(assignee) => UserOrRoleId::Role(assignee.role_id()),
    }
}

impl CatalogActionCheckOperation {
    /// Map this operation to the `(entity, action)` pair that the audit layer
    /// records. The shape mirrors what other audit events emit for the same
    /// resource type, so consumers see one uniform schema across single-check
    /// and batch-check events.
    ///
    /// `ambient_project_id` is the request-scoped fallback used for `Project`
    /// operations whose request body did not specify a `project_id` — the
    /// same value [`RequestMetadata::require_project_id`] would resolve.
    /// Passing it ensures every project audit entry carries
    /// `FIELD_NAME_PROJECT_ID`, matching what the actual authorization check
    /// runs against.
    fn to_audit_entity_action(
        &self,
        ambient_project_id: Option<&ProjectId>,
    ) -> (EntityDescriptor, ActionDescriptor) {
        match self {
            CatalogActionCheckOperation::Server { action } => (
                EntityDescriptor::new(ENTITY_TYPE_SERVER),
                action.action_descriptor(),
            ),
            CatalogActionCheckOperation::Project { action, project_id } => {
                let mut entity = EntityDescriptor::new(ENTITY_TYPE_PROJECT);
                if let Some(pid) = project_id.as_ref().or(ambient_project_id) {
                    entity = entity.field(FIELD_NAME_PROJECT_ID, pid);
                }
                (entity, action.action_descriptor())
            }
            CatalogActionCheckOperation::Warehouse {
                action,
                warehouse_id,
            } => (
                EntityDescriptor::new(ENTITY_TYPE_WAREHOUSE)
                    .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id),
                action.action_descriptor(),
            ),
            CatalogActionCheckOperation::Namespace { action, namespace } => {
                let entity = match namespace {
                    NamespaceIdentOrUuid::Id {
                        namespace_id,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_NAMESPACE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE_ID, namespace_id),
                    NamespaceIdentOrUuid::Name {
                        namespace,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_NAMESPACE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE, &namespace.to_url_string()),
                };
                (entity, action.action_descriptor())
            }
            CatalogActionCheckOperation::Table { action, table } => {
                let entity = match table {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_TABLE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_TABLE_ID, table_id),
                    TabularIdentOrUuid::Name {
                        namespace,
                        table,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_TABLE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE, &namespace.to_url_string())
                        .field(FIELD_NAME_TABLE, table),
                };
                (entity, action.action_descriptor())
            }
            CatalogActionCheckOperation::View { action, view } => {
                let entity = match view {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_VIEW)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_VIEW_ID, table_id),
                    TabularIdentOrUuid::Name {
                        namespace,
                        table,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_VIEW)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE, &namespace.to_url_string())
                        .field(FIELD_NAME_VIEW, table),
                };
                (entity, action.action_descriptor())
            }
            CatalogActionCheckOperation::GenericTable {
                action,
                generic_table,
            } => {
                let entity = match generic_table {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_GENERIC_TABLE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_GENERIC_TABLE_ID, table_id),
                    TabularIdentOrUuid::Name {
                        namespace,
                        table,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_GENERIC_TABLE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE, &namespace.to_url_string())
                        .field(FIELD_NAME_GENERIC_TABLE, table),
                };
                (entity, action.action_descriptor())
            }
        }
    }
}

/// Convert a single batch-check tuple into an [`Authorization`] suitable for
/// the audit `authorizations` array. Self-contained: each entry carries its
/// own id, principal, entity, action, and decision.
///
/// `index` is the position of this tuple in the request array; it's used as
/// a stable fallback `id` when the client did not supply one. This mirrors
/// the API response's index-as-id convention so every audit entry can be
/// correlated 1:1 with the corresponding response entry, and so individual
/// decisions can be pinpointed in the logs even when no client id was set.
fn check_to_authorization(
    item: &CatalogActionCheckItem,
    index: usize,
    ambient_project_id: Option<&ProjectId>,
    allowed: Option<bool>,
    determined_by: Vec<DeterminingFactor>,
) -> Authorization {
    let (entity, action) = item.operation.to_audit_entity_action(ambient_project_id);
    Authorization {
        id: Some(item.id.clone().unwrap_or_else(|| index.to_string())),
        for_principal: item.identity.as_ref().map(principal_for_audit),
        action,
        entity,
        allowed,
        determined_by,
    }
}

// Type aliases for complex grouped check types
type ServerChecksMap = HashMap<Option<UserOrRole>, Vec<(usize, CatalogServerAction)>>;
type ProjectChecksMap =
    HashMap<ArcProjectId, HashMap<Option<UserOrRole>, Vec<(usize, CatalogProjectAction)>>>;
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
type TabularActionPair = (
    Option<CatalogTableAction>,
    Option<CatalogViewAction>,
    Option<CatalogGenericTableAction>,
);
type TabularChecksByIdMap =
    HashMap<(WarehouseId, Option<UserOrRole>), HashMap<TabularId, Vec<(usize, TabularActionPair)>>>;
type TabularChecksByIdentMap = HashMap<
    (WarehouseId, Option<UserOrRole>),
    HashMap<TabularIdentOwned, Vec<(usize, TabularActionPair)>>,
>;
type AuthzTaskJoinSet =
    tokio::task::JoinSet<Result<(Vec<usize>, MustUse<Vec<AuthorizationDecision>>), AuthZError>>;

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
            determined_by: Vec::new(),
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
                            .push((i, (Some(action), None, None)));
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
                            .push((i, (Some(action), None, None)));
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
                            .push((i, (None, Some(action), None)));
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
                            .push((i, (None, Some(action), None)));
                    }
                }
            }
            CatalogActionCheckOperation::GenericTable {
                action,
                generic_table,
            } => {
                grouped
                    .seen_warehouse_ids
                    .insert(generic_table.warehouse_id());
                match generic_table {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => {
                        let tabular_id = TabularId::GenericTable(table_id.into());
                        grouped
                            .tabular_checks_by_id
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(tabular_id)
                            .or_default()
                            .push((i, (None, None, Some(action))));
                    }
                    TabularIdentOrUuid::Name {
                        namespace,
                        table: gt_name,
                        warehouse_id,
                    } => {
                        let tabular_ident =
                            TabularIdentOwned::GenericTable(TableIdent::new(namespace, gt_name));
                        grouped
                            .tabular_checks_by_ident
                            .entry((warehouse_id, for_user))
                            .or_default()
                            .entry(tabular_ident)
                            .or_default()
                            .push((i, (None, None, Some(action))));
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
    AuthZError,
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
                    .await
                    .map(|m| m.into_values().collect()),
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
                ViewOrTableInfo::GenericTable(info) => {
                    TabularIdentOwned::GenericTable(info.tabular_ident().clone())
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
) -> Result<HashMap<WarehouseId, Arc<ResolvedWarehouse>>, AuthZError> {
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
fn convert_tabular_action<'a, 'u>(
    tabular_info: &'a ViewOrTableInfo,
    table_action: Option<CatalogTableAction>,
    view_action: Option<CatalogViewAction>,
    generic_table_action: Option<CatalogGenericTableAction>,
    user: Option<&'u AuthzUserOrRole>,
) -> Option<
    ActionOnTableOrView<
        'a,
        'u,
        TableInfo,
        ViewInfo,
        CatalogTableAction,
        CatalogViewAction,
        GenericTabularInfo,
        CatalogGenericTableAction,
    >,
> {
    match tabular_info {
        ViewOrTableInfo::Table(table_info) => table_action.map(|action| {
            ActionOnTableOrView::Table(ActionOnTable {
                info: table_info,
                action,
                user,
                is_delegated_execution: false,
            })
        }),
        ViewOrTableInfo::View(view_info) => view_action.map(|action| {
            ActionOnTableOrView::View(ActionOnView {
                info: view_info,
                action,
                user,
                is_delegated_execution: false,
            })
        }),
        ViewOrTableInfo::GenericTable(gt_info) => generic_table_action.map(|action| {
            ActionOnTableOrView::GenericTable(ActionOnGenericTable {
                info: gt_info,
                action,
                user,
                is_delegated_execution: false,
            })
        }),
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
        HashMap<(WarehouseId, Vec<String>), NamespaceId>,
    ),
    AuthZError,
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
                .await
                .map_err(RequireNamespaceActionError::from)?;

                // Refetch namespaces that don't meet minimum version requirements
                let re_fetched_namespaces = refetch_outdated_namespaces::<C>(
                    warehouse_id,
                    &namespaces,
                    &min_namespace_versions,
                    catalog_state.clone(),
                )
                .await
                .map_err(RequireNamespaceActionError::from)?;
                for ns_hierarchy in re_fetched_namespaces {
                    namespaces.insert(
                        ns_hierarchy.namespace.namespace_id(),
                        ns_hierarchy.namespace,
                    );
                    for ns in ns_hierarchy.parents {
                        namespaces.insert(ns.namespace_id(), ns);
                    }
                }

                Ok::<_, AuthZError>((true, namespaces))
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
                        .await
                        .map_err(RequireNamespaceActionError::from)?;

                // Refetch namespaces that don't meet minimum version requirements
                let re_fetched_namespaces = refetch_outdated_namespaces::<C>(
                    warehouse_id,
                    &namespaces,
                    &min_namespace_versions,
                    catalog_state.clone(),
                )
                .await
                .map_err(RequireNamespaceActionError::from)?;

                for ns_hierarchy in re_fetched_namespaces {
                    namespaces.insert(
                        ns_hierarchy.namespace.namespace_id(),
                        ns_hierarchy.namespace,
                    );
                    for ns in ns_hierarchy.parents {
                        namespaces.insert(ns.namespace_id(), ns);
                    }
                }

                Ok::<_, AuthZError>((false, namespaces))
            });
        }
    }

    // Collect results
    let mut namespaces_by_id: HashMap<WarehouseId, HashMap<NamespaceId, _>> = HashMap::new();
    let mut namespace_ident_lookup = HashMap::new();

    while let Some(res) = tasks.join_next().await {
        let (is_by_ident, namespace_list) = res.map_err(|e| {
            RequireNamespaceActionError::from(
                AuthorizationBackendUnavailable::new(Box::new(e))
                    .append_detail("Failed to join fetch namespace task"),
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

/// Fetch `Arc<Role>` for every `Role(RoleId)` identity referenced in the check items.
/// Returns an error if any requested role ID is not found in the catalog.
async fn fetch_identity_roles<C: CatalogStore>(
    checks: &[CatalogActionCheckItem],
    catalog_state: C::State,
) -> Result<HashMap<RoleId, ArcRole>, AuthZError> {
    let mut role_ids: Vec<RoleId> = checks
        .iter()
        .filter_map(|c| {
            if let Some(UserOrRole::Role(r)) = c.identity.as_ref() {
                Some(r.role_id())
            } else {
                None
            }
        })
        .collect();
    role_ids.sort_unstable();
    role_ids.dedup();

    if role_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let filter = CatalogListRolesByIdFilter::builder()
        .role_ids(Some(&role_ids))
        .build();
    let response = C::list_roles_across_projects(
        filter,
        PaginationQuery::new_with_page_size(i64::try_from(role_ids.len()).unwrap_or(i64::MAX)),
        catalog_state,
    )
    .await?;

    let role_map: HashMap<RoleId, ArcRole> =
        response.roles.into_iter().map(|r| (r.id(), r)).collect();

    for rid in &role_ids {
        if !role_map.contains_key(rid) {
            return Err(GetRoleAcrossProjectsError::from(RoleIdNotFound::new(*rid)).into());
        }
    }
    Ok(role_map)
}

/// Convert an API-level `UserOrRole` (which carries only a `RoleId`) into the
/// internal `AuthzUserOrRole` (which carries the full `Arc<Role>`).
/// Unreachable if a role ID is not present in `roles` — callers must pre-populate
/// the map via `fetch_identity_roles`.
fn resolve_identity(
    identity: Option<UserOrRole>,
    roles: &HashMap<RoleId, ArcRole>,
) -> Option<AuthzUserOrRole> {
    match identity {
        None => None,
        Some(UserOrRole::User(id)) => Some(AuthzUserOrRole::User(id)),
        Some(UserOrRole::Role(rid)) => {
            let arc_role = roles
                .get(&rid.role_id())
                .unwrap_or_else(|| {
                    unreachable!(
                        "role {rid:?} missing from pre-fetched map — bug in fetch_identity_roles"
                    )
                })
                .clone();
            Some(AuthzUserOrRole::Role(AuthZRoleAssignee::from_role(
                arc_role,
            )))
        }
    }
}

/// Spawn server authorization check tasks
fn spawn_server_checks<A: Authorizer>(
    authz_tasks: &mut AuthzTaskJoinSet,
    server_checks: ServerChecksMap,
    authorizer: &A,
    metadata: &RequestMetadata,
    roles: &HashMap<RoleId, ArcRole>,
) {
    for (for_user, actions) in server_checks {
        let authz_for_user = resolve_identity(for_user, roles);
        let authorizer = authorizer.clone();
        let metadata = metadata.clone();
        authz_tasks.spawn(async move {
            let (original_indices, actions): (Vec<_>, Vec<_>) = actions.into_iter().unzip();
            let allowed = authorizer
                .are_allowed_server_actions_vec(&metadata, authz_for_user.as_ref(), &actions)
                .await?;
            Ok::<_, AuthZError>((original_indices, allowed))
        });
    }
}

/// Spawn project authorization check tasks
fn spawn_project_checks<A: Authorizer>(
    authz_tasks: &mut AuthzTaskJoinSet,
    project_checks: ProjectChecksMap,
    authorizer: &A,
    metadata: &RequestMetadata,
    roles: &HashMap<RoleId, ArcRole>,
) {
    for (project_id, user_map) in project_checks {
        for (for_user, actions) in user_map {
            let authz_for_user = resolve_identity(for_user, roles);
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
                        authz_for_user.as_ref(),
                        &projects_with_actions,
                    )
                    .await?;
                Ok::<_, AuthZError>((original_indices, allowed))
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
    roles: &HashMap<RoleId, ArcRole>,
) {
    for ((warehouse_id, for_user), actions) in warehouse_checks {
        let authz_for_user = resolve_identity(for_user, roles);
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
                        authz_for_user.as_ref(),
                        &warehouses_with_actions,
                    )
                    .await?;
                Ok::<_, AuthZError>((original_indices, allowed))
            });
        }
    }
}

/// Parameters for namespace check spawning by ID
struct NamespaceCheckByIdParams<'a, A: Authorizer> {
    authz_tasks: &'a mut AuthzTaskJoinSet,
    namespace_checks_by_id: NamespaceChecksByIdMap,
    warehouses: &'a HashMap<WarehouseId, Arc<ResolvedWarehouse>>,
    namespaces_by_id: &'a HashMap<WarehouseId, HashMap<NamespaceId, NamespaceWithParent>>,
    authorizer: &'a A,
    metadata: &'a RequestMetadata,
    error_on_not_found: bool,
    roles: &'a HashMap<RoleId, ArcRole>,
}

/// Spawn namespace authorization check tasks (by ID)
fn spawn_namespace_checks_by_id<A: Authorizer>(
    params: NamespaceCheckByIdParams<'_, A>,
) -> Result<(), AuthZError> {
    let NamespaceCheckByIdParams {
        authz_tasks,
        namespace_checks_by_id,
        warehouses,
        namespaces_by_id,
        authorizer,
        metadata,
        error_on_not_found,
        roles,
    } = params;
    for ((warehouse_id, for_user), actions) in namespace_checks_by_id {
        let authz_for_user = resolve_identity(for_user, roles);
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
                    authz_for_user.as_ref(),
                    &warehouse,
                    &parent_namespaces,
                    &namespace_with_actions,
                )
                .await?;
            Ok::<_, AuthZError>((original_indices, allowed))
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
    namespace_ident_lookup: &'a HashMap<(WarehouseId, Vec<String>), NamespaceId>,
    authorizer: &'a A,
    metadata: &'a RequestMetadata,
    error_on_not_found: bool,
    roles: &'a HashMap<RoleId, ArcRole>,
}

/// Spawn namespace authorization check tasks (by ident)
fn spawn_namespace_checks_by_ident<A: Authorizer>(
    params: NamespaceCheckByIdentParams<'_, A>,
) -> Result<(), AuthZError> {
    let NamespaceCheckByIdentParams {
        authz_tasks,
        namespace_checks_by_ident,
        warehouses,
        namespaces_by_id,
        namespace_ident_lookup,
        authorizer,
        metadata,
        error_on_not_found,
        roles,
    } = params;
    for ((warehouse_id, for_user), actions) in namespace_checks_by_ident {
        let authz_for_user = resolve_identity(for_user, roles);
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
                    RequireNamespaceActionError::from(AuthorizationBackendUnavailable::new(Box::new(std::io::Error::other(
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
                    authz_for_user.as_ref(),
                    &warehouse,
                    &parent_namespaces,
                    &namespace_with_actions,
                )
                .await?;
            Ok::<_, AuthZError>((original_indices, allowed))
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
    roles: &'a HashMap<RoleId, ArcRole>,
}

/// Spawn tabular authorization check tasks (by ID)
fn spawn_tabular_checks_by_id<A: Authorizer>(
    params: TabularCheckByIdParams<'_, A>,
) -> Result<(), AuthZError> {
    let TabularCheckByIdParams {
        authz_tasks,
        tabular_checks_by_id,
        warehouses,
        tabular_infos_by_id,
        namespaces_by_id,
        authorizer,
        metadata,
        error_on_not_found,
        roles,
    } = params;
    for ((warehouse_id, for_user), actions) in tabular_checks_by_id {
        let authz_for_user = resolve_identity(for_user, roles);
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
                            TabularId::GenericTable(gt_id) => {
                                return Err(AuthZCannotSeeGenericTable::new_not_found(warehouse_id, *gt_id).into());
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

                for (i, (table_action, view_action, gt_action)) in actions_on_tabular {
                    if let Some(action) = convert_tabular_action(tabular_info, table_action.clone(), view_action.clone(), gt_action.clone(), authz_for_user.as_ref()) {
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
                    &warehouse,
                    parent_namespaces,
                    &tabular_with_actions,
                )
                .await?;
            Ok::<_, AuthZError>((original_indices, allowed))
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
    roles: &'a HashMap<RoleId, ArcRole>,
}

/// Spawn tabular authorization check tasks (by ident)
fn spawn_tabular_checks_by_ident<A: Authorizer>(
    params: TabularCheckByIdentParams<'_, A>,
) -> Result<(), AuthZError> {
    let TabularCheckByIdentParams {
        authz_tasks,
        tabular_checks_by_ident,
        warehouses,
        tabular_infos_by_ident,
        namespaces_by_id,
        authorizer,
        metadata,
        error_on_not_found,
        roles,
    } = params;
    for ((warehouse_id, for_user), actions) in tabular_checks_by_ident {
        let authz_for_user = resolve_identity(for_user, roles);
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
                            TabularIdentOwned::GenericTable(gt_ident) => {
                                return Err(AuthZCannotSeeGenericTable::new_not_found(warehouse_id, gt_ident.clone()).into());
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

                for (i, (table_action, view_action, gt_action)) in actions_on_tabular {
                    if let Some(action) = convert_tabular_action(tabular_info, table_action.clone(), view_action.clone(), gt_action.clone(), authz_for_user.as_ref()) {
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
                    &warehouse,
                    parent_namespaces,
                    &tabular_with_actions,
                )
                .await?;
            Ok::<_, AuthZError>((original_indices, allowed))
        });
    }
    Ok(())
}

/// Collect authorization results and update the results array
async fn collect_authz_results(
    authz_tasks: &mut AuthzTaskJoinSet,
    results: &mut [CatalogActionsBatchCheckResult],
) -> Result<(), AuthZError> {
    while let Some(res) = authz_tasks.join_next().await {
        let (original_indices, allowed) = res.map_err(|e| {
            RequireWarehouseActionError::from(
                AuthorizationBackendUnavailable::new(Box::new(e))
                    .append_detail("Failed to join authorization check task"),
            )
        })??;
        let decision_vec = allowed.into_inner();
        if original_indices.len() != decision_vec.len() {
            return Err(AuthorizationCountMismatch::new(
                original_indices.len(),
                decision_vec.len(),
                "check endpoint",
            )
            .into());
        }
        for (i, decision) in original_indices.into_iter().zip(decision_vec) {
            results[i].allowed = decision.allowed;
            results[i].determined_by = decision.determined_by;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub async fn check_internal<A: Authorizer, C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<A, C, S>>,
    metadata: RequestMetadata,
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

    let mut event_ctx = APIEventContext::new(
        Arc::new(metadata),
        api_context.v1_state.events,
        (authorizer.server_id(), checks.clone()),
        IntrospectPermissions {},
    );

    // Keep the input tuples so we can pair them with their decisions and
    // attach one structured `Authorization` per tuple to the audit event.
    let checks_for_audit = checks.clone();

    let authz_result = spawn_check_and_collect_results::<C, _>(
        checks,
        catalog_state,
        authorizer,
        event_ctx.request_metadata(),
        error_on_not_found,
    )
    .await;

    // Build one structured `Authorization` per input tuple. Each entry carries
    // its own `id`, `for-principal`, `entity`, `action` and (when the batch
    // produced decisions) the `allowed` flag — so the single
    // `introspect_permissions` audit event records both *what was asked* and
    // *what was answered*, in the same shape as every other audit event, with
    // no index-zipping required by log consumers. Order matches the input.
    let audit_decisions: Option<&[CatalogActionsBatchCheckResult]> =
        authz_result.as_ref().ok().map(Vec::as_slice);
    // Resolve the ambient project once so `Project` checks that omit
    // `project_id` in the request body still record a fully-populated
    // entity (matching what the authorizer actually evaluated).
    let ambient_project_id = event_ctx.request_metadata().preferred_project_id();
    let ambient_project_id_ref = ambient_project_id.as_deref();
    let authorizations: Vec<Authorization> = checks_for_audit
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let result = audit_decisions.and_then(|r| r.get(i));
            let allowed = result.map(|r| r.allowed);
            let determined_by = result.map(|r| r.determined_by.clone()).unwrap_or_default();
            check_to_authorization(c, i, ambient_project_id_ref, allowed, determined_by)
        })
        .collect();
    // For an empty batch (`POST {"checks": []}`) `set_authorizations`
    // intentionally treats this as "unset" so the emit-path's synthesised
    // default fires instead, recording a single
    // "server / introspect_permissions / allowed" row — meaningful as
    // "the call succeeded but checked nothing".
    event_ctx.set_authorizations(authorizations);

    let (_event_ctx, results) = event_ctx.emit_authz(authz_result)?;

    Ok(CatalogActionsBatchCheckResponse { results })
}

#[allow(clippy::too_many_lines)]
async fn spawn_check_and_collect_results<C: CatalogStore, A: Authorizer>(
    checks: Vec<CatalogActionCheckItem>,
    catalog_state: C::State,
    authorizer: A,
    metadata: &RequestMetadata,
    error_on_not_found: bool,
) -> Result<Vec<CatalogActionsBatchCheckResult>, AuthZError> {
    // 0. Resolve all role IDs referenced in identity fields to full Arc<Role>.
    //    Single batched catalog call; errors if any identity role ID is unknown.
    //    Must be called before group_checks consumes `checks`.
    let roles = fetch_identity_roles::<C>(&checks, catalog_state.clone()).await?;

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
    spawn_server_checks(
        &mut authz_tasks,
        server_checks,
        &authorizer,
        metadata,
        &roles,
    );

    // Project checks
    spawn_project_checks(
        &mut authz_tasks,
        project_checks,
        &authorizer,
        metadata,
        &roles,
    );

    // Warehouse checks
    spawn_warehouse_checks(
        &mut authz_tasks,
        warehouse_checks,
        &warehouses,
        &authorizer,
        metadata,
        &roles,
    );

    // Namespace checks by ID
    spawn_namespace_checks_by_id(NamespaceCheckByIdParams {
        authz_tasks: &mut authz_tasks,
        namespace_checks_by_id,
        warehouses: &warehouses,
        namespaces_by_id: &namespaces_by_id,
        authorizer: &authorizer,
        metadata,
        error_on_not_found,
        roles: &roles,
    })?;

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
        roles: &roles,
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
        roles: &roles,
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
        roles: &roles,
    })?;

    collect_authz_results(&mut authz_tasks, &mut results).await?;

    Ok(results)
}
