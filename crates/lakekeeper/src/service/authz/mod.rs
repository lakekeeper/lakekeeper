use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, LazyLock},
};

use axum::Router;
use serde::{Deserialize, Deserializer, Serialize};
use strum::{EnumIter, VariantArray};
use strum_macros::EnumString;

use super::{
    CatalogStore, NamespaceId, ProjectId, RoleId, SecretStore, State, TableId, ViewId, WarehouseId,
    health::HealthExt,
};
use crate::{
    api::{iceberg::v1::Result, management::v1::role::Role},
    request_metadata::RequestMetadata,
    service::{
        Actor, AuthZNamespaceInfo, AuthZTableInfo, AuthZViewInfo, NamespaceWithParent,
        ResolvedWarehouse, ServerId, TableInfo,
    },
};

mod error;
pub mod implementations;
pub use error::*;
mod warehouse;
pub use implementations::allow_all::AllowAllAuthorizer;
pub use warehouse::*;
mod namespace;
pub use namespace::*;
mod role;
pub use role::*;
mod table;
pub use table::*;
mod view;
pub use view::*;
mod project;
pub use project::*;
mod server;
pub use server::*;
mod user;
pub use user::*;

use crate::{api::ApiContext, service::authn::UserId};

/// Custom deserializer that converts various JSON values to strings
fn deserialize_string_map<'de, D>(
    deserializer: D,
) -> Result<Arc<BTreeMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value_map: BTreeMap<String, serde_json::Value> = BTreeMap::deserialize(deserializer)?;
    let string_map = value_map
        .into_iter()
        .map(|(k, v)| {
            let string_val = match v {
                serde_json::Value::String(s) => s,
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => "null".to_string(),
                _ => v.to_string(),
            };
            (k, string_val)
        })
        .collect();
    Ok(Arc::new(string_map))
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
    pub fn role(&self) -> RoleId {
        self.0
    }
}

impl RoleId {
    #[must_use]
    pub fn into_assignees(self) -> RoleAssignee {
        RoleAssignee::from_role(self)
    }
}

impl Actor {
    #[must_use]
    pub fn to_user_or_role(&self) -> Option<UserOrRole> {
        match self {
            Actor::Principal(user) => Some(UserOrRole::User(user.clone())),
            Actor::Role {
                assumed_role,
                principal: _,
            } => Some(UserOrRole::Role(RoleAssignee::from_role(*assumed_role))),
            Actor::Anonymous => None,
        }
    }
}

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

pub trait CatalogAction
where
    Self: std::fmt::Debug + Send + Sync,
{
    fn as_log_str(&self) -> String {
        format!("{self:?}")
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    strum_macros::Display,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
    VariantArray,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperUserAction))]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogUserAction {
    /// Can get all details of the user given its id
    Read,
    /// Can update the user.
    Update,
    /// Can delete this user
    Delete,
}

impl CatalogAction for CatalogUserAction {}

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    strum_macros::Display,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
    VariantArray,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperServerAction))]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogServerAction {
    /// Can create items inside the server (can create Warehouses).
    CreateProject,
    /// Can update all users on this server.
    UpdateUsers,
    /// Can delete all users on this server.
    DeleteUsers,
    /// Can List all users on this server.
    ListUsers,
    /// Can provision user
    ProvisionUsers,
}
impl CatalogAction for CatalogServerAction {}

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    strum_macros::Display,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
    VariantArray,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperProjectAction))]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogProjectAction {
    CreateWarehouse,
    Delete,
    Rename,
    GetMetadata,
    ListWarehouses,
    IncludeInList,
    CreateRole,
    ListRoles,
    SearchRoles,
    GetEndpointStatistics,
    ModifyTaskQueueConfig,
    GetTaskQueueConfig,
    GetProjectTasks,
    ControlProjectTasks,
}
impl CatalogAction for CatalogProjectAction {}

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    strum_macros::Display,
    EnumIter,
    EnumString,
    VariantArray,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperRoleAction))]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogRoleAction {
    Read,
    // Read high level metadata about the role (name & project_id).
    // Meant for cross-project role listing of assignments.
    ReadMetadata,
    Delete,
    Update,
}
impl CatalogAction for CatalogRoleAction {}

#[derive(Debug, Hash, Clone, Eq, PartialEq, Serialize, Deserialize, strum_macros::EnumCount)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperWarehouseAction))]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogWarehouseAction {
    CreateNamespace {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(deserialize_with = "deserialize_string_map")]
        properties: Arc<BTreeMap<String, String>>,
    },
    Delete,
    UpdateStorage,
    UpdateStorageCredential,
    GetMetadata,
    GetConfig,
    ListNamespaces,
    ListEverything,
    Use,
    IncludeInList,
    Deactivate,
    Activate,
    Rename,
    ListDeletedTabulars,
    ModifySoftDeletion,
    GetTaskQueueConfig,
    ModifyTaskQueueConfig,
    GetAllTasks,
    ControlAllTasks,
    SetProtection,
    GetEndpointStatistics,
}
static WAREHOUSE_ACTION_VARIANTS: LazyLock<[CatalogWarehouseAction; 21]> = LazyLock::new(|| {
    [
        CatalogWarehouseAction::CreateNamespace {
            properties: Arc::new(BTreeMap::new()),
        },
        CatalogWarehouseAction::Delete,
        CatalogWarehouseAction::UpdateStorage,
        CatalogWarehouseAction::UpdateStorageCredential,
        CatalogWarehouseAction::GetMetadata,
        CatalogWarehouseAction::GetConfig,
        CatalogWarehouseAction::ListNamespaces,
        CatalogWarehouseAction::ListEverything,
        CatalogWarehouseAction::Use,
        CatalogWarehouseAction::IncludeInList,
        CatalogWarehouseAction::Deactivate,
        CatalogWarehouseAction::Activate,
        CatalogWarehouseAction::Rename,
        CatalogWarehouseAction::ListDeletedTabulars,
        CatalogWarehouseAction::ModifySoftDeletion,
        CatalogWarehouseAction::GetTaskQueueConfig,
        CatalogWarehouseAction::ModifyTaskQueueConfig,
        CatalogWarehouseAction::GetAllTasks,
        CatalogWarehouseAction::ControlAllTasks,
        CatalogWarehouseAction::SetProtection,
        CatalogWarehouseAction::GetEndpointStatistics,
    ]
});
impl CatalogWarehouseAction {
    #[must_use]
    pub fn variants() -> &'static [CatalogWarehouseAction; 21] {
        &WAREHOUSE_ACTION_VARIANTS
    }
}
impl CatalogAction for CatalogWarehouseAction {}

#[derive(Debug, Hash, Clone, Eq, PartialEq, Serialize, Deserialize, strum_macros::EnumCount)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperNamespaceAction))]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogNamespaceAction {
    CreateTable {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(deserialize_with = "deserialize_string_map")]
        properties: Arc<BTreeMap<String, String>>,
    },
    CreateView {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(deserialize_with = "deserialize_string_map")]
        properties: Arc<BTreeMap<String, String>>,
    },
    CreateNamespace {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(deserialize_with = "deserialize_string_map")]
        properties: Arc<BTreeMap<String, String>>,
    },
    Delete,
    UpdateProperties {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        removed_properties: Arc<Vec<String>>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(deserialize_with = "deserialize_string_map")]
        updated_properties: Arc<BTreeMap<String, String>>,
    },
    GetMetadata,
    ListTables,
    ListViews,
    ListNamespaces,
    ListEverything,
    SetProtection,
    IncludeInList,
}
static NAMESPACE_ACTION_VARIANTS: LazyLock<[CatalogNamespaceAction; 12]> = LazyLock::new(|| {
    [
        CatalogNamespaceAction::CreateTable {
            properties: Arc::new(BTreeMap::new()),
        },
        CatalogNamespaceAction::CreateView {
            properties: Arc::new(BTreeMap::new()),
        },
        CatalogNamespaceAction::CreateNamespace {
            properties: Arc::new(BTreeMap::new()),
        },
        CatalogNamespaceAction::Delete,
        CatalogNamespaceAction::UpdateProperties {
            removed_properties: Arc::new(Vec::new()),
            updated_properties: Arc::new(BTreeMap::new()),
        },
        CatalogNamespaceAction::GetMetadata,
        CatalogNamespaceAction::ListTables,
        CatalogNamespaceAction::ListViews,
        CatalogNamespaceAction::ListNamespaces,
        CatalogNamespaceAction::ListEverything,
        CatalogNamespaceAction::SetProtection,
        CatalogNamespaceAction::IncludeInList,
    ]
});
impl CatalogNamespaceAction {
    #[must_use]
    pub fn variants() -> &'static [CatalogNamespaceAction; 12] {
        &NAMESPACE_ACTION_VARIANTS
    }
}
impl CatalogAction for CatalogNamespaceAction {}

#[derive(Debug, Hash, Clone, Eq, PartialEq, Serialize, Deserialize, strum_macros::EnumCount)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperTableAction))]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogTableAction {
    Drop,
    WriteData,
    ReadData,
    GetMetadata,
    Commit {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(deserialize_with = "deserialize_string_map")]
        updated_properties: Arc<BTreeMap<String, String>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        removed_properties: Arc<Vec<String>>,
    },
    Rename,
    IncludeInList,
    Undrop,
    GetTasks,
    ControlTasks,
    SetProtection,
}
static TABLE_ACTION_VARIANTS: LazyLock<[CatalogTableAction; 11]> = LazyLock::new(|| {
    [
        CatalogTableAction::Drop,
        CatalogTableAction::WriteData,
        CatalogTableAction::ReadData,
        CatalogTableAction::GetMetadata,
        CatalogTableAction::Commit {
            updated_properties: Arc::new(BTreeMap::new()),
            removed_properties: Arc::new(Vec::new()),
        },
        CatalogTableAction::Rename,
        CatalogTableAction::IncludeInList,
        CatalogTableAction::Undrop,
        CatalogTableAction::GetTasks,
        CatalogTableAction::ControlTasks,
        CatalogTableAction::SetProtection,
    ]
});
impl CatalogTableAction {
    #[must_use]
    pub fn variants() -> &'static [CatalogTableAction; 11] {
        &TABLE_ACTION_VARIANTS
    }
}
impl CatalogAction for CatalogTableAction {}

#[derive(Debug, Hash, Clone, Eq, PartialEq, Serialize, Deserialize, strum_macros::EnumCount)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=LakekeeperViewAction))]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum CatalogViewAction {
    Drop,
    GetMetadata,
    Commit {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(deserialize_with = "deserialize_string_map")]
        updated_properties: Arc<BTreeMap<String, String>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        removed_properties: Arc<Vec<String>>,
    },
    IncludeInList,
    Rename,
    Undrop,
    GetTasks,
    ControlTasks,
    SetProtection,
}
static VIEW_ACTION_VARIANTS: LazyLock<[CatalogViewAction; 9]> = LazyLock::new(|| {
    [
        CatalogViewAction::Drop,
        CatalogViewAction::GetMetadata,
        CatalogViewAction::Commit {
            updated_properties: Arc::new(BTreeMap::new()),
            removed_properties: Arc::new(Vec::new()),
        },
        CatalogViewAction::IncludeInList,
        CatalogViewAction::Rename,
        CatalogViewAction::Undrop,
        CatalogViewAction::GetTasks,
        CatalogViewAction::ControlTasks,
        CatalogViewAction::SetProtection,
    ]
});
impl CatalogViewAction {
    #[must_use]
    pub fn variants() -> &'static [CatalogViewAction; 9] {
        &VIEW_ACTION_VARIANTS
    }
}
impl CatalogAction for CatalogViewAction {}

pub trait AsTableId {
    fn as_table_id(&self) -> TableId;
}

impl AsTableId for TableId {
    fn as_table_id(&self) -> TableId {
        *self
    }
}

impl AsTableId for TableInfo {
    fn as_table_id(&self) -> TableId {
        self.tabular_id
    }
}

#[derive(Debug, Clone)]
pub enum NamespaceParent {
    Warehouse(WarehouseId),
    Namespace(NamespaceId),
}

#[must_use]
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct MustUse<T>(T);

impl<T> From<T> for MustUse<T> {
    fn from(v: T) -> Self {
        Self(v)
    }
}

impl<T> MustUse<T> {
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}
#[async_trait::async_trait]
/// Interface to provide Authorization functions to the catalog.
/// For metadata passed into all methods except `check_actor`, the `actor()` in `RequestMetadata`
/// has been validate with `check_actor` beforehand during the auth middleware step.
///
/// If the `for_user` argument to `is_allowed_x_action` methods is `Some`, then the request user
/// (from `RequestMetadata`) is requesting to know whether the `for_user` is allowed to perform the action.
/// Authorizers must return the error `CannotInspectPermissions` if the request user is not authorized to know about the permissions
/// of `for_user`.
///
/// # Single vs batch checks
///
/// Methods `is_allowed_x_action` check a single tuple. When checking many tuples, sending a
/// separate request for each check is inefficient. Use `are_allowed_x_actions` in these cases
/// for checking tuples in batches, which sends fewer requests.
///
/// Note that doing checks in batches is up to the implementers this trait. The default
/// implementations of `are_allowed_x_actions` just call `is_allowed_x_action` in parallel for
/// every item. These default implementations are provided for backwards compatibility.
pub trait Authorizer
where
    Self: Send + Sync + 'static + HealthExt + Clone + std::fmt::Debug,
{
    type ServerAction: ServerAction;
    type ProjectAction: ProjectAction;
    type WarehouseAction: WarehouseAction;
    type NamespaceAction: NamespaceAction;
    type TableAction: TableAction;
    type ViewAction: ViewAction;
    type UserAction: UserAction;
    type RoleAction: RoleAction;

    fn implementation_name() -> &'static str;

    /// The server ID that was passed to the authorizer during initialization.
    /// Must remain stable for the lifetime of the running process (typically generated at startup).
    fn server_id(&self) -> ServerId;

    /// API Doc
    #[cfg(feature = "open-api")]
    fn api_doc() -> utoipa::openapi::OpenApi;

    /// Router for the API
    fn new_router<C: CatalogStore, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>>;

    /// Check if the requested actor combination is allowed - especially if the user
    /// is allowed to assume the specified role.
    async fn check_assume_role_impl(
        &self,
        principal: &UserId,
        assumed_role: RoleId,
        request_metadata: &RequestMetadata,
    ) -> Result<bool, AuthorizationBackendUnavailable>;

    /// Check if this server can be bootstrapped by the provided user.
    async fn can_bootstrap(&self, metadata: &RequestMetadata) -> Result<()>;

    /// Perform bootstrapping, including granting the provided user the highest level of access.
    async fn bootstrap(&self, metadata: &RequestMetadata, is_operator: bool) -> Result<()>;

    /// Return Err only for internal errors.
    /// If unsupported is returned, Lakekeeper will run checks for every project individually using
    /// `are_allowed_project_actions`.
    async fn list_projects_impl(
        &self,
        _metadata: &RequestMetadata,
    ) -> Result<ListProjectsResponse, AuthorizationBackendUnavailable> {
        Ok(ListProjectsResponse::Unsupported)
    }

    /// Search users
    async fn can_search_users_impl(
        &self,
        metadata: &RequestMetadata,
    ) -> Result<bool, AuthorizationBackendUnavailable>;

    async fn are_allowed_user_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        users_with_actions: &[(&UserId, Self::UserAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    async fn are_allowed_role_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        roles_with_actions: &[(&Role, Self::RoleAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    async fn are_allowed_server_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[Self::ServerAction],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    async fn are_allowed_project_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        projects_with_actions: &[(&ProjectId, Self::ProjectAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    async fn are_allowed_warehouse_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, Self::WarehouseAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    async fn are_allowed_namespace_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, Self::NamespaceAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    /// Checks if actions are allowed on tables. If supported by the concrete implementation, these
    /// checks may happen in batches to avoid sending a separate request for each tuple.
    ///
    /// Returns `Vec<bool>` indicating for each tuple whether the action is allowed. Returns
    /// `Err` for internal errors.
    ///
    /// The default implementation is provided for backwards compatibility and does not support
    /// batch requests.
    async fn are_allowed_table_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            &impl AuthZTableInfo,
            Self::TableAction,
        )],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    /// Checks if actions are allowed on views. If supported by the concrete implementation, these
    /// checks may happen in batches to avoid sending a separate request for each tuple.
    ///
    /// Returns `Vec<bool>` indicating for each tuple whether the action is allowed. Returns
    /// `Err` for internal errors.
    ///
    /// The default implementation is provided for backwards compatibility and does not support
    /// batch requests.
    async fn are_allowed_view_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        views_with_actions: &[(&NamespaceWithParent, &impl AuthZViewInfo, Self::ViewAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError>;

    /// Hook that is called when a user is deleted.
    async fn delete_user(&self, metadata: &RequestMetadata, user_id: UserId) -> Result<()>;

    /// Hook that is called when a new project is created.
    /// This is used to set up the initial permissions for the project.
    async fn create_role(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        parent_project_id: ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a role is deleted.
    /// This is used to clean up permissions for the role.
    async fn delete_role(&self, metadata: &RequestMetadata, role_id: RoleId) -> Result<()>;

    /// Hook that is called when a new project is created.
    /// This is used to set up the initial permissions for the project.
    async fn create_project(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a project is deleted.
    /// This is used to clean up permissions for the project.
    async fn delete_project(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a new warehouse is created.
    /// This is used to set up the initial permissions for the warehouse.
    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        parent_project_id: &ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a warehouse is deleted.
    /// This is used to clean up permissions for the warehouse.
    async fn delete_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
    ) -> Result<()>;

    /// Hook that is called when a new namespace is created.
    /// This is used to set up the initial permissions for the namespace.
    async fn create_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceId,
        parent: NamespaceParent,
    ) -> Result<()>;

    /// Hook that is called when a namespace is deleted.
    /// This is used to clean up permissions for the namespace.
    async fn delete_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceId,
    ) -> Result<()>;

    /// Hook that is called when a new table is created.
    /// This is used to set up the initial permissions for the table.
    async fn create_table(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        table_id: TableId,
        parent: NamespaceId,
    ) -> Result<()>;

    /// Hook that is called when a table is deleted.
    /// This is used to clean up permissions for the table.
    async fn delete_table(&self, warehouse_id: WarehouseId, table_id: TableId) -> Result<()>;

    /// Hook that is called when a new view is created.
    /// This is used to set up the initial permissions for the view.
    async fn create_view(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        view_id: ViewId,
        parent: NamespaceId,
    ) -> Result<()>;

    /// Hook that is called when a view is deleted.
    /// This is used to clean up permissions for the view.
    async fn delete_view(&self, warehouse_id: WarehouseId, view_id: ViewId) -> Result<()>;
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        collections::HashSet,
        sync::{Arc, RwLock},
    };

    use iceberg::NamespaceIdent;
    use pastey::paste;
    use strum::EnumCount;
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::management::v1::role::Role,
        service::{Namespace, NamespaceHierarchy, health::Health},
    };

    #[test]
    fn test_warehouse_action_variant_completeness() {
        let variants = CatalogWarehouseAction::variants();
        assert_eq!(variants.len(), CatalogWarehouseAction::COUNT);
    }

    #[test]
    fn test_table_action_variant_completeness() {
        let variants = CatalogTableAction::variants();
        assert_eq!(variants.len(), CatalogTableAction::COUNT);
    }

    #[test]
    fn test_namespace_action_variant_completeness() {
        let variants = CatalogNamespaceAction::variants();
        assert_eq!(variants.len(), CatalogNamespaceAction::COUNT);
    }

    #[test]
    fn test_view_action_variant_completeness() {
        let variants = CatalogViewAction::variants();
        assert_eq!(variants.len(), CatalogViewAction::COUNT);
    }

    #[test]
    fn test_catalog_namespace_action_serde_no_properties() {
        for (action, expected) in [
            (
                CatalogNamespaceAction::GetMetadata,
                serde_json::json!({"action": "get_metadata"}),
            ),
            (
                CatalogNamespaceAction::ListTables,
                serde_json::json!({"action": "list_tables"}),
            ),
            (
                CatalogNamespaceAction::ListViews,
                serde_json::json!({"action": "list_views"}),
            ),
            (
                CatalogNamespaceAction::ListNamespaces,
                serde_json::json!({"action": "list_namespaces"}),
            ),
            (
                CatalogNamespaceAction::ListEverything,
                serde_json::json!({"action": "list_everything"}),
            ),
            (
                CatalogNamespaceAction::Delete,
                serde_json::json!({"action": "delete"}),
            ),
            (
                CatalogNamespaceAction::SetProtection,
                serde_json::json!({"action": "set_protection"}),
            ),
            (
                CatalogNamespaceAction::IncludeInList,
                serde_json::json!({"action": "include_in_list"}),
            ),
            (
                CatalogNamespaceAction::CreateTable {
                    properties: Arc::new(BTreeMap::new()),
                },
                serde_json::json!({"action": "create_table"}),
            ),
            (
                CatalogNamespaceAction::CreateView {
                    properties: Arc::new(BTreeMap::new()),
                },
                serde_json::json!({"action": "create_view"}),
            ),
            (
                CatalogNamespaceAction::CreateNamespace {
                    properties: Arc::new(BTreeMap::new()),
                },
                serde_json::json!({"action": "create_namespace"}),
            ),
            (
                CatalogNamespaceAction::UpdateProperties {
                    removed_properties: Arc::new(Vec::new()),
                    updated_properties: Arc::new(BTreeMap::new()),
                },
                serde_json::json!({"action": "update_properties"}),
            ),
        ] {
            let serialized = serde_json::to_value(&action).expect("Failed to serialize");
            let expected_serialized =
                serde_json::to_value(expected).expect("Failed to serialize expected");
            assert_eq!(serialized, expected_serialized);

            let deserialized: CatalogNamespaceAction =
                serde_json::from_value(serialized).expect("Failed to deserialize");
            assert_eq!(deserialized, action);
        }
    }

    #[test]
    fn test_catalog_view_action_serde_no_properties() {
        for (action, expected) in [
            (
                CatalogViewAction::Drop,
                serde_json::json!({"action": "drop"}),
            ),
            (
                CatalogViewAction::GetMetadata,
                serde_json::json!({"action": "get_metadata"}),
            ),
            (
                CatalogViewAction::IncludeInList,
                serde_json::json!({"action": "include_in_list"}),
            ),
            (
                CatalogViewAction::Rename,
                serde_json::json!({"action": "rename"}),
            ),
            (
                CatalogViewAction::Undrop,
                serde_json::json!({"action": "undrop"}),
            ),
            (
                CatalogViewAction::GetTasks,
                serde_json::json!({"action": "get_tasks"}),
            ),
            (
                CatalogViewAction::ControlTasks,
                serde_json::json!({"action": "control_tasks"}),
            ),
            (
                CatalogViewAction::SetProtection,
                serde_json::json!({"action": "set_protection"}),
            ),
            (
                CatalogViewAction::Commit {
                    updated_properties: Arc::new(BTreeMap::new()),
                    removed_properties: Arc::new(Vec::new()),
                },
                serde_json::json!({"action": "commit"}),
            ),
        ] {
            let serialized = serde_json::to_value(&action).expect("Failed to serialize");
            let expected_serialized =
                serde_json::to_value(expected).expect("Failed to serialize expected");
            assert_eq!(serialized, expected_serialized);

            let deserialized: CatalogViewAction =
                serde_json::from_value(serialized).expect("Failed to deserialize");
            assert_eq!(deserialized, action);
        }
    }

    #[test]
    fn test_catalog_table_action_serde_no_properties() {
        for (action, expected) in [
            (
                CatalogTableAction::Drop,
                serde_json::json!({"action": "drop"}),
            ),
            (
                CatalogTableAction::WriteData,
                serde_json::json!({"action": "write_data"}),
            ),
            (
                CatalogTableAction::ReadData,
                serde_json::json!({"action": "read_data"}),
            ),
            (
                CatalogTableAction::GetMetadata,
                serde_json::json!({"action": "get_metadata"}),
            ),
            (
                CatalogTableAction::Rename,
                serde_json::json!({"action": "rename"}),
            ),
            (
                CatalogTableAction::IncludeInList,
                serde_json::json!({"action": "include_in_list"}),
            ),
            (
                CatalogTableAction::Undrop,
                serde_json::json!({"action": "undrop"}),
            ),
            (
                CatalogTableAction::GetTasks,
                serde_json::json!({"action": "get_tasks"}),
            ),
            (
                CatalogTableAction::ControlTasks,
                serde_json::json!({"action": "control_tasks"}),
            ),
            (
                CatalogTableAction::SetProtection,
                serde_json::json!({"action": "set_protection"}),
            ),
            (
                CatalogTableAction::Commit {
                    updated_properties: Arc::new(BTreeMap::new()),
                    removed_properties: Arc::new(Vec::new()),
                },
                serde_json::json!({"action": "commit"}),
            ),
        ] {
            let serialized = serde_json::to_value(&action).expect("Failed to serialize");
            let expected_serialized =
                serde_json::to_value(expected).expect("Failed to serialize expected");
            assert_eq!(serialized, expected_serialized);

            let deserialized: CatalogTableAction =
                serde_json::from_value(serialized).expect("Failed to deserialize");
            assert_eq!(deserialized, action);
        }
    }

    #[test]
    fn test_catalog_table_action_commit_with_properties_serde() {
        let action = CatalogTableAction::Commit {
            updated_properties: Arc::new(
                [("key1".to_string(), "value1".to_string())]
                    .into_iter()
                    .collect(),
            ),
            removed_properties: Arc::new(vec!["key2".to_string(), "key3".to_string()]),
        };
        let serialized = serde_json::to_value(&action).expect("Failed to serialize");
        let expected_serialized = serde_json::json!({
            "action": "commit",
            "updated_properties": {
                "key1": "value1"
            },
            "removed_properties": ["key2", "key3"]
        });
        assert_eq!(serialized, expected_serialized);

        let deserialized: CatalogTableAction =
            serde_json::from_value(serialized).expect("Failed to deserialize");
        assert_eq!(deserialized, action);
    }

    #[derive(Clone, Debug)]
    /// A mock of the [`Authorizer`] that allows to hide objects.
    /// This is useful to test the behavior of the authorizer when objects are hidden.
    ///
    /// Objects that have been hidden will return `allowed: false` for any check request. This
    /// means all checks for an object that was *not* hidden return `allowed: true`.
    ///
    /// Some tests require blocking certain actions without hiding the object, for instance
    /// forbid an action on a namespace without hiding the namespace. This can be achieved by
    /// blocking the action.
    ///
    /// # Note on unexpected visibility
    ///
    /// Due to `can_list_everything`, permissions on hidden objects may behave unexpectedly.
    /// Consider calling [`Self::block_can_list_everything`] in such cases.
    pub(crate) struct HidingAuthorizer {
        /// Strings encode `object_type:object_id` e.g. `namespace:id_of_namespace_to_hide`.
        pub(crate) hidden: Arc<RwLock<HashSet<String>>>,
        /// Strings encode `object_type:action` e.g. `namespace:can_create_table`.
        blocked_actions: Arc<RwLock<HashSet<String>>>,
        server_id: ServerId,
    }

    impl HidingAuthorizer {
        pub(crate) fn new() -> Self {
            Self {
                hidden: Arc::new(RwLock::new(HashSet::new())),
                blocked_actions: Arc::new(RwLock::new(HashSet::new())),
                server_id: ServerId::new_random(),
            }
        }

        fn check_available(&self, object: &str) -> bool {
            !self.hidden.read().unwrap().contains(object)
        }

        pub(crate) fn hide(&self, object: &str) {
            self.hidden.write().unwrap().insert(object.to_string());
        }

        fn action_is_blocked(&self, action: &str) -> bool {
            self.blocked_actions.read().unwrap().contains(action)
        }

        pub(crate) fn block_action(&self, object: &str) {
            self.blocked_actions
                .write()
                .unwrap()
                .insert(object.to_string());
        }

        /// Blocks `can_list_everything` action on every object it is defined for.
        ///
        /// This is helpful for tests that hide a subset of objects, e.g. *some* but not all
        /// tables. `can_list_everything` may work against that when it triggers short check paths
        /// that skip checking individual permissions.
        pub(crate) fn block_can_list_everything(&self) {
            self.block_action(
                format!("namespace:{:?}", CatalogNamespaceAction::ListEverything).as_str(),
            );
            self.block_action(
                format!("warehouse:{:?}", CatalogWarehouseAction::ListEverything).as_str(),
            );
        }
    }

    #[async_trait::async_trait]
    impl HealthExt for HidingAuthorizer {
        async fn health(&self) -> Vec<Health> {
            vec![]
        }
        async fn update_health(&self) {
            // Do nothing
        }
    }

    #[async_trait::async_trait]
    impl Authorizer for HidingAuthorizer {
        type ServerAction = CatalogServerAction;
        type ProjectAction = CatalogProjectAction;
        type WarehouseAction = CatalogWarehouseAction;
        type NamespaceAction = CatalogNamespaceAction;
        type TableAction = CatalogTableAction;
        type ViewAction = CatalogViewAction;
        type UserAction = CatalogUserAction;
        type RoleAction = CatalogRoleAction;

        fn implementation_name() -> &'static str {
            "test-hiding-authorizer"
        }

        fn server_id(&self) -> ServerId {
            self.server_id
        }

        #[cfg(feature = "open-api")]
        fn api_doc() -> utoipa::openapi::OpenApi {
            AllowAllAuthorizer::api_doc()
        }

        fn new_router<C: CatalogStore, S: SecretStore>(
            &self,
        ) -> Router<ApiContext<State<Self, C, S>>> {
            Router::new()
        }

        async fn check_assume_role_impl(
            &self,
            _principal: &UserId,
            _assumed_role: RoleId,
            _request_metadata: &RequestMetadata,
        ) -> Result<bool, AuthorizationBackendUnavailable> {
            Ok(true)
        }

        async fn can_bootstrap(&self, _metadata: &RequestMetadata) -> Result<()> {
            Ok(())
        }

        async fn bootstrap(&self, _metadata: &RequestMetadata, _is_operator: bool) -> Result<()> {
            Ok(())
        }

        async fn list_projects_impl(
            &self,
            _metadata: &RequestMetadata,
        ) -> Result<ListProjectsResponse, AuthorizationBackendUnavailable> {
            Ok(ListProjectsResponse::All)
        }

        async fn can_search_users_impl(
            &self,
            _metadata: &RequestMetadata,
        ) -> Result<bool, AuthorizationBackendUnavailable> {
            Ok(true)
        }

        async fn are_allowed_user_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            users_with_actions: &[(&UserId, Self::UserAction)],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            Ok(vec![true; users_with_actions.len()])
        }

        async fn are_allowed_role_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            roles_with_actions: &[(&Role, Self::RoleAction)],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            let results: Vec<bool> = roles_with_actions
                .iter()
                .map(|(role, action)| {
                    if self.action_is_blocked(format!("role:{action:?}").as_str()) {
                        return false;
                    }
                    self.check_available(format!("role:{}", role.id).as_str())
                })
                .collect();
            Ok(results)
        }

        async fn are_allowed_server_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            actions: &[Self::ServerAction],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            Ok(vec![true; actions.len()])
        }

        async fn are_allowed_project_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            projects_with_actions: &[(&ProjectId, Self::ProjectAction)],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            let results: Vec<bool> = projects_with_actions
                .iter()
                .map(|(project_id, action)| {
                    if self.action_is_blocked(format!("project:{action:?}").as_str()) {
                        return false;
                    }
                    self.check_available(format!("project:{project_id}").as_str())
                })
                .collect();
            Ok(results)
        }

        async fn are_allowed_warehouse_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            warehouses_with_actions: &[(&ResolvedWarehouse, Self::WarehouseAction)],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            let results: Vec<bool> = warehouses_with_actions
                .iter()
                .map(|(warehouse, action)| {
                    if self.action_is_blocked(format!("warehouse:{action:?}").as_str()) {
                        return false;
                    }
                    let warehouse_id = warehouse.warehouse_id;
                    self.check_available(format!("warehouse:{warehouse_id}").as_str())
                })
                .collect();
            Ok(results)
        }

        async fn are_allowed_namespace_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            _warehouse: &ResolvedWarehouse,
            _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
            actions: &[(&impl AuthZNamespaceInfo, Self::NamespaceAction)],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            let results: Vec<bool> = actions
                .iter()
                .map(|(namespace, action)| {
                    if self.action_is_blocked(format!("namespace:{action:?}").as_str()) {
                        return false;
                    }
                    let namespace_id = namespace.namespace().namespace_id;
                    self.check_available(format!("namespace:{namespace_id}").as_str())
                })
                .collect();
            Ok(results)
        }

        async fn are_allowed_table_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            _warehouse: &ResolvedWarehouse,
            _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
            actions: &[(
                &NamespaceWithParent,
                &impl AuthZTableInfo,
                Self::TableAction,
            )],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            let results: Vec<bool> = actions
                .iter()
                .map(|(_parent_namespace, table, action)| {
                    if self.action_is_blocked(format!("table:{action:?}").as_str()) {
                        return false;
                    }
                    let table_id = table.table_id();
                    let warehouse_id = table.warehouse_id();
                    self.check_available(format!("table:{warehouse_id}/{table_id}").as_str())
                })
                .collect();
            Ok(results)
        }

        async fn are_allowed_view_actions_impl(
            &self,
            _metadata: &RequestMetadata,
            _for_user: Option<&UserOrRole>,
            _warehouse: &ResolvedWarehouse,
            _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
            views_with_actions: &[(&NamespaceWithParent, &impl AuthZViewInfo, Self::ViewAction)],
        ) -> Result<Vec<bool>, IsAllowedActionError> {
            let results: Vec<bool> = views_with_actions
                .iter()
                .map(|(_parent_namespace, view, action)| {
                    if self.action_is_blocked(format!("view:{action:?}").as_str()) {
                        return false;
                    }
                    let view_id = view.view_id();
                    let warehouse_id = view.warehouse_id();
                    self.check_available(format!("view:{warehouse_id}/{view_id}").as_str())
                })
                .collect();
            Ok(results)
        }

        async fn delete_user(&self, _metadata: &RequestMetadata, _user_id: UserId) -> Result<()> {
            Ok(())
        }

        async fn create_role(
            &self,
            _metadata: &RequestMetadata,
            _role_id: RoleId,
            _parent_project_id: ProjectId,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_role(&self, _metadata: &RequestMetadata, _role_id: RoleId) -> Result<()> {
            Ok(())
        }

        async fn create_project(
            &self,
            _metadata: &RequestMetadata,
            _project_id: &ProjectId,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_project(
            &self,
            _metadata: &RequestMetadata,
            _project_id: &ProjectId,
        ) -> Result<()> {
            Ok(())
        }

        async fn create_warehouse(
            &self,
            _metadata: &RequestMetadata,
            _warehouse_id: WarehouseId,
            _parent_project_id: &ProjectId,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_warehouse(
            &self,
            _metadata: &RequestMetadata,
            _warehouse_id: WarehouseId,
        ) -> Result<()> {
            Ok(())
        }

        async fn create_namespace(
            &self,
            _metadata: &RequestMetadata,
            _namespace_id: NamespaceId,
            _parent: NamespaceParent,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_namespace(
            &self,
            _metadata: &RequestMetadata,
            _namespace_id: NamespaceId,
        ) -> Result<()> {
            Ok(())
        }

        async fn create_table(
            &self,
            _metadata: &RequestMetadata,
            _warehouse_id: WarehouseId,
            _table_id: TableId,
            _parent: NamespaceId,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_table(&self, _warehouse_id: WarehouseId, _table_id: TableId) -> Result<()> {
            Ok(())
        }

        async fn create_view(
            &self,
            _metadata: &RequestMetadata,
            _warehouse_id: WarehouseId,
            _view_id: ViewId,
            _parent: NamespaceId,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_view(&self, _warehouse_id: WarehouseId, _view_id: ViewId) -> Result<()> {
            Ok(())
        }
    }

    macro_rules! test_block_action {
        ($entity:ident, $action:expr, $($check_arguments:expr),+) => {
            paste! {
                #[tokio::test]
                async fn [<test_block_ $entity _action>]() {
                    let authz = HidingAuthorizer::new();

                    // Nothing is hidden, so the action is allowed.
                    assert!(authz
                        .[<is_allowed_ $entity _action>](
                            &RequestMetadata::new_unauthenticated(),
                            None,
                            $($check_arguments),+,
                            $action
                        )
                        .await
                        .unwrap()
                        .into_inner());

                    // Generates "namespace:can_list_everything" for macro invoked with
                    // (namespace, CatalogNamespaceAction::CanListEverything)
                    authz.block_action(format!("{}:{:?}", stringify!($entity), $action).as_str());

                    // After blocking the action it must not be allowed anymore.
                    assert!(!authz
                        .[<is_allowed_ $entity _action>](
                            &RequestMetadata::new_unauthenticated(),
                            None,
                            $($check_arguments),+,
                            $action
                        )
                        .await
                        .unwrap()
                        .into_inner());
                }
            }
        };
    }
    test_block_action!(role, CatalogRoleAction::Delete, &Role::new_random());
    test_block_action!(
        project,
        CatalogProjectAction::Rename,
        &ProjectId::new_random()
    );
    test_block_action!(
        warehouse,
        CatalogWarehouseAction::CreateNamespace {
            properties: Arc::new(BTreeMap::new())
        },
        &ResolvedWarehouse::new_random()
    );
    test_block_action!(
        namespace,
        CatalogNamespaceAction::ListViews,
        &ResolvedWarehouse::new_with_id(Uuid::nil().into()),
        &[],
        &NamespaceWithParent {
            namespace: Arc::new(Namespace {
                namespace_ident: NamespaceIdent::new("test".to_string()),
                namespace_id: NamespaceId::new_random(),
                warehouse_id: Uuid::nil().into(),
                protected: false,
                properties: None,
                created_at: chrono::Utc::now(),
                updated_at: Some(chrono::Utc::now()),
                version: 0.into(),
            }),
            parent: None,
        }
    );
    test_block_action!(
        table,
        CatalogTableAction::Drop,
        &ResolvedWarehouse::new_with_id(Uuid::nil().into()),
        &NamespaceHierarchy {
            namespace: NamespaceWithParent {
                namespace: Arc::new(Namespace {
                    namespace_ident: NamespaceIdent::new("test".to_string()),
                    namespace_id: NamespaceId::new_random(),
                    warehouse_id: Uuid::nil().into(),
                    protected: false,
                    properties: None,
                    created_at: chrono::Utc::now(),
                    updated_at: Some(chrono::Utc::now()),
                    version: 0.into(),
                }),
                parent: None,
            },
            parents: vec![]
        },
        &crate::service::TableInfo::new_random(Uuid::nil().into())
    );
    test_block_action!(
        view,
        CatalogViewAction::Drop,
        &ResolvedWarehouse::new_with_id(Uuid::nil().into()),
        &NamespaceHierarchy {
            namespace: NamespaceWithParent {
                namespace: Arc::new(Namespace {
                    namespace_ident: NamespaceIdent::new("test".to_string()),
                    namespace_id: NamespaceId::new_random(),
                    warehouse_id: Uuid::nil().into(),
                    protected: false,
                    properties: None,
                    created_at: chrono::Utc::now(),
                    updated_at: Some(chrono::Utc::now()),
                    version: 0.into(),
                }),
                parent: None,
            },
            parents: vec![]
        },
        &crate::service::ViewInfo::new_random(Uuid::nil().into())
    );
}
