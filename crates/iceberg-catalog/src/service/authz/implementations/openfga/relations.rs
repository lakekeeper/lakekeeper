use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use utoipa::ToSchema;

use super::{
    entities::{OpenFgaEntity, ParseOpenFgaEntity},
    OpenFGAError, OpenFGAResult, RoleAssignee,
};
use crate::service::{
    authn::UserId,
    authz::{
        implementations::FgaType, CatalogNamespaceAction, CatalogProjectAction, CatalogRoleAction,
        CatalogServerAction, CatalogTableAction, CatalogViewAction, CatalogWarehouseAction,
    },
    Actor, RoleId,
};

pub(super) trait Assignment: Sized {
    type Relation: ReducedRelation + GrantableRelation;
    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self>;

    fn openfga_user(&self) -> String;

    fn relation(&self) -> Self::Relation;
}

pub(super) trait OpenFgaRelation:
    std::fmt::Display + Eq + PartialEq + Clone + Sized + Copy + std::hash::Hash
{
}

/// Trait for a subset of relations (i.e. actions)
/// that can be converted to the corresponding full type
pub(super) trait ReducedRelation:
    Clone + Sized + Copy + IntoEnumIterator + Eq + PartialEq
{
    type OpenFgaRelation: OpenFgaRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation;
}

pub(super) trait GrantableRelation: ReducedRelation {
    fn grant_relation(&self) -> Self::OpenFgaRelation;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "kebab-case")]
/// Identifies a user or a role
pub(super) enum UserOrRole {
    #[schema(value_type = uuid::Uuid)]
    #[schema(title = "UserOrRoleUser")]
    /// Id of the user
    User(UserId),
    #[schema(value_type = uuid::Uuid)]
    #[schema(title = "UserOrRoleRole")]
    /// Id of the role
    Role(RoleAssignee),
}

impl Actor {
    #[must_use]
    pub(super) fn to_user_or_role(&self) -> Option<UserOrRole> {
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

impl From<UserId> for UserOrRole {
    fn from(user: UserId) -> Self {
        UserOrRole::User(user)
    }
}

impl From<RoleId> for UserOrRole {
    fn from(role: RoleId) -> Self {
        UserOrRole::Role(RoleAssignee::from_role(role))
    }
}

impl ParseOpenFgaEntity for UserOrRole {
    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self> {
        match r#type {
            FgaType::User => Ok(UserOrRole::User(UserId::try_from_openfga_id(r#type, id)?)),
            FgaType::Role => Ok(UserOrRole::Role(RoleAssignee::try_from_openfga_id(
                r#type, id,
            )?)),
            _ => Err(OpenFGAError::UnexpectedEntity {
                r#type: vec![FgaType::User],
                value: id.to_string(),
            }),
        }
    }
}

impl OpenFgaEntity for UserOrRole {
    fn to_openfga(&self) -> String {
        match self {
            UserOrRole::User(user) => user.to_openfga(),
            UserOrRole::Role(role) => role.to_openfga(),
        }
    }

    fn openfga_type(&self) -> FgaType {
        match self {
            UserOrRole::User(_) => FgaType::User,
            UserOrRole::Role(_) => FgaType::Role,
        }
    }
}

/// Role Relations in the `OpenFGA` schema
#[derive(Debug, Copy, Clone, strum_macros::Display, Hash, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum RoleRelation {
    // -- Hierarchical relations --
    Project,
    // -- Direct relations --
    Assignee,
    Ownership,
    // -- Actions --
    CanAssume,
    CanGrantAssignee,
    CanChangeOwnership,
    CanDelete,
    CanUpdate,
    CanRead,
    CanReadAssignments,
}

impl OpenFgaRelation for RoleRelation {}

#[derive(Debug, Clone, Deserialize, Copy, Eq, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=RoleRelation)]
pub(super) enum APIRoleRelation {
    Assignee,
    Ownership,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum RoleAssignment {
    #[schema(title = "RoleAssignmentAssignee")]
    Assignee(UserOrRole),
    #[schema(title = "RoleAssignmentOwnership")]
    Ownership(UserOrRole),
}

impl GrantableRelation for APIRoleRelation {
    fn grant_relation(&self) -> Self::OpenFgaRelation {
        match self {
            APIRoleRelation::Assignee => RoleRelation::CanGrantAssignee,
            APIRoleRelation::Ownership => RoleRelation::CanChangeOwnership,
        }
    }
}

impl Assignment for RoleAssignment {
    type Relation = APIRoleRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIRoleRelation::Assignee => {
                UserOrRole::parse_from_openfga(user).map(RoleAssignment::Assignee)
            }
            APIRoleRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(RoleAssignment::Ownership)
            }
        }
    }

    fn openfga_user(&self) -> String {
        match self {
            RoleAssignment::Ownership(user) | RoleAssignment::Assignee(user) => user.to_openfga(),
        }
    }

    fn relation(&self) -> Self::Relation {
        match self {
            RoleAssignment::Ownership(_) => APIRoleRelation::Ownership,
            RoleAssignment::Assignee(_) => APIRoleRelation::Assignee,
        }
    }
}

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[schema(as=RoleAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APIRoleAction {
    Assume,
    CanGrantAssignee,
    CanChangeOwnership,
    Delete,
    Update,
    Read,
    ReadAssignments,
}

impl ReducedRelation for APIRoleRelation {
    type OpenFgaRelation = RoleRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIRoleRelation::Assignee => RoleRelation::Assignee,
            APIRoleRelation::Ownership => RoleRelation::Ownership,
        }
    }
}

impl ReducedRelation for APIRoleAction {
    type OpenFgaRelation = RoleRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIRoleAction::Assume => RoleRelation::CanAssume,
            APIRoleAction::CanGrantAssignee => RoleRelation::CanGrantAssignee,
            APIRoleAction::CanChangeOwnership => RoleRelation::CanChangeOwnership,
            APIRoleAction::Delete => RoleRelation::CanDelete,
            APIRoleAction::Update => RoleRelation::CanUpdate,
            APIRoleAction::Read => RoleRelation::CanRead,
            APIRoleAction::ReadAssignments => RoleRelation::CanReadAssignments,
        }
    }
}

impl ReducedRelation for CatalogRoleAction {
    type OpenFgaRelation = RoleRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogRoleAction::CanDelete => RoleRelation::CanDelete,
            CatalogRoleAction::CanUpdate => RoleRelation::CanUpdate,
            CatalogRoleAction::CanRead => RoleRelation::CanRead,
        }
    }
}

/// Server Relations in the `OpenFGA` schema
#[derive(Copy, Debug, Clone, strum_macros::Display, Hash, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum ServerRelation {
    // -- Hierarchical relations --
    Project,
    // -- Direct relations --
    Admin,
    Operator,
    // -- Actions --
    CanCreateProject,
    CanListAllProjects,
    CanListUsers,
    CanProvisionUsers,
    CanUpdateUsers,
    CanDeleteUsers,
    CanReadAssignments,
    CanGrantAdmin,
    CanGrantOperator,
}

impl OpenFgaRelation for ServerRelation {}

#[derive(Debug, Clone, Deserialize, Copy, Hash, Eq, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ServerRelation)]
pub(super) enum APIServerRelation {
    Admin,
    Operator,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ServerAssignment {
    #[schema(title = "ServerAssignmentAdmin")]
    Admin(UserOrRole),
    #[schema(title = "ServerAssignmentOperator")]
    Operator(UserOrRole),
}

impl GrantableRelation for APIServerRelation {
    fn grant_relation(&self) -> ServerRelation {
        match self {
            APIServerRelation::Admin => ServerRelation::CanGrantAdmin,
            APIServerRelation::Operator => ServerRelation::CanGrantOperator,
        }
    }
}

impl Assignment for ServerAssignment {
    type Relation = APIServerRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIServerRelation::Admin => {
                UserOrRole::parse_from_openfga(user).map(ServerAssignment::Admin)
            }
            APIServerRelation::Operator => {
                UserOrRole::parse_from_openfga(user).map(ServerAssignment::Operator)
            }
        }
    }

    fn openfga_user(&self) -> String {
        match self {
            ServerAssignment::Admin(user) | ServerAssignment::Operator(user) => user.to_openfga(),
        }
    }

    fn relation(&self) -> Self::Relation {
        match self {
            ServerAssignment::Admin(_) => APIServerRelation::Admin,
            ServerAssignment::Operator(_) => APIServerRelation::Operator,
        }
    }
}

#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[schema(as=ServerAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APIServerAction {
    /// Can create items inside the server (can create Warehouses).
    CreateProject,
    /// Can update all users on this server.
    UpdateUsers,
    /// Can delete users on this server apart from myself.
    DeleteUsers,
    /// Can List all users on this server.
    ListUsers,
    /// Can grant global Admin
    GrantAdmin,
    /// Can provision user
    ProvisionUsers,
    /// Can read assignments
    ReadAssignments,
}

impl ReducedRelation for APIServerRelation {
    type OpenFgaRelation = ServerRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIServerRelation::Admin => ServerRelation::Admin,
            APIServerRelation::Operator => ServerRelation::Operator,
        }
    }
}

impl ReducedRelation for CatalogServerAction {
    type OpenFgaRelation = ServerRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogServerAction::CanCreateProject => ServerRelation::CanCreateProject,
            CatalogServerAction::CanUpdateUsers => ServerRelation::CanUpdateUsers,
            CatalogServerAction::CanDeleteUsers => ServerRelation::CanDeleteUsers,
            CatalogServerAction::CanListUsers => ServerRelation::CanListAllProjects,
            CatalogServerAction::CanProvisionUsers => ServerRelation::CanProvisionUsers,
        }
    }
}

impl ReducedRelation for APIServerAction {
    type OpenFgaRelation = ServerRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIServerAction::CreateProject => ServerRelation::CanCreateProject,
            APIServerAction::UpdateUsers => ServerRelation::CanUpdateUsers,
            APIServerAction::DeleteUsers => ServerRelation::CanDeleteUsers,
            APIServerAction::ListUsers => ServerRelation::CanListUsers,
            APIServerAction::ProvisionUsers => ServerRelation::CanProvisionUsers,
            APIServerAction::ReadAssignments => ServerRelation::CanReadAssignments,
            APIServerAction::GrantAdmin => ServerRelation::CanGrantAdmin,
        }
    }
}

#[derive(Copy, Debug, Clone, strum_macros::Display, Hash, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum ProjectRelation {
    // -- Hierarchical relations --
    Warehouse,
    Server,
    // -- Direct relations --
    ProjectAdmin,
    SecurityAdmin,
    DataAdmin,
    RoleCreator,
    Describe,
    Select,
    Create,
    Modify,
    // -- Actions --
    CanCreateWarehouse,
    CanDelete,
    CanRename,
    CanGetMetadata,
    CanListWarehouses,
    CanIncludeInList,
    CanCreateRole,
    CanListRoles,
    CanSearchRoles,
    CanReadAssignments,
    CanGrantRoleCreator,
    CanGrantCreate,
    CanGrantDescribe,
    CanGrantModify,
    CanGrantSelect,
    CanGrantProjectAdmin,
    CanGrantSecurityAdmin,
    CanGrantDataAdmin,
}

impl OpenFgaRelation for ProjectRelation {}

#[derive(Debug, Clone, Deserialize, Copy, Eq, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ProjectRelation)]
pub(super) enum APIProjectRelation {
    ProjectAdmin,
    SecurityAdmin,
    DataAdmin,
    RoleCreator,
    Describe,
    Select,
    Create,
    Modify,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ProjectAssignment {
    #[schema(title = "ProjectAssignmentProjectAdmin")]
    ProjectAdmin(UserOrRole),
    #[schema(title = "ProjectAssignmentSecurityAdmin")]
    SecurityAdmin(UserOrRole),
    #[schema(title = "ProjectAssignmentDataAdmin")]
    DataAdmin(UserOrRole),
    #[schema(title = "ProjectAssignmentRoleCreator")]
    RoleCreator(UserOrRole),
    #[schema(title = "ProjectAssignmentDescribe")]
    Describe(UserOrRole),
    #[schema(title = "ProjectAssignmentSelect")]
    Select(UserOrRole),
    #[schema(title = "ProjectAssignmentCreate")]
    Create(UserOrRole),
    #[schema(title = "ProjectAssignmentModify")]
    Modify(UserOrRole),
}

impl GrantableRelation for APIProjectRelation {
    fn grant_relation(&self) -> ProjectRelation {
        match self {
            APIProjectRelation::ProjectAdmin => ProjectRelation::CanGrantProjectAdmin,
            APIProjectRelation::SecurityAdmin => ProjectRelation::CanGrantSecurityAdmin,
            APIProjectRelation::DataAdmin => ProjectRelation::CanGrantDataAdmin,
            APIProjectRelation::RoleCreator => ProjectRelation::CanGrantRoleCreator,
            APIProjectRelation::Describe => ProjectRelation::CanGrantDescribe,
            APIProjectRelation::Select => ProjectRelation::CanGrantSelect,
            APIProjectRelation::Create => ProjectRelation::CanGrantCreate,
            APIProjectRelation::Modify => ProjectRelation::CanGrantModify,
        }
    }
}

impl Assignment for ProjectAssignment {
    type Relation = APIProjectRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIProjectRelation::ProjectAdmin => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::ProjectAdmin)
            }
            APIProjectRelation::SecurityAdmin => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::SecurityAdmin)
            }
            APIProjectRelation::DataAdmin => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::DataAdmin)
            }
            APIProjectRelation::RoleCreator => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::RoleCreator)
            }
            APIProjectRelation::Describe => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::Describe)
            }
            APIProjectRelation::Select => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::Select)
            }
            APIProjectRelation::Create => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::Create)
            }
            APIProjectRelation::Modify => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::Modify)
            }
        }
    }

    fn openfga_user(&self) -> String {
        match self {
            ProjectAssignment::ProjectAdmin(user)
            | ProjectAssignment::SecurityAdmin(user)
            | ProjectAssignment::DataAdmin(user)
            | ProjectAssignment::RoleCreator(user)
            | ProjectAssignment::Describe(user)
            | ProjectAssignment::Select(user)
            | ProjectAssignment::Create(user)
            | ProjectAssignment::Modify(user) => user.to_openfga(),
        }
    }

    fn relation(&self) -> Self::Relation {
        match self {
            ProjectAssignment::ProjectAdmin(_) => APIProjectRelation::ProjectAdmin,
            ProjectAssignment::SecurityAdmin(_) => APIProjectRelation::SecurityAdmin,
            ProjectAssignment::DataAdmin(_) => APIProjectRelation::DataAdmin,
            ProjectAssignment::RoleCreator(_) => APIProjectRelation::RoleCreator,
            ProjectAssignment::Describe { .. } => APIProjectRelation::Describe,
            ProjectAssignment::Select { .. } => APIProjectRelation::Select,
            ProjectAssignment::Create { .. } => APIProjectRelation::Create,
            ProjectAssignment::Modify { .. } => APIProjectRelation::Modify,
        }
    }
}

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ProjectAction)]
pub(super) enum APIProjectAction {
    CreateWarehouse,
    Delete,
    Rename,
    ListWarehouses,
    CreateRole,
    ListRoles,
    SearchRoles,
    ReadAssignments,
    GrantRoleCreator,
    GrantCreate,
    GrantDescribe,
    GrantModify,
    GrantSelect,
    GrantProjectAdmin,
    GrantSecurityAdmin,
    GrantDataAdmin,
}

impl ReducedRelation for APIProjectRelation {
    type OpenFgaRelation = ProjectRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIProjectRelation::ProjectAdmin => ProjectRelation::ProjectAdmin,
            APIProjectRelation::SecurityAdmin => ProjectRelation::SecurityAdmin,
            APIProjectRelation::DataAdmin => ProjectRelation::DataAdmin,
            APIProjectRelation::RoleCreator => ProjectRelation::RoleCreator,
            APIProjectRelation::Describe => ProjectRelation::Describe,
            APIProjectRelation::Select => ProjectRelation::Select,
            APIProjectRelation::Create => ProjectRelation::Create,
            APIProjectRelation::Modify => ProjectRelation::Modify,
        }
    }
}

impl ReducedRelation for APIProjectAction {
    type OpenFgaRelation = ProjectRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIProjectAction::CreateWarehouse => ProjectRelation::CanCreateWarehouse,
            APIProjectAction::Delete => ProjectRelation::CanDelete,
            APIProjectAction::Rename => ProjectRelation::CanRename,
            APIProjectAction::ListWarehouses => ProjectRelation::CanListWarehouses,
            APIProjectAction::CreateRole => ProjectRelation::CanCreateRole,
            APIProjectAction::ListRoles => ProjectRelation::CanListRoles,
            APIProjectAction::SearchRoles => ProjectRelation::CanSearchRoles,
            APIProjectAction::ReadAssignments => ProjectRelation::CanReadAssignments,
            APIProjectAction::GrantRoleCreator => ProjectRelation::CanGrantRoleCreator,
            APIProjectAction::GrantCreate => ProjectRelation::CanGrantCreate,
            APIProjectAction::GrantDescribe => ProjectRelation::CanGrantDescribe,
            APIProjectAction::GrantModify => ProjectRelation::CanGrantModify,
            APIProjectAction::GrantSelect => ProjectRelation::CanGrantSelect,
            APIProjectAction::GrantProjectAdmin => ProjectRelation::CanGrantProjectAdmin,
            APIProjectAction::GrantSecurityAdmin => ProjectRelation::CanGrantSecurityAdmin,
            APIProjectAction::GrantDataAdmin => ProjectRelation::CanGrantDataAdmin,
        }
    }
}

impl ReducedRelation for CatalogProjectAction {
    type OpenFgaRelation = ProjectRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogProjectAction::CanCreateWarehouse => ProjectRelation::CanCreateWarehouse,
            CatalogProjectAction::CanDelete => ProjectRelation::CanDelete,
            CatalogProjectAction::CanRename => ProjectRelation::CanRename,
            CatalogProjectAction::CanGetMetadata => ProjectRelation::CanGetMetadata,
            CatalogProjectAction::CanListWarehouses => ProjectRelation::CanListWarehouses,
            CatalogProjectAction::CanIncludeInList => ProjectRelation::CanIncludeInList,
            CatalogProjectAction::CanCreateRole => ProjectRelation::CanCreateRole,
            CatalogProjectAction::CanListRoles => ProjectRelation::CanListRoles,
            CatalogProjectAction::CanSearchRoles => ProjectRelation::CanSearchRoles,
        }
    }
}

#[derive(Copy, Debug, Clone, strum_macros::Display, Hash, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum WarehouseRelation {
    // -- Hierarchical relations --
    Project,
    Namespace,
    // -- Managed relations --
    _ManagedAccess,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
    // -- Actions --
    CanCreateNamespace,
    CanDelete,
    CanUpdateStorage,
    CanUpdateStorageCredential,
    CanGetMetadata,
    CanGetConfig,
    CanListNamespaces,
    CanModifySoftDeletion,
    CanUse,
    CanIncludeInList,
    CanDeactivate,
    CanActivate,
    CanRename,
    CanListDeletedTabulars,
    CanReadAssignments,
    CanGrantCreate,
    CanGrantDescribe,
    CanGrantModify,
    CanGrantSelect,
    CanGrantPassGrants,
    CanGrantManageGrants,
    CanChangeOwnership,
    CanSetManagedAccess,
}

impl OpenFgaRelation for WarehouseRelation {}

#[derive(Debug, Clone, Deserialize, Copy, Eq, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=WarehouseRelation)]
pub(super) enum APIWarehouseRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum WarehouseAssignment {
    #[schema(title = "WarehouseAssignmentOwnership")]
    Ownership(UserOrRole),
    #[schema(title = "WarehouseAssignmentPassGrants")]
    PassGrants(UserOrRole),
    #[schema(title = "WarehouseAssignmentManageGrants")]
    ManageGrants(UserOrRole),
    #[schema(title = "WarehouseAssignmentDescribe")]
    Describe(UserOrRole),
    #[schema(title = "WarehouseAssignmentSelect")]
    Select(UserOrRole),
    #[schema(title = "WarehouseAssignmentCreate")]
    Create(UserOrRole),
    #[schema(title = "WarehouseAssignmentModify")]
    Modify(UserOrRole),
}

impl GrantableRelation for APIWarehouseRelation {
    fn grant_relation(&self) -> WarehouseRelation {
        match self {
            APIWarehouseRelation::Ownership => WarehouseRelation::CanChangeOwnership,
            APIWarehouseRelation::PassGrants => WarehouseRelation::CanGrantPassGrants,
            APIWarehouseRelation::ManageGrants => WarehouseRelation::CanGrantManageGrants,
            APIWarehouseRelation::Describe => WarehouseRelation::CanGrantDescribe,
            APIWarehouseRelation::Select => WarehouseRelation::CanGrantSelect,
            APIWarehouseRelation::Create => WarehouseRelation::CanGrantCreate,
            APIWarehouseRelation::Modify => WarehouseRelation::CanGrantModify,
        }
    }
}

impl Assignment for WarehouseAssignment {
    type Relation = APIWarehouseRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIWarehouseRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::Ownership)
            }
            APIWarehouseRelation::PassGrants => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::PassGrants)
            }
            APIWarehouseRelation::ManageGrants => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::ManageGrants)
            }
            APIWarehouseRelation::Describe => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::Describe)
            }
            APIWarehouseRelation::Select => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::Select)
            }
            APIWarehouseRelation::Create => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::Create)
            }
            APIWarehouseRelation::Modify => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::Modify)
            }
        }
    }

    fn openfga_user(&self) -> String {
        match self {
            WarehouseAssignment::Ownership(user)
            | WarehouseAssignment::PassGrants(user)
            | WarehouseAssignment::Describe(user)
            | WarehouseAssignment::Select(user)
            | WarehouseAssignment::Create(user)
            | WarehouseAssignment::Modify(user)
            | WarehouseAssignment::ManageGrants(user) => user.to_openfga(),
        }
    }

    fn relation(&self) -> Self::Relation {
        match self {
            WarehouseAssignment::Ownership(_) => APIWarehouseRelation::Ownership,
            WarehouseAssignment::PassGrants { .. } => APIWarehouseRelation::PassGrants,
            WarehouseAssignment::ManageGrants { .. } => APIWarehouseRelation::ManageGrants,
            WarehouseAssignment::Describe { .. } => APIWarehouseRelation::Describe,
            WarehouseAssignment::Select { .. } => APIWarehouseRelation::Select,
            WarehouseAssignment::Create { .. } => APIWarehouseRelation::Create,
            WarehouseAssignment::Modify { .. } => APIWarehouseRelation::Modify,
        }
    }
}

#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=WarehouseAction)]
pub(super) enum APIWarehouseAction {
    CreateNamespace,
    Delete,
    ModifyStorage,
    ModifyStorageCredential,
    GetConfig,
    GetMetadata,
    ListNamespaces,
    IncludeInList,
    Deactivate,
    Activate,
    Rename,
    ListDeletedTabulars,
    ReadAssignments,
    GrantCreate,
    GrantDescribe,
    GrantModify,
    GrantSelect,
    GrantPassGrants,
    GrantManageGrants,
    ChangeOwnership,
}

impl ReducedRelation for APIWarehouseRelation {
    type OpenFgaRelation = WarehouseRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIWarehouseRelation::Ownership => WarehouseRelation::Ownership,
            APIWarehouseRelation::PassGrants => WarehouseRelation::PassGrants,
            APIWarehouseRelation::ManageGrants => WarehouseRelation::ManageGrants,
            APIWarehouseRelation::Describe => WarehouseRelation::Describe,
            APIWarehouseRelation::Select => WarehouseRelation::Select,
            APIWarehouseRelation::Create => WarehouseRelation::Create,
            APIWarehouseRelation::Modify => WarehouseRelation::Modify,
        }
    }
}

impl ReducedRelation for APIWarehouseAction {
    type OpenFgaRelation = WarehouseRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIWarehouseAction::CreateNamespace => WarehouseRelation::CanCreateNamespace,
            APIWarehouseAction::Delete => WarehouseRelation::CanDelete,
            APIWarehouseAction::ModifyStorage => WarehouseRelation::CanUpdateStorage,
            APIWarehouseAction::ModifyStorageCredential => {
                WarehouseRelation::CanUpdateStorageCredential
            }
            APIWarehouseAction::GetMetadata => WarehouseRelation::CanGetMetadata,
            APIWarehouseAction::GetConfig => WarehouseRelation::CanGetConfig,
            APIWarehouseAction::ListNamespaces => WarehouseRelation::CanListNamespaces,
            APIWarehouseAction::IncludeInList => WarehouseRelation::CanIncludeInList,
            APIWarehouseAction::Deactivate => WarehouseRelation::CanDeactivate,
            APIWarehouseAction::Activate => WarehouseRelation::CanActivate,
            APIWarehouseAction::Rename => WarehouseRelation::CanRename,
            APIWarehouseAction::ListDeletedTabulars => WarehouseRelation::CanListDeletedTabulars,
            APIWarehouseAction::ReadAssignments => WarehouseRelation::CanReadAssignments,
            APIWarehouseAction::GrantCreate => WarehouseRelation::CanGrantCreate,
            APIWarehouseAction::GrantDescribe => WarehouseRelation::CanGrantDescribe,
            APIWarehouseAction::GrantModify => WarehouseRelation::CanGrantModify,
            APIWarehouseAction::GrantSelect => WarehouseRelation::CanGrantSelect,
            APIWarehouseAction::GrantPassGrants => WarehouseRelation::CanGrantPassGrants,
            APIWarehouseAction::GrantManageGrants => WarehouseRelation::CanGrantManageGrants,
            APIWarehouseAction::ChangeOwnership => WarehouseRelation::CanChangeOwnership,
        }
    }
}

impl ReducedRelation for CatalogWarehouseAction {
    type OpenFgaRelation = WarehouseRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogWarehouseAction::CanCreateNamespace => WarehouseRelation::CanCreateNamespace,
            CatalogWarehouseAction::CanDelete => WarehouseRelation::CanDelete,
            CatalogWarehouseAction::CanUpdateStorage => WarehouseRelation::CanUpdateStorage,
            CatalogWarehouseAction::CanUpdateStorageCredential => {
                WarehouseRelation::CanUpdateStorageCredential
            }
            CatalogWarehouseAction::CanGetMetadata => WarehouseRelation::CanGetMetadata,
            CatalogWarehouseAction::CanGetConfig => WarehouseRelation::CanGetConfig,
            CatalogWarehouseAction::CanListNamespaces => WarehouseRelation::CanListNamespaces,
            CatalogWarehouseAction::CanModifySoftDeletion => {
                WarehouseRelation::CanModifySoftDeletion
            }
            CatalogWarehouseAction::CanUse => WarehouseRelation::CanUse,
            CatalogWarehouseAction::CanIncludeInList => WarehouseRelation::CanIncludeInList,
            CatalogWarehouseAction::CanDeactivate => WarehouseRelation::CanDeactivate,
            CatalogWarehouseAction::CanActivate => WarehouseRelation::CanActivate,
            CatalogWarehouseAction::CanRename => WarehouseRelation::CanRename,
            CatalogWarehouseAction::CanListDeletedTabulars => {
                WarehouseRelation::CanListDeletedTabulars
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub(super) enum NamespaceRelation {
    // -- Hierarchical relations --
    Parent,
    Child,
    // -- Managed relations --
    ManagedAccess,
    ManagedAccessInheritance,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
    // -- Actions --
    CanCreateTable,
    CanCreateView,
    CanCreateNamespace,
    CanDelete,
    CanUpdateProperties,
    CanGetMetadata,
    CanListTables,
    CanListViews,
    CanListNamespaces,
    _CanIncludeInList,
    CanReadAssignments,
    CanGrantCreate,
    CanGrantDescribe,
    CanGrantModify,
    CanGrantSelect,
    CanGrantPassGrants,
    CanGrantManageGrants,
    CanChangeOwnership,
    CanSetManagedAccess,
}

impl OpenFgaRelation for NamespaceRelation {}

impl From<CatalogNamespaceAction> for NamespaceRelation {
    fn from(namespace: CatalogNamespaceAction) -> Self {
        namespace.to_openfga()
    }
}

impl From<&CatalogNamespaceAction> for NamespaceRelation {
    fn from(namespace: &CatalogNamespaceAction) -> Self {
        namespace.to_openfga()
    }
}

#[derive(Debug, Clone, Deserialize, Copy, Eq, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=NamespaceRelation)]
pub(super) enum APINamespaceRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum NamespaceAssignment {
    #[schema(title = "NamespaceAssignmentOwnership")]
    Ownership(UserOrRole),
    #[schema(title = "NamespaceAssignmentPassGrants")]
    PassGrants(UserOrRole),
    #[schema(title = "NamespaceAssignmentManageGrants")]
    ManageGrants(UserOrRole),
    #[schema(title = "NamespaceAssignmentDescribe")]
    Describe(UserOrRole),
    #[schema(title = "NamespaceAssignmentSelect")]
    Select(UserOrRole),
    #[schema(title = "NamespaceAssignmentCreate")]
    Create(UserOrRole),
    #[schema(title = "NamespaceAssignmentModify")]
    Modify(UserOrRole),
}

impl GrantableRelation for APINamespaceRelation {
    fn grant_relation(&self) -> NamespaceRelation {
        match self {
            APINamespaceRelation::Ownership => NamespaceRelation::CanChangeOwnership,
            APINamespaceRelation::PassGrants => NamespaceRelation::CanGrantPassGrants,
            APINamespaceRelation::ManageGrants => NamespaceRelation::CanGrantManageGrants,
            APINamespaceRelation::Describe => NamespaceRelation::CanGrantDescribe,
            APINamespaceRelation::Select => NamespaceRelation::CanGrantSelect,
            APINamespaceRelation::Create => NamespaceRelation::CanCreateNamespace,
            APINamespaceRelation::Modify => NamespaceRelation::CanUpdateProperties,
        }
    }
}

impl Assignment for NamespaceAssignment {
    type Relation = APINamespaceRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APINamespaceRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::Ownership)
            }
            APINamespaceRelation::PassGrants => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::PassGrants)
            }
            APINamespaceRelation::ManageGrants => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::ManageGrants)
            }
            APINamespaceRelation::Describe => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::Describe)
            }
            APINamespaceRelation::Select => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::Select)
            }
            APINamespaceRelation::Create => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::Create)
            }
            APINamespaceRelation::Modify => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::Modify)
            }
        }
    }

    fn openfga_user(&self) -> String {
        match self {
            NamespaceAssignment::Ownership(user)
            | NamespaceAssignment::PassGrants(user)
            | NamespaceAssignment::ManageGrants(user)
            | NamespaceAssignment::Describe(user)
            | NamespaceAssignment::Select(user)
            | NamespaceAssignment::Create(user)
            | NamespaceAssignment::Modify(user) => user.to_openfga(),
        }
    }

    fn relation(&self) -> Self::Relation {
        match self {
            NamespaceAssignment::Ownership(_) => APINamespaceRelation::Ownership,
            NamespaceAssignment::PassGrants { .. } => APINamespaceRelation::PassGrants,
            NamespaceAssignment::ManageGrants { .. } => APINamespaceRelation::ManageGrants,
            NamespaceAssignment::Describe { .. } => APINamespaceRelation::Describe,
            NamespaceAssignment::Select { .. } => APINamespaceRelation::Select,
            NamespaceAssignment::Create { .. } => APINamespaceRelation::Create,
            NamespaceAssignment::Modify { .. } => APINamespaceRelation::Modify,
        }
    }
}

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[schema(as=NamespaceAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APINamespaceAction {
    CreateTable,
    CreateView,
    CreateNamespace,
    Delete,
    UpdateProperties,
    GetMetadata,
    ReadAssignments,
    GrantCreate,
    GrantDescribe,
    GrantModify,
    GrantSelect,
    GrantPassGrants,
    GrantManageGrants,
}

impl ReducedRelation for APINamespaceRelation {
    type OpenFgaRelation = NamespaceRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APINamespaceRelation::Ownership => NamespaceRelation::Ownership,
            APINamespaceRelation::PassGrants => NamespaceRelation::PassGrants,
            APINamespaceRelation::ManageGrants => NamespaceRelation::ManageGrants,
            APINamespaceRelation::Describe => NamespaceRelation::Describe,
            APINamespaceRelation::Select => NamespaceRelation::Select,
            APINamespaceRelation::Create => NamespaceRelation::Create,
            APINamespaceRelation::Modify => NamespaceRelation::Modify,
        }
    }
}

impl ReducedRelation for APINamespaceAction {
    type OpenFgaRelation = NamespaceRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APINamespaceAction::CreateTable => NamespaceRelation::CanCreateTable,
            APINamespaceAction::CreateView => NamespaceRelation::CanCreateView,
            APINamespaceAction::CreateNamespace => NamespaceRelation::CanCreateNamespace,
            APINamespaceAction::Delete => NamespaceRelation::CanDelete,
            APINamespaceAction::UpdateProperties => NamespaceRelation::CanUpdateProperties,
            APINamespaceAction::GetMetadata => NamespaceRelation::CanGetMetadata,
            APINamespaceAction::ReadAssignments => NamespaceRelation::CanReadAssignments,
            APINamespaceAction::GrantCreate => NamespaceRelation::CanGrantCreate,
            APINamespaceAction::GrantDescribe => NamespaceRelation::CanGrantDescribe,
            APINamespaceAction::GrantModify => NamespaceRelation::CanGrantModify,
            APINamespaceAction::GrantSelect => NamespaceRelation::CanGrantSelect,
            APINamespaceAction::GrantPassGrants => NamespaceRelation::CanGrantPassGrants,
            APINamespaceAction::GrantManageGrants => NamespaceRelation::CanGrantManageGrants,
        }
    }
}

impl ReducedRelation for CatalogNamespaceAction {
    type OpenFgaRelation = NamespaceRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogNamespaceAction::CanCreateTable => NamespaceRelation::CanCreateTable,
            CatalogNamespaceAction::CanCreateView => NamespaceRelation::CanCreateView,
            CatalogNamespaceAction::CanCreateNamespace => NamespaceRelation::CanCreateNamespace,
            CatalogNamespaceAction::CanDelete => NamespaceRelation::CanDelete,
            CatalogNamespaceAction::CanUpdateProperties => NamespaceRelation::CanUpdateProperties,
            CatalogNamespaceAction::CanGetMetadata => NamespaceRelation::CanGetMetadata,
            CatalogNamespaceAction::CanListTables => NamespaceRelation::CanListTables,
            CatalogNamespaceAction::CanListViews => NamespaceRelation::CanListViews,
            CatalogNamespaceAction::CanListNamespaces => NamespaceRelation::CanListNamespaces,
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub(super) enum TableRelation {
    // -- Hierarchical relations --
    Parent,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Modify,
    // -- Actions --
    CanDrop,
    CanWriteData,
    CanReadData,
    CanGetMetadata,
    CanCommit,
    CanRename,
    CanIncludeInList,
    CanReadAssignments,
    CanGrantPassGrants,
    CanGrantManageGrants,
    CanGrantDescribe,
    CanGrantSelect,
    CanGrantModify,
    CanChangeOwnership,
    CanUndrop,
}

impl OpenFgaRelation for TableRelation {}

impl From<CatalogTableAction> for TableRelation {
    fn from(action: CatalogTableAction) -> Self {
        action.to_openfga()
    }
}

impl From<&CatalogTableAction> for TableRelation {
    fn from(action: &CatalogTableAction) -> Self {
        action.to_openfga()
    }
}

#[derive(Debug, Clone, Deserialize, Copy, Eq, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=TableRelation)]
pub(super) enum APITableRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Modify,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum TableAssignment {
    #[schema(title = "TableAssignmentOwnership")]
    Ownership(UserOrRole),
    #[schema(title = "TableAssignmentPassGrants")]
    PassGrants(UserOrRole),
    #[schema(title = "TableAssignmentManageGrants")]
    ManageGrants(UserOrRole),
    #[schema(title = "TableAssignmentDescribe")]
    Describe(UserOrRole),
    #[schema(title = "TableAssignmentSelect")]
    Select(UserOrRole),
    #[schema(title = "TableAssignmentCreate")]
    Modify(UserOrRole),
}

impl GrantableRelation for APITableRelation {
    fn grant_relation(&self) -> TableRelation {
        match self {
            APITableRelation::Ownership => TableRelation::CanChangeOwnership,
            APITableRelation::PassGrants => TableRelation::CanGrantPassGrants,
            APITableRelation::ManageGrants => TableRelation::CanGrantManageGrants,
            APITableRelation::Describe => TableRelation::CanGrantDescribe,
            APITableRelation::Select => TableRelation::CanGrantSelect,
            APITableRelation::Modify => TableRelation::CanGrantModify,
        }
    }
}

impl Assignment for TableAssignment {
    type Relation = APITableRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APITableRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(TableAssignment::Ownership)
            }
            APITableRelation::PassGrants => {
                UserOrRole::parse_from_openfga(user).map(TableAssignment::PassGrants)
            }
            APITableRelation::ManageGrants => {
                UserOrRole::parse_from_openfga(user).map(TableAssignment::ManageGrants)
            }
            APITableRelation::Describe => {
                UserOrRole::parse_from_openfga(user).map(TableAssignment::Describe)
            }
            APITableRelation::Select => {
                UserOrRole::parse_from_openfga(user).map(TableAssignment::Select)
            }
            APITableRelation::Modify => {
                UserOrRole::parse_from_openfga(user).map(TableAssignment::Modify)
            }
        }
    }

    fn openfga_user(&self) -> String {
        match self {
            TableAssignment::Ownership(user)
            | TableAssignment::PassGrants(user)
            | TableAssignment::ManageGrants(user)
            | TableAssignment::Describe(user)
            | TableAssignment::Select(user)
            | TableAssignment::Modify(user) => user.to_openfga(),
        }
    }

    fn relation(&self) -> Self::Relation {
        match self {
            TableAssignment::Ownership(_) => APITableRelation::Ownership,
            TableAssignment::PassGrants { .. } => APITableRelation::PassGrants,
            TableAssignment::ManageGrants { .. } => APITableRelation::ManageGrants,
            TableAssignment::Describe { .. } => APITableRelation::Describe,
            TableAssignment::Select { .. } => APITableRelation::Select,
            TableAssignment::Modify { .. } => APITableRelation::Modify,
        }
    }
}

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[schema(as=TableAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APITableAction {
    Drop,
    WriteData,
    ReadData,
    GetMetadata,
    Commit,
    Rename,
    ReadAssignments,
    GrantPassGrants,
    GrantManageGrants,
    GrantDescribe,
    GrantSelect,
    GrantModify,
    ChangeOwnership,
}

impl ReducedRelation for APITableRelation {
    type OpenFgaRelation = TableRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APITableRelation::Ownership => TableRelation::Ownership,
            APITableRelation::PassGrants => TableRelation::PassGrants,
            APITableRelation::ManageGrants => TableRelation::ManageGrants,
            APITableRelation::Describe => TableRelation::Describe,
            APITableRelation::Select => TableRelation::Select,
            APITableRelation::Modify => TableRelation::Modify,
        }
    }
}

impl ReducedRelation for APITableAction {
    type OpenFgaRelation = TableRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APITableAction::Drop => TableRelation::CanDrop,
            APITableAction::WriteData => TableRelation::CanWriteData,
            APITableAction::ReadData => TableRelation::CanReadData,
            APITableAction::GetMetadata => TableRelation::CanGetMetadata,
            APITableAction::Commit => TableRelation::CanCommit,
            APITableAction::Rename => TableRelation::CanRename,
            APITableAction::ReadAssignments => TableRelation::CanReadAssignments,
            APITableAction::GrantPassGrants => TableRelation::CanGrantPassGrants,
            APITableAction::GrantManageGrants => TableRelation::CanGrantManageGrants,
            APITableAction::GrantDescribe => TableRelation::CanGrantDescribe,
            APITableAction::GrantSelect => TableRelation::CanGrantSelect,
            APITableAction::GrantModify => TableRelation::CanGrantModify,
            APITableAction::ChangeOwnership => TableRelation::CanChangeOwnership,
        }
    }
}

impl ReducedRelation for CatalogTableAction {
    type OpenFgaRelation = TableRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogTableAction::CanDrop => TableRelation::CanDrop,
            CatalogTableAction::CanWriteData => TableRelation::CanWriteData,
            CatalogTableAction::CanReadData => TableRelation::CanReadData,
            CatalogTableAction::CanGetMetadata => TableRelation::CanGetMetadata,
            CatalogTableAction::CanCommit => TableRelation::CanCommit,
            CatalogTableAction::CanRename => TableRelation::CanRename,
            CatalogTableAction::CanIncludeInList => TableRelation::CanIncludeInList,
            CatalogTableAction::CanUndrop => TableRelation::CanUndrop,
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub(super) enum ViewRelation {
    // -- Hierarchical relations --
    Parent,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Modify,
    // -- Actions --
    CanDrop,
    CanCommit,
    CanGetMetadata,
    CanRename,
    CanIncludeInList,
    CanReadAssignments,
    CanGrantPassGrants,
    CanGrantManageGrants,
    CanGrantDescribe,
    CanGrantModify,
    CanChangeOwnership,
    CanUndrop,
}

impl OpenFgaRelation for ViewRelation {}

impl From<CatalogViewAction> for ViewRelation {
    fn from(action: CatalogViewAction) -> Self {
        action.to_openfga()
    }
}

impl From<&CatalogViewAction> for ViewRelation {
    fn from(action: &CatalogViewAction) -> Self {
        action.to_openfga()
    }
}

#[derive(Debug, Clone, Deserialize, Copy, Eq, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ViewRelation)]
pub(super) enum APIViewRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Modify,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ViewAssignment {
    #[schema(title = "ViewAssignmentOwnership")]
    Ownership(UserOrRole),
    #[schema(title = "ViewAssignmentPassGrants")]
    PassGrants(UserOrRole),
    #[schema(title = "ViewAssignmentManageGrants")]
    ManageGrants(UserOrRole),
    #[schema(title = "ViewAssignmentDescribe")]
    Describe(UserOrRole),
    #[schema(title = "ViewAssignmentModify")]
    Modify(UserOrRole),
}

impl GrantableRelation for APIViewRelation {
    fn grant_relation(&self) -> ViewRelation {
        match self {
            APIViewRelation::Ownership => ViewRelation::CanChangeOwnership,
            APIViewRelation::PassGrants => ViewRelation::CanGrantPassGrants,
            APIViewRelation::ManageGrants => ViewRelation::CanGrantManageGrants,
            APIViewRelation::Describe => ViewRelation::CanGrantDescribe,
            APIViewRelation::Modify => ViewRelation::CanGrantModify,
        }
    }
}

impl Assignment for ViewAssignment {
    type Relation = APIViewRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIViewRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(ViewAssignment::Ownership)
            }
            APIViewRelation::PassGrants => {
                UserOrRole::parse_from_openfga(user).map(ViewAssignment::PassGrants)
            }
            APIViewRelation::ManageGrants => {
                UserOrRole::parse_from_openfga(user).map(ViewAssignment::ManageGrants)
            }
            APIViewRelation::Describe => {
                UserOrRole::parse_from_openfga(user).map(ViewAssignment::Describe)
            }
            APIViewRelation::Modify => {
                UserOrRole::parse_from_openfga(user).map(ViewAssignment::Modify)
            }
        }
    }

    fn openfga_user(&self) -> String {
        match self {
            ViewAssignment::Ownership(user)
            | ViewAssignment::PassGrants(user)
            | ViewAssignment::ManageGrants(user)
            | ViewAssignment::Describe(user)
            | ViewAssignment::Modify(user) => user.to_openfga(),
        }
    }

    fn relation(&self) -> Self::Relation {
        match self {
            ViewAssignment::Ownership(_) => APIViewRelation::Ownership,
            ViewAssignment::PassGrants { .. } => APIViewRelation::PassGrants,
            ViewAssignment::ManageGrants { .. } => APIViewRelation::ManageGrants,
            ViewAssignment::Describe { .. } => APIViewRelation::Describe,
            ViewAssignment::Modify { .. } => APIViewRelation::Modify,
        }
    }
}

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[schema(as=ViewAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APIViewAction {
    Drop,
    Commit,
    GetMetadata,
    Rename,
    ReadAssignments,
    GrantPassGrants,
    GrantManageGrants,
    GrantDescribe,
    GrantModify,
    ChangeOwnership,
}

impl ReducedRelation for APIViewRelation {
    type OpenFgaRelation = ViewRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIViewRelation::Ownership => ViewRelation::Ownership,
            APIViewRelation::PassGrants => ViewRelation::PassGrants,
            APIViewRelation::ManageGrants => ViewRelation::ManageGrants,
            APIViewRelation::Describe => ViewRelation::Describe,
            APIViewRelation::Modify => ViewRelation::Modify,
        }
    }
}

impl ReducedRelation for APIViewAction {
    type OpenFgaRelation = ViewRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIViewAction::Drop => ViewRelation::CanDrop,
            APIViewAction::Commit => ViewRelation::CanCommit,
            APIViewAction::GetMetadata => ViewRelation::CanGetMetadata,
            APIViewAction::Rename => ViewRelation::CanRename,
            APIViewAction::ReadAssignments => ViewRelation::CanReadAssignments,
            APIViewAction::GrantPassGrants => ViewRelation::CanGrantPassGrants,
            APIViewAction::GrantManageGrants => ViewRelation::CanGrantManageGrants,
            APIViewAction::GrantDescribe => ViewRelation::CanGrantDescribe,
            APIViewAction::GrantModify => ViewRelation::CanGrantModify,
            APIViewAction::ChangeOwnership => ViewRelation::CanChangeOwnership,
        }
    }
}

impl ReducedRelation for CatalogViewAction {
    type OpenFgaRelation = ViewRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogViewAction::CanDrop => ViewRelation::CanDrop,
            CatalogViewAction::CanCommit => ViewRelation::CanCommit,
            CatalogViewAction::CanGetMetadata => ViewRelation::CanGetMetadata,
            CatalogViewAction::CanRename => ViewRelation::CanRename,
            CatalogViewAction::CanIncludeInList => ViewRelation::CanIncludeInList,
            CatalogViewAction::CanUndrop => ViewRelation::CanUndrop,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_assignment_serialization() {
        let user_id = UserId::new_unchecked("oidc", "my_user");
        let user_or_role = UserOrRole::User(user_id);
        let assignment = ServerAssignment::Admin(user_or_role);
        let serialized = serde_json::to_string(&assignment).unwrap();
        let expected = serde_json::json!({
            "type": "admin",
            "user": "oidc~my_user"
        });
        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&serialized).unwrap()
        );
    }
}
