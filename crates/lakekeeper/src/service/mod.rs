pub mod authn;
pub mod authz;
mod catalog;
pub mod contract_verification;
pub mod endpoint_hooks;
pub mod endpoint_statistics;
pub mod event_publisher;
pub mod health;
pub mod secrets;
pub mod storage;
mod tabular_idents;
pub mod task_queue;

use std::{ops::Deref, str::FromStr};

pub use authn::{Actor, UserId};
pub use catalog::{
    Catalog, CommitTableResponse, CreateNamespaceRequest, CreateNamespaceResponse,
    CreateOrUpdateUserResponse, CreateTableRequest, CreateTableResponse, DeletionDetails,
    DropFlags, GetNamespaceResponse, GetProjectResponse, GetStorageConfigResponse,
    GetTableMetadataResponse, GetWarehouseResponse, ListFlags, ListNamespacesQuery,
    ListNamespacesResponse, LoadTableResponse, NamespaceDropInfo, NamespaceIdent, NamespaceInfo,
    Result, ServerInfo, TableCommit, TableCreation, TableIdent, TableInfo, TabularInfo,
    Transaction, UndropTabularResponse, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse, ViewCommit, ViewMetadataWithLocation,
};
pub use endpoint_statistics::EndpointStatisticsTrackerTx;
use http::StatusCode;
pub use secrets::{SecretIdent, SecretStore};
use serde::{Deserialize, Serialize};
pub(crate) use tabular_idents::TabularIdentBorrowed;
pub use tabular_idents::{TabularId, TabularIdentOwned};
use task_queue::RegisteredTaskQueues;

use self::authz::Authorizer;
pub use crate::api::{ErrorModel, IcebergErrorResponse};
use crate::{
    api::{iceberg::v1::Prefix, ThreadSafe as ServiceState},
    service::{contract_verification::ContractVerifiers, endpoint_hooks::EndpointHookCollection},
};

// ---------------- State ----------------
#[derive(Clone, Debug)]
pub struct State<A: Authorizer + Clone, C: Catalog, S: SecretStore> {
    pub authz: A,
    pub catalog: C::State,
    pub secrets: S,
    pub contract_verifiers: ContractVerifiers,
    pub hooks: EndpointHookCollection,
    pub registered_task_queues: RegisteredTaskQueues,
}

impl<A: Authorizer + Clone, C: Catalog, S: SecretStore> ServiceState for State<A, C, S> {}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
pub struct ViewId(uuid::Uuid);

impl From<uuid::Uuid> for ViewId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
pub struct NamespaceId(uuid::Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
pub struct TableId(uuid::Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
pub struct WarehouseId(pub(crate) uuid::Uuid);

impl NamespaceId {
    #[must_use]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl TableId {
    #[must_use]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl WarehouseId {
    #[must_use]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl ViewId {
    #[must_use]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

/// Status of a warehouse
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    strum_macros::Display,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "warehouse_status", rename_all = "kebab-case")
)]
pub enum WarehouseStatus {
    /// The warehouse is active and can be used
    Active,
    /// The warehouse is inactive and cannot be used.
    Inactive,
}

#[derive(Debug, serde::Serialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
pub struct ProjectId(String);

impl<'de> serde::Deserialize<'de> for ProjectId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<ProjectId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ProjectId::try_new(s).map_err(|e| serde::de::Error::custom(e.error.message))
    }
}

impl From<ProjectId> for String {
    fn from(ident: ProjectId) -> Self {
        ident.0
    }
}

impl ProjectId {
    #[must_use]
    pub fn new(id: uuid::Uuid) -> Self {
        Self(id.to_string())
    }

    #[must_use]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::now_v7().to_string())
    }

    /// Create a new project id from a string.
    ///
    /// # Errors
    /// Returns an error if the provided string is not a valid project id.
    /// Valid project ids may only contain alphanumeric characters, hyphens and underscores.
    pub fn try_new(id: String) -> Result<Self> {
        // Only allow the following characters in the project id:
        // - Alphanumeric characters
        // - Hyphens
        // - Underscores

        if id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            Ok(Self(id))
        } else {
            Err(ErrorModel::bad_request(format!(
                "Project IDs may only contain alphanumeric characters, hyphens and underscores. Got: `{id}`",
            ), "MalformedProjectID", None).into())
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn from_db_unchecked(id: String) -> Self {
        Self(id)
    }
}

impl Deref for ProjectId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
#[serde(transparent)]
pub struct RoleId(uuid::Uuid);

impl<'de> serde::Deserialize<'de> for RoleId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<RoleId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        RoleId::from_str(&s).map_err(|e| serde::de::Error::custom(e.error.message))
    }
}

impl RoleId {
    #[must_use]
    pub fn new(id: uuid::Uuid) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl FromStr for RoleId {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RoleId(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided role id is not a valid UUID".to_string())
                .r#type("RoleIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl std::fmt::Display for RoleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for RoleId {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RoleId> for uuid::Uuid {
    fn from(ident: RoleId) -> Self {
        ident.0
    }
}

impl Deref for ViewId {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ViewId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ViewId {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ViewId(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided view id is not a valid UUID".to_string())
                .r#type("ViewIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl Deref for NamespaceId {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for NamespaceId {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(NamespaceId(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided namespace id is not a valid UUID".to_string())
                .r#type("NamespaceIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl From<uuid::Uuid> for NamespaceId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<&uuid::Uuid> for NamespaceId {
    fn from(uuid: &uuid::Uuid) -> Self {
        Self(*uuid)
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for TableId {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<uuid::Uuid> for TableId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl FromStr for TableId {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TableId(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided table id is not a valid UUID".to_string())
                .r#type("TableIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl From<TableId> for uuid::Uuid {
    fn from(ident: TableId) -> Self {
        ident.0
    }
}

impl TryFrom<TabularId> for TableId {
    type Error = IcebergErrorResponse;

    fn try_from(value: TabularId) -> Result<Self, Self::Error> {
        match value {
            TabularId::Table(value) => Ok(value.into()),
            TabularId::View(_) => Err(ErrorModel::internal(
                "Provided identifier is not a table id",
                "IdentifierIsNotTableID",
                None,
            )
            .into()),
        }
    }
}

// ---------------- Identifier ----------------
impl std::fmt::Display for ProjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ProjectId {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ProjectId::try_new(s.to_string())
    }
}

impl WarehouseId {
    #[must_use]
    pub fn to_uuid(&self) -> uuid::Uuid {
        **self
    }

    #[must_use]
    pub fn as_uuid(&self) -> &uuid::Uuid {
        self
    }
}

impl Deref for WarehouseId {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<uuid::Uuid> for WarehouseId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for WarehouseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WarehouseId {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(WarehouseId(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided warehouse id is not a valid UUID".to_string())
                .r#type("WarehouseIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl From<uuid::Uuid> for ProjectId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self::new(uuid)
    }
}

impl TryFrom<Prefix> for WarehouseId {
    type Error = IcebergErrorResponse;

    fn try_from(value: Prefix) -> Result<Self, Self::Error> {
        let prefix = uuid::Uuid::parse_str(value.as_str()).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!(
                    "Provided prefix is not a warehouse id. Expected UUID, got: {}",
                    value.as_str()
                ))
                .r#type("PrefixIsNotWarehouseID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?;
        Ok(WarehouseId(prefix))
    }
}

#[derive(Debug, Clone)]
pub struct TabularDetails {
    pub ident: TableId,
    pub location: String,
}
