pub mod authn;
pub mod authz;
mod catalog_store;
pub mod contract_verification;
pub mod endpoint_hooks;
pub mod endpoint_statistics;
pub mod event_publisher;
pub mod health;
pub mod secrets;
pub mod storage;
pub mod task_queue;
pub use authn::{Actor, UserId};
pub use catalog_store::{
    CatalogStore, CommitTableResponse, CreateNamespaceRequest, CreateNamespaceResponse,
    CreateOrUpdateUserResponse, CreateTableRequest, CreateTableResponse, DeletionDetails,
    DropFlags, GetNamespaceResponse, GetProjectResponse, GetStorageConfigResponse,
    GetTableMetadataResponse, GetWarehouseResponse, ListFlags, ListNamespacesQuery,
    ListNamespacesResponse, LoadTableResponse, NamespaceDropInfo, NamespaceIdent, NamespaceInfo,
    Result, ServerInfo, TableCommit, TableCreation, TableIdent, TableInfo, TabularInfo,
    Transaction, UndropTabularResponse, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse, ViewCommit, ViewMetadataWithLocation,
};
pub use endpoint_statistics::EndpointStatisticsTrackerTx;
#[allow(unused_imports)]
pub(crate) use identifier::tabular::TabularIdentBorrowed;
pub use identifier::tabular::{TabularId, TabularIdentOwned};
pub use secrets::{SecretIdent, SecretStore};
use task_queue::RegisteredTaskQueues;

use self::authz::Authorizer;
pub use crate::api::{ErrorModel, IcebergErrorResponse};
use crate::{
    api::{management::v1::server::LicenseStatus, ThreadSafe as ServiceState},
    service::{contract_verification::ContractVerifiers, endpoint_hooks::EndpointHookCollection},
};

mod identifier;

pub use identifier::{generic::*, project::ProjectId};

// ---------------- State ----------------
#[derive(Clone, Debug)]
pub struct State<A: Authorizer + Clone, C: CatalogStore, S: SecretStore> {
    pub authz: A,
    pub catalog: C::State,
    pub secrets: S,
    pub contract_verifiers: ContractVerifiers,
    pub hooks: EndpointHookCollection,
    pub registered_task_queues: RegisteredTaskQueues,
    pub license_status: &'static LicenseStatus,
}

impl<A: Authorizer + Clone, C: CatalogStore, S: SecretStore> ServiceState for State<A, C, S> {}

impl<A: Authorizer + Clone, C: CatalogStore, S: SecretStore> State<A, C, S> {
    pub fn server_id(&self) -> ServerId {
        self.authz.server_id()
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
    strum_macros::EnumIter,
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

#[derive(Debug, Clone)]
/// Metadata for a tabular dataset, including its `warehouse_id`, `table_id` and the storage
/// `location` where its data lives.
///
/// Note that `table_id`s can be reused across warehouses. So `table_id` may not be unique, but
/// `(warehouse_id, table_id)` is.
pub struct TabularDetails {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub location: String,
}
