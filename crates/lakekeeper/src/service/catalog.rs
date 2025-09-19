use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
    time::{Duration, Instant},
};

use iceberg::{
    spec::{TableMetadata, ViewMetadata},
    TableUpdate,
};
use iceberg_ext::catalog::rest::{CatalogConfig, ErrorModel};
pub use iceberg_ext::catalog::rest::{CommitTableResponse, CreateTableRequest};
use lakekeeper_io::Location;

use super::{
    authz::TableUuid, storage::StorageProfile, NamespaceId, ProjectId, RoleId, TableId,
    TabularDetails, ViewId, WarehouseId, WarehouseStatus,
};
pub use crate::api::iceberg::v1::{
    CreateNamespaceRequest, CreateNamespaceResponse, ListNamespacesQuery, NamespaceIdent, Result,
    TableIdent, UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use crate::{
    api::{
        iceberg::v1::{namespace::NamespaceDropFlags, PaginatedMapping, PaginationQuery},
        management::v1::{
            project::{EndpointStatisticsResponse, TimeWindowSelector, WarehouseFilter},
            role::{ListRolesResponse, Role, SearchRoleResponse},
            tasks::{GetTaskDetailsResponse, ListTasksRequest, ListTasksResponse},
            user::{ListUsersResponse, SearchUserResponse, User, UserLastUpdatedWith, UserType},
            warehouse::{
                GetTaskQueueConfigResponse, SetTaskQueueConfigRequest, TabularDeleteProfile,
                WarehouseStatisticsResponse,
            },
            DeleteWarehouseQuery, ProtectionResponse,
        },
    },
    catalog::tables::TableMetadataDiffs,
    request_metadata::RequestMetadata,
    service::{
        authn::UserId,
        health::HealthExt,
        tabular_idents::{TabularId, TabularIdentOwned},
        task_queue::{
            Task, TaskAttemptId, TaskCheckState, TaskEntity, TaskFilter, TaskId, TaskInput,
            TaskQueueName,
        },
    },
    SecretIdent,
};

struct TasksCacheExpiry;
const TASKS_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
impl<K, V> moka::Expiry<K, V> for TasksCacheExpiry {
    fn expire_after_create(&self, _key: &K, _value: &V, _created_at: Instant) -> Option<Duration> {
        Some(TASKS_CACHE_TTL)
    }
}
static TASKS_CACHE: LazyLock<moka::future::Cache<TaskId, (TaskEntity, TaskQueueName)>> =
    LazyLock::new(|| {
        moka::future::Cache::builder()
            .max_capacity(10000)
            .expire_after(TasksCacheExpiry)
            .build()
    });

#[async_trait::async_trait]
pub trait Transaction<D>
where
    Self: Sized + Send + Sync,
{
    type Transaction<'a>: Send + Sync + 'a
    where
        Self: 'static;

    async fn begin_write(db_state: D) -> Result<Self>;

    async fn begin_read(db_state: D) -> Result<Self>;

    async fn commit(self) -> Result<()>;

    async fn rollback(self) -> Result<()>;

    fn transaction(&mut self) -> Self::Transaction<'_>;
}

#[derive(Debug)]
pub struct GetNamespaceResponse {
    /// Reference to one or more levels of a namespace
    pub namespace: NamespaceIdent,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListNamespacesResponse {
    pub next_page_tokens: Vec<(NamespaceId, String)>,
    pub namespaces: HashMap<NamespaceId, NamespaceIdent>,
}

#[derive(Debug)]
pub struct CreateTableResponse {
    pub table_metadata: TableMetadata,
    pub staged_table_id: Option<TableId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LoadTableResponse {
    pub table_id: TableId,
    pub namespace_id: NamespaceId,
    pub table_metadata: TableMetadata,
    pub metadata_location: Option<Location>,
    pub storage_secret_ident: Option<SecretIdent>,
    pub storage_profile: StorageProfile,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GetTableMetadataResponse {
    pub table: TableIdent,
    pub table_id: TableId,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub location: String,
    pub metadata_location: Option<String>,
    pub storage_secret_ident: Option<SecretIdent>,
    pub storage_profile: StorageProfile,
}

impl TableUuid for GetTableMetadataResponse {
    fn table_uuid(&self) -> TableId {
        self.table_id
    }
}

#[derive(Debug)]
pub struct GetStorageConfigResponse {
    pub storage_profile: StorageProfile,
    pub storage_secret_ident: Option<SecretIdent>,
}

#[derive(Debug, Clone)]
pub struct GetWarehouseResponse {
    /// ID of the warehouse.
    pub id: WarehouseId,
    /// Name of the warehouse.
    pub name: String,
    /// Project ID in which the warehouse is created.
    pub project_id: ProjectId,
    /// Storage profile used for the warehouse.
    pub storage_profile: StorageProfile,
    /// Storage secret ID used for the warehouse.
    pub storage_secret_id: Option<SecretIdent>,
    /// Whether the warehouse is active.
    pub status: WarehouseStatus,
    /// Tabular delete profile used for the warehouse.
    pub tabular_delete_profile: TabularDeleteProfile,
    /// Whether the warehouse is protected from being deleted.
    pub protected: bool,
}

#[derive(Debug, Clone)]
pub struct GetProjectResponse {
    /// ID of the project.
    pub project_id: ProjectId,
    /// Name of the project.
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct TableCommit {
    pub new_metadata: TableMetadata,
    pub new_metadata_location: Location,
    pub previous_metadata_location: Option<Location>,
    pub updates: Vec<TableUpdate>,
    pub diffs: TableMetadataDiffs,
}

#[derive(Debug, Clone)]
pub struct ViewCommit<'a> {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub view_id: ViewId,
    pub view_ident: &'a TableIdent,
    pub new_metadata_location: &'a Location,
    pub previous_metadata_location: &'a Location,
    pub metadata: ViewMetadata,
    pub new_location: &'a Location,
}

#[derive(Debug, Clone)]
pub struct TableCreation<'c> {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub table_ident: &'c TableIdent,
    pub metadata_location: Option<&'c Location>,
    pub table_metadata: TableMetadata,
}

#[derive(Debug, Clone)]
pub enum CreateOrUpdateUserResponse {
    Created(User),
    Updated(User),
}

#[derive(Debug, Clone)]
pub struct UndropTabularResponse {
    pub table_id: TableId,
    pub expiration_task_id: Option<TaskId>,
    pub name: String,
    pub namespace: NamespaceIdent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerInfo {
    /// Catalog is not bootstrapped
    NotBootstrapped,
    /// Catalog is bootstrapped
    Bootstrapped {
        /// Server ID of the catalog at the time of bootstrapping
        server_id: uuid::Uuid,
        /// Whether the terms have been accepted
        terms_accepted: bool,
        /// Whether the catalog is open for re-bootstrap,
        /// i.e. to recover admin access.
        open_for_bootstrap: bool,
    },
}

impl ServerInfo {
    /// Returns the server ID if the catalog is bootstrapped.
    #[must_use]
    pub fn server_id(&self) -> Option<uuid::Uuid> {
        match self {
            ServerInfo::NotBootstrapped => None,
            ServerInfo::Bootstrapped { server_id, .. } => Some(*server_id),
        }
    }

    /// Returns true if the catalog is bootstrapped.
    #[must_use]
    pub fn is_bootstrapped(&self) -> bool {
        matches!(self, ServerInfo::Bootstrapped { .. })
    }
}

#[derive(Debug, PartialEq)]
pub struct NamespaceInfo {
    pub namespace_ident: NamespaceIdent,
    pub protected: bool,
}

#[derive(Debug)]
pub struct NamespaceDropInfo {
    pub child_namespaces: Vec<NamespaceId>,
    pub child_tables: Vec<(TabularId, String)>,
    pub open_tasks: Vec<TaskId>,
}

#[derive(Debug, PartialEq)]
pub struct TableInfo {
    pub table_ident: TableIdent,
    pub deletion_details: Option<DeletionDetails>,
    pub protected: bool,
}

#[derive(Debug, PartialEq)]
pub struct TabularInfo {
    pub table_ident: TabularIdentOwned,
    pub deletion_details: Option<DeletionDetails>,
    pub protected: bool,
}

impl TabularInfo {
    /// Verifies that `self` is a table before converting the `TabularInfo` into a `TableInfo`.
    ///
    /// # Errors
    /// If the `TabularInfo` is a view, this will return an error.
    pub fn into_table_info(self) -> Result<TableInfo> {
        Ok(TableInfo {
            table_ident: self.table_ident.into_table()?,
            deletion_details: self.deletion_details,
            protected: self.protected,
        })
    }

    /// Verifies that `self` is a view before converting the `TabularInfo` into a `TableInfo`.
    ///
    /// # Errors
    /// If the `TabularInfo` is a table, this will return an error.
    pub fn into_view_info(self) -> Result<TableInfo> {
        Ok(TableInfo {
            table_ident: self.table_ident.into_view()?,
            deletion_details: self.deletion_details,
            protected: self.protected,
        })
    }
}

#[async_trait::async_trait]
pub trait Catalog
where
    Self: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    type Transaction: Transaction<Self::State>;
    type State: Clone + std::fmt::Debug + Send + Sync + 'static + HealthExt;

    async fn determine_server_id(state: Self::State) -> anyhow::Result<uuid::Uuid> {
        let server_info = Self::get_server_info(state.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e).context("Failed to determine server id"))?;
        let previous_server_id = match server_info {
            ServerInfo::Bootstrapped { server_id, .. } => Some(server_id),
            ServerInfo::NotBootstrapped => None,
        };

        let server_id = previous_server_id.unwrap_or_else(uuid::Uuid::now_v7);
        Ok(server_id)
    }

    /// Get data required for startup validations and server info endpoint
    async fn get_server_info(
        catalog_state: Self::State,
    ) -> std::result::Result<ServerInfo, ErrorModel>;

    /// Bootstrap the catalog.
    /// Use this hook to persist the provided `server_id`.
    /// Must return Ok(false) if the catalog is not open for bootstrap.
    /// If bootstrapping succeeds, return Ok(true).
    async fn bootstrap<'a>(
        terms_accepted: bool,
        server_id: uuid::Uuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<bool>;

    // Should only return a warehouse if the warehouse is active.
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> Result<Option<WarehouseId>>;

    /// Wrapper around get_warehouse_by_name that returns
    /// not found error if the warehouse does not exist.
    async fn require_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> Result<WarehouseId> {
        Self::get_warehouse_by_name(warehouse_name, project_id, catalog_state)
            .await?
            .ok_or(
                ErrorModel::not_found(
                    format!("Warehouse {warehouse_name} not found"),
                    "WarehouseNotFound",
                    None,
                )
                .into(),
            )
    }

    // Should only return a warehouse if the warehouse is active.
    async fn get_config_for_warehouse(
        warehouse_id: WarehouseId,
        catalog_state: Self::State,
        request_metadata: &RequestMetadata,
    ) -> Result<Option<CatalogConfig>>;

    /// Wrapper around get_config_for_warehouse that returns
    /// not found error if the warehouse does not exist.
    async fn require_config_for_warehouse(
        warehouse_id: WarehouseId,
        request_metadata: &RequestMetadata,
        catalog_state: Self::State,
    ) -> Result<CatalogConfig> {
        Self::get_config_for_warehouse(warehouse_id, catalog_state, request_metadata)
            .await?
            .ok_or(
                ErrorModel::not_found(
                    format!("Warehouse {warehouse_id} not found"),
                    "WarehouseNotFound",
                    None,
                )
                .into(),
            )
    }

    // Should only return namespaces if the warehouse is active.
    async fn list_namespaces<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<PaginatedMapping<NamespaceId, NamespaceInfo>>;

    async fn create_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CreateNamespaceResponse>;

    // Should only return a namespace if the warehouse is active.
    async fn get_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<GetNamespaceResponse>;

    /// Return Err only on unexpected errors, not if the namespace does not exist.
    /// If the namespace does not exist, return Ok(false).
    ///
    /// We use this function also to handle the `namespace_exists` endpoint.
    /// Also return Ok(false) if the warehouse is not active.
    async fn namespace_to_id<'a>(
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<NamespaceId>>;

    async fn drop_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        flags: NamespaceDropFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<NamespaceDropInfo>;

    /// Update the properties of a namespace.
    ///
    /// The properties are the final key-value properties that should
    /// be persisted as-is in the catalog.
    async fn update_namespace_properties<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    async fn create_table<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CreateTableResponse>;

    async fn list_tables<'a>(
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TableId, TableInfo>>;

    /// Return Err only on unexpected errors, not if the table does not exist.
    /// If include_staged is true, also return staged tables.
    /// If the table does not exist, return Ok(None).
    ///
    /// We use this function also to handle the `table_exists` endpoint.
    /// Also return Ok(None) if the warehouse is not active.
    async fn resolve_table_ident<'a>(
        warehouse_id: WarehouseId,
        table: &TableIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<TabularDetails>>;

    async fn table_to_id<'a>(
        warehouse_id: WarehouseId,
        table: &TableIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<TableId>> {
        Ok(
            Self::resolve_table_ident(warehouse_id, table, list_flags, transaction)
                .await?
                .map(|t| t.table_id),
        )
    }

    async fn table_idents_to_ids(
        warehouse_id: WarehouseId,
        tables: HashSet<&TableIdent>,
        list_flags: ListFlags,
        catalog_state: Self::State,
    ) -> Result<HashMap<TableIdent, Option<TableId>>>;

    /// Load tables by table id.
    /// Does not return staged tables.
    /// If a table does not exist, do not include it in the response.
    async fn load_tables<'a>(
        warehouse_id: WarehouseId,
        tables: impl IntoIterator<Item = TableId> + Send,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<HashMap<TableId, LoadTableResponse>>;

    /// Get table metadata by table id.
    /// If include_staged is true, also return staged tables,
    /// i.e. tables with no metadata file yet.
    /// Return Ok(None) if the table does not exist.
    async fn get_table_metadata_by_id(
        warehouse_id: WarehouseId,
        table: TableId,
        list_flags: ListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<GetTableMetadataResponse>>;

    /// Get table metadata by location.
    /// Return Ok(None) if the table does not exist.
    async fn get_table_metadata_by_s3_location(
        warehouse_id: WarehouseId,
        location: &Location,
        list_flags: ListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<GetTableMetadataResponse>>;

    /// Rename a table. Tables may be moved across namespaces.
    async fn rename_table<'a>(
        warehouse_id: WarehouseId,
        source_id: TableId,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Drop a table.
    /// Should drop staged and non-staged tables.
    ///
    /// Consider in your implementation to implement an UNDROP feature.
    ///
    /// Returns the table location
    async fn drop_table<'a>(
        warehouse_id: WarehouseId,
        table_id: TableId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String>;

    /// Undrop a table.
    ///
    /// Undrops a soft-deleted table. Does not work if the table was hard-deleted.
    /// Returns the task id of the expiration task associated with the soft-deletion.
    async fn clear_tabular_deleted_at(
        table_id: &[TableId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<UndropTabularResponse>>;

    async fn mark_tabular_as_deleted(
        warehouse_id: WarehouseId,
        table_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    /// Commit changes to a table.
    /// The table might be staged or not.
    async fn commit_table_transaction<'a>(
        warehouse_id: WarehouseId,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    // ---------------- Role Management API ----------------
    async fn create_role<'a>(
        role_id: RoleId,
        project_id: &ProjectId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Role>;

    /// Return Ok(None) if the role does not exist.
    async fn update_role<'a>(
        role_id: RoleId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<Role>>;

    async fn list_roles<'a>(
        filter_project_id: Option<ProjectId>,
        filter_role_id: Option<Vec<RoleId>>,
        filter_name: Option<String>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse>;

    /// Return Ok(None) if the role does not exist.
    async fn delete_role<'a>(
        role_id: RoleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<()>>;

    async fn search_role(
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse>;

    // ---------------- User Management API ----------------
    async fn create_or_update_user<'a>(
        user_id: &UserId,
        name: &str,
        // If None, set the email to None.
        email: Option<&str>,
        last_updated_with: UserLastUpdatedWith,
        user_type: UserType,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CreateOrUpdateUserResponse>;

    async fn search_user(
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchUserResponse>;

    /// Return Ok(vec[]) if the user does not exist.
    async fn list_user(
        filter_user_id: Option<Vec<UserId>>,
        filter_name: Option<String>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListUsersResponse>;

    async fn delete_user<'a>(
        user_id: UserId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<()>>;

    // ---------------- Warehouse Management API ----------------

    /// Create a warehouse.
    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: &ProjectId,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<WarehouseId>;

    /// Create a project
    async fn create_project<'a>(
        project_id: &ProjectId,
        project_name: String,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Delete a project
    async fn delete_project<'a>(
        project_id: &ProjectId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Get the project metadata
    async fn get_project<'a>(
        project_id: &ProjectId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<GetProjectResponse>>;

    /// Return a list of all project ids in the catalog
    ///
    /// If project_ids is None, return all projects, otherwise return only the projects in the set
    async fn list_projects(
        project_ids: Option<HashSet<ProjectId>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<GetProjectResponse>>;

    /// Get endpoint statistics for the project
    ///
    /// We'll return statistics for the time-frame end - interval until end.
    /// If status_codes is None, return all status codes.
    async fn get_endpoint_statistics(
        project_id: ProjectId,
        warehouse_id: WarehouseFilter,
        range_specifier: TimeWindowSelector,
        status_codes: Option<&[u16]>,
        catalog_state: Self::State,
    ) -> Result<EndpointStatisticsResponse>;

    /// Return a list of all warehouse in a project
    async fn list_warehouses(
        project_id: &ProjectId,
        // If None, return only active warehouses
        // If Some, return only warehouses with any of the statuses in the set
        include_inactive: Option<Vec<WarehouseStatus>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<GetWarehouseResponse>>;

    /// Get the warehouse metadata - should only return active warehouses.
    ///
    /// Return Ok(None) if the warehouse does not exist.
    async fn get_warehouse<'a>(
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<GetWarehouseResponse>>;

    /// Wrapper around get_warehouse that returns a not-found error if the warehouse does not exist.
    async fn require_warehouse<'a>(
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<GetWarehouseResponse> {
        Self::get_warehouse(warehouse_id, transaction).await?.ok_or(
            ErrorModel::not_found(
                format!("Warehouse {warehouse_id} not found"),
                "WarehouseNotFound",
                None,
            )
            .into(),
        )
    }

    async fn get_warehouse_stats(
        warehouse_id: WarehouseId,
        pagination_query: PaginationQuery,
        state: Self::State,
    ) -> Result<WarehouseStatisticsResponse>;

    /// Delete a warehouse.
    async fn delete_warehouse<'a>(
        warehouse_id: WarehouseId,
        query: DeleteWarehouseQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Rename a warehouse.
    async fn rename_warehouse<'a>(
        warehouse_id: WarehouseId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Set warehouse deletion profile
    async fn set_warehouse_deletion_profile<'a>(
        warehouse_id: WarehouseId,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Rename a project.
    async fn rename_project<'a>(
        project_id: &ProjectId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Set the status of a warehouse.
    async fn set_warehouse_status<'a>(
        warehouse_id: WarehouseId,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    async fn update_storage_profile<'a>(
        warehouse_id: WarehouseId,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    /// Return Err only on unexpected errors, not if the table does not exist.
    /// If include_staged is true, also return staged tables.
    /// If the table does not exist, return Ok(None).
    ///
    /// We use this function also to handle the `view_exists` endpoint.
    /// Also return Ok(None) if the warehouse is not active.
    async fn view_to_id<'a>(
        warehouse_id: WarehouseId,
        view: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<ViewId>>;

    async fn create_view<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        view: &TableIdent,
        request: ViewMetadata,
        metadata_location: &Location,
        location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    async fn load_view<'a>(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<ViewMetadataWithLocation>;

    async fn list_views<'a>(
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<ViewId, TableInfo>>;

    async fn update_view_metadata(
        commit: ViewCommit<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    /// Returns location of the dropped view.
    /// Used for cleanup
    async fn drop_view<'a>(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String>;

    async fn rename_view(
        warehouse_id: WarehouseId,
        source_id: ViewId,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn list_tabulars(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>, // Filter by namespace
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TabularId, TabularInfo>>;

    async fn load_storage_profile(
        warehouse_id: WarehouseId,
        tabular_id: TableId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<(Option<SecretIdent>, StorageProfile)>;

    async fn set_tabular_protected(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    async fn get_tabular_protected(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    async fn set_namespace_protected(
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    async fn get_namespace_protected(
        namespace_id: NamespaceId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    async fn set_warehouse_protected(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    // ------------- Tasks -------------

    async fn pick_new_task_impl(
        queue_name: &TaskQueueName,
        default_max_time_since_last_heartbeat: chrono::Duration,
        state: Self::State,
    ) -> Result<Option<Task>>;

    /// `default_max_time_since_last_heartbeat` is only used if no task configuration is found
    /// in the DB for the given `queue_name`, typically before a user has configured the value explicitly.
    #[tracing::instrument(
        name = "catalog_pick_new_task",
        skip(state, default_max_time_since_last_heartbeat)
    )]
    async fn pick_new_task(
        queue_name: &TaskQueueName,
        default_max_time_since_last_heartbeat: chrono::Duration,
        state: Self::State,
    ) -> Result<Option<Task>> {
        Self::pick_new_task_impl(queue_name, default_max_time_since_last_heartbeat, state).await
    }

    /// Resolve tasks among all known active and historical tasks.
    /// Returns a map of task_id to (TaskEntity, queue_name).
    /// If `warehouse_id` is `Some`, only resolve tasks for that warehouse.
    async fn resolve_tasks_impl(
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, (TaskEntity, TaskQueueName)>>;

    /// Resolve tasks among all known active and historical tasks.
    /// Returns a map of task_id to (TaskEntity, queue_name).
    /// If a task does not exist, it is not included in the map.
    async fn resolve_tasks(
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, (TaskEntity, TaskQueueName)>> {
        if task_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut cached_results = HashMap::new();
        for id in task_ids {
            if let Some(cached_value) = TASKS_CACHE.get(id).await {
                if let Some(w) = warehouse_id {
                    match &cached_value.0 {
                        TaskEntity::Table {
                            warehouse_id: wid, ..
                        } if *wid != w => continue,
                        TaskEntity::Table { .. } => (),
                    }
                }
                cached_results.insert(*id, cached_value);
            }
        }
        let not_cached_ids: Vec<TaskId> = task_ids
            .iter()
            .copied()
            .filter(|id| !cached_results.contains_key(id))
            .collect();
        if not_cached_ids.is_empty() {
            return Ok(cached_results);
        }
        let resolve_uncached_result =
            Self::resolve_tasks_impl(warehouse_id, &not_cached_ids, state).await?;
        for (id, value) in resolve_uncached_result {
            cached_results.insert(id, value.clone());
            TASKS_CACHE.insert(id, value).await;
        }
        Ok(cached_results)
    }

    async fn resolve_required_tasks(
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, (TaskEntity, TaskQueueName)>> {
        let tasks = Self::resolve_tasks(warehouse_id, task_ids, state).await?;

        for task_id in task_ids {
            if !tasks.contains_key(task_id) {
                return Err(ErrorModel::not_found(
                    format!("Task with id `{task_id}` not found"),
                    "TaskNotFound",
                    None,
                )
                .into());
            }
        }

        Ok(tasks)
    }

    async fn record_task_success_impl(
        id: TaskAttemptId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn record_task_success(
        id: TaskAttemptId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::record_task_success_impl(id, message, transaction).await
    }

    async fn record_task_failure_impl(
        id: TaskAttemptId,
        error_details: &str,
        max_retries: i32, // Max retries from task config, used to determine if we should mark the task as failed or retry
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn record_task_failure(
        id: TaskAttemptId,
        error_details: &str,
        max_retries: i32,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::record_task_failure_impl(id, error_details, max_retries, transaction).await
    }

    /// Get task details by task id.
    /// Return Ok(None) if the task does not exist.
    async fn get_task_details_impl(
        warehouse_id: WarehouseId,
        task_id: TaskId,
        num_attempts: u16, // Number of attempts to retrieve in the task details
        state: Self::State,
    ) -> Result<Option<GetTaskDetailsResponse>>;

    /// Get task details by task id.
    /// Return Ok(None) if the task does not exist.
    async fn get_task_details(
        warehouse_id: WarehouseId,
        task_id: TaskId,
        num_attempts: u16,
        state: Self::State,
    ) -> Result<Option<GetTaskDetailsResponse>> {
        Self::get_task_details_impl(warehouse_id, task_id, num_attempts, state).await
    }

    /// List tasks
    async fn list_tasks_impl(
        warehouse_id: WarehouseId,
        query: ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ListTasksResponse>;

    /// List tasks
    async fn list_tasks(
        warehouse_id: WarehouseId,
        query: ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ListTasksResponse> {
        Self::list_tasks_impl(warehouse_id, query, transaction).await
    }

    /// Enqueue a batch of tasks to a task queue.
    ///
    /// There can only be a single task running or pending for a (entity_id, queue_name) tuple.
    /// Any resubmitted pending/running task will be omitted from the returned task ids.
    ///
    /// CAUTION: `tasks` may be longer than the returned `Vec<TaskId>`.
    async fn enqueue_tasks(
        queue_name: &'static TaskQueueName,
        tasks: Vec<TaskInput>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>>;

    /// Enqueue a single task to a task queue.
    ///
    /// There can only be a single active task for a (entity_id, queue_name) tuple.
    /// Resubmitting a pending/running task will return a `None` instead of a new `TaskId`
    async fn enqueue_task(
        queue_name: &'static TaskQueueName,
        task: TaskInput,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<TaskId>> {
        Ok(Self::enqueue_tasks(queue_name, vec![task], transaction)
            .await
            .map(|v| v.into_iter().next())?)
    }

    /// Cancel scheduled tasks matching the filter.
    ///
    /// If `cancel_running_and_should_stop` is true, also cancel tasks in the `running` and `should-stop` states.
    /// If `queue_name` is `None`, cancel tasks in all queues.
    async fn cancel_scheduled_tasks(
        queue_name: Option<&TaskQueueName>,
        filter: TaskFilter,
        cancel_running_and_should_stop: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    /// Report progress and heartbeat the task. Also checks whether the task should continue to run.
    async fn check_and_heartbeat_task(
        id: TaskAttemptId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        progress: f32,
        execution_details: Option<serde_json::Value>,
    ) -> Result<TaskCheckState> {
        Self::check_and_heartbeat_task_impl(id, transaction, progress, execution_details).await
    }

    /// Report progress and heartbeat the task. Also checks whether the task should continue to run.
    async fn check_and_heartbeat_task_impl(
        id: TaskAttemptId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        progress: f32,
        execution_details: Option<serde_json::Value>,
    ) -> Result<TaskCheckState>;

    /// Sends stop signals to the tasks.
    /// Only affects tasks in the `running` state.
    ///
    /// It is up to the task handler to decide if it can stop.
    async fn stop_tasks(
        task_ids: &[TaskId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    /// Reschedule tasks to run at a specific time by setting `scheduled_for` to the provided timestamp.
    /// If no `scheduled_for` is `None`, the tasks will be scheduled to run immediately.
    /// Only affects tasks in the `Scheduled` or `Stopping` state.
    async fn run_tasks_at(
        task_ids: &[TaskId],
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::run_tasks_at_impl(task_ids, scheduled_for, transaction).await
    }

    /// Reschedule tasks to run at a specific time by setting `scheduled_for` to the provided timestamp.
    /// If no `scheduled_for` is `None`, the tasks will be scheduled to run immediately.
    /// Only affects tasks in the `Scheduled` or `Stopping` state.
    async fn run_tasks_at_impl(
        task_ids: &[TaskId],
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn set_task_queue_config(
        warehouse_id: WarehouseId,
        queue_name: &TaskQueueName,
        config: SetTaskQueueConfigRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn get_task_queue_config(
        warehouse_id: WarehouseId,
        queue_name: &TaskQueueName,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<GetTaskQueueConfigResponse>>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ListFlags {
    pub include_active: bool,
    pub include_staged: bool,
    pub include_deleted: bool,
}

impl Default for ListFlags {
    fn default() -> Self {
        Self {
            include_active: true,
            include_staged: false,
            include_deleted: false,
        }
    }
}

impl ListFlags {
    #[must_use]
    pub fn all() -> Self {
        Self {
            include_staged: true,
            include_deleted: true,
            include_active: true,
        }
    }

    #[must_use]
    pub fn only_deleted() -> Self {
        Self {
            include_staged: false,
            include_deleted: true,
            include_active: false,
        }
    }
}

#[derive(Clone, Default, Debug, Copy, PartialEq, Eq)]
pub struct DropFlags {
    pub hard_delete: bool,
    pub purge: bool,
}

impl DropFlags {
    #[must_use]
    pub fn purge(mut self) -> Self {
        self.purge = true;
        self
    }

    #[must_use]
    pub fn hard_delete(mut self) -> Self {
        self.hard_delete = true;
        self
    }
}

#[derive(Debug, Clone)]
pub struct ViewMetadataWithLocation {
    pub metadata_location: String,
    pub metadata: ViewMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeletionDetails {
    pub expiration_task_id: uuid::Uuid,
    pub expiration_date: chrono::DateTime<chrono::Utc>,
    pub deleted_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
