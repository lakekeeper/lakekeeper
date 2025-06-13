use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use iceberg::{
    spec::{TableMetadata, ViewMetadata},
    TableUpdate,
};
pub use iceberg_ext::catalog::rest::{CommitTableResponse, CreateTableRequest};
use iceberg_ext::{
    catalog::rest::{CatalogConfig, ErrorModel},
    configs::Location,
};

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
            tabular_expiration_queue, tabular_expiration_queue::TabularExpirationPayload,
            tabular_purge_queue, tabular_purge_queue::TabularPurgePayload, Status, Task,
            TaskCheckState, TaskFilter, TaskId, TaskInput, TaskMetadata,
        },
    },
    SecretIdent,
};

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
    pub(crate) diffs: TableMetadataDiffs,
}

#[derive(Debug, Clone)]
pub struct ViewCommit<'a> {
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
    pub(crate) namespace_id: NamespaceId,
    pub(crate) table_ident: &'c TableIdent,
    pub(crate) metadata_location: Option<&'c Location>,
    pub(crate) table_metadata: TableMetadata,
}

#[derive(Debug, Clone)]
pub enum CreateOrUpdateUserResponse {
    Created(User),
    Updated(User),
}

#[derive(Debug, Clone)]
pub struct UndropTabularResponse {
    pub table_ident: TableId,
    pub task_id: TaskId,
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
    },
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
    Self: Clone + Send + Sync + 'static,
{
    type Transaction: Transaction<Self::State>;
    type State: Clone + Send + Sync + 'static + HealthExt;

    /// Get data required for startup validations and server info endpoint
    async fn get_server_info(
        catalog_state: Self::State,
    ) -> std::result::Result<ServerInfo, ErrorModel>;

    /// Bootstrap the catalog.
    /// Use this hook to store the current `CONFIG.server_id`.
    /// Must not update anything if the catalog is already bootstrapped.
    /// If bootstrapped succeeded, return Ok(true).
    /// If the catalog is already bootstrapped, return Ok(false).
    async fn bootstrap<'a>(
        terms_accepted: bool,
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
    async fn table_to_id<'a>(
        warehouse_id: WarehouseId,
        table: &TableIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<TableId>>;

    /// Same as `table_ident_to_id`, but for multiple tables.
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
        table_id: TableId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String>;

    /// Undrop a table.
    ///
    /// Undrops a soft-deleted table. Does not work if the table was hard-deleted.
    /// Returns the task id of the expiration task associated with the soft-deletion.
    async fn undrop_tabulars(
        table_id: &[TableId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<UndropTabularResponse>>;

    async fn mark_tabular_as_deleted(
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
        namespace_id: NamespaceId,
        view: &TableIdent,
        request: ViewMetadata,
        metadata_location: &Location,
        location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    async fn load_view<'a>(
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

    async fn resolve_table_ident(
        warehouse_id: WarehouseId,
        table: &TableIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<TabularDetails>>;

    async fn set_tabular_protected(
        tabular_id: TabularId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    async fn get_tabular_protected(
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

    // Tasks
    async fn pick_new_task(
        queue_name: &str,
        max_time_since_last_heartbeat: chrono::Duration,
        state: Self::State,
    ) -> Result<Option<Task>>;
    async fn record_task_success(
        id: TaskId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;
    async fn record_task_failure(
        id: TaskId,
        error_details: &str,
        max_retries: i32,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn retrying_record_task_success(
        task_id: TaskId,
        details: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) {
        Self::retrying_record_success_or_failure(task_id, Status::Success(details), transaction)
            .await;
    }

    async fn retrying_record_task_failure(
        task: TaskId,
        details: &str,
        max_retries: i32,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) {
        Self::retrying_record_success_or_failure(
            task,
            Status::Failure(details, max_retries),
            transaction,
        )
        .await;
    }

    async fn retrying_record_success_or_failure(
        task_id: TaskId,
        result: Status<'_>,
        mut transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) {
        let mut retry = 0;
        while let Err(e) = match result {
            Status::Success(details) => {
                Self::record_task_success(task_id, details, &mut transaction).await
            }
            Status::Failure(details, max_retries) => {
                Self::record_task_failure(task_id, details, max_retries, &mut transaction).await
            }
        } {
            tracing::error!("Failed to record {}: {:?}", result, e);
            tokio::time::sleep(Duration::from_secs(1 + retry)).await;
            retry += 1;
            if retry > 5 {
                tracing::error!("Giving up trying to record {}.", result);
                break;
            }
        }
    }
    /// Enqueue a batch of tasks to a task queue.
    ///
    /// There can only be a single task running or pending for a (entity_id, queue_name) tuple.
    /// Any resubmitted pending/running task will be omitted from the returned task ids.
    ///
    /// CAUTION: `tasks` may be longer than the returned `Vec<TaskId>`.
    async fn enqueue_task_batch(
        queue_name: &'static str,
        tasks: Vec<TaskInput>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>>;

    /// Enqueue a single task to a task queue.
    ///
    /// There can only be a single task running or pending for a (entity_id, queue_name) tuple.
    /// Resubmitting a pending/running task will return a `None` instead of a new `TaskId`
    async fn enqueue_task(
        queue_name: &'static str,
        task: TaskInput,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<TaskId>> {
        Ok(
            Self::enqueue_task_batch(queue_name, vec![task], transaction)
                .await
                .map(|v| v.into_iter().next())?,
        )
    }
    async fn cancel_pending_tasks(
        queue_name: &str,
        filter: TaskFilter,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    #[tracing::instrument(skip(transaction))]
    async fn queue_tabular_expiration(
        task_metadata: TaskMetadata,
        payload: TabularExpirationPayload,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<TaskId>> {
        Self::enqueue_task(
            tabular_expiration_queue::QUEUE_NAME,
            TaskInput {
                task_metadata,
                payload: serde_json::to_value(&payload).map_err(|e| {
                    ErrorModel::internal(
                        format!("Failed to serialize task payload: {e}"),
                        "TaskPayloadSerializationError",
                        Some(Box::new(e)),
                    )
                })?,
            },
            transaction,
        )
        .await
    }

    #[tracing::instrument(skip(transaction))]
    async fn cancel_tabular_expiration(
        filter: TaskFilter,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Self::cancel_pending_tasks(
            tabular_expiration_queue::QUEUE_NAME,
            filter,
            false,
            transaction,
        )
        .await
    }

    #[tracing::instrument(skip(transaction))]
    async fn queue_tabular_purge(
        task_metadata: TaskMetadata,
        task: TabularPurgePayload,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<TaskId>> {
        Self::enqueue_task(
            tabular_purge_queue::QUEUE_NAME,
            TaskInput {
                task_metadata,
                payload: serde_json::to_value(&task).map_err(|e| {
                    ErrorModel::internal(
                        format!("Failed to serialize task payload: {e}"),
                        "TaskPayloadSerializationError",
                        Some(Box::new(e)),
                    )
                })?,
            },
            transaction,
        )
        .await
    }

    /// Checks task state and sends a hearbeat.
    ///
    /// This is used to send a heartbeat and check whether this task should continue to run.
    async fn check_and_heartbeat_task(
        task_id: TaskId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<TaskCheckState>>;

    /// Sends a stop signal to the task.
    ///
    /// This does by no means guarantee that the task will be actually stop. It is up to the task
    /// handler to decide if it can stop.
    async fn stop_task(
        task_id: TaskId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn set_task_queue_config(
        warehouse_id: WarehouseId,
        queue_name: &str,
        config: SetTaskQueueConfigRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn get_task_queue_config(
        warehouse_id: WarehouseId,
        queue_name: &str,
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
