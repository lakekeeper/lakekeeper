use std::collections::{HashMap, HashSet};

use iceberg::spec::ViewMetadata;
use iceberg_ext::catalog::rest::{CatalogConfig, ErrorModel};
pub use iceberg_ext::catalog::rest::{CommitTableResponse, CreateTableRequest};
use lakekeeper_io::Location;

use super::{
    storage::StorageProfile, NamespaceId, ProjectId, RoleId, TableId, TabularDetails, ViewId,
    WarehouseId,
};
pub use crate::api::iceberg::v1::{
    CreateNamespaceRequest, CreateNamespaceResponse, ListNamespacesQuery, NamespaceIdent, Result,
    TableIdent, UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use crate::{
    api::{
        iceberg::v1::{
            namespace::NamespaceDropFlags, tables::LoadTableFilters, PaginatedMapping,
            PaginationQuery,
        },
        management::v1::{
            project::{EndpointStatisticsResponse, TimeWindowSelector, WarehouseFilter},
            role::{ListRolesResponse, Role, SearchRoleResponse},
            tabular::SearchTabularResponse,
            tasks::{GetTaskDetailsResponse, ListTasksRequest, ListTasksResponse},
            user::{ListUsersResponse, SearchUserResponse, UserLastUpdatedWith, UserType},
            warehouse::{
                GetTaskQueueConfigResponse, SetTaskQueueConfigRequest, TabularDeleteProfile,
                WarehouseStatisticsResponse,
            },
            DeleteWarehouseQuery, ProtectionResponse,
        },
    },
    request_metadata::RequestMetadata,
    service::{
        authn::UserId,
        health::HealthExt,
        tasks::{
            Task, TaskAttemptId, TaskCheckState, TaskEntity, TaskFilter, TaskId, TaskInput,
            TaskQueueName,
        },
        TabularId,
    },
    SecretIdent,
};
mod namespace;
pub use namespace::*;
mod tabular;
pub use tabular::*;
mod warehouse;
pub use warehouse::*;
mod project;
pub use project::*;
mod server;
pub use server::*;
mod user;
pub use user::*;
mod tasks;
pub use tasks::*;

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

#[async_trait::async_trait]
pub trait CatalogStore
where
    Self: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    type Transaction: Transaction<Self::State>;
    type State: Clone + std::fmt::Debug + Send + Sync + 'static + HealthExt;

    // ---------------- Server Management ----------------
    /// Get data required for startup validations and server info endpoint
    async fn get_server_info(catalog_state: Self::State) -> Result<ServerInfo, ErrorModel>;

    /// Bootstrap the catalog.
    /// Must return Ok(false) if the catalog is not open for bootstrap.
    /// If bootstrapping succeeds, return Ok(true).
    async fn bootstrap<'a>(
        terms_accepted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<bool>;

    // ---------------- Project Management ----------------
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
    /// If `project_ids` is None, return all projects, otherwise return only the projects in the set
    async fn list_projects(
        project_ids: Option<HashSet<ProjectId>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<GetProjectResponse>>;

    /// Rename a project.
    async fn rename_project<'a>(
        project_id: &ProjectId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()>;

    // ---------------- Warehouse Management ----------------
    /// Create a warehouse.
    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: &ProjectId,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<WarehouseId>;

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

    /// Wrapper around `get_warehouse` that returns a not-found error if the warehouse does not exist.
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

    // Should only return a warehouse if the warehouse is active.
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> Result<Option<WarehouseId>>;

    /// Wrapper around `get_warehouse_by_name` that returns
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

    /// Wrapper around `get_config_for_warehouse` that returns
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

    async fn set_warehouse_protected(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    async fn load_storage_profile(
        warehouse_id: WarehouseId,
        tabular_id: TableId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<(Option<SecretIdent>, StorageProfile)>;

    // ---------------- Namespace Management ----------------
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

    async fn set_namespace_protected(
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    async fn get_namespace_protected(
        namespace_id: NamespaceId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse>;

    // ---------------- Tabular Management ----------------
    async fn list_tabulars(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>, // Filter by namespace
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TabularId, TabularInfo>>;

    async fn search_tabular(
        warehouse_id: WarehouseId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchTabularResponse>;

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

    // ---------------- Table Management ----------------
    async fn create_table<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CreateTableResponse>;

    async fn list_tables<'a>(
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TableId, TableInfo>>;

    /// Return Err only on unexpected errors, not if the table does not exist.
    /// If `include_staged` is true, also return staged tables.
    /// If the table does not exist, return Ok(None).
    ///
    /// We use this function also to handle the `table_exists` endpoint.
    /// Also return Ok(None) if the warehouse is not active.
    async fn resolve_table_ident<'a>(
        warehouse_id: WarehouseId,
        table: &TableIdent,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<TabularDetails>>;

    async fn table_to_id<'a>(
        warehouse_id: WarehouseId,
        table: &TableIdent,
        list_flags: TabularListFlags,
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
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<HashMap<TableIdent, Option<TableId>>>;

    /// Load tables by table id.
    /// Does not return staged tables.
    /// If a table does not exist, do not include it in the response.
    async fn load_tables<'a>(
        warehouse_id: WarehouseId,
        tables: impl IntoIterator<Item = TableId> + Send,
        include_deleted: bool,
        filters: &LoadTableFilters,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<HashMap<TableId, LoadTableResponse>>;

    /// Get table metadata by table id.
    /// If `include_staged` is true, also return staged tables,
    /// i.e. tables with no metadata file yet.
    /// Return Ok(None) if the table does not exist.
    async fn get_table_metadata_by_id(
        warehouse_id: WarehouseId,
        table: TableId,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<GetTableMetadataResponse>>;

    /// Get table metadata by location.
    /// Return Ok(None) if the table does not exist.
    async fn get_table_metadata_by_s3_location(
        warehouse_id: WarehouseId,
        location: &Location,
        list_flags: TabularListFlags,
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

    /// Undrop a table or view.
    ///
    /// Undrops a soft-deleted table. Does not work if the table was hard-deleted.
    /// Returns the task id of the expiration task associated with the soft-deletion.
    async fn clear_tabular_deleted_at(
        tabular_id: &[TabularId],
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

    // ---------------- View Management ----------------
    /// Return Err only on unexpected errors, not if the table does not exist.
    /// If `include_staged` is true, also return staged tables.
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

    // ---------------- Endpoint Statistics ----------------
    /// Get endpoint statistics for the project
    ///
    /// We'll return statistics for the time-frame end - interval until end.
    /// If `status_codes` is None, return all status codes.
    async fn get_endpoint_statistics(
        project_id: ProjectId,
        warehouse_id: WarehouseFilter,
        range_specifier: TimeWindowSelector,
        status_codes: Option<&[u16]>,
        catalog_state: Self::State,
    ) -> Result<EndpointStatisticsResponse>;

    // ------------- Tasks -------------
    async fn pick_new_task_impl(
        queue_name: &TaskQueueName,
        default_max_time_since_last_heartbeat: chrono::Duration,
        state: Self::State,
    ) -> Result<Option<Task>>;

    /// Resolve tasks among all known active and historical tasks.
    /// Returns a map of `task_id` to `(TaskEntity, queue_name)`.
    /// If `warehouse_id` is `Some`, only resolve tasks for that warehouse.
    async fn resolve_tasks_impl(
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, (TaskEntity, TaskQueueName)>>;

    async fn record_task_success_impl(
        id: TaskAttemptId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn record_task_failure_impl(
        id: TaskAttemptId,
        error_details: &str,
        max_retries: i32, // Max retries from task config, used to determine if we should mark the task as failed or retry
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    /// Get task details by task id.
    /// Return Ok(None) if the task does not exist.
    async fn get_task_details_impl(
        warehouse_id: WarehouseId,
        task_id: TaskId,
        num_attempts: u16, // Number of attempts to retrieve in the task details
        state: Self::State,
    ) -> Result<Option<GetTaskDetailsResponse>>;

    /// List tasks
    async fn list_tasks_impl(
        warehouse_id: WarehouseId,
        query: ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ListTasksResponse>;

    /// Enqueue a batch of tasks to a task queue.
    ///
    /// There can only be a single task running or pending for a (`entity_id`, `queue_name`) tuple.
    /// Any resubmitted pending/running task will be omitted from the returned task ids.
    ///
    /// CAUTION: `tasks` may be longer than the returned `Vec<TaskId>`.
    async fn enqueue_tasks_impl(
        queue_name: &'static TaskQueueName,
        tasks: Vec<TaskInput>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>>;

    /// Cancel scheduled tasks matching the filter.
    ///
    /// If `cancel_running_and_should_stop` is true, also cancel tasks in the `running` and `should-stop` states.
    /// If `queue_name` is `None`, cancel tasks in all queues.
    async fn cancel_scheduled_tasks_impl(
        queue_name: Option<&TaskQueueName>,
        filter: TaskFilter,
        cancel_running_and_should_stop: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

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
    async fn stop_tasks_impl(
        task_ids: &[TaskId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    /// Reschedule tasks to run at a specific time by setting `scheduled_for` to the provided timestamp.
    /// If no `scheduled_for` is `None`, the tasks will be scheduled to run immediately.
    /// Only affects tasks in the `Scheduled` or `Stopping` state.
    async fn run_tasks_at_impl(
        task_ids: &[TaskId],
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn set_task_queue_config_impl(
        warehouse_id: WarehouseId,
        queue_name: &TaskQueueName,
        config: SetTaskQueueConfigRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()>;

    async fn get_task_queue_config_impl(
        warehouse_id: WarehouseId,
        queue_name: &TaskQueueName,
        state: Self::State,
    ) -> Result<Option<GetTaskQueueConfigResponse>>;
}
