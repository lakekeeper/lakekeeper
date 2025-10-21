use std::collections::{HashMap, HashSet};

use iceberg::spec::ViewMetadata;
use iceberg_ext::catalog::rest::ErrorModel;
pub use iceberg_ext::catalog::rest::{CommitTableResponse, CreateTableRequest};
use lakekeeper_io::Location;

use super::{
    storage::StorageProfile, NamespaceId, ProjectId, RoleId, TableId, ViewId, WarehouseId,
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
            tasks::{GetTaskDetailsResponse, ListTasksRequest, ListTasksResponse},
            user::{ListUsersResponse, SearchUserResponse, UserLastUpdatedWith, UserType},
            warehouse::{
                GetTaskQueueConfigResponse, SetTaskQueueConfigRequest, TabularDeleteProfile,
                WarehouseStatisticsResponse,
            },
            DeleteWarehouseQuery, ProtectionResponse, TabularType,
        },
    },
    service::{
        authn::UserId,
        health::HealthExt,
        tasks::{
            Task, TaskAttemptId, TaskCheckState, TaskFilter, TaskId, TaskInput, TaskQueueName,
        },
        TabularId, TabularIdentBorrowed,
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
mod error;
pub use error::*;
mod view;
pub use view::*;
mod table;
pub use table::*;

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
    async fn create_warehouse_impl<'a>(
        warehouse_name: String,
        project_id: &ProjectId,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<WarehouseId, CatalogCreateWarehouseError>;

    /// Delete a warehouse.
    async fn delete_warehouse_impl<'a>(
        warehouse_id: WarehouseId,
        query: DeleteWarehouseQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CatalogDeleteWarehouseError>;

    /// Rename a warehouse.
    async fn rename_warehouse_impl<'a>(
        warehouse_id: WarehouseId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CatalogRenameWarehouseError>;

    /// Return a list of all warehouse in a project
    async fn list_warehouses_impl(
        project_id: &ProjectId,
        // If None, return only active warehouses
        // If Some, return only warehouses with any of the statuses in the set
        status_filter: Option<Vec<WarehouseStatus>>,
        state: Self::State,
    ) -> std::result::Result<Vec<GetWarehouseResponse>, CatalogListWarehousesError>;

    /// Get the warehouse metadata. Return only active warehouses.
    ///
    /// Return Ok(None) if the warehouse does not exist or is not active.
    async fn get_warehouse_by_id_impl<'a>(
        warehouse_id: WarehouseId,
        state: Self::State,
    ) -> std::result::Result<Option<GetWarehouseResponse>, CatalogGetWarehouseByIdError>;

    /// Get the warehouse metadata. Return only active warehouses.
    ///
    /// Return Ok(None) if the warehouse does not exist or is not active.
    async fn get_warehouse_by_name_impl(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> Result<Option<GetWarehouseResponse>, CatalogGetWarehouseByNameError>;

    async fn get_warehouse_stats(
        warehouse_id: WarehouseId,
        pagination_query: PaginationQuery,
        state: Self::State,
    ) -> Result<WarehouseStatisticsResponse>;

    /// Set warehouse deletion profile
    async fn set_warehouse_deletion_profile_impl<'a>(
        warehouse_id: WarehouseId,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), SetWarehouseDeletionProfileError>;

    /// Set the status of a warehouse.
    async fn set_warehouse_status_impl<'a>(
        warehouse_id: WarehouseId,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), SetWarehouseStatusError>;

    async fn update_storage_profile_impl<'a>(
        warehouse_id: WarehouseId,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), UpdateWarehouseStorageProfileError>;

    async fn set_warehouse_protected_impl(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ProtectionResponse, SetWarehouseProtectedError>;

    // ---------------- Namespace Management ----------------
    // Should only return namespaces if the warehouse is active.
    async fn list_namespaces_impl<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<PaginatedMapping<NamespaceId, Namespace>, CatalogListNamespaceError>;

    async fn create_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Namespace, CatalogCreateNamespaceError>;

    // Should only return a namespace if the warehouse is active.
    async fn get_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace: NamespaceIdentOrId,
        state: Self::State,
    ) -> std::result::Result<Option<Namespace>, CatalogGetNamespaceError>;

    async fn drop_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        flags: NamespaceDropFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<NamespaceDropInfo, CatalogNamespaceDropError>;

    /// Update the properties of a namespace.
    ///
    /// The properties are the final key-value properties that should
    /// be persisted as-is in the catalog.
    async fn update_namespace_properties_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Namespace, CatalogUpdateNamespacePropertiesError>;

    async fn set_namespace_protected_impl(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Namespace, CatalogSetNamespaceProtectedError>;

    // ---------------- Tabular Management ----------------
    async fn list_tabulars_impl(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>, // Filter by namespace
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        typ: Option<TabularType>, // Optional type filter
        pagination_query: PaginationQuery,
    ) -> std::result::Result<PaginatedMapping<TabularId, ViewOrTableDeletionInfo>, ListTabularsError>;

    async fn search_tabular_impl(
        warehouse_id: WarehouseId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> std::result::Result<CatalogSearchTabularResponse, SearchTabularError>;

    async fn set_tabular_protected_impl(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, SetTabularProtectionError>;

    async fn get_tabular_infos_by_ident_impl(
        warehouse_id: WarehouseId,
        tabulars: &[TabularIdentBorrowed<'_>],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Vec<ViewOrTableInfo>, GetTabularInfoError>;

    async fn get_tabular_infos_by_id_impl(
        warehouse_id: WarehouseId,
        tabulars: &[TabularId],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Vec<ViewOrTableInfo>, GetTabularInfoError>;

    async fn get_tabular_infos_by_s3_location_impl(
        warehouse_id: WarehouseId,
        location: &Location,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Option<ViewOrTableInfo>, GetTabularInfoByLocationError>;

    async fn rename_tabular_impl(
        warehouse_id: WarehouseId,
        source_id: TabularId,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, RenameTabularError>;

    /// Undrop a table or view.
    ///
    /// Undrops a soft-deleted table. Does not work if the table was hard-deleted.
    /// Returns the task id of the expiration task associated with the soft-deletion.
    async fn clear_tabular_deleted_at_impl(
        tabular_id: &[TabularId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError>;

    async fn mark_tabular_as_deleted_impl(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, MarkTabularAsDeletedError>;

    /// Drops staged and non-staged tables and views.
    ///
    /// Returns the table location
    async fn drop_tabular_impl<'a>(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Location, DropTabularError>;

    // ---------------- Table Management ----------------
    async fn create_table_impl<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(TableInfo, Option<StagedTableId>), CreateTableError>;

    /// Load tables by table id.
    /// Does not return staged tables.
    /// If a table does not exist, it is not included in the response.
    async fn load_tables_impl<'a>(
        warehouse_id: WarehouseId,
        tables: impl IntoIterator<Item = TableId> + Send,
        include_deleted: bool,
        filters: &LoadTableFilters,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<LoadTableResponse>, LoadTableError>;

    /// Commit changes to a table.
    /// The table might be staged or not.
    async fn commit_table_transaction_impl<'a>(
        warehouse_id: WarehouseId,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<TableInfo>, CommitTableTransactionError>;

    // ---------------- View Management ----------------
    async fn create_view_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        view_ident: &TableIdent,
        request: &ViewMetadata,
        metadata_location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ViewInfo, CreateViewError>;

    async fn load_view_impl<'a>(
        warehouse_id: WarehouseId,
        view: ViewId,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<CatalogView, LoadViewError>;

    async fn commit_view_impl<'a>(
        commit: ViewCommit<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ViewInfo, CommitViewError>;

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

    async fn resolve_tasks_impl(
        warehouse_id: WarehouseId,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<Vec<ResolvedTask>>;

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
