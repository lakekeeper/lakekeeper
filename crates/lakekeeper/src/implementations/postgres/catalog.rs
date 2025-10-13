use std::collections::{HashMap, HashSet};

use chrono::Duration;
use iceberg::spec::ViewMetadata;
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::Location;

use super::{
    bootstrap::{bootstrap, get_validation_data},
    namespace::{
        create_namespace, drop_namespace, get_namespace, list_namespaces,
        update_namespace_properties,
    },
    role::{create_role, delete_role, list_roles, update_role},
    tabular::table::{
        drop_table, get_table_metadata_by_id, get_table_metadata_by_s3_location, list_tables,
        load_tables, rename_table, resolve_table_ident, table_idents_to_ids,
    },
    warehouse::{
        create_project, create_warehouse, delete_project, delete_warehouse, get_project,
        get_warehouse_by_id, get_warehouse_by_name, list_projects, list_warehouses, rename_project,
        rename_warehouse, set_warehouse_deletion_profile, set_warehouse_status,
        update_storage_profile,
    },
    CatalogState, PostgresTransaction,
};
use crate::{
    SecretIdent, api::{
        iceberg::v1::{
            PaginatedMapping, PaginationQuery, namespace::NamespaceDropFlags, tables::LoadTableFilters
        },
        management::v1::{
            DeleteWarehouseQuery, ProtectionResponse, project::{EndpointStatisticsResponse, TimeWindowSelector, WarehouseFilter}, role::{ListRolesResponse, Role, SearchRoleResponse}, tabular::SearchTabularResponse, tasks::{GetTaskDetailsResponse, ListTasksRequest, ListTasksResponse}, user::{ListUsersResponse, SearchUserResponse, UserLastUpdatedWith, UserType}, warehouse::{
                GetTaskQueueConfigResponse, SetTaskQueueConfigRequest, TabularDeleteProfile,
                WarehouseStatisticsResponse,
            }
        },
    }, implementations::postgres::{
        endpoint_statistics::list::list_statistics,
        namespace::set_namespace_protected,
        role::search_role,
        tabular::{
            clear_tabular_deleted_at, get_tabular_protected, list_tabulars,
            mark_tabular_as_deleted, search_tabular, set_tabular_protected,
            table::{commit_table_transaction, create_table},
            view::{create_view, drop_view, list_views, load_view, rename_view, view_ident_to_id},
        },
        tasks::{
            cancel_scheduled_tasks, check_and_heartbeat_task, get_task_details,
            get_task_queue_config, list_tasks, pick_task, queue_task_batch, record_failure,
            record_success, request_tasks_stop, reschedule_tasks_for, resolve_tasks,
            set_task_queue_config,
        },
        user::{create_or_update_user, delete_user, list_users, search_user},
        warehouse::{get_warehouse_stats, set_warehouse_protection},
    }, service::{
        CatalogCreateWarehouseError, CatalogDeleteWarehouseError, CatalogGetNamespaceError, CatalogGetWarehouseByIdError, CatalogGetWarehouseByNameError, CatalogListNamespaceError, CatalogListWarehousesError, CatalogRenameWarehouseError, CatalogStore, CreateNamespaceRequest, CreateNamespaceResponse, CreateOrUpdateUserResponse, CreateTableResponse, GetNamespaceResponse, GetProjectResponse, GetTableMetadataResponse, GetWarehouseResponse, ListNamespacesQuery, LoadTableResponse, NamespaceDropInfo, NamespaceId, NamespaceIdentOrId, ProjectId, Result, RoleId, ServerInfo, SetWarehouseDeletionProfileError, SetWarehouseProtectedError, SetWarehouseStatusError, TableCommit, TableCreation, TableId, TableIdent, TableInfo, TabularId, TabularInfo, TabularListFlags, Transaction, UndropTabularResponse, UpdateWarehouseStorageProfileError, ViewCommit, ViewId, WarehouseId, WarehouseStatus, authn::UserId, storage::StorageProfile, tasks::{
            Task, TaskAttemptId, TaskCheckState, TaskEntity, TaskFilter, TaskId, TaskInput,
            TaskQueueName,
        }
    }
};

#[async_trait::async_trait]
impl CatalogStore for super::PostgresBackend {
    type Transaction = PostgresTransaction;
    type State = CatalogState;

    async fn get_server_info(
        catalog_state: Self::State,
    ) -> std::result::Result<ServerInfo, ErrorModel> {
        get_validation_data(&catalog_state.read_pool()).await
    }

    // ---------------- Bootstrap ----------------
    async fn bootstrap<'a>(
        terms_accepted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<bool> {
        bootstrap(terms_accepted, &mut **transaction).await
    }

    async fn get_warehouse_by_name_impl(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: CatalogState,
    ) -> std::result::Result<Option<GetWarehouseResponse>, CatalogGetWarehouseByNameError> {
        get_warehouse_by_name(warehouse_name, project_id, catalog_state).await
    }

    async fn list_namespaces_impl<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<
        PaginatedMapping<NamespaceId, GetNamespaceResponse>,
        CatalogListNamespaceError,
    > {
        list_namespaces(warehouse_id, query, transaction).await
    }

    async fn create_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<CreateNamespaceResponse> {
        create_namespace(warehouse_id, namespace_id, request, transaction).await
    }

    async fn get_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace: NamespaceIdentOrId,
        state: Self::State,
    ) -> std::result::Result<GetNamespaceResponse, CatalogGetNamespaceError> {
        get_namespace(warehouse_id, namespace, &state.read_pool()).await
    }

    async fn drop_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        flags: NamespaceDropFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<NamespaceDropInfo> {
        drop_namespace(warehouse_id, namespace_id, flags, transaction).await
    }

    async fn update_namespace_properties<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        update_namespace_properties(warehouse_id, namespace_id, properties, transaction).await
    }

    async fn create_table<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<CreateTableResponse> {
        create_table(table_creation, transaction).await
    }

    async fn list_tables<'a>(
        namespace: &GetNamespaceResponse,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TableId, TableInfo>> {
        list_tables(
            namespace.warehouse_id,
            Some(namespace.namespace_id),
            list_flags,
            &mut **transaction,
            pagination_query,
        )
        .await
    }

    async fn resolve_table_ident<'a>(
        warehouse_id: WarehouseId,
        table: &TableIdent,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<crate::service::TabularDetails>> {
        resolve_table_ident(warehouse_id, table, list_flags, &mut **transaction).await
    }

    async fn table_idents_to_ids(
        warehouse_id: WarehouseId,
        tables: HashSet<&TableIdent>,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<HashMap<TableIdent, Option<TableId>>> {
        table_idents_to_ids(warehouse_id, tables, list_flags, &catalog_state.read_pool()).await
    }

    // Should also load staged tables but not tables of inactive warehouses
    async fn load_tables<'a>(
        warehouse_id: WarehouseId,
        tables: impl IntoIterator<Item = TableId> + Send,
        include_deleted: bool,
        filters: &LoadTableFilters,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<HashMap<TableId, LoadTableResponse>> {
        load_tables(warehouse_id, tables, include_deleted, filters, transaction).await
    }

    async fn get_table_metadata_by_id(
        warehouse_id: WarehouseId,
        table: TableId,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<GetTableMetadataResponse>> {
        get_table_metadata_by_id(warehouse_id, table, list_flags, catalog_state).await
    }

    async fn get_table_metadata_by_s3_location(
        warehouse_id: WarehouseId,
        location: &Location,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<GetTableMetadataResponse>> {
        get_table_metadata_by_s3_location(warehouse_id, location, list_flags, catalog_state).await
    }

    async fn rename_table<'a>(
        warehouse_id: WarehouseId,
        source_id: TableId,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        rename_table(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn drop_table<'a>(
        warehouse_id: WarehouseId,
        table_id: TableId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        drop_table(warehouse_id, table_id, force, transaction).await
    }

    async fn clear_tabular_deleted_at(
        tabular_ids: &[TabularId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<UndropTabularResponse>> {
        clear_tabular_deleted_at(tabular_ids, warehouse_id, transaction).await
    }

    async fn mark_tabular_as_deleted(
        warehouse_id: WarehouseId,
        table_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        mark_tabular_as_deleted(warehouse_id, table_id, force, None, transaction).await
    }

    async fn commit_table_transaction<'a>(
        warehouse_id: WarehouseId,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        commit_table_transaction(warehouse_id, commits, transaction).await
    }

    // ---------------- Role Management API ----------------
    async fn create_role<'a>(
        role_id: RoleId,
        project_id: &ProjectId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Role> {
        create_role(
            role_id,
            project_id,
            role_name,
            description,
            &mut **transaction,
        )
        .await
    }

    async fn update_role<'a>(
        role_id: RoleId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<Role>> {
        update_role(role_id, role_name, description, &mut **transaction).await
    }

    async fn list_roles<'a>(
        filter_project_id: Option<ProjectId>,
        filter_role_id: Option<Vec<RoleId>>,
        filter_name: Option<String>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse> {
        list_roles(
            filter_project_id,
            filter_role_id,
            filter_name,
            pagination,
            &catalog_state.read_pool(),
        )
        .await
    }

    async fn delete_role<'a>(
        role_id: RoleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<()>> {
        delete_role(role_id, &mut **transaction).await
    }

    async fn search_role(
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse> {
        search_role(search_term, &catalog_state.read_pool()).await
    }

    // ---------------- User Management API ----------------
    async fn create_or_update_user<'a>(
        user_id: &UserId,
        name: &str,
        email: Option<&str>,
        last_updated_with: UserLastUpdatedWith,
        user_type: UserType,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CreateOrUpdateUserResponse> {
        create_or_update_user(
            user_id,
            name,
            email,
            last_updated_with,
            user_type,
            &mut **transaction,
        )
        .await
    }

    async fn search_user(
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchUserResponse> {
        search_user(search_term, &catalog_state.read_pool()).await
    }

    /// Return Ok(vec[]) if the user does not exist.
    async fn list_user(
        filter_user_id: Option<Vec<UserId>>,
        filter_name: Option<String>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListUsersResponse> {
        list_users(
            filter_user_id,
            filter_name,
            pagination,
            &catalog_state.read_pool(),
        )
        .await
    }

    async fn delete_user<'a>(
        user_id: UserId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<()>> {
        delete_user(user_id, &mut **transaction).await
    }

    async fn create_warehouse_impl<'a>(
        warehouse_name: String,
        project_id: &ProjectId,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<WarehouseId, CatalogCreateWarehouseError> {
        create_warehouse(
            warehouse_name,
            project_id,
            storage_profile,
            tabular_delete_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    // ---------------- Management API ----------------
    async fn create_project<'a>(
        project_id: &ProjectId,
        project_name: String,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        create_project(project_id, project_name, transaction).await
    }

    /// Delete a project
    async fn delete_project<'a>(
        project_id: &ProjectId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        delete_project(project_id, transaction).await
    }

    /// Get the project metadata
    async fn get_project<'a>(
        project_id: &ProjectId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<GetProjectResponse>> {
        get_project(project_id, transaction).await
    }

    async fn list_projects(
        project_ids: Option<HashSet<ProjectId>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<GetProjectResponse>> {
        list_projects(project_ids, &mut **transaction).await
    }

    async fn get_endpoint_statistics(
        project_id: ProjectId,
        warehouse_id: WarehouseFilter,
        range_specifier: TimeWindowSelector,
        status_codes: Option<&[u16]>,
        catalog_state: Self::State,
    ) -> Result<EndpointStatisticsResponse> {
        list_statistics(
            project_id,
            warehouse_id,
            status_codes,
            range_specifier,
            &catalog_state.read_pool(),
        )
        .await
    }

    async fn list_warehouses_impl(
        project_id: &ProjectId,
        status_filter: Option<Vec<WarehouseStatus>>,
        catalog_state: Self::State,
    ) -> std::result::Result<Vec<GetWarehouseResponse>, CatalogListWarehousesError> {
        list_warehouses(project_id, status_filter, &catalog_state.read_pool()).await
    }

    async fn get_warehouse_by_id_impl<'a>(
        warehouse_id: WarehouseId,
        state: Self::State,
    ) -> std::result::Result<Option<GetWarehouseResponse>, CatalogGetWarehouseByIdError> {
        get_warehouse_by_id(warehouse_id, &state.read_pool()).await
    }

    async fn get_warehouse_stats(
        warehouse_id: WarehouseId,
        pagination_query: PaginationQuery,
        state: Self::State,
    ) -> Result<WarehouseStatisticsResponse> {
        get_warehouse_stats(state.read_pool(), warehouse_id, pagination_query).await
    }

    async fn delete_warehouse_impl<'a>(
        warehouse_id: WarehouseId,
        query: DeleteWarehouseQuery,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<(), CatalogDeleteWarehouseError> {
        delete_warehouse(warehouse_id, query, transaction).await
    }

    async fn rename_warehouse_impl<'a>(
        warehouse_id: WarehouseId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<(), CatalogRenameWarehouseError> {
        rename_warehouse(warehouse_id, new_name, transaction).await
    }

    async fn set_warehouse_deletion_profile_impl<'a>(
        warehouse_id: WarehouseId,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), SetWarehouseDeletionProfileError> {
        set_warehouse_deletion_profile(warehouse_id, deletion_profile, &mut **transaction).await
    }

    async fn rename_project<'a>(
        project_id: &ProjectId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        rename_project(project_id, new_name, transaction).await
    }

    async fn set_warehouse_status_impl<'a>(
        warehouse_id: WarehouseId,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<(), SetWarehouseStatusError> {
        set_warehouse_status(warehouse_id, status, transaction).await
    }

    async fn update_storage_profile_impl<'a>(
        warehouse_id: WarehouseId,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<(), UpdateWarehouseStorageProfileError> {
        update_storage_profile(
            warehouse_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn view_to_id<'a>(
        warehouse_id: WarehouseId,
        view: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<ViewId>> {
        view_ident_to_id(warehouse_id, view, false, &mut **transaction).await
    }

    async fn create_view<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        view: &TableIdent,
        request: ViewMetadata,
        metadata_location: &'_ Location,
        location: &'_ Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        create_view(
            warehouse_id,
            namespace_id,
            metadata_location,
            transaction,
            view.name.as_str(),
            request,
            location,
        )
        .await
    }

    async fn load_view<'a>(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<crate::implementations::postgres::tabular::view::ViewMetadataWithLocation> {
        load_view(warehouse_id, view_id, include_deleted, &mut *transaction).await
    }

    async fn list_views<'a>(
        namespace: &GetNamespaceResponse,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<ViewId, TableInfo>> {
        list_views(
            namespace.warehouse_id,
            Some(namespace.namespace_id),
            include_deleted,
            &mut **transaction,
            pagination_query,
        )
        .await
    }

    async fn update_view_metadata(
        ViewCommit {
            warehouse_id,
            namespace_id,
            new_metadata_location,
            previous_metadata_location,
            new_location,
            view_id,
            view_ident,
            metadata,
        }: ViewCommit<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        drop_view(
            warehouse_id,
            view_id,
            true,
            Some(previous_metadata_location),
            transaction,
        )
        .await?;
        create_view(
            warehouse_id,
            namespace_id,
            new_metadata_location,
            transaction,
            &view_ident.name,
            metadata,
            new_location,
        )
        .await
    }

    async fn drop_view<'a>(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        drop_view(warehouse_id, view_id, force, None, transaction).await
    }

    async fn rename_view(
        warehouse_id: WarehouseId,
        source_id: ViewId,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        rename_view(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn search_tabular(
        warehouse_id: WarehouseId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchTabularResponse> {
        search_tabular(warehouse_id, search_term, &catalog_state.read_pool()).await
    }

    async fn list_tabulars(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TabularId, TabularInfo>> {
        list_tabulars(
            warehouse_id,
            namespace_id,
            list_flags,
            &mut **transaction,
            None,
            pagination_query,
        )
        .await
    }
    async fn set_tabular_protected(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse> {
        set_tabular_protected(warehouse_id, tabular_id, protect, transaction).await
    }

    async fn get_tabular_protected(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse> {
        get_tabular_protected(warehouse_id, tabular_id, transaction).await
    }

    async fn set_namespace_protected(
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ProtectionResponse> {
        set_namespace_protected(namespace_id, protect, transaction).await
    }

    async fn set_warehouse_protected_impl(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ProtectionResponse, SetWarehouseProtectedError> {
        set_warehouse_protection(warehouse_id, protect, transaction).await
    }

    async fn pick_new_task_impl(
        queue_name: &TaskQueueName,
        default_max_time_since_last_heartbeat: Duration,
        state: Self::State,
    ) -> Result<Option<Task>> {
        pick_task(
            &state.write_pool(),
            queue_name,
            default_max_time_since_last_heartbeat,
        )
        .await
    }

    async fn resolve_tasks_impl(
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<HashMap<TaskId, (TaskEntity, TaskQueueName)>> {
        resolve_tasks(warehouse_id, task_ids, &state.read_pool()).await
    }

    async fn record_task_success_impl(
        id: TaskAttemptId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        record_success(&&id, transaction, message).await
    }

    async fn record_task_failure_impl(
        id: TaskAttemptId,
        error_details: &str,
        max_retries: i32,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        record_failure(&id, max_retries, error_details, transaction).await
    }

    async fn get_task_details_impl(
        warehouse_id: WarehouseId,
        task_id: TaskId,
        num_attempts: u16,
        state: Self::State,
    ) -> Result<Option<GetTaskDetailsResponse>> {
        get_task_details(warehouse_id, task_id, num_attempts, &state.read_pool()).await
    }

    /// List tasks
    async fn list_tasks_impl(
        warehouse_id: WarehouseId,
        query: ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ListTasksResponse> {
        list_tasks(warehouse_id, query, &mut *transaction).await
    }

    async fn enqueue_tasks_impl(
        queue_name: &'static TaskQueueName,
        tasks: Vec<TaskInput>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(vec![]);
        }
        let queued = queue_task_batch(transaction, queue_name, tasks).await?;

        tracing::trace!("Queued {} tasks", queued.len());

        Ok(queued.into_iter().map(|t| t.task_id).collect())
    }

    async fn cancel_scheduled_tasks_impl(
        queue_name: Option<&TaskQueueName>,
        filter: TaskFilter,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        cancel_scheduled_tasks(&mut *transaction, filter, queue_name, force).await
    }

    async fn check_and_heartbeat_task_impl(
        id: TaskAttemptId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        progress: f32,
        execution_details: Option<serde_json::Value>,
    ) -> Result<TaskCheckState> {
        check_and_heartbeat_task(&mut *transaction, &id, progress, execution_details).await
    }

    async fn stop_tasks_impl(
        task_ids: &[TaskId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        request_tasks_stop(&mut *transaction, task_ids).await
    }

    async fn run_tasks_at_impl(
        task_ids: &[TaskId],
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        reschedule_tasks_for(&mut *transaction, task_ids, scheduled_for).await
    }

    async fn set_task_queue_config_impl(
        warehouse_id: WarehouseId,
        queue_name: &TaskQueueName,
        config: SetTaskQueueConfigRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        set_task_queue_config(transaction, queue_name, warehouse_id, config).await
    }

    async fn get_task_queue_config_impl(
        warehouse_id: WarehouseId,
        queue_name: &TaskQueueName,
        state: Self::State,
    ) -> Result<Option<GetTaskQueueConfigResponse>> {
        get_task_queue_config(&state.read_pool(), warehouse_id, queue_name).await
    }
}
