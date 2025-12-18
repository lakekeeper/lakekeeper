use std::collections::{HashMap, HashSet};

use chrono::Duration;
use iceberg::{NamespaceIdent, spec::ViewMetadata};
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::Location;

use super::{
    CatalogState, PostgresTransaction,
    bootstrap::{bootstrap, get_validation_data},
    namespace::{create_namespace, drop_namespace, list_namespaces, update_namespace_properties},
    role::{create_roles, delete_roles, list_roles, update_role},
    tabular::table::load_tables,
    warehouse::{
        create_project, create_warehouse, delete_project, delete_warehouse, get_project,
        get_warehouse_by_id, get_warehouse_by_name, list_projects, list_warehouses, rename_project,
        rename_warehouse, set_warehouse_deletion_profile, set_warehouse_status,
        update_storage_profile,
    },
};
use crate::{
    SecretId,
    api::{
        iceberg::v1::{
            PaginatedMapping, PaginationQuery, namespace::NamespaceDropFlags,
            tables::LoadTableFilters,
        },
        management::v1::{
            DeleteWarehouseQuery, TabularType,
            project::{EndpointStatisticsResponse, TimeWindowSelector, WarehouseFilter},
            role::{ListRolesResponse, Role, SearchRoleResponse, UpdateRoleSourceSystemRequest},
            task_queue::{GetTaskQueueConfigResponse, SetTaskQueueConfigRequest},
            tasks::ListTasksRequest,
            user::{ListUsersResponse, SearchUserResponse, UserLastUpdatedWith, UserType},
            warehouse::{TabularDeleteProfile, WarehouseStatisticsResponse},
        },
    },
    implementations::postgres::{
        endpoint_statistics::list::list_statistics,
        namespace::{get_namespaces_by_id, get_namespaces_by_name, set_namespace_protected},
        role::{search_role, update_role_source_system},
        tabular::{
            clear_tabular_deleted_at, drop_tabular, get_tabular_infos_by_idents,
            get_tabular_infos_by_ids, get_tabular_infos_by_s3_location, list_tabulars,
            mark_tabular_as_deleted, rename_tabular, search_tabular, set_tabular_protected,
            table::{commit_table_transaction, create_table},
            view::{create_view, load_view},
        },
        tasks::{
            cancel_scheduled_tasks, check_and_heartbeat_task, get_task_details,
            get_task_queue_config, list_tasks, pick_task, queue_task_batch, record_failure,
            record_success, request_tasks_stop, reschedule_tasks_for, resolve_tasks,
            set_task_queue_config,
        },
        user::{create_or_update_user, delete_user, list_users, search_user},
        warehouse::{get_warehouse_stats, set_warehouse_protection},
    },
    service::{
        CatalogBackendError, CatalogCreateNamespaceError, CatalogCreateRoleRequest,
        CatalogCreateWarehouseError, CatalogDeleteWarehouseError, CatalogGetNamespaceError,
        CatalogGetWarehouseByIdError, CatalogGetWarehouseByNameError, CatalogListNamespaceError,
        CatalogListRolesFilter, CatalogListWarehousesError, CatalogNamespaceDropError,
        CatalogRenameWarehouseError, CatalogSearchTabularResponse,
        CatalogSetNamespaceProtectedError, CatalogStore, CatalogUpdateNamespacePropertiesError,
        CatalogView, ClearTabularDeletedAtError, CommitTableTransactionError, CommitViewError,
        CreateNamespaceRequest, CreateOrUpdateUserResponse, CreateRoleError, CreateTableError,
        CreateViewError, DropTabularError, GetProjectResponse, GetTabularInfoByLocationError,
        GetTabularInfoError, ListNamespacesQuery, ListRolesError, ListTabularsError,
        LoadTableError, LoadTableResponse, LoadViewError, MarkTabularAsDeletedError,
        NamespaceDropInfo, NamespaceHierarchy, NamespaceId, NamespaceWithParent, ProjectId,
        RenameTabularError, ResolvedTask, ResolvedWarehouse, Result, RoleId, SearchRolesError,
        SearchTabularError, ServerInfo, SetTabularProtectionError,
        SetWarehouseDeletionProfileError, SetWarehouseProtectedError, SetWarehouseStatusError,
        StagedTableId, TableCommit, TableCreation, TableId, TableIdent, TableInfo, TabularId,
        TabularIdentBorrowed, TabularListFlags, TaskDetails, TaskList, Transaction,
        UpdateRoleError, UpdateWarehouseStorageProfileError, ViewCommit, ViewId, ViewInfo,
        ViewOrTableDeletionInfo, ViewOrTableInfo, WarehouseId, WarehouseStatus,
        authn::UserId,
        storage::StorageProfile,
        task_configs::TaskQueueConfigFilter,
        tasks::{
            Task, TaskAttemptId, TaskCheckState, TaskDetailsScope, TaskFilter, TaskId, TaskInput,
            TaskQueueName,
        },
    },
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
    ) -> std::result::Result<Option<ResolvedWarehouse>, CatalogGetWarehouseByNameError> {
        get_warehouse_by_name(warehouse_name, project_id, catalog_state).await
    }

    async fn list_namespaces_impl<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<
        PaginatedMapping<NamespaceId, NamespaceHierarchy>,
        CatalogListNamespaceError,
    > {
        list_namespaces(warehouse_id, query, transaction).await
    }

    async fn create_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<NamespaceWithParent, CatalogCreateNamespaceError> {
        create_namespace(warehouse_id, namespace_id, request, transaction).await
    }

    async fn get_namespaces_by_id_impl<'a, 'b, SOT>(
        warehouse_id: WarehouseId,
        namespaces: &[NamespaceId],
        state_or_transaction: &'b mut SOT,
    ) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError>
    where
        SOT: crate::service::StateOrTransaction<
                Self::State,
                <Self::Transaction as crate::service::Transaction<Self::State>>::Transaction<'a>,
            >,
        'a: 'b,
    {
        use crate::service::StateOrTransactionEnum;
        match state_or_transaction.as_enum_mut() {
            StateOrTransactionEnum::State(state) => {
                get_namespaces_by_id(warehouse_id, namespaces, &state.read_pool()).await
            }
            StateOrTransactionEnum::Transaction(transaction) => {
                get_namespaces_by_id(warehouse_id, namespaces, &mut ***transaction).await
            }
        }
    }

    async fn get_namespaces_by_ident_impl<'a, 'b, SOT>(
        warehouse_id: WarehouseId,
        namespaces: &[&NamespaceIdent],
        state_or_transaction: &'b mut SOT,
    ) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError>
    where
        SOT: crate::service::StateOrTransaction<
                Self::State,
                <Self::Transaction as crate::service::Transaction<Self::State>>::Transaction<'a>,
            >,
        'a: 'b,
    {
        use crate::service::StateOrTransactionEnum;
        match state_or_transaction.as_enum_mut() {
            StateOrTransactionEnum::State(state) => {
                get_namespaces_by_name(warehouse_id, namespaces, &state.read_pool()).await
            }
            StateOrTransactionEnum::Transaction(transaction) => {
                get_namespaces_by_name(warehouse_id, namespaces, &mut ***transaction).await
            }
        }
    }

    async fn drop_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        flags: NamespaceDropFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<NamespaceDropInfo, CatalogNamespaceDropError> {
        drop_namespace(warehouse_id, namespace_id, flags, transaction).await
    }

    async fn update_namespace_properties_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<NamespaceWithParent, CatalogUpdateNamespacePropertiesError> {
        update_namespace_properties(warehouse_id, namespace_id, properties, transaction).await
    }

    async fn create_table_impl<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<(TableInfo, Option<StagedTableId>), CreateTableError> {
        create_table(table_creation, transaction).await
    }

    async fn rename_tabular_impl(
        warehouse_id: WarehouseId,
        source_id: TabularId,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, RenameTabularError> {
        rename_tabular(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn drop_tabular_impl<'a>(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Location, DropTabularError> {
        drop_tabular(warehouse_id, tabular_id, force, None, transaction).await
    }

    async fn get_tabular_infos_by_ident_impl(
        warehouse_id: WarehouseId,
        tabulars: &[TabularIdentBorrowed<'_>],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Vec<ViewOrTableInfo>, GetTabularInfoError> {
        get_tabular_infos_by_idents(
            warehouse_id,
            tabulars,
            list_flags,
            &catalog_state.read_pool(),
        )
        .await
    }

    async fn get_tabular_infos_by_id_impl(
        warehouse_id: WarehouseId,
        tabulars: &[TabularId],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Vec<ViewOrTableInfo>, GetTabularInfoError> {
        get_tabular_infos_by_ids(
            warehouse_id,
            tabulars,
            list_flags,
            &catalog_state.read_pool(),
        )
        .await
    }

    async fn get_tabular_infos_by_s3_location_impl(
        warehouse_id: WarehouseId,
        location: &Location,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Option<ViewOrTableInfo>, GetTabularInfoByLocationError> {
        get_tabular_infos_by_s3_location(warehouse_id, location, list_flags, catalog_state).await
    }

    // Should also load staged tables but not tables of inactive warehouses
    async fn load_tables_impl<'a>(
        warehouse_id: WarehouseId,
        tables: impl IntoIterator<Item = TableId> + Send,
        include_deleted: bool,
        filters: &LoadTableFilters,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<LoadTableResponse>, LoadTableError> {
        load_tables(warehouse_id, tables, include_deleted, filters, transaction).await
    }

    async fn clear_tabular_deleted_at_impl(
        tabular_ids: &[TabularId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError> {
        clear_tabular_deleted_at(tabular_ids, warehouse_id, transaction).await
    }

    async fn mark_tabular_as_deleted_impl(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, MarkTabularAsDeletedError> {
        mark_tabular_as_deleted(warehouse_id, tabular_id, force, None, transaction).await
    }

    async fn commit_table_transaction_impl<'a>(
        warehouse_id: WarehouseId,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<Vec<TableInfo>, CommitTableTransactionError> {
        commit_table_transaction(warehouse_id, commits, transaction).await
    }

    // ---------------- Role Management API ----------------
    async fn create_roles_impl<'a>(
        project_id: &ProjectId,
        roles_to_create: Vec<CatalogCreateRoleRequest<'_>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<Role>, CreateRoleError> {
        create_roles(project_id, roles_to_create, &mut **transaction).await
    }

    async fn update_role_impl<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Role, UpdateRoleError> {
        update_role(
            project_id,
            role_id,
            role_name,
            description,
            &mut **transaction,
        )
        .await
    }

    async fn set_role_source_system_impl<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        request: &UpdateRoleSourceSystemRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Role, UpdateRoleError> {
        update_role_source_system(project_id, role_id, request, &mut **transaction).await
    }

    async fn list_roles_impl(
        project_id: Option<&ProjectId>,
        filter: CatalogListRolesFilter<'_>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse, ListRolesError> {
        list_roles(project_id, filter, pagination, &catalog_state.read_pool()).await
    }

    async fn delete_roles_impl<'a>(
        project_id: &ProjectId,
        role_id_filter: Option<&[RoleId]>,
        source_id_filter: Option<&[&str]>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<RoleId>, CatalogBackendError> {
        delete_roles(
            project_id,
            role_id_filter,
            source_id_filter,
            &mut **transaction,
        )
        .await
    }

    async fn search_role_impl(
        project_id: &ProjectId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse, SearchRolesError> {
        search_role(project_id, search_term, &catalog_state.read_pool()).await
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
        storage_secret_id: Option<SecretId>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<ResolvedWarehouse, CatalogCreateWarehouseError> {
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
    ) -> std::result::Result<Vec<ResolvedWarehouse>, CatalogListWarehousesError> {
        list_warehouses(project_id, status_filter, &catalog_state.read_pool()).await
    }

    async fn get_warehouse_by_id_impl<'a>(
        warehouse_id: WarehouseId,
        state: Self::State,
    ) -> std::result::Result<Option<ResolvedWarehouse>, CatalogGetWarehouseByIdError> {
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
    ) -> std::result::Result<ResolvedWarehouse, CatalogRenameWarehouseError> {
        rename_warehouse(warehouse_id, new_name, transaction).await
    }

    async fn set_warehouse_deletion_profile_impl<'a>(
        warehouse_id: WarehouseId,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseDeletionProfileError> {
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
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseStatusError> {
        set_warehouse_status(warehouse_id, status, transaction).await
    }

    async fn update_storage_profile_impl<'a>(
        warehouse_id: WarehouseId,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretId>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> std::result::Result<ResolvedWarehouse, UpdateWarehouseStorageProfileError> {
        update_storage_profile(
            warehouse_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn create_view_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        view_ident: &TableIdent,
        request: &ViewMetadata,
        metadata_location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ViewInfo, CreateViewError> {
        create_view(
            warehouse_id,
            namespace_id,
            metadata_location,
            transaction,
            view_ident.name.as_str(),
            request,
        )
        .await
    }

    async fn load_view_impl<'a>(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<CatalogView, LoadViewError> {
        load_view(warehouse_id, view_id, include_deleted, &mut *transaction).await
    }

    async fn commit_view_impl<'a>(
        ViewCommit {
            view_ident,
            namespace_id,
            warehouse_id,
            previous_view,
            new_view,
        }: ViewCommit<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ViewInfo, CommitViewError> {
        drop_tabular(
            warehouse_id,
            ViewId::from(previous_view.metadata.uuid()).into(),
            true,
            Some(&previous_view.metadata_location),
            transaction,
        )
        .await?;
        create_view(
            warehouse_id,
            namespace_id,
            &new_view.metadata_location,
            transaction,
            &view_ident.name,
            &new_view.metadata,
        )
        .await
        .map_err(Into::into)
    }

    async fn search_tabular_impl(
        warehouse_id: WarehouseId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> std::result::Result<CatalogSearchTabularResponse, SearchTabularError> {
        search_tabular(warehouse_id, search_term, &catalog_state.read_pool()).await
    }

    async fn list_tabulars_impl(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        typ: Option<TabularType>,
        pagination_query: PaginationQuery,
    ) -> std::result::Result<PaginatedMapping<TabularId, ViewOrTableDeletionInfo>, ListTabularsError>
    {
        list_tabulars(
            warehouse_id,
            namespace_id,
            list_flags,
            &mut **transaction,
            typ.map(Into::into),
            pagination_query,
        )
        .await
    }
    async fn set_tabular_protected_impl(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, SetTabularProtectionError> {
        set_tabular_protected(warehouse_id, tabular_id, protect, transaction).await
    }

    async fn set_namespace_protected_impl(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<NamespaceWithParent, CatalogSetNamespaceProtectedError> {
        set_namespace_protected(warehouse_id, namespace_id, protect, transaction).await
    }

    async fn set_warehouse_protected_impl(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseProtectedError> {
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
        project_id: Option<ProjectId>,
        warehouse_id: Option<WarehouseId>,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<Vec<ResolvedTask>> {
        resolve_tasks(project_id, warehouse_id, task_ids, &state.read_pool()).await
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
        task_id: TaskId,
        scope: TaskDetailsScope,
        num_attempts: u16,
        state: Self::State,
    ) -> Result<Option<TaskDetails>> {
        get_task_details(task_id, scope, num_attempts, &state.read_pool()).await
    }

    /// List tasks
    async fn list_tasks_impl(
        filter: &TaskFilter,
        query: ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<TaskList> {
        list_tasks(filter, query, &mut *transaction).await
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
        project_id: ProjectId,
        warehouse_id: Option<WarehouseId>,
        queue_name: &TaskQueueName,
        config: SetTaskQueueConfigRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        set_task_queue_config(transaction, queue_name, project_id, warehouse_id, config).await
    }

    async fn get_task_queue_config_impl(
        filter: &TaskQueueConfigFilter,
        queue_name: &TaskQueueName,
        state: Self::State,
    ) -> Result<Option<GetTaskQueueConfigResponse>> {
        get_task_queue_config(&state.read_pool(), filter, queue_name).await
    }
}
