use super::{
    bootstrap::{bootstrap, get_validation_data},
    namespace::{
        create_namespace, drop_namespace, get_namespace, list_namespaces, namespace_to_id,
        update_namespace_properties,
    },
    role::{create_role, delete_role, list_roles, update_role},
    stats,
    tabular::table::{
        drop_table, get_table_metadata_by_id, get_table_metadata_by_s3_location, list_tables,
        load_tables, rename_table, resolve_table_ident, table_idents_to_ids,
    },
    warehouse::{
        create_project, create_warehouse, delete_project, delete_warehouse,
        get_config_for_warehouse, get_project, get_warehouse, get_warehouse_by_name, list_projects,
        list_warehouses, rename_project, rename_warehouse, set_warehouse_deletion_profile,
        set_warehouse_status, update_storage_profile,
    },
    CatalogState, PostgresTransaction,
};
use crate::api::management::v1::task::{
    ListTaskInstancesResponse, ListTasksRequest, ListTasksResponse,
};
use crate::api::management::v1::user::{
    ListUsersResponse, SearchUserResponse, UserLastUpdatedWith, UserType,
};
use crate::api::management::v1::warehouse::{WarehouseStatistics, WarehouseStatsResponse};
use crate::implementations::postgres::role::search_role;
use crate::implementations::postgres::stats::update_stats;
use crate::implementations::postgres::tabular::table::create_table;
use crate::implementations::postgres::tabular::table::{
    commit_table_transaction, load_storage_profile,
};
use crate::implementations::postgres::tabular::{
    clear_tabular_deleted_at, list_tabulars, mark_tabular_as_deleted,
};
use crate::implementations::postgres::task_queues::{list_task_instances, list_tasks};
use crate::implementations::postgres::user::{
    create_or_update_user, delete_user, list_users, search_user,
};
use crate::service::authn::UserId;
use crate::service::task_queue::TaskId;
use crate::service::{
    storage::StorageProfile, Catalog, CreateNamespaceRequest, CreateNamespaceResponse,
    CreateOrUpdateUserResponse, CreateTableResponse, DeletionDetails, GetNamespaceResponse,
    GetProjectResponse, GetTableMetadataResponse, GetWarehouseResponse, ListFlags,
    ListNamespacesQuery, LoadTableResponse, NamespaceIdent, NamespaceIdentUuid, ProjectIdent,
    Result, RoleId, StartupValidationData, TableCreation, TableIdent, TableIdentUuid, Transaction,
    WarehouseIdent, WarehouseStatus,
};
use crate::SecretIdent;
use crate::{
    api::iceberg::v1::{PaginatedMapping, PaginationQuery},
    service::TableCommit,
};
use crate::{
    api::management::v1::role::{ListRolesResponse, Role, SearchRoleResponse},
    service::ViewIdentUuid,
};
use crate::{api::management::v1::warehouse::TabularDeleteProfile, service::TabularIdentUuid};
use crate::{
    implementations::postgres::tabular::view::{
        create_view, drop_view, list_views, load_view, rename_view, view_ident_to_id,
    },
    service::TabularIdentOwned,
};
use iceberg::spec::ViewMetadata;
use iceberg_ext::catalog::rest::ErrorModel;
use iceberg_ext::{catalog::rest::CatalogConfig, configs::Location};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};

#[async_trait::async_trait]
impl Catalog for super::PostgresCatalog {
    type Transaction = PostgresTransaction;
    type State = CatalogState;

    // ---------------- Bootstrap ----------------
    async fn bootstrap<'a>(
        terms_accepted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<bool> {
        bootstrap(terms_accepted, &mut **transaction).await
    }

    async fn get_server_info(
        catalog_state: Self::State,
    ) -> std::result::Result<StartupValidationData, ErrorModel> {
        get_validation_data(&catalog_state.read_pool()).await
    }

    // ---------------- Role Management API ----------------
    async fn create_role<'a>(
        role_id: RoleId,
        project_id: ProjectIdent,
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

    async fn search_role(
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse> {
        search_role(search_term, &catalog_state.read_pool()).await
    }

    async fn list_roles<'a>(
        filter_project_id: Option<ProjectIdent>,
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

    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: ProjectIdent,
        catalog_state: CatalogState,
    ) -> Result<Option<WarehouseIdent>> {
        get_warehouse_by_name(warehouse_name, project_id, catalog_state).await
    }

    async fn get_config_for_warehouse(
        warehouse_id: WarehouseIdent,
        catalog_state: CatalogState,
    ) -> Result<Option<CatalogConfig>> {
        get_config_for_warehouse(warehouse_id, catalog_state).await
    }

    async fn list_namespaces<'a>(
        warehouse_id: WarehouseIdent,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<PaginatedMapping<NamespaceIdentUuid, NamespaceIdent>> {
        list_namespaces(warehouse_id, query, transaction).await
    }

    async fn create_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<CreateNamespaceResponse> {
        create_namespace(warehouse_id, namespace_id, request, transaction).await
    }

    async fn get_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<GetNamespaceResponse> {
        get_namespace(warehouse_id, namespace_id, transaction).await
    }

    async fn namespace_to_id<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<NamespaceIdentUuid>> {
        namespace_to_id(warehouse_id, namespace, transaction).await
    }

    async fn drop_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        drop_namespace(warehouse_id, namespace_id, transaction).await
    }

    async fn update_namespace_properties<'a>(
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
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
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TableIdentUuid, TableIdent>> {
        list_tables(
            warehouse_id,
            namespace,
            list_flags,
            &mut **transaction,
            pagination_query,
        )
        .await
    }

    async fn resolve_table_ident(
        warehouse_id: WarehouseIdent,
        table: &TableIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Option<crate::service::TabularDetails>> {
        resolve_table_ident(warehouse_id, table, list_flags, &mut **transaction).await
    }

    async fn table_to_id<'a>(
        warehouse_id: WarehouseIdent,
        table: &TableIdent,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<TableIdentUuid>> {
        resolve_table_ident(warehouse_id, table, list_flags, &mut **transaction)
            .await
            .map(|x| x.map(|x| x.ident))
    }

    async fn table_idents_to_ids(
        warehouse_id: WarehouseIdent,
        tables: HashSet<&TableIdent>,
        list_flags: ListFlags,
        catalog_state: Self::State,
    ) -> Result<HashMap<TableIdent, Option<TableIdentUuid>>> {
        table_idents_to_ids(warehouse_id, tables, list_flags, &catalog_state.read_pool()).await
    }

    // Should also load staged tables but not tables of inactive warehouses
    async fn load_tables<'a>(
        warehouse_id: WarehouseIdent,
        tables: impl IntoIterator<Item = TableIdentUuid> + Send,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<HashMap<TableIdentUuid, LoadTableResponse>> {
        load_tables(warehouse_id, tables, include_deleted, transaction).await
    }

    async fn load_storage_profile(
        warehouse_id: WarehouseIdent,
        tabular_id: TableIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<(Option<SecretIdent>, StorageProfile)> {
        load_storage_profile(warehouse_id, tabular_id, transaction).await
    }

    async fn get_table_metadata_by_id(
        warehouse_id: WarehouseIdent,
        table: TableIdentUuid,
        list_flags: ListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<GetTableMetadataResponse>> {
        get_table_metadata_by_id(warehouse_id, table, list_flags, catalog_state).await
    }

    async fn get_table_metadata_by_s3_location(
        warehouse_id: WarehouseIdent,
        location: &Location,
        list_flags: ListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<GetTableMetadataResponse>> {
        get_table_metadata_by_s3_location(warehouse_id, location, list_flags, catalog_state).await
    }

    async fn rename_table<'a>(
        warehouse_id: WarehouseIdent,
        source_id: TableIdentUuid,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        rename_table(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn drop_table<'a>(
        table_id: TableIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        drop_table(table_id, transaction).await
    }

    async fn undrop_tabulars(
        tabular_ids: &[TableIdentUuid],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>> {
        clear_tabular_deleted_at(&tabular_ids.iter().map(|i| **i).collect_vec(), transaction).await
    }

    async fn mark_tabular_as_deleted(
        table_id: TabularIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        mark_tabular_as_deleted(table_id, None, transaction).await
    }

    async fn commit_table_transaction<'a>(
        warehouse_id: WarehouseIdent,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        commit_table_transaction(warehouse_id, commits, transaction).await
    }

    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: ProjectIdent,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<WarehouseIdent> {
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
        project_id: ProjectIdent,
        project_name: String,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        create_project(project_id, project_name, transaction).await
    }

    /// Delete a project
    async fn delete_project<'a>(
        project_id: ProjectIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        delete_project(project_id, transaction).await
    }

    /// Get the project metadata
    async fn get_project<'a>(
        project_id: ProjectIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<GetProjectResponse>> {
        get_project(project_id, transaction).await
    }

    async fn list_projects(
        project_ids: Option<HashSet<ProjectIdent>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<GetProjectResponse>> {
        list_projects(project_ids, &mut **transaction).await
    }

    async fn rename_project<'a>(
        project_id: ProjectIdent,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        rename_project(project_id, new_name, transaction).await
    }

    async fn list_warehouses(
        project_id: ProjectIdent,
        include_inactive: Option<Vec<WarehouseStatus>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<GetWarehouseResponse>> {
        list_warehouses(project_id, include_inactive, &mut **transaction).await
    }

    async fn get_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<Option<GetWarehouseResponse>> {
        get_warehouse(warehouse_id, transaction).await
    }

    async fn delete_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        delete_warehouse(warehouse_id, transaction).await
    }

    async fn rename_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        rename_warehouse(warehouse_id, new_name, transaction).await
    }

    async fn set_warehouse_deletion_profile<'a>(
        warehouse_id: WarehouseIdent,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        set_warehouse_deletion_profile(warehouse_id, deletion_profile, &mut **transaction).await
    }

    async fn set_warehouse_status<'a>(
        warehouse_id: WarehouseIdent,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        set_warehouse_status(warehouse_id, status, transaction).await
    }

    async fn update_storage_profile<'a>(
        warehouse_id: WarehouseIdent,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        update_storage_profile(
            warehouse_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn view_to_id<'a>(
        warehouse_id: WarehouseIdent,
        view: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<ViewIdentUuid>> {
        view_ident_to_id(warehouse_id, view, false, &mut **transaction).await
    }

    async fn create_view<'a>(
        namespace_id: NamespaceIdentUuid,
        view: &TableIdent,
        request: ViewMetadata,
        metadata_location: &'_ Location,
        location: &'_ Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        create_view(
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
        view_id: ViewIdentUuid,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<crate::implementations::postgres::tabular::view::ViewMetadataWithLocation> {
        load_view(view_id, include_deleted, &mut *transaction).await
    }

    async fn list_views<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<ViewIdentUuid, TableIdent>> {
        list_views(
            warehouse_id,
            namespace,
            include_deleted,
            &mut **transaction,
            pagination_query,
        )
        .await
    }

    async fn update_view_metadata(
        namespace_id: NamespaceIdentUuid,
        view_id: ViewIdentUuid,
        view: &TableIdent,
        metadata_location: &Location,
        metadata: ViewMetadata,
        location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        drop_view(view_id, transaction).await?;
        create_view(
            namespace_id,
            metadata_location,
            transaction,
            &view.name,
            metadata,
            location,
        )
        .await
    }

    async fn drop_view<'a>(
        view_id: ViewIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        drop_view(view_id, transaction).await
    }

    async fn rename_view(
        warehouse_id: WarehouseIdent,
        source_id: ViewIdentUuid,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        rename_view(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn list_tabulars(
        warehouse_id: WarehouseIdent,
        namespace_id: Option<NamespaceIdentUuid>,
        list_flags: ListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TabularIdentUuid, (TabularIdentOwned, Option<DeletionDetails>)>>
    {
        list_tabulars(
            warehouse_id,
            None,
            namespace_id,
            list_flags,
            &mut **transaction,
            None,
            pagination_query,
            false,
        )
        .await
    }

    async fn update_warehouse_statistics(
        warehouse_id: WarehouseIdent,
        list_flags: ListFlags,
        transaction: Self::State,
    ) -> Result<WarehouseStatistics> {
        update_stats(transaction.write_pool(), warehouse_id, list_flags).await
    }

    async fn list_tasks(
        pagination: PaginationQuery,
        body: ListTasksRequest,
        state: Self::State,
    ) -> Result<ListTasksResponse> {
        list_tasks(pagination, body, &state.read_write.read_pool).await
    }

    async fn list_task_instances(
        task_id: Option<TaskId>,
        pagination: PaginationQuery,
        state: Self::State,
    ) -> Result<ListTaskInstancesResponse> {
        list_task_instances(task_id, pagination, &state.read_write.read_pool).await
    }

    async fn get_warehouse_stats(
        warehouse_id: WarehouseIdent,
        state: Self::State,
    ) -> Result<WarehouseStatsResponse> {
        stats::get_warehouse_stats(state.read_pool(), warehouse_id).await
    }
}
