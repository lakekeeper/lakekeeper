#![allow(clippy::needless_for_each)]

use async_trait::async_trait;
use axum::Router;
#[cfg(feature = "open-api")]
use utoipa::OpenApi;

use crate::{
    api::{iceberg::v1::Result, ApiContext},
    request_metadata::RequestMetadata,
    service::{
        authn::UserId,
        authz::{
            AuthorizationBackendUnavailable, Authorizer, CatalogNamespaceAction,
            CatalogProjectAction, CatalogRoleAction, CatalogServerAction, CatalogTableAction,
            CatalogUserAction, CatalogViewAction, CatalogWarehouseAction, ListProjectsResponse,
            NamespaceParent,
        },
        health::{Health, HealthExt},
        Actor, AuthZTableInfo, AuthZViewInfo, CatalogStore, NamespaceHierarchy, NamespaceId,
        ProjectId, ResolvedWarehouse, RoleId, SecretStore, ServerId, State, TableId, ViewId,
        WarehouseId,
    },
};

#[derive(Clone, Debug)]
pub struct AllowAllAuthorizer {
    pub server_id: ServerId,
}

#[cfg(test)]
impl std::default::Default for AllowAllAuthorizer {
    fn default() -> Self {
        Self {
            server_id: ServerId::new_random(),
        }
    }
}

#[async_trait]
impl HealthExt for AllowAllAuthorizer {
    async fn health(&self) -> Vec<Health> {
        vec![]
    }
    async fn update_health(&self) {
        // Do nothing
    }
}

#[cfg(feature = "open-api")]
#[derive(Debug, OpenApi)]
#[openapi()]
pub(super) struct ApiDoc;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    type ServerAction = CatalogServerAction;
    type ProjectAction = CatalogProjectAction;
    type WarehouseAction = CatalogWarehouseAction;
    type NamespaceAction = CatalogNamespaceAction;
    type TableAction = CatalogTableAction;
    type ViewAction = CatalogViewAction;

    fn implementation_name() -> &'static str {
        "allow-all"
    }

    fn server_id(&self) -> ServerId {
        self.server_id
    }

    #[cfg(feature = "open-api")]
    fn api_doc() -> utoipa::openapi::OpenApi {
        ApiDoc::openapi()
    }

    fn new_router<C: CatalogStore, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>> {
        Router::new()
    }

    async fn check_actor(&self, _actor: &Actor) -> Result<()> {
        Ok(())
    }

    async fn can_bootstrap(&self, _metadata: &RequestMetadata) -> Result<()> {
        Ok(())
    }

    async fn bootstrap(&self, _metadata: &RequestMetadata, _is_operator: bool) -> Result<()> {
        Ok(())
    }

    async fn list_projects_impl(
        &self,
        _metadata: &RequestMetadata,
    ) -> std::result::Result<ListProjectsResponse, AuthorizationBackendUnavailable> {
        Ok(ListProjectsResponse::All)
    }

    async fn can_search_users_impl(&self, _metadata: &RequestMetadata) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_user_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _user_id: &UserId,
        _action: CatalogUserAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_role_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _role_id: RoleId,
        _action: CatalogRoleAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_server_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _action: CatalogServerAction,
    ) -> std::result::Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
    }

    async fn is_allowed_project_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _project_id: &ProjectId,
        _action: CatalogProjectAction,
    ) -> std::result::Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
    }

    async fn is_allowed_warehouse_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _warehouse: &ResolvedWarehouse,
        _action: Self::WarehouseAction,
    ) -> std::result::Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
    }

    async fn is_allowed_namespace_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _warehouse: &ResolvedWarehouse,
        _namespace: &NamespaceHierarchy,
        _action: Self::NamespaceAction,
    ) -> std::result::Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
    }

    async fn is_allowed_table_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _warehouse: &ResolvedWarehouse,
        _namespace: &NamespaceHierarchy,
        _table: &impl AuthZTableInfo,
        _action: Self::TableAction,
    ) -> std::result::Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
    }

    async fn is_allowed_view_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _warehouse: &ResolvedWarehouse,
        _namespace: &NamespaceHierarchy,
        _view: &impl AuthZViewInfo,
        _action: Self::ViewAction,
    ) -> std::result::Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
    }

    async fn delete_user(&self, _metadata: &RequestMetadata, _user_id: UserId) -> Result<()> {
        Ok(())
    }

    async fn create_role(
        &self,
        _metadata: &RequestMetadata,
        _role_id: RoleId,
        _parent_project_id: ProjectId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_role(&self, _metadata: &RequestMetadata, _role_id: RoleId) -> Result<()> {
        Ok(())
    }

    async fn create_project(
        &self,
        _metadata: &RequestMetadata,
        _project_id: &ProjectId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_project(
        &self,
        _metadata: &RequestMetadata,
        _project_id: ProjectId,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_warehouse(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseId,
        _parent_project_id: &ProjectId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_warehouse(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseId,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_namespace(
        &self,
        _metadata: &RequestMetadata,
        _namespace_id: NamespaceId,
        _parent: NamespaceParent,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_namespace(
        &self,
        _metadata: &RequestMetadata,
        _namespace_id: NamespaceId,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_table(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseId,
        _table_id: TableId,
        _parent: NamespaceId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_table(&self, _warehouse_id: WarehouseId, _table_id: TableId) -> Result<()> {
        Ok(())
    }

    async fn create_view(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseId,
        _view_id: ViewId,
        _parent: NamespaceId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_view(&self, _warehouse_id: WarehouseId, _view_id: ViewId) -> Result<()> {
        Ok(())
    }
}
