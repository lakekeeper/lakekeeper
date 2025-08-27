use async_trait::async_trait;
use axum::Router;
use utoipa::OpenApi;

use crate::{
    api::{iceberg::v1::Result, ApiContext},
    request_metadata::RequestMetadata,
    service::{
        authn::UserId,
        authz::{
            Authorizer, CatalogNamespaceAction, CatalogProjectAction, CatalogRoleAction,
            CatalogServerAction, CatalogTableAction, CatalogUserAction, CatalogViewAction,
            CatalogWarehouseAction, ListProjectsResponse, NamespaceParent,
        },
        health::{Health, HealthExt},
        Actor, Catalog, NamespaceId, ProjectId, RoleId, SecretStore, State, TableId, ViewId,
        WarehouseId,
    },
};

#[derive(Clone, Debug, Default)]
pub struct AllowAllAuthorizer;

#[async_trait]
impl HealthExt for AllowAllAuthorizer {
    async fn health(&self) -> Vec<Health> {
        vec![]
    }
    async fn update_health(&self) {
        // Do nothing
    }
}

#[derive(Debug, OpenApi)]
#[openapi()]
pub(super) struct ApiDoc;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    fn api_doc() -> utoipa::openapi::OpenApi {
        ApiDoc::openapi()
    }

    fn new_router<C: Catalog, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>> {
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
    ) -> Result<ListProjectsResponse> {
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
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_project_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _project_id: &ProjectId,
        _action: CatalogProjectAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_warehouse_action_impl(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseId,
        _action: CatalogWarehouseAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_namespace_action_impl<A>(
        &self,
        _metadata: &RequestMetadata,
        _namespace_id: NamespaceId,
        _action: A,
    ) -> Result<bool>
    where
        A: From<CatalogNamespaceAction> + std::fmt::Display + Send,
    {
        Ok(true)
    }

    async fn is_allowed_table_action_impl<A>(
        &self,
        _metadata: &RequestMetadata,
        _table_id: TableId,
        _action: A,
    ) -> Result<bool>
    where
        A: From<CatalogTableAction> + std::fmt::Display + Send,
    {
        Ok(true)
    }

    async fn is_allowed_view_action_impl<A>(
        &self,
        _metadata: &RequestMetadata,
        _view_id: ViewId,
        _action: A,
    ) -> Result<bool>
    where
        A: From<CatalogViewAction> + std::fmt::Display + Send,
    {
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
        _table_id: TableId,
        _parent: NamespaceId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_table(&self, _table_id: TableId) -> Result<()> {
        Ok(())
    }

    async fn create_view(
        &self,
        _metadata: &RequestMetadata,
        _view_id: ViewId,
        _parent: NamespaceId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_view(&self, _view_id: ViewId) -> Result<()> {
        Ok(())
    }
}
