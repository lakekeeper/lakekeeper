#![allow(clippy::needless_for_each)]

use std::collections::HashMap;

use async_trait::async_trait;
use axum::Router;
#[cfg(feature = "open-api")]
use utoipa::OpenApi;

use crate::{
    api::{ApiContext, iceberg::v1::Result},
    request_metadata::RequestMetadata,
    service::{
        ArcProjectId, AuthZNamespaceInfo, AuthZTableInfo, AuthZViewInfo, CatalogStore, NamespaceId,
        NamespaceWithParent, ProjectId, ResolvedWarehouse, Role, RoleId, SecretStore, ServerId,
        State, TableId, ViewId, WarehouseId,
        authn::UserId,
        authz::{
            AuthorizationBackendUnavailable, Authorizer, CatalogNamespaceAction,
            CatalogProjectAction, CatalogRoleAction, CatalogServerAction, CatalogTableAction,
            CatalogUserAction, CatalogViewAction, CatalogWarehouseAction, IsAllowedActionError,
            ListProjectsResponse, NamespaceParent, UserOrRole,
        },
        health::{Health, HealthExt},
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
    type UserAction = CatalogUserAction;
    type RoleAction = CatalogRoleAction;

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

    async fn check_assume_role_impl(
        &self,
        _principal: &UserId,
        _assumed_role: &Role,
        _request_metadata: &RequestMetadata,
    ) -> Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
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
    ) -> Result<ListProjectsResponse, AuthorizationBackendUnavailable> {
        Ok(ListProjectsResponse::All)
    }

    async fn can_search_users_impl(
        &self,
        _metadata: &RequestMetadata,
    ) -> Result<bool, AuthorizationBackendUnavailable> {
        Ok(true)
    }

    async fn are_allowed_user_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        users_with_actions: &[(&UserId, Self::UserAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; users_with_actions.len()])
    }

    async fn are_allowed_role_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        roles_with_actions: &[(&Role, Self::RoleAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; roles_with_actions.len()])
    }

    async fn are_allowed_server_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        actions: &[Self::ServerAction],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; actions.len()])
    }

    async fn are_allowed_project_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        projects_with_actions: &[(&ArcProjectId, Self::ProjectAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; projects_with_actions.len()])
    }

    async fn are_allowed_warehouse_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, Self::WarehouseAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; warehouses_with_actions.len()])
    }

    async fn are_allowed_namespace_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, Self::NamespaceAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; actions.len()])
    }

    async fn are_allowed_table_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            &impl AuthZTableInfo,
            Self::TableAction,
        )],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; actions.len()])
    }

    async fn are_allowed_view_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        views_with_actions: &[(&NamespaceWithParent, &impl AuthZViewInfo, Self::ViewAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        Ok(vec![true; views_with_actions.len()])
    }

    async fn delete_user(&self, _metadata: &RequestMetadata, _user_id: UserId) -> Result<()> {
        Ok(())
    }

    async fn create_role(
        &self,
        _metadata: &RequestMetadata,
        _role_id: RoleId,
        _parent_project_id: ArcProjectId,
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
        _project_id: &ProjectId,
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
