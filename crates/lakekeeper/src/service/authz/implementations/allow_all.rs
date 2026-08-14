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
        ArcProjectId, AuthZGenericTableInfo, AuthZNamespaceInfo, AuthZTableInfo, AuthZViewInfo,
        CatalogStore, GenericTableId, NamespaceId, NamespaceWithParent, ProjectId,
        ResolvedWarehouse, Role, RoleId, SecretStore, ServerId, State, TableId, TagDefinition,
        TagDefinitionId, ViewId, WarehouseId,
        authn::UserId,
        authz::{
            ActionOnGenericTable, ActionOnTable, ActionOnView, AuthorizationDecision, Authorizer,
            AuthzBackendErrorOrBadRequest, CatalogAction, CatalogGenericTableAction,
            CatalogNamespaceAction, CatalogProjectAction, CatalogRoleAction, CatalogServerAction,
            CatalogTableAction, CatalogTagAction, CatalogUserAction, CatalogViewAction,
            CatalogWarehouseAction, GrantAuthorityCheck, GrantTarget, IsAllowedActionError,
            ListProjectsResponse, NamespaceParent, PrivilegeDescriptor, ResourceType, UserOrRole,
        },
        health::{Health, HealthExt},
    },
};

#[derive(Clone, Debug)]
pub struct AllowAllAuthorizer {
    pub server_id: ServerId,
}

#[cfg(any(test, feature = "test-utils"))]
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

/// Gate action for reading grants; not itself a grantable privilege.
const READ_GRANTS_ACTION: &str = "read_grants";

/// The grantable vocabulary of one resource level: every catalog action on it,
/// except the grant-reading gate.
fn privileges_from_actions<A: CatalogAction>(
    actions: &'static [A],
    resource_type: ResourceType,
) -> Vec<PrivilegeDescriptor> {
    actions
        .iter()
        .filter_map(|action| {
            let name = action.action_descriptor().action_name;
            (name != READ_GRANTS_ACTION).then(|| PrivilegeDescriptor {
                name: name.to_string(),
                display_name: name.replace('_', " "),
                // Nothing to describe or group by: this vocabulary is the whole catalog
                // action set, and an authorizer that enforces nothing would be inventing
                // meaning for a hundred names. Real authorizers supply both.
                description: None,
                category: None,
                resource_type,
            })
        })
        .collect()
}

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    type ServerAction = CatalogServerAction;
    type ProjectAction = CatalogProjectAction;
    type WarehouseAction = CatalogWarehouseAction;
    type NamespaceAction = CatalogNamespaceAction;
    type TableAction = CatalogTableAction;
    type ViewAction = CatalogViewAction;
    type GenericTableAction = CatalogGenericTableAction;
    type UserAction = CatalogUserAction;
    type RoleAction = CatalogRoleAction;
    type TagAction = CatalogTagAction;

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
    ) -> Result<bool, AuthzBackendErrorOrBadRequest> {
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
    ) -> Result<ListProjectsResponse, AuthzBackendErrorOrBadRequest> {
        Ok(ListProjectsResponse::All)
    }

    async fn can_search_users_impl(
        &self,
        _metadata: &RequestMetadata,
    ) -> Result<bool, AuthzBackendErrorOrBadRequest> {
        Ok(true)
    }

    async fn are_allowed_user_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        users_with_actions: &[(&UserId, Self::UserAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![
            AuthorizationDecision::allow();
            users_with_actions.len()
        ])
    }

    async fn are_allowed_role_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        roles_with_actions: &[(&Role, Self::RoleAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![
            AuthorizationDecision::allow();
            roles_with_actions.len()
        ])
    }

    async fn are_allowed_tag_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        tags_with_actions: &[(&TagDefinition, Self::TagAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![
            AuthorizationDecision::allow();
            tags_with_actions.len()
        ])
    }

    async fn are_allowed_server_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        actions: &[Self::ServerAction],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![AuthorizationDecision::allow(); actions.len()])
    }

    async fn are_allowed_project_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        projects_with_actions: &[(&ArcProjectId, Self::ProjectAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![
            AuthorizationDecision::allow();
            projects_with_actions.len()
        ])
    }

    async fn are_allowed_warehouse_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, Self::WarehouseAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![
            AuthorizationDecision::allow();
            warehouses_with_actions.len()
        ])
    }

    async fn are_allowed_namespace_actions_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, Self::NamespaceAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![AuthorizationDecision::allow(); actions.len()])
    }

    async fn are_allowed_table_actions_impl<A: Into<Self::TableAction> + Send + Clone + Sync>(
        &self,
        _metadata: &RequestMetadata,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnTable<'_, '_, impl AuthZTableInfo, A>,
        )],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![AuthorizationDecision::allow(); actions.len()])
    }

    async fn are_allowed_view_actions_impl<A: Into<Self::ViewAction> + Send + Clone + Sync>(
        &self,
        _metadata: &RequestMetadata,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnView<'_, '_, impl AuthZViewInfo, A>,
        )],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![AuthorizationDecision::allow(); actions.len()])
    }

    async fn are_allowed_generic_table_actions_impl<
        A: Into<Self::GenericTableAction> + Send + Clone + Sync,
    >(
        &self,
        _metadata: &RequestMetadata,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnGenericTable<'_, '_, impl AuthZGenericTableInfo, A>,
        )],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![AuthorizationDecision::allow(); actions.len()])
    }

    /// Every catalog action, at every level, is grantable here.
    ///
    /// This authorizer allows every action regardless of grants, so grant rows
    /// recorded under it are an inventory only and enforce nothing.
    fn grantable_privileges(&self, resource_type: ResourceType) -> &'static [PrivilegeDescriptor] {
        static VOCABULARIES: std::sync::LazyLock<
            std::collections::HashMap<ResourceType, Vec<PrivilegeDescriptor>>,
        > = std::sync::LazyLock::new(|| {
            <ResourceType as strum::VariantArray>::VARIANTS
                .iter()
                .map(|resource_type| (*resource_type, build_vocabulary(*resource_type)))
                .collect()
        });
        VOCABULARIES.get(&resource_type).map_or(&[], Vec::as_slice)
    }

    async fn are_allowed_grants_impl(
        &self,
        _metadata: &RequestMetadata,
        _for_user: Option<&UserOrRole>,
        _target: &GrantTarget<'_>,
        checks: &[GrantAuthorityCheck<'_>],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        Ok(vec![AuthorizationDecision::allow(); checks.len()])
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

    async fn create_tag(
        &self,
        _metadata: &RequestMetadata,
        _tag_definition_id: TagDefinitionId,
        _parent_project_id: ArcProjectId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_tag(
        &self,
        _metadata: &RequestMetadata,
        _tag_definition_id: TagDefinitionId,
    ) -> Result<()> {
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

    async fn create_generic_table(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseId,
        _generic_table_id: GenericTableId,
        _parent: NamespaceId,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_generic_table(
        &self,
        _warehouse_id: WarehouseId,
        _generic_table_id: GenericTableId,
    ) -> Result<()> {
        Ok(())
    }
}

/// The catalog's own action set per resource type, which is this authorizer's whole
/// vocabulary: it enforces nothing, so everything the catalog can express is grantable.
fn build_vocabulary(resource_type: ResourceType) -> Vec<PrivilegeDescriptor> {
    match resource_type {
        ResourceType::Server => {
            privileges_from_actions(CatalogServerAction::variants(), resource_type)
        }
        ResourceType::Project => {
            privileges_from_actions(CatalogProjectAction::variants(), resource_type)
        }
        ResourceType::Warehouse => {
            privileges_from_actions(CatalogWarehouseAction::variants(), resource_type)
        }
        ResourceType::Namespace => {
            privileges_from_actions(CatalogNamespaceAction::variants(), resource_type)
        }
        ResourceType::Table => {
            privileges_from_actions(CatalogTableAction::variants(), resource_type)
        }
        ResourceType::View => privileges_from_actions(CatalogViewAction::variants(), resource_type),
        ResourceType::GenericTable => {
            privileges_from_actions(CatalogGenericTableAction::variants(), resource_type)
        }
        ResourceType::Tag => privileges_from_actions(CatalogTagAction::variants(), resource_type),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::super::{AuthZGrantOps, GrantOp, InvalidGrantPrivilege, UserOrRoleId},
        *,
    };

    fn privilege_names(resource_type: ResourceType) -> Vec<String> {
        AllowAllAuthorizer::default()
            .grantable_privileges(resource_type)
            .iter()
            .map(|descriptor| descriptor.name.clone())
            .collect()
    }

    #[test]
    fn read_grants_action_name_is_the_excluded_one() {
        assert_eq!(
            CatalogWarehouseAction::ReadGrants
                .action_descriptor()
                .action_name,
            READ_GRANTS_ACTION
        );
    }

    #[test]
    fn warehouse_vocabulary_has_actions_but_not_the_grant_gate() {
        let names = privilege_names(ResourceType::Warehouse);
        assert!(
            names.contains(&"get_metadata".to_string()),
            "expected get_metadata in {names:?}"
        );
        assert!(
            !names.contains(&"read_grants".to_string()),
            "read_grants must not be grantable, got {names:?}"
        );
    }

    #[test]
    fn descriptors_have_readable_display_names() {
        let authorizer = AllowAllAuthorizer::default();
        let descriptor = authorizer
            .grantable_privileges(ResourceType::Warehouse)
            .iter()
            .find(|descriptor| descriptor.name == "get_metadata")
            .expect("get_metadata is grantable on a warehouse");
        assert_eq!(descriptor.display_name, "get metadata");
        assert_eq!(descriptor.description, None);
        assert_eq!(descriptor.resource_type, ResourceType::Warehouse);
    }

    #[test]
    fn every_resource_type_has_a_non_empty_vocabulary() {
        let authorizer = AllowAllAuthorizer::default();
        for resource_type in <ResourceType as strum::VariantArray>::VARIANTS {
            let descriptors = authorizer.grantable_privileges(*resource_type);
            assert!(
                !descriptors.is_empty(),
                "`{}` has no grantable privileges",
                resource_type.as_str()
            );
            for descriptor in descriptors {
                assert_eq!(descriptor.resource_type, *resource_type);
                assert_ne!(descriptor.name, READ_GRANTS_ACTION);
            }
        }
    }

    #[test]
    fn validate_accepts_only_names_from_the_vocabulary() {
        let authorizer = AllowAllAuthorizer::default();
        assert_eq!(
            authorizer.validate_grant_privilege(ResourceType::Warehouse, "get_metadata"),
            Ok(())
        );
        assert_eq!(
            authorizer.validate_grant_privilege(ResourceType::Warehouse, "read_grants"),
            Err(InvalidGrantPrivilege {
                resource_type: ResourceType::Warehouse,
                privilege: "read_grants".to_string(),
            })
        );
        assert_eq!(
            authorizer.validate_grant_privilege(ResourceType::Warehouse, "not_a_privilege"),
            Err(InvalidGrantPrivilege {
                resource_type: ResourceType::Warehouse,
                privilege: "not_a_privilege".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn grant_authority_is_allowed_for_every_privilege() {
        let authorizer = AllowAllAuthorizer::default();
        let warehouse = ResolvedWarehouse::new_random();
        let target = GrantTarget::Warehouse(&warehouse);
        let bob = UserOrRoleId::User(UserId::new_unchecked("oidc", "bob"));
        let decisions = authorizer
            .are_allowed_grants(
                &RequestMetadata::new_unauthenticated(),
                None,
                &target,
                &[
                    GrantAuthorityCheck::entry("get_metadata", &bob, GrantOp::Grant),
                    GrantAuthorityCheck::any("not_a_privilege"),
                ],
            )
            .await
            .expect("allow-all never fails a grant-authority check");
        assert_eq!(decisions, vec![true, true]);
    }

    #[test]
    fn grants_are_stored_in_the_catalog() {
        // No `ManagesGrants` facet: grant rows land in the catalog's own table.
        assert!(AllowAllAuthorizer::default().grants().is_none());
    }
}
