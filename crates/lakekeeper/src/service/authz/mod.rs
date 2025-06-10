use std::collections::HashSet;

use axum::Router;
use strum::EnumIter;
use strum_macros::EnumString;

use super::{
    health::HealthExt, Actor, Catalog, NamespaceId, ProjectId, RoleId, SecretStore, State, TableId,
    TabularDetails, ViewId, WarehouseId,
};
use crate::{api::iceberg::v1::Result, request_metadata::RequestMetadata};

pub mod implementations;

use iceberg_ext::catalog::rest::ErrorModel;
pub use implementations::allow_all::AllowAllAuthorizer;

use crate::{api::ApiContext, service::authn::UserId};

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogUserAction {
    /// Can get all details of the user given its id
    CanRead,
    /// Can update the user.
    CanUpdate,
    /// Can delete this user
    CanDelete,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogServerAction {
    /// Can create items inside the server (can create Warehouses).
    CanCreateProject,
    /// Can update all users on this server.
    CanUpdateUsers,
    /// Can delete all users on this server.
    CanDeleteUsers,
    /// Can List all users on this server.
    CanListUsers,
    /// Can provision user
    CanProvisionUsers,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogProjectAction {
    CanCreateWarehouse,
    CanDelete,
    CanRename,
    CanGetMetadata,
    CanListWarehouses,
    CanIncludeInList,
    CanCreateRole,
    CanListRoles,
    CanSearchRoles,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogRoleAction {
    CanDelete,
    CanUpdate,
    CanRead,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogWarehouseAction {
    CanCreateNamespace,
    CanDelete,
    CanUpdateStorage,
    CanUpdateStorageCredential,
    CanGetMetadata,
    CanGetConfig,
    CanListNamespaces,
    CanUse,
    CanIncludeInList,
    CanDeactivate,
    CanActivate,
    CanRename,
    CanListDeletedTabulars,
    CanModifySoftDeletion,
    CanGetTaskQueueConfig,
    CanModifyTaskQueueConfig,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogNamespaceAction {
    CanCreateTable,
    CanCreateView,
    CanCreateNamespace,
    CanDelete,
    CanUpdateProperties,
    CanGetMetadata,
    CanListTables,
    CanListViews,
    CanListNamespaces,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogTableAction {
    CanDrop,
    CanWriteData,
    CanReadData,
    CanGetMetadata,
    CanCommit,
    CanRename,
    CanIncludeInList,
    CanUndrop,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogViewAction {
    CanDrop,
    CanGetMetadata,
    CanCommit,
    CanIncludeInList,
    CanRename,
    CanUndrop,
}

pub trait TableUuid {
    fn table_uuid(&self) -> TableId;
}

impl TableUuid for TableId {
    fn table_uuid(&self) -> TableId {
        *self
    }
}

impl TableUuid for TabularDetails {
    fn table_uuid(&self) -> TableId {
        self.ident
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ListProjectsResponse {
    /// List of projects that the user is allowed to see.
    Projects(HashSet<ProjectId>),
    /// The user is allowed to see all projects.
    All,
}

#[derive(Debug, Clone)]
pub enum NamespaceParent {
    Warehouse(WarehouseId),
    Namespace(NamespaceId),
}

#[async_trait::async_trait]
/// Interface to provide AuthZ functions to the catalog.
/// The provided `Actor` argument of all methods except `check_actor`
/// are assumed to be valid. Please ensure to call `check_actor` before, preferably
/// during Authentication.
/// `check_actor` ensures that the Actor itself is valid, especially that the principal
/// is allowed to assume the role.
pub trait Authorizer
where
    Self: Send + Sync + 'static + HealthExt + Clone,
{
    /// API Doc
    fn api_doc() -> utoipa::openapi::OpenApi;

    /// Router for the API
    fn new_router<C: Catalog, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>>;

    /// Check if the requested actor combination is allowed - especially if the user
    /// is allowed to assume the specified role.
    async fn check_actor(&self, actor: &Actor) -> Result<()>;

    /// Check if this server can be bootstrapped.
    async fn can_bootstrap(&self, metadata: &RequestMetadata) -> Result<()>;

    /// Perform bootstrapping, including granting the provided user the highest level of access.
    async fn bootstrap(&self, metadata: &RequestMetadata, is_operator: bool) -> Result<()>;

    /// Return Err only for internal errors.
    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse>;

    /// Search users
    async fn can_search_users(&self, metadata: &RequestMetadata) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: CatalogUserAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: CatalogRoleAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: CatalogServerAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
        action: CatalogProjectAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        action: CatalogWarehouseAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_namespace_action<A>(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceId,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogNamespaceAction> + std::fmt::Display + Send;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_table_action<A>(
        &self,
        metadata: &RequestMetadata,
        table_id: TableId,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogTableAction> + std::fmt::Display + Send;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_view_action<A>(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewId,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogViewAction> + std::fmt::Display + Send;

    /// Hook that is called when a user is deleted.
    async fn delete_user(&self, metadata: &RequestMetadata, user_id: UserId) -> Result<()>;

    /// Hook that is called when a new project is created.
    /// This is used to set up the initial permissions for the project.
    async fn create_role(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        parent_project_id: ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a role is deleted.
    /// This is used to clean up permissions for the role.
    async fn delete_role(&self, metadata: &RequestMetadata, role_id: RoleId) -> Result<()>;

    /// Hook that is called when a new project is created.
    /// This is used to set up the initial permissions for the project.
    async fn create_project(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a project is deleted.
    /// This is used to clean up permissions for the project.
    async fn delete_project(&self, metadata: &RequestMetadata, project_id: ProjectId)
        -> Result<()>;

    /// Hook that is called when a new warehouse is created.
    /// This is used to set up the initial permissions for the warehouse.
    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        parent_project_id: &ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a warehouse is deleted.
    /// This is used to clean up permissions for the warehouse.
    async fn delete_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
    ) -> Result<()>;

    /// Hook that is called when a new namespace is created.
    /// This is used to set up the initial permissions for the namespace.
    async fn create_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceId,
        parent: NamespaceParent,
    ) -> Result<()>;

    /// Hook that is called when a namespace is deleted.
    /// This is used to clean up permissions for the namespace.
    async fn delete_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceId,
    ) -> Result<()>;

    /// Hook that is called when a new table is created.
    /// This is used to set up the initial permissions for the table.
    async fn create_table(
        &self,
        metadata: &RequestMetadata,
        table_id: TableId,
        parent: NamespaceId,
    ) -> Result<()>;

    /// Hook that is called when a table is deleted.
    /// This is used to clean up permissions for the table.
    async fn delete_table(&self, table_id: TableId) -> Result<()>;

    /// Hook that is called when a new view is created.
    /// This is used to set up the initial permissions for the view.
    async fn create_view(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewId,
        parent: NamespaceId,
    ) -> Result<()>;

    /// Hook that is called when a view is deleted.
    /// This is used to clean up permissions for the view.
    async fn delete_view(&self, view_id: ViewId) -> Result<()>;

    async fn require_search_users(&self, metadata: &RequestMetadata) -> Result<()> {
        if self.can_search_users(metadata).await? {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                "Forbidden action search_users",
                "SearchUsersForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: CatalogUserAction,
    ) -> Result<()> {
        if self
            .is_allowed_user_action(metadata, user_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on user {user_id}"),
                "UserActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: CatalogRoleAction,
    ) -> Result<()> {
        if self
            .is_allowed_role_action(metadata, role_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on role {role_id}"),
                "RoleActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_server_action(
        &self,
        metadata: &RequestMetadata,
        action: CatalogServerAction,
    ) -> Result<()> {
        if self.is_allowed_server_action(metadata, action).await? {
            Ok(())
        } else {
            let actor = metadata.actor();
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on server for {actor}"),
                "ServerActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
        action: CatalogProjectAction,
    ) -> Result<()> {
        if self
            .is_allowed_project_action(metadata, project_id, action)
            .await?
        {
            Ok(())
        } else {
            let actor = metadata.actor();
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on project {project_id} for {actor}"),
                "ProjectActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        action: CatalogWarehouseAction,
    ) -> Result<()> {
        if self
            .is_allowed_warehouse_action(metadata, warehouse_id, action)
            .await?
        {
            Ok(())
        } else {
            let actor = metadata.actor();
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on warehouse {warehouse_id} for {actor}"),
                "WarehouseActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_namespace_action(
        &self,
        metadata: &RequestMetadata,
        // Outer error: Internal error that failed to fetch the namespace.
        // Ok(None): Namespace does not exist.
        // Ok(Some(namespace_id)): Namespace exists.
        namespace_id: Result<Option<NamespaceId>>,
        action: impl From<CatalogNamespaceAction> + std::fmt::Display + Send,
    ) -> Result<NamespaceId> {
        // It is important to throw the same error if the namespace does not exist (None) or if the action is not allowed,
        // to avoid leaking information about the existence of the namespace.
        let actor = metadata.actor();
        let msg = format!("Namespace not found or action {action} forbidden for {actor}");
        let typ = "NamespaceActionForbidden";

        match namespace_id {
            Ok(None) => {
                tracing::debug!("Namespace not found.");
                Err(ErrorModel::forbidden(msg, typ, None).into())
            }
            Ok(Some(namespace_id)) => {
                if self
                    .is_allowed_namespace_action(metadata, namespace_id, action)
                    .await?
                {
                    Ok(namespace_id)
                } else {
                    tracing::trace!("Namespace action forbidden.");
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }

    async fn require_table_action<T: TableUuid + Send>(
        &self,
        metadata: &RequestMetadata,
        table_id: Result<Option<T>>,
        action: impl From<CatalogTableAction> + std::fmt::Display + Send,
    ) -> Result<T> {
        let actor = metadata.actor();
        let msg = format!("Table not found or action {action} forbidden for {actor}");
        let typ = "TableActionForbidden";

        match table_id {
            Ok(None) => {
                tracing::debug!("Table not found, returning forbidden.");
                Err(ErrorModel::forbidden(msg, typ, None).into())
            }
            Ok(Some(table_id)) => {
                if self
                    .is_allowed_table_action(metadata, table_id.table_uuid(), action)
                    .await?
                {
                    Ok(table_id)
                } else {
                    tracing::trace!("Table action forbidden.");
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }

    async fn require_view_action(
        &self,
        metadata: &RequestMetadata,
        view_id: Result<Option<ViewId>>,
        action: impl From<CatalogViewAction> + std::fmt::Display + Send,
    ) -> Result<ViewId> {
        let actor = metadata.actor();
        let msg = format!("View not found or action {action} forbidden for {actor}");
        let typ = "ViewActionForbidden";

        match view_id {
            Ok(None) => {
                tracing::debug!("View not found, returning forbidden.");
                Err(ErrorModel::forbidden(msg, typ, None).into())
            }
            Ok(Some(view_id)) => {
                if self
                    .is_allowed_view_action(metadata, view_id, action)
                    .await?
                {
                    Ok(view_id)
                } else {
                    tracing::trace!("View action forbidden.");
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, RwLock},
    };

    use paste::paste;

    use super::*;
    use crate::service::health::Health;

    #[test]
    fn test_catalog_resource_action() {
        // server action
        assert_eq!(
            CatalogServerAction::CanCreateProject.to_string(),
            "can_create_project"
        );
        assert_eq!(
            CatalogServerAction::from_str("can_create_project").unwrap(),
            CatalogServerAction::CanCreateProject
        );
        // user action
        assert_eq!(CatalogUserAction::CanDelete.to_string(), "can_delete");
        assert_eq!(
            CatalogUserAction::from_str("can_delete").unwrap(),
            CatalogUserAction::CanDelete
        );
        // role action
        assert_eq!(CatalogRoleAction::CanUpdate.to_string(), "can_update");
        assert_eq!(
            CatalogRoleAction::from_str("can_update").unwrap(),
            CatalogRoleAction::CanUpdate
        );
        // project action
        assert_eq!(
            CatalogProjectAction::CanCreateWarehouse.to_string(),
            "can_create_warehouse"
        );
        assert_eq!(
            CatalogProjectAction::from_str("can_create_warehouse").unwrap(),
            CatalogProjectAction::CanCreateWarehouse
        );
        // warehouse action
        assert_eq!(
            CatalogWarehouseAction::CanCreateNamespace.to_string(),
            "can_create_namespace"
        );
        assert_eq!(
            CatalogWarehouseAction::from_str("can_create_namespace").unwrap(),
            CatalogWarehouseAction::CanCreateNamespace
        );
        // namespace action
        assert_eq!(
            CatalogNamespaceAction::CanCreateTable.to_string(),
            "can_create_table"
        );
        assert_eq!(
            CatalogNamespaceAction::from_str("can_create_table").unwrap(),
            CatalogNamespaceAction::CanCreateTable
        );
        // table action
        assert_eq!(CatalogTableAction::CanCommit.to_string(), "can_commit");
        assert_eq!(
            CatalogTableAction::from_str("can_commit").unwrap(),
            CatalogTableAction::CanCommit
        );
        // view action
        assert_eq!(
            CatalogViewAction::CanGetMetadata.to_string(),
            "can_get_metadata"
        );
        assert_eq!(
            CatalogViewAction::from_str("can_get_metadata").unwrap(),
            CatalogViewAction::CanGetMetadata
        );
    }

    #[derive(Clone, Debug)]
    /// A mock of the [`Authorizer`] that allows to hide objects.
    /// This is useful to test the behavior of the authorizer when objects are hidden.
    ///
    /// Objects that have been hidden will return `allowed: false` for any check request. This
    /// means all checks for an object that was *not* hidden return `allowed: true`.
    ///
    /// Some tests require blocking certain actions without hiding the object, for instance
    /// forbid an action on a namespace without hiding the namespace. This can be achieved by
    /// blocking the action.
    pub(crate) struct HidingAuthorizer {
        /// Strings encode `object_type:object_id` e.g. `namespace:id_of_namespace_to_hide`.
        pub(crate) hidden: Arc<RwLock<HashSet<String>>>,
        /// Strings encode `object_type:action` e.g. `namespace:can_create_table`.
        blocked_actions: Arc<RwLock<HashSet<String>>>,
    }

    impl HidingAuthorizer {
        pub(crate) fn new() -> Self {
            Self {
                hidden: Arc::new(RwLock::new(HashSet::new())),
                blocked_actions: Arc::new(RwLock::new(HashSet::new())),
            }
        }

        fn check_available(&self, object: &str) -> bool {
            !self.hidden.read().unwrap().contains(object)
        }

        pub(crate) fn hide(&self, object: &str) {
            self.hidden.write().unwrap().insert(object.to_string());
        }

        fn action_is_blocked(&self, action: &str) -> bool {
            self.blocked_actions.read().unwrap().contains(action)
        }

        pub(crate) fn block_action(&self, object: &str) {
            self.blocked_actions
                .write()
                .unwrap()
                .insert(object.to_string());
        }
    }

    #[async_trait::async_trait]
    impl HealthExt for HidingAuthorizer {
        async fn health(&self) -> Vec<Health> {
            vec![]
        }
        async fn update_health(&self) {
            // Do nothing
        }
    }
    #[async_trait::async_trait]
    impl Authorizer for HidingAuthorizer {
        fn api_doc() -> utoipa::openapi::OpenApi {
            AllowAllAuthorizer::api_doc()
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

        async fn list_projects(&self, _metadata: &RequestMetadata) -> Result<ListProjectsResponse> {
            Ok(ListProjectsResponse::All)
        }

        async fn can_search_users(&self, _metadata: &RequestMetadata) -> Result<bool> {
            Ok(true)
        }

        async fn is_allowed_user_action(
            &self,
            _metadata: &RequestMetadata,
            _user_id: &UserId,
            _action: CatalogUserAction,
        ) -> Result<bool> {
            Ok(true)
        }

        async fn is_allowed_role_action(
            &self,
            _metadata: &RequestMetadata,
            role_id: RoleId,
            action: CatalogRoleAction,
        ) -> Result<bool> {
            if self.action_is_blocked(format!("role:{action}").as_str()) {
                return Ok(false);
            }
            Ok(self.check_available(format!("role:{role_id}").as_str()))
        }

        async fn is_allowed_server_action(
            &self,
            _metadata: &RequestMetadata,
            _action: CatalogServerAction,
        ) -> Result<bool> {
            Ok(true)
        }

        async fn is_allowed_project_action(
            &self,
            _metadata: &RequestMetadata,
            project_id: &ProjectId,
            action: CatalogProjectAction,
        ) -> Result<bool> {
            if self.action_is_blocked(format!("project:{action}").as_str()) {
                return Ok(false);
            }
            Ok(self.check_available(format!("project:{project_id}").as_str()))
        }

        async fn is_allowed_warehouse_action(
            &self,
            _metadata: &RequestMetadata,
            warehouse_id: WarehouseId,
            action: CatalogWarehouseAction,
        ) -> Result<bool> {
            if self.action_is_blocked(format!("warehouse:{action}").as_str()) {
                return Ok(false);
            }
            Ok(self.check_available(format!("warehouse:{warehouse_id}").as_str()))
        }

        async fn is_allowed_namespace_action<A>(
            &self,
            _metadata: &RequestMetadata,
            namespace_id: NamespaceId,
            action: A,
        ) -> Result<bool>
        where
            A: From<CatalogNamespaceAction> + std::fmt::Display + Send,
        {
            if self.action_is_blocked(format!("namespace:{action}").as_str()) {
                return Ok(false);
            }
            Ok(self.check_available(format!("namespace:{namespace_id}").as_str()))
        }

        async fn is_allowed_table_action<A>(
            &self,
            _metadata: &RequestMetadata,
            table_id: TableId,
            action: A,
        ) -> Result<bool>
        where
            A: From<CatalogTableAction> + std::fmt::Display + Send,
        {
            if self.action_is_blocked(format!("table:{action}").as_str()) {
                return Ok(false);
            }
            Ok(self.check_available(format!("table:{table_id}").as_str()))
        }

        async fn is_allowed_view_action<A>(
            &self,
            _metadata: &RequestMetadata,
            view_id: ViewId,
            action: A,
        ) -> Result<bool>
        where
            A: From<CatalogViewAction> + std::fmt::Display + Send,
        {
            if self.action_is_blocked(format!("view:{action}").as_str()) {
                return Ok(false);
            }
            Ok(self.check_available(format!("view:{view_id}").as_str()))
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

    macro_rules! test_block_action {
        ($entity:ident, $action:path, $object_id:expr) => {
            paste! {
                #[tokio::test]
                async fn [<test_block_ $entity _action>]() {
                    let authz = HidingAuthorizer::new();

                    // Nothing is hidden, so the action is allowed.
                    assert!(authz
                        .[<is_allowed_ $entity _action>](
                            &RequestMetadata::new_unauthenticated(),
                            $object_id,
                            $action
                        )
                        .await
                        .unwrap());

                    // Generates "namespace:can_list_everything" for macro invoked with
                    // (namespace, CatalogNamespaceAction::CanListEverything)
                    authz.block_action(format!("{}:{}", stringify!($entity), $action).as_str());

                    // After blocking the action it must not be allowed anymore.
                    assert!(!authz
                        .[<is_allowed_ $entity _action>](
                            &RequestMetadata::new_unauthenticated(),
                            $object_id,
                            $action
                        )
                        .await
                        .unwrap());
                }
            }
        };
    }

    test_block_action!(role, CatalogRoleAction::CanDelete, RoleId::new_random());
    test_block_action!(
        project,
        CatalogProjectAction::CanRename,
        &ProjectId::new_random()
    );
    test_block_action!(
        warehouse,
        CatalogWarehouseAction::CanCreateNamespace,
        WarehouseId::new_random()
    );
    test_block_action!(
        namespace,
        CatalogNamespaceAction::CanListViews,
        NamespaceId::new_random()
    );
    test_block_action!(table, CatalogTableAction::CanDrop, TableId::new_random());
    test_block_action!(view, CatalogViewAction::CanDrop, ViewId::new_random());
}
