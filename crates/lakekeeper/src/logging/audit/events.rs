use std::string::ToString;

use http::StatusCode;
use url::Url;

use crate::{
    ProjectId, WarehouseId,
    api::management::v1::user::{User, UserType},
    logging::audit::{AUDIT_LOG_EVENT_SOURCE, AuditContextData, AuditEvent},
    service::{NamespaceId, RoleId, TableId, UserId, ViewId, authz::CatalogTableAction},
};

// ============================================================================
// Debug Events
// ============================================================================

// Maybe improve AuditEvent derive macro to support debug level logging by adding a new attribute?

#[derive(Debug)]
pub struct UserCreatedDebugEvent {
    pub user: User,
}

impl AuditEvent for UserCreatedDebugEvent {
    fn action(&self) -> &'static str {
        "user_created_debug_event"
    }

    fn log<D: AuditContextData>(&self, ctx: &D) {
        let request_metadata = ctx.request_metadata();
        let user = request_metadata
            .user_id()
            .map_or("anonymous".to_string(), ToString::to_string);
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            request_id = %request_metadata.request_id(),
            user,
            action = self.action(),
            created_user = %self.user,
        );
    }

    fn log_without_context(&self) {
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            action = self.action(),
            created_user = %self.user,
        );
    }
}

#[derive(Debug)]
pub struct NATSConnectionEvent {
    pub nats_address: Url,
    pub user: String,
}

impl AuditEvent for NATSConnectionEvent {
    fn action(&self) -> &'static str {
        "nats_connection_event"
    }

    fn log<D: AuditContextData>(&self, ctx: &D) {
        let request_metadata = ctx.request_metadata();
        let user = request_metadata
            .user_id()
            .map_or("anonymous".to_string(), ToString::to_string);
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            request_id = %request_metadata.request_id(),
            user,
            action = self.action(),
            nats_address = %self.nats_address,
            nats_user = %self.user,
        );
    }

    fn log_without_context(&self) {
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            action = self.action(),
            nats_address = %self.nats_address,
            nats_user = %self.user,
        );
    }
}

#[derive(Debug)]
pub struct BufferingRequestBodyDebugEvent {
    pub method: String,
    pub path: String,
    pub request_id: String,
    pub request_body: String,
    pub user_agent: String,
}

impl AuditEvent for BufferingRequestBodyDebugEvent {
    fn action(&self) -> &'static str {
        "buffering_request_body_debug_event"
    }

    fn log<D: AuditContextData>(&self, ctx: &D) {
        let request_metadata = ctx.request_metadata();
        let user = request_metadata
            .user_id()
            .map_or("anonymous".to_string(), ToString::to_string);
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            user,
            action = self.action(),
            method = %self.method,
            path = %self.path,
            request_id = %self.request_id,
            request_body = %self.request_body,
            user_agent = %self.user_agent,
        );
    }

    fn log_without_context(&self) {
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            action = self.action(),
            method = %self.method,
            path = %self.path,
            request_id = %self.request_id,
            request_body = %self.request_body,
            user_agent = %self.user_agent,
        );
    }
}

#[derive(Debug)]
pub struct BufferingResponseBodyDebugEvent {
    pub method: String,
    pub path: String,
    pub request_id: String,
    pub user_agent: String,
    pub status: StatusCode,
    pub response_body: String,
}

impl AuditEvent for BufferingResponseBodyDebugEvent {
    fn action(&self) -> &'static str {
        "buffering_response_body_debug_event"
    }

    fn log<D: AuditContextData>(&self, ctx: &D) {
        let request_metadata = ctx.request_metadata();
        let user = request_metadata
            .user_id()
            .map_or("anonymous".to_string(), ToString::to_string);
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            user,
            action = self.action(),
            method = %self.method,
            path = %self.path,
            request_id = %self.request_id,
            user_agent = %self.user_agent,
            status = %self.status,
            response_body = %self.response_body,
        );
    }

    fn log_without_context(&self) {
        tracing::debug!(
            event_source = AUDIT_LOG_EVENT_SOURCE,
            action = self.action(),
            method = %self.method,
            path = %self.path,
            request_id = %self.request_id,
            user_agent = %self.user_agent,
            status = %self.status,
            response_body = %self.response_body,
        );
    }
}

// ============================================================================
// Authorization Events
// ============================================================================

/// Logged when authorization is denied for a request
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct AuthorizationDeniedEvent {
    pub denied_action: String,
    pub error: String,
}

// ============================================================================
// Bootstrap Events
// ============================================================================

/// Logged when bootstrap endpoint is accessed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct BootstrapEvent {}

/// Logged when a user is created during bootstrap
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct BootstrapCreateUserEvent {
    pub user_name: String,
    pub user_id: UserId,
    pub user_type: UserType,
    pub user_email: String,
}

// ============================================================================
// CatalogV1 - Table Events
// ============================================================================

/// Logged when `commit_tables` authorization is performed for a tabular
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CommitTablesAccessTabularEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub action: CatalogTableAction,
}

/// Logged when `commit_tables` fails
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CommitTablesFailedEvent {
    pub error: String,
}

/// Logged when a table is dropped
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DropTableEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub purge: bool,
}

/// Logged when a table is renamed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct RenameTableEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub new_name: String,
}

/// Logged when a table is registered
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct RegisterTableEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
}

/// Logged when a table is created
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CreateTableEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
}

/// Logged when table credentials are loaded
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct LoadTableCredentialsEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
}

/// Logged when a commit transaction is executed (multi-table commit)
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CommitTransactionEvent {
    pub warehouse_id: WarehouseId,
    pub table_count: usize,
}

/// Logged when a single table commit (update) is performed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CommitTableEvent {
    pub warehouse_id: WarehouseId,
    pub table_name: String,
}

// ============================================================================
// CatalogV1 - Config Events
// ============================================================================

/// Logged when catalog config is retrieved
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetConfigEvent {
    pub warehouse_id: WarehouseId,
}

// ============================================================================
// CatalogV1 - View Events
// ============================================================================

/// Logged when a view is created
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CreateViewEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
}

/// Logged when a view is dropped
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DropViewEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
    pub purge: bool,
}

/// Logged when a view is renamed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct RenameViewEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
    pub new_name: String,
}

/// Logged when a view is updated (`commit_view`)
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CommitViewEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
}

// ============================================================================
// CatalogV1 - Namespace Events
// ============================================================================

/// Logged when a namespace is created
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CreateNamespaceEvent {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
}

/// Logged when a namespace is dropped
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DropNamespaceEvent {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
}

/// Logged when namespace properties are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateNamespacePropertiesEvent {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
}

// ============================================================================
// S3 Sign Events
// ============================================================================

/// Logged when S3 signing is performed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct S3SignEvent {
    pub warehouse_id: WarehouseId,
    /// Table ID if known, otherwise "unknown"
    pub table_id: String,
}

// ============================================================================
// Management - User Events
// ============================================================================

/// Logged when a user is created
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CreateUserEvent {
    pub user_id: UserId,
    pub user_type: UserType,
    pub is_self_provision: bool,
}

/// Logged when a user is updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateUserEvent {
    pub user_id: UserId,
}

/// Logged when a user is deleted
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DeleteUserEvent {
    pub user_id: UserId,
}

// ============================================================================
// Management - Role Events
// ============================================================================

/// Logged when a role is created
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CreateRoleEvent {
    pub project_id: ProjectId,
    pub role_id: RoleId,
}

/// Logged when a role is updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateRoleEvent {
    pub role_id: RoleId,
}

/// Logged when a role is deleted
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DeleteRoleEvent {
    pub role_id: RoleId,
}

// ============================================================================
// Management - Project Events
// ============================================================================

/// Logged when a project is created
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CreateProjectEvent {
    pub project_id: ProjectId,
}

/// Logged when a project is renamed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct RenameProjectEvent {
    pub project_id: ProjectId,
}

/// Logged when a project is deleted
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DeleteProjectEvent {
    pub project_id: ProjectId,
}

// ============================================================================
// Management - Warehouse Events
// ============================================================================

/// Logged when a warehouse is created
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CreateWarehouseEvent {
    pub project_id: ProjectId,
    pub warehouse_name: String,
}

/// Logged when a warehouse is renamed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct RenameWarehouseEvent {
    pub warehouse_id: WarehouseId,
    pub new_name: String,
}

/// Logged when a warehouse is deleted
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DeleteWarehouseEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when a warehouse is activated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct ActivateWarehouseEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when a warehouse is deactivated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct DeactivateWarehouseEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when warehouse storage profile is updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateWarehouseStorageEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when warehouse storage credential is updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateWarehouseCredentialEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when warehouse delete profile is updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateWarehouseDeleteProfileEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when warehouse protection status is changed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetWarehouseProtectionEvent {
    pub warehouse_id: WarehouseId,
    pub protected: bool,
}

/// Logged when tabulars are undropped
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UndropTabularsEvent {
    pub warehouse_id: WarehouseId,
    pub tabular_count: usize,
}

// ============================================================================
// Management - Table Protection Events
// ============================================================================

/// Logged when table protection status is changed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetTableProtectionEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub protected: bool,
}

// ============================================================================
// Management - View Protection Events
// ============================================================================

/// Logged when view protection status is changed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetViewProtectionEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
    pub protected: bool,
}

// ============================================================================
// Management - Namespace Protection Events
// ============================================================================

/// Logged when namespace protection status is changed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetNamespaceProtectionEvent {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub protected: bool,
}

// ============================================================================
// Management - Task Queue Events
// ============================================================================

/// Logged when task queue configuration is updated for a warehouse
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetWarehouseTaskQueueConfigEvent {
    pub warehouse_id: WarehouseId,
    pub queue_name: String,
}

/// Logged when task queue configuration is updated for a project
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetProjectTaskQueueConfigEvent {
    pub project_id: ProjectId,
    pub queue_name: String,
}

/// Logged when tasks are controlled (cancel, retry, etc.) for a project
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct ControlProjectTasksEvent {
    pub project_id: ProjectId,
}

/// Logged when tasks are controlled (cancel, retry, etc.) for a warehouse
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct ControlWarehouseTasksEvent {
    pub warehouse_id: WarehouseId,
}

// ============================================================================
// OpenFGA Authorization Events
// ============================================================================

/// Logged when server assignments are read
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetServerAssignmentsEvent {}

/// Logged when server assignments are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateServerAssignmentsEvent {
    /// Number of assignments being written
    pub writes_count: usize,
    /// Number of assignments being deleted
    pub deletes_count: usize,
}

/// Logged when project assignments are read
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetProjectAssignmentsEvent {
    pub project_id: ProjectId,
}

/// Logged when project assignments are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateProjectAssignmentsEvent {
    pub project_id: ProjectId,
    pub writes_count: usize,
    pub deletes_count: usize,
}

/// Logged when warehouse assignments are read
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetWarehouseAssignmentsEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when warehouse assignments are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateWarehouseAssignmentsEvent {
    pub warehouse_id: WarehouseId,
    pub writes_count: usize,
    pub deletes_count: usize,
}

/// Logged when namespace assignments are read
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetNamespaceAssignmentsEvent {
    pub namespace_id: NamespaceId,
}

/// Logged when namespace assignments are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateNamespaceAssignmentsEvent {
    pub namespace_id: NamespaceId,
    pub writes_count: usize,
    pub deletes_count: usize,
}

/// Logged when table assignments are read
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetTableAssignmentsEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
}

/// Logged when table assignments are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateTableAssignmentsEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub writes_count: usize,
    pub deletes_count: usize,
}

/// Logged when view assignments are read
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetViewAssignmentsEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
}

/// Logged when view assignments are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateViewAssignmentsEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
    pub writes_count: usize,
    pub deletes_count: usize,
}

/// Logged when role assignments are read
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetRoleAssignmentsEvent {
    pub role_id: RoleId,
}

/// Logged when role assignments are updated
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct UpdateRoleAssignmentsEvent {
    pub role_id: RoleId,
    pub writes_count: usize,
    pub deletes_count: usize,
}

/// Logged when warehouse managed access is changed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetWarehouseManagedAccessEvent {
    pub warehouse_id: WarehouseId,
    pub managed_access: bool,
}

/// Logged when namespace managed access is changed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct SetNamespaceManagedAccessEvent {
    pub namespace_id: NamespaceId,
    pub managed_access: bool,
}

/// Logged when a permission check is performed
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct PermissionCheckEvent {
    /// The type of object being checked (server, project, warehouse, etc.)
    pub object_type: String,
    /// The action being checked
    pub action: String,
}

// ============================================================================
// OpenFGA Authorizer Actions Events
// ============================================================================

/// Logged when role authorizer actions are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetRoleActionsEvent {
    pub role_id: RoleId,
}

/// Logged when server authorizer actions are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetServerActionsEvent {}

/// Logged when project authorizer actions are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetProjectActionsEvent {
    pub project_id: ProjectId,
}

/// Logged when warehouse authorizer actions are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetWarehouseActionsEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when namespace authorizer actions are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetNamespaceActionsEvent {
    pub namespace_id: NamespaceId,
}

/// Logged when table authorizer actions are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetTableActionsEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
}

/// Logged when view authorizer actions are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetViewActionsEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
}

// ============================================================================
// OpenFGA Authorization Properties Events
// ============================================================================

/// Logged when warehouse authorization properties are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetWarehouseAuthPropertiesEvent {
    pub warehouse_id: WarehouseId,
}

/// Logged when namespace authorization properties are queried
#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct GetNamespaceAuthPropertiesEvent {
    pub namespace_id: NamespaceId,
}

