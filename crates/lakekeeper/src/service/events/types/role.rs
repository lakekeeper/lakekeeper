use std::sync::Arc;

use crate::{
    ProjectId,
    api::RequestMetadata,
    service::{
        ArcRole, RoleId,
        authn::UserIdRef,
        authz::{CatalogProjectAction, CatalogRoleAction},
        catalog_store::{ListRoleMembersResult, ListUserRoleAssignmentsResult},
        events::{
            APIEventContext,
            context::{AuthzChecked, Resolved},
        },
    },
};

// ===== Role Events =====

/// Event emitted when a role is created
#[derive(Clone, Debug)]
pub struct CreateRoleEvent {
    pub role: ArcRole,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a role is deleted
#[derive(Clone, Debug)]
pub struct DeleteRoleEvent {
    pub role: ArcRole,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a role is updated
#[derive(Clone, Debug)]
pub struct UpdateRoleEvent {
    pub role: ArcRole,
    pub request_metadata: Arc<RequestMetadata>,
}

// ===== Role Assignment Sync Events =====

/// Event emitted after a role's member list has been successfully synced by an
/// external provider (e.g. LDAP, SCIM).
///
/// `result` carries the authoritative post-sync member list so that listeners
/// can **populate** the role-members cache instead of merely invalidating it.
/// `result.role_id`, `result.project_id`, and `result.role_ident` identify the
/// role and allow listeners to also warm `IDENT_TO_ID_CACHE` without an extra
/// DB round-trip.
#[derive(Clone, Debug)]
pub struct RoleMembersSyncedEvent {
    /// Users added during this sync run.
    pub added: Arc<[UserIdRef]>,
    /// Users removed during this sync run.
    pub removed: Arc<[UserIdRef]>,
    /// Timestamp written to the role member sync log by this sync run.
    pub synced_at: chrono::DateTime<chrono::Utc>,
    /// The complete, authoritative member list after this sync run.
    pub result: Arc<ListRoleMembersResult>,
}

/// Event emitted after a user's role assignments have been successfully synced
/// by an external provider for one `(project_id, provider_id)` scope.
///
/// `result` carries the authoritative post-sync assignment list (all providers
/// merged) so that listeners can **populate** the user-assignments cache
/// instead of merely invalidating it.
#[derive(Clone, Debug)]
pub struct UserRoleAssignmentsSyncedEvent {
    /// The user whose assignments were synced.
    pub user_id: UserIdRef,
    /// IDs of roles newly assigned to the user during this sync run.
    pub added: Arc<[RoleId]>,
    /// IDs of roles removed from the user during this sync run.
    pub removed: Arc<[RoleId]>,
    /// Timestamp written to the user role sync log by this sync run.
    pub synced_at: chrono::DateTime<chrono::Utc>,
    /// The complete, authoritative assignment list after this sync run
    /// (covering all providers for this user).
    pub result: Arc<ListUserRoleAssignmentsResult>,
}

impl APIEventContext<ProjectId, Resolved<ArcRole>, CatalogProjectAction, AuthzChecked> {
    /// Emit role created event
    pub(crate) fn emit_role_created(self) {
        let event = CreateRoleEvent {
            role: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.role_created(event).await;
        });
    }
}

impl APIEventContext<RoleId, Resolved<ArcRole>, CatalogRoleAction, AuthzChecked> {
    /// Emit role deleted event
    pub(crate) fn emit_role_deleted(self) {
        let event = DeleteRoleEvent {
            role: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.role_deleted(event).await;
        });
    }

    /// Emit role updated event
    pub(crate) fn emit_role_updated(self) {
        let event = UpdateRoleEvent {
            role: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.role_updated(event).await;
        });
    }
}
