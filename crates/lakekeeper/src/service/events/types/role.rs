use std::sync::Arc;

use crate::{
    ProjectId,
    api::RequestMetadata,
    service::{
        ArcRole, RoleId,
        authz::{CatalogProjectAction, CatalogRoleAction},
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
