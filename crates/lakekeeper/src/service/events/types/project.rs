use std::sync::Arc;

use crate::{
    ProjectId,
    api::RequestMetadata,
    service::{
        ResolvedWarehouse,
        authz::CatalogProjectAction,
        events::{
            APIEventContext,
            context::{AuthzChecked, Unresolved},
        },
    },
};

// ===== Project Events =====

/// Event emitted when a warehouse is created (within a project)
#[derive(Clone, Debug)]
pub struct CreateWarehouseEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub request_metadata: Arc<RequestMetadata>,
}

impl APIEventContext<ProjectId, Unresolved, CatalogProjectAction, AuthzChecked> {
    /// Emit warehouse created event
    pub(crate) fn emit_warehouse_created(self, warehouse: Arc<ResolvedWarehouse>) {
        let event = CreateWarehouseEvent {
            warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_created(event).await;
        });
    }
}
