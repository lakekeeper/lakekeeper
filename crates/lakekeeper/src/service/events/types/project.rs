use crate::{
    ProjectId,
    api::ArcRequestMetadata,
    service::{
        ArcResolvedWarehouse,
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
    pub warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

impl APIEventContext<ProjectId, Unresolved, CatalogProjectAction, AuthzChecked> {
    /// Emit warehouse created event
    pub(crate) fn emit_warehouse_created(self, warehouse: ArcResolvedWarehouse) {
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
