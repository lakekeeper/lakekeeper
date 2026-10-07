use std::sync::Arc;

use crate::{
    WarehouseId,
    api::{
        RequestMetadata,
        management::v1::{tasks::ControlTasksRequest, warehouse::UndropTabularsRequest},
    },
    service::{
        ResolvedWarehouse, TabularId, ViewOrTableInfo,
        events::{
            APIEventContext,
            context::{AuthzChecked, Resolved, TabularAction, UserProvidedTabularsIDs},
        },
    },
};

// ===== Tabular Events =====

/// Event emitted when tables or views are undeleted
#[derive(Clone, Debug)]
pub struct UndropTabularEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub request: Arc<UndropTabularsRequest>,
    pub responses: Arc<Vec<ViewOrTableInfo>>,
    pub request_metadata: Arc<RequestMetadata>,
}

impl
    APIEventContext<
        UserProvidedTabularsIDs,
        Resolved<Arc<ResolvedWarehouse>>,
        TabularAction,
        AuthzChecked,
    >
{
    pub(crate) fn emit_tabular_undropped(
        self,
        warehouse: Arc<ResolvedWarehouse>,
        request: Arc<UndropTabularsRequest>,
        responses: Arc<Vec<ViewOrTableInfo>>,
    ) {
        let event = super::UndropTabularEvent {
            warehouse,
            request,
            responses,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.tabular_undropped(event).await;
        });
    }
}

impl APIEventContext<WarehouseId, Resolved<Vec<TabularId>>, ControlTasksRequest, AuthzChecked> {
    /// Cancelling a soft-deletion task undrops its tabular; emit what `undrop_tabulars` emits.
    pub(crate) fn emit_tabular_undropped_by_task_cancel(
        &self,
        warehouse: Arc<ResolvedWarehouse>,
        responses: Arc<Vec<ViewOrTableInfo>>,
    ) {
        let event = super::UndropTabularEvent {
            warehouse,
            request: Arc::new(UndropTabularsRequest {
                targets: self.resolved().clone(),
            }),
            responses,
            request_metadata: self.request_metadata.clone(),
        };
        let dispatcher = self.dispatcher.clone();
        tokio::spawn(async move {
            let () = dispatcher.tabular_undropped(event).await;
        });
    }
}
