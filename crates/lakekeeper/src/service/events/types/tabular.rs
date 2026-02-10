use std::sync::Arc;

use crate::{
    api::{RequestMetadata, management::v1::warehouse::UndropTabularsRequest},
    service::{
        ResolvedWarehouse, ViewOrTableInfo,
        events::{
            APIEventContext,
            context::{AuthzChecked, Resolved, TabularAction, UserProvidedTabularsById},
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
        UserProvidedTabularsById,
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
