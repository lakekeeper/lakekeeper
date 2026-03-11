use std::sync::Arc;

use crate::{
    api::RequestMetadata,
    service::{
        ArcProjectId, ServerId,
        events::{
            APIEventContext,
            context::{APIEventActions, AuthzChecked, Unresolved},
        },
    },
};

/// Event emitted when a project is created
#[derive(Clone, Debug)]
pub struct CreateProjectEvent {
    pub project_id: ArcProjectId,
    pub project_name: String,
    pub request_metadata: Arc<RequestMetadata>,
}

impl<A: APIEventActions> APIEventContext<ServerId, Unresolved, A, AuthzChecked> {
    pub(crate) fn emit_project_created(self, project_id: ArcProjectId, project_name: String) {
        let event = CreateProjectEvent {
            project_id,
            project_name,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.project_created(event).await;
        });
    }
}
