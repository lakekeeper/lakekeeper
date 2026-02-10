use std::sync::Arc;

use crate::{
    ProjectId,
    api::RequestMetadata,
    service::events::{
        APIEventContext,
        context::{APIEventAction, AuthzChecked, Unresolved, UserProvidedEntityServer},
    },
};

/// Event emitted when a project is created
#[derive(Clone, Debug)]
pub struct CreateProjectEvent {
    pub project_id: ProjectId,
    pub project_name: String,
    pub request_metadata: Arc<RequestMetadata>,
}

impl<A: APIEventAction> APIEventContext<UserProvidedEntityServer, Unresolved, A, AuthzChecked> {
    pub(crate) fn emit_project_created(self, project_id: ProjectId, project_name: String) {
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
