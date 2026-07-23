use std::sync::Arc;

use crate::{
    ProjectId,
    api::RequestMetadata,
    service::{
        Tag, TagDefinition, TagDefinitionId,
        authz::{CatalogProjectAction, CatalogTagAction},
        events::{
            APIEventContext,
            context::{APIEventActions, AuthzChecked, Resolved, UserProvidedEntity},
        },
    },
};

// ===== Tag Definition Events =====

/// Event emitted when a tag definition is created
#[derive(Clone, Debug)]
pub struct CreateTagDefinitionEvent {
    pub tag_definition: Arc<TagDefinition>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a tag definition is updated
#[derive(Clone, Debug)]
pub struct UpdateTagDefinitionEvent {
    pub tag_definition: Arc<TagDefinition>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a tag definition is deleted
#[derive(Clone, Debug)]
pub struct DeleteTagDefinitionEvent {
    pub tag_definition: Arc<TagDefinition>,
    pub request_metadata: Arc<RequestMetadata>,
}

impl APIEventContext<ProjectId, Resolved<Arc<TagDefinition>>, CatalogProjectAction, AuthzChecked> {
    /// Emit tag definition created event
    pub(crate) fn emit_tag_definition_created(self) {
        let event = CreateTagDefinitionEvent {
            tag_definition: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.tag_definition_created(event).await;
        });
    }
}

impl
    APIEventContext<TagDefinitionId, Resolved<Arc<TagDefinition>>, CatalogTagAction, AuthzChecked>
{
    /// Emit tag definition updated event
    pub(crate) fn emit_tag_definition_updated(self) {
        let event = UpdateTagDefinitionEvent {
            tag_definition: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.tag_definition_updated(event).await;
        });
    }

    /// Emit tag definition deleted event
    pub(crate) fn emit_tag_definition_deleted(self) {
        let event = DeleteTagDefinitionEvent {
            tag_definition: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.tag_definition_deleted(event).await;
        });
    }
}

// ===== Tag Attachment Events =====

/// Event emitted when a tag is applied to a target
#[derive(Clone, Debug)]
pub struct TagAppliedEvent {
    pub tag: Arc<Tag>,
    pub tag_definition: Arc<TagDefinition>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a tag is removed from a target
#[derive(Clone, Debug)]
pub struct TagRemovedEvent {
    pub tag: Arc<Tag>,
    pub tag_definition: Arc<TagDefinition>,
    pub request_metadata: Arc<RequestMetadata>,
}

// Attachment endpoints authorize the tag's target (warehouse, namespace,
// tabular), so the context's entity and action types vary per target type.
// The events carry the tag itself; the emitters are generic over both.
impl<P, A> APIEventContext<P, Resolved<(Arc<Tag>, Arc<TagDefinition>)>, A, AuthzChecked>
where
    P: UserProvidedEntity,
    A: APIEventActions,
{
    /// Emit tag applied event
    pub(crate) fn emit_tag_applied(self) {
        let (tag, tag_definition) = self.resolved_entity.data;
        let event = TagAppliedEvent {
            tag,
            tag_definition,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.tag_applied(event).await;
        });
    }

    /// Emit tag removed event
    pub(crate) fn emit_tag_removed(self) {
        let (tag, tag_definition) = self.resolved_entity.data;
        let event = TagRemovedEvent {
            tag,
            tag_definition,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.tag_removed(event).await;
        });
    }
}
