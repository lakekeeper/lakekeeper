use std::sync::Arc;

use iceberg::spec::ViewMetadataRef;
use iceberg_ext::catalog::rest::{CommitViewRequest, RenameTableRequest};
use lakekeeper_io::Location;

use crate::{
    api::{
        RequestMetadata,
        iceberg::{types::DropParams, v1::DataAccessMode},
    },
    service::{
        NamespaceWithParent, ResolvedWarehouse, ViewInfo,
        authz::CatalogViewAction,
        events::{
            APIEventContext,
            context::{AuthzChecked, Resolved, ResolvedView, UserProvidedView},
        },
    },
};

// ===== View Events =====

/// Event emitted when a view is updated
#[derive(Clone, Debug)]
pub struct CommitViewEvent {
    pub view: Arc<ViewInfo>,
    pub warehouse: Arc<ResolvedWarehouse>,
    pub request: Arc<CommitViewRequest>,
    pub view_commit: Arc<ViewEventTransition>,
    pub data_access: DataAccessMode,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a view is dropped
#[derive(Clone, Debug)]
pub struct DropViewEvent {
    pub view: ResolvedView,
    pub drop_params: DropParams,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a view is renamed
#[derive(Clone, Debug)]
pub struct RenameViewEvent {
    pub source_view: ResolvedView,
    pub destination_namespace: NamespaceWithParent,
    pub request: Arc<RenameTableRequest>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Load View Event - emitted when a user loads a view's metadata
#[derive(Clone, Debug)]
pub struct LoadViewEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub view: Arc<ViewInfo>,
    pub metadata: ViewMetadataRef,
    pub metadata_location: Arc<Location>,
}

/// View commit metadata containing old and new view states
#[derive(Debug, Clone)]
pub struct ViewEventTransition {
    pub old_metadata: ViewMetadataRef,
    pub new_metadata: ViewMetadataRef,
    pub old_metadata_location: Location,
    pub new_metadata_location: Location,
}

pub type ViewEventContext =
    APIEventContext<UserProvidedView, Resolved<ResolvedView>, CatalogViewAction>;

impl APIEventContext<UserProvidedView, Resolved<ResolvedView>, CatalogViewAction, AuthzChecked> {
    /// Emit view_loaded event using context fields
    pub(crate) fn emit_view_loaded_async(
        self,
        metadata: ViewMetadataRef,
        metadata_location: Arc<lakekeeper_io::Location>,
    ) {
        let event = LoadViewEvent {
            warehouse: self.resolved_entity.data.warehouse.clone(),
            view: self.resolved_entity.data.view.clone(),
            metadata,
            metadata_location,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.view_loaded(event).await;
        });
    }

    /// Emit view renamed event
    pub(crate) fn emit_view_renamed_async(
        self,
        destination_namespace: NamespaceWithParent,
        request: Arc<RenameTableRequest>,
    ) {
        let event = RenameViewEvent {
            source_view: self.resolved_entity.data,
            destination_namespace,
            request,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.view_renamed(event).await;
        });
    }

    /// Emit view dropped event
    pub(crate) fn emit_view_dropped_async(self, drop_parameters: DropParams) {
        let event = DropViewEvent {
            view: self.resolved_entity.data,
            drop_params: drop_parameters,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.view_dropped(event).await;
        });
    }

    pub(crate) fn emit_view_committed_async(
        self,
        view_commit: Arc<ViewEventTransition>,
        data_access: DataAccessMode,
        request: Arc<CommitViewRequest>,
    ) {
        let event = CommitViewEvent {
            view: self.resolved_entity.data.view,
            warehouse: self.resolved_entity.data.warehouse,
            view_commit,
            data_access,
            request_metadata: self.request_metadata,
            request,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.view_committed(event).await;
        });
    }
}
