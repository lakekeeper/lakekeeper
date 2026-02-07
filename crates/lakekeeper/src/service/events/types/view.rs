use std::sync::Arc;

use iceberg::spec::{ViewMetadata, ViewMetadataRef};
use iceberg_ext::catalog::rest::{CommitViewRequest, CreateViewRequest, RenameTableRequest};
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::{
        RequestMetadata,
        iceberg::{
            types::DropParams,
            v1::{DataAccessMode, NamespaceParameters, ViewParameters},
        },
    },
    service::ViewId,
};

// ===== View Events =====

/// Event emitted when a view is created
#[derive(Clone, Debug)]
pub struct CreateViewEvent {
    pub warehouse_id: WarehouseId,
    pub parameters: NamespaceParameters,
    pub request: Arc<CreateViewRequest>,
    pub metadata: Arc<ViewMetadata>,
    pub metadata_location: Arc<Location>,
    pub data_access: DataAccessMode,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a view is updated
#[derive(Clone, Debug)]
pub struct CommitViewEvent {
    pub warehouse_id: WarehouseId,
    pub parameters: ViewParameters,
    pub request: Arc<CommitViewRequest>,
    pub view_commit: Arc<ViewEventTransition>,
    pub data_access: DataAccessMode,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a view is dropped
#[derive(Clone, Debug)]
pub struct DropViewEvent {
    pub warehouse_id: WarehouseId,
    pub parameters: ViewParameters,
    pub drop_params: DropParams,
    pub view_id: ViewId,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a view is renamed
#[derive(Clone, Debug)]
pub struct RenameViewEvent {
    pub warehouse_id: WarehouseId,
    pub view_id: ViewId,
    pub request: Arc<RenameTableRequest>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// View commit metadata containing old and new view states
#[derive(Debug, Clone)]
pub struct ViewEventTransition {
    pub old_metadata: ViewMetadataRef,
    pub new_metadata: ViewMetadataRef,
    pub old_metadata_location: Location,
    pub new_metadata_location: Location,
}
