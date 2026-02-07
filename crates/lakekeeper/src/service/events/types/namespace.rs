use std::sync::Arc;

use iceberg_ext::catalog::rest::UpdateNamespacePropertiesResponse;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{NamespaceId, NamespaceWithParent},
};

// ===== Namespace Events =====

/// Event emitted when a namespace is created
#[derive(Clone, Debug)]
pub struct CreateNamespaceEvent {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceWithParent,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a namespace is dropped
#[derive(Clone, Debug)]
pub struct DropNamespaceEvent {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when namespace protection status changes
#[derive(Clone, Debug)]
pub struct SetNamespaceProtectionEvent {
    pub requested_protected: bool,
    pub updated_namespace: NamespaceWithParent,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when namespace properties are updated
#[derive(Clone, Debug)]
pub struct UpdateNamespacePropertiesEvent {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceWithParent,
    pub updated_properties: Arc<UpdateNamespacePropertiesResponse>,
    pub request_metadata: Arc<RequestMetadata>,
}
