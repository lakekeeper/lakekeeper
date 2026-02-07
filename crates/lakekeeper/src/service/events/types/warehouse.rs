use std::sync::Arc;

use crate::{
    SecretId, WarehouseId,
    api::{
        RequestMetadata,
        management::v1::warehouse::{
            RenameWarehouseRequest, UpdateWarehouseCredentialRequest,
            UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest,
        },
    },
    service::ResolvedWarehouse,
};

// ===== Warehouse Events =====

/// Event emitted when a warehouse is created
#[derive(Clone, Debug)]
pub struct CreateWarehouseEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a warehouse is deleted
#[derive(Clone, Debug)]
pub struct DeleteWarehouseEvent {
    pub warehouse_id: WarehouseId,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when warehouse protection status changes
#[derive(Clone, Debug)]
pub struct SetWarehouseProtectionEvent {
    pub requested_protected: bool,
    pub updated_warehouse: Arc<ResolvedWarehouse>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a warehouse is renamed
#[derive(Clone, Debug)]
pub struct RenameWarehouseEvent {
    pub request: Arc<RenameWarehouseRequest>,
    pub updated_warehouse: Arc<ResolvedWarehouse>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when warehouse delete profile is updated
#[derive(Clone, Debug)]
pub struct UpdateWarehouseDeleteProfileEvent {
    pub request: Arc<UpdateWarehouseDeleteProfileRequest>,
    pub updated_warehouse: Arc<ResolvedWarehouse>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when warehouse storage configuration is updated
#[derive(Clone, Debug)]
pub struct UpdateWarehouseStorageEvent {
    pub request: Arc<UpdateWarehouseStorageRequest>,
    pub updated_warehouse: Arc<ResolvedWarehouse>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when warehouse storage credentials are updated
#[derive(Clone, Debug)]
pub struct UpdateWarehouseStorageCredentialEvent {
    pub request: Arc<UpdateWarehouseCredentialRequest>,
    pub old_secret_id: Option<SecretId>,
    pub updated_warehouse: Arc<ResolvedWarehouse>,
    pub request_metadata: Arc<RequestMetadata>,
}
