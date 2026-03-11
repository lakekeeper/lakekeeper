use std::sync::Arc;

use crate::{
    SecretId, WarehouseId,
    api::{
        RequestMetadata,
        management::v1::{
            task_queue::SetTaskQueueConfigRequest,
            warehouse::{
                RenameWarehouseRequest, UpdateWarehouseCredentialRequest,
                UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest,
            },
        },
    },
    service::{
        ResolvedWarehouse,
        authz::CatalogWarehouseAction,
        events::{
            APIEventContext,
            context::{AuthzChecked, Resolved},
        },
        tasks::TaskQueueName,
    },
};

// ===== Warehouse Events =====

/// Event emitted when a warehouse is deleted
#[derive(Clone, Debug)]
pub struct DeleteWarehouseEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
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

/// Event emitted when a warehouse task queue config is set
#[derive(Clone, Debug)]
pub struct SetTaskQueueConfigEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub queue_name: TaskQueueName,
    pub request: Arc<SetTaskQueueConfigRequest>,
    pub request_metadata: Arc<RequestMetadata>,
}

impl
    APIEventContext<
        WarehouseId,
        Resolved<Arc<ResolvedWarehouse>>,
        CatalogWarehouseAction,
        AuthzChecked,
    >
{
    /// Emit warehouse deleted event
    pub(crate) fn emit_warehouse_deleted(self) {
        let event = DeleteWarehouseEvent {
            warehouse: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_deleted(event).await;
        });
    }

    /// Emit warehouse renamed event
    pub(crate) fn emit_warehouse_renamed(
        self,
        request: Arc<RenameWarehouseRequest>,
        updated_warehouse: Arc<ResolvedWarehouse>,
    ) {
        let event = RenameWarehouseEvent {
            request,
            updated_warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_renamed(event).await;
        });
    }

    /// Emit warehouse delete profile updated event
    pub(crate) fn emit_warehouse_delete_profile_updated(
        self,
        request: Arc<UpdateWarehouseDeleteProfileRequest>,
        updated_warehouse: Arc<ResolvedWarehouse>,
    ) {
        let event = UpdateWarehouseDeleteProfileEvent {
            request,
            updated_warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_delete_profile_updated(event).await;
        });
    }

    /// Emit warehouse protection set event
    pub(crate) fn emit_warehouse_protection_set(
        self,
        requested_protected: bool,
        updated_warehouse: Arc<ResolvedWarehouse>,
    ) {
        let event = SetWarehouseProtectionEvent {
            requested_protected,
            updated_warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_protection_set(event).await;
        });
    }

    /// Emit warehouse storage updated event
    pub(crate) fn emit_warehouse_storage_updated(
        self,
        request: Arc<UpdateWarehouseStorageRequest>,
        updated_warehouse: Arc<ResolvedWarehouse>,
    ) {
        let event = UpdateWarehouseStorageEvent {
            request,
            updated_warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_storage_updated(event).await;
        });
    }

    /// Emit warehouse storage credential updated event
    pub(crate) fn emit_warehouse_storage_credential_updated(
        self,
        request: Arc<UpdateWarehouseCredentialRequest>,
        old_secret_id: Option<SecretId>,
        updated_warehouse: Arc<ResolvedWarehouse>,
    ) {
        let event = UpdateWarehouseStorageCredentialEvent {
            request,
            old_secret_id,
            updated_warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_storage_credential_updated(event).await;
        });
    }

    /// Emit task queue config set event
    pub(crate) fn emit_set_task_queue_config(
        self,
        queue_name: TaskQueueName,
        request: Arc<SetTaskQueueConfigRequest>,
    ) {
        let event = SetTaskQueueConfigEvent {
            warehouse: self.resolved_entity.data,
            queue_name,
            request,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.task_queue_config_set(event).await;
        });
    }
}
