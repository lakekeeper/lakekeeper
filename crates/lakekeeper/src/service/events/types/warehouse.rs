use std::sync::Arc;

use crate::{
    SecretId, WarehouseId,
    api::{
        ArcRequestMetadata,
        management::v1::{
            task_queue::SetTaskQueueConfigRequest,
            warehouse::{
                RenameWarehouseRequest, UpdateWarehouseCredentialRequest,
                UpdateWarehouseDeleteProfileRequest, UpdateWarehouseFormatVersionPolicyRequest,
                UpdateWarehouseStorageRequest,
            },
        },
    },
    service::{
        ArcResolvedWarehouse,
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
    pub warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when warehouse protection status changes
#[derive(Clone, Debug)]
pub struct SetWarehouseProtectionEvent {
    pub requested_protected: bool,
    pub updated_warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when the warehouse managed-by marker changes
#[derive(Clone, Debug)]
pub struct SetWarehouseManagedByEvent {
    pub requested_managed_by: crate::service::ManagedBy,
    pub updated_warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when a warehouse is renamed
#[derive(Clone, Debug)]
pub struct RenameWarehouseEvent {
    pub request: Arc<RenameWarehouseRequest>,
    pub updated_warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when warehouse delete profile is updated
#[derive(Clone, Debug)]
pub struct UpdateWarehouseDeleteProfileEvent {
    pub request: Arc<UpdateWarehouseDeleteProfileRequest>,
    pub updated_warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when warehouse format version policy is updated
#[derive(Clone, Debug)]
pub struct UpdateWarehouseFormatVersionPolicyEvent {
    pub request: Arc<UpdateWarehouseFormatVersionPolicyRequest>,
    pub updated_warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when warehouse storage configuration is updated
#[derive(Clone, Debug)]
pub struct UpdateWarehouseStorageEvent {
    pub request: Arc<UpdateWarehouseStorageRequest>,
    pub updated_warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when warehouse storage credentials are updated
#[derive(Clone, Debug)]
pub struct UpdateWarehouseStorageCredentialEvent {
    pub request: Arc<UpdateWarehouseCredentialRequest>,
    pub old_secret_id: Option<SecretId>,
    pub updated_warehouse: ArcResolvedWarehouse,
    pub request_metadata: ArcRequestMetadata,
}

/// Event emitted when a warehouse task queue config is set
#[derive(Clone, Debug)]
pub struct SetTaskQueueConfigEvent {
    pub warehouse: ArcResolvedWarehouse,
    pub queue_name: TaskQueueName,
    pub request: Arc<SetTaskQueueConfigRequest>,
    pub request_metadata: ArcRequestMetadata,
}

impl
    APIEventContext<
        WarehouseId,
        Resolved<ArcResolvedWarehouse>,
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
        updated_warehouse: ArcResolvedWarehouse,
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
        updated_warehouse: ArcResolvedWarehouse,
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
        updated_warehouse: ArcResolvedWarehouse,
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

    /// Emit warehouse format version policy updated event
    pub(crate) fn emit_warehouse_format_version_policy_updated(
        self,
        request: Arc<UpdateWarehouseFormatVersionPolicyRequest>,
        updated_warehouse: ArcResolvedWarehouse,
    ) {
        let event = UpdateWarehouseFormatVersionPolicyEvent {
            request,
            updated_warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher
                .warehouse_format_version_policy_updated(event)
                .await;
        });
    }

    /// Emit warehouse storage updated event
    pub(crate) fn emit_warehouse_storage_updated(
        self,
        request: Arc<UpdateWarehouseStorageRequest>,
        updated_warehouse: ArcResolvedWarehouse,
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
        updated_warehouse: ArcResolvedWarehouse,
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

// The managed-by marker is set via an instance-admin action (not a
// `CatalogWarehouseAction`) and without resolving the warehouse for authz, so
// this emitter is generic over the action and resolution state. It needs only
// the request metadata plus the freshly-written warehouse.
impl<R, A, Z> APIEventContext<WarehouseId, R, A, Z>
where
    R: crate::service::events::context::ResolutionState,
    A: crate::service::events::context::APIEventActions,
    Z: crate::service::events::context::AuthzState,
{
    /// Emit warehouse managed-by set event
    pub(crate) fn emit_warehouse_managed_by_set(
        self,
        requested_managed_by: crate::service::ManagedBy,
        updated_warehouse: ArcResolvedWarehouse,
    ) {
        let event = SetWarehouseManagedByEvent {
            requested_managed_by,
            updated_warehouse,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.warehouse_managed_by_set(event).await;
        });
    }
}
