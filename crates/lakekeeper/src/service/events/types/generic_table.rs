use std::sync::Arc;

use crate::{
    api::{
        RequestMetadata,
        data::v1::generic_tables::{CreateGenericTableRequest, RenameGenericTableRequest},
        iceberg::{types::DropParams, v1::DataAccessMode},
    },
    service::{
        GenericTableInfo, NamespaceWithParent, ResolvedWarehouse,
        authz::{CatalogGenericTableAction, CatalogNamespaceAction},
        events::{
            APIEventContext,
            context::{
                AuthzChecked, Resolved, ResolvedGenericTable, ResolvedNamespace,
                UserProvidedGenericTable, UserProvidedNamespace,
            },
        },
        storage::StoragePermissions,
    },
};

// ===== Generic Table Events =====

/// Event emitted when a generic table is created (within a namespace)
#[derive(Clone, Debug)]
pub struct CreateGenericTableEvent {
    pub namespace: ResolvedNamespace,
    pub generic_table: Arc<GenericTableInfo>,
    pub request_metadata: Arc<RequestMetadata>,
    pub request: Arc<CreateGenericTableRequest>,
}

/// Event emitted when a generic table is dropped
#[derive(Clone, Debug)]
pub struct DropGenericTableEvent {
    pub generic_table: ResolvedGenericTable,
    pub drop_params: DropParams,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a generic table's metadata is loaded
#[derive(Clone, Debug)]
pub struct LoadGenericTableEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub generic_table: Arc<GenericTableInfo>,
    pub storage_permissions: Option<StoragePermissions>,
    pub data_access: DataAccessMode,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a generic table is renamed
#[derive(Clone, Debug)]
pub struct RenameGenericTableEvent {
    pub source_generic_table: ResolvedGenericTable,
    pub destination_namespace: NamespaceWithParent,
    pub request: Arc<RenameGenericTableRequest>,
    pub request_metadata: Arc<RequestMetadata>,
}

pub type GenericTableEventContext = APIEventContext<
    UserProvidedGenericTable,
    Resolved<ResolvedGenericTable>,
    CatalogGenericTableAction,
>;

impl
    APIEventContext<
        UserProvidedNamespace,
        Resolved<ResolvedNamespace>,
        CatalogNamespaceAction,
        AuthzChecked,
    >
{
    /// Emit `generic_table_created` event
    pub(crate) fn emit_generic_table_created_async(
        self,
        generic_table: Arc<GenericTableInfo>,
        request: Arc<CreateGenericTableRequest>,
    ) {
        let event = CreateGenericTableEvent {
            namespace: self.resolved_entity.data,
            generic_table,
            request_metadata: self.request_metadata,
            request,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.generic_table_created(event).await;
        });
    }
}

impl
    APIEventContext<
        UserProvidedGenericTable,
        Resolved<ResolvedGenericTable>,
        CatalogGenericTableAction,
        AuthzChecked,
    >
{
    /// Emit `generic_table_loaded` event
    pub(crate) fn emit_generic_table_loaded_async(self, data_access: DataAccessMode) {
        let event = LoadGenericTableEvent {
            warehouse: self.resolved_entity.data.warehouse,
            generic_table: self.resolved_entity.data.generic_table,
            storage_permissions: self.resolved_entity.data.storage_permissions,
            data_access,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.generic_table_loaded(event).await;
        });
    }

    /// Emit `generic_table_dropped` event
    pub(crate) fn emit_generic_table_dropped_async(self, drop_parameters: DropParams) {
        let event = DropGenericTableEvent {
            generic_table: self.resolved_entity.data,
            drop_params: drop_parameters,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.generic_table_dropped(event).await;
        });
    }

    /// Emit `generic_table_renamed` event
    pub(crate) fn emit_generic_table_renamed_async(
        self,
        destination_namespace: NamespaceWithParent,
        request: Arc<RenameGenericTableRequest>,
    ) {
        let event = RenameGenericTableEvent {
            source_generic_table: self.resolved_entity.data,
            destination_namespace,
            request,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.generic_table_renamed(event).await;
        });
    }
}
