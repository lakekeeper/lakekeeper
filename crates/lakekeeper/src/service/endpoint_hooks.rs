use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::TryFutureExt;
use iceberg::{
    TableIdent,
    spec::{TableMetadataRef, ViewMetadata, ViewMetadataRef},
};
use iceberg_ext::catalog::rest::{
    CommitTransactionRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest,
    RegisterTableRequest, RenameTableRequest, UpdateNamespacePropertiesResponse,
};
use lakekeeper_io::Location;

use crate::{
    SecretId, WarehouseId,
    api::{
        RequestMetadata,
        iceberg::{
            types::DropParams,
            v1::{DataAccessMode, NamespaceParameters, TableParameters, ViewParameters},
        },
        management::v1::warehouse::{
            RenameWarehouseRequest, UndropTabularsRequest, UpdateWarehouseCredentialRequest,
            UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest,
        },
    },
    server::tables::CommitContext,
    service::{
        NamespaceId, NamespaceWithParent, ResolvedWarehouse, TableId, ViewId, ViewOrTableInfo,
    },
};

/// Event structs for endpoint hooks
pub mod events {
    use super::{
        Arc, CommitContext, CommitTransactionRequest, CommitViewRequest, CreateTableRequest,
        CreateViewRequest, DataAccessMode, Debug, DropParams, Location, NamespaceId,
        NamespaceParameters, NamespaceWithParent, RegisterTableRequest, RenameTableRequest,
        RenameWarehouseRequest, RequestMetadata, ResolvedWarehouse, SecretId, TableId,
        TableIdentToIdFn, TableMetadataRef, TableParameters, UndropTabularsRequest,
        UpdateNamespacePropertiesResponse, UpdateWarehouseCredentialRequest,
        UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest, ViewId, ViewMetadata,
        ViewOrTableInfo, ViewParameters, WarehouseId,
    };

    // ===== Table Events =====
    /// Event emitted when a transaction is committed containing multiple table changes
    #[derive(Clone)]
    pub struct CommitTransactionEvent {
        pub warehouse_id: WarehouseId,
        pub request: Arc<CommitTransactionRequest>,
        pub commits: Arc<Vec<CommitContext>>,
        pub table_ident_to_id_fn: Arc<TableIdentToIdFn>,
        pub request_metadata: Arc<RequestMetadata>,
    }

    impl std::fmt::Debug for CommitTransactionEvent {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("CommitTransactionEvent")
                .field("warehouse_id", &self.warehouse_id)
                .field("request", &self.request)
                .field("commits_len", &self.commits.len())
                .field("request_metadata", &self.request_metadata)
                .field("table_ident_to_id_fn", &"TableIdentToIdFn(...)")
                .finish()
        }
    }

    /// Event emitted when a table is created
    #[derive(Clone, Debug)]
    pub struct CreateTableEvent {
        pub warehouse_id: WarehouseId,
        pub parameters: NamespaceParameters,
        pub request: Arc<CreateTableRequest>,
        pub metadata: TableMetadataRef,
        pub metadata_location: Option<Arc<Location>>,
        pub data_access: DataAccessMode,
        pub request_metadata: Arc<RequestMetadata>,
    }

    /// Event emitted when a table is registered (imported with existing metadata)
    #[derive(Clone, Debug)]
    pub struct RegisterTableEvent {
        pub warehouse_id: WarehouseId,
        pub parameters: NamespaceParameters,
        pub request: Arc<RegisterTableRequest>,
        pub metadata: TableMetadataRef,
        pub metadata_location: Arc<Location>,
        pub request_metadata: Arc<RequestMetadata>,
    }

    /// Event emitted when a table is dropped
    #[derive(Clone, Debug)]
    pub struct DropTableEvent {
        pub warehouse_id: WarehouseId,
        pub parameters: TableParameters,
        pub drop_params: DropParams,
        pub table_id: TableId,
        pub request_metadata: Arc<RequestMetadata>,
    }

    /// Event emitted when a table is renamed
    #[derive(Clone, Debug)]
    pub struct RenameTableEvent {
        pub warehouse_id: WarehouseId,
        pub table_id: TableId,
        pub request: Arc<RenameTableRequest>,
        pub request_metadata: Arc<RequestMetadata>,
    }

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
        pub view_commit: Arc<super::ViewCommit>,
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

    // ===== Tabular Events =====

    /// Event emitted when tables or views are undeleted
    #[derive(Clone, Debug)]
    pub struct UndropTabularEvent {
        pub warehouse_id: WarehouseId,
        pub request: Arc<UndropTabularsRequest>,
        pub responses: Arc<Vec<ViewOrTableInfo>>,
        pub request_metadata: Arc<RequestMetadata>,
    }

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
}

#[derive(Clone)]
pub struct EndpointHookCollection(pub(crate) Vec<Arc<dyn EndpointHook>>);

impl core::fmt::Debug for EndpointHookCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Hooks").field(&self.0.len()).finish()
    }
}

impl EndpointHookCollection {
    #[must_use]
    pub fn new(hooks: Vec<Arc<dyn EndpointHook>>) -> Self {
        Self(hooks)
    }

    pub fn append(&mut self, hook: Arc<dyn EndpointHook>) -> &mut Self {
        self.0.push(hook);
        self
    }
}

impl Display for EndpointHookCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EndpointHookCollection with [")?;
        for idx in 0..self.0.len() {
            if idx == self.0.len() - 1 {
                write!(f, "{}", self.0[idx])?;
            } else {
                write!(f, "{}, ", self.0[idx])?;
            }
        }
        write!(f, "]")
    }
}

#[derive(Debug, Clone)]
pub struct ViewCommit {
    pub old_metadata: ViewMetadataRef,
    pub new_metadata: ViewMetadataRef,
    pub old_metadata_location: Location,
    pub new_metadata_location: Location,
}

/// Function type used by hooks to resolve a `TableIdent` to its `TableId`.
/// Implementations should be cheap and non-blocking.
/// Note: Hooks receive this as a borrowed reference valid only for the duration of the call.
/// Do not store it for use outside the async method invocation.
pub type TableIdentToIdFn = dyn Fn(&TableIdent) -> Option<TableId> + Send + Sync;

impl EndpointHookCollection {
    pub(crate) async fn commit_transaction(&self, event: events::CommitTransactionEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.commit_transaction(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on commit_transaction: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn drop_table(&self, event: events::DropTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.drop_table(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on drop_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn register_table(&self, event: events::RegisterTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.register_table(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on register_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn create_table(&self, event: events::CreateTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.create_table(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on create_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn rename_table(&self, event: events::RenameTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.rename_table(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on rename_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn create_view(&self, event: events::CreateViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.create_view(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on create_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn commit_view(&self, event: events::CommitViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.commit_view(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on commit_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn drop_view(&self, event: events::DropViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.drop_view(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on drop_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn rename_view(&self, event: events::RenameViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.rename_view(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on rename_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn undrop_tabular(&self, event: events::UndropTabularEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.undrop_tabular(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on undrop_tabular: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn create_warehouse(&self, event: events::CreateWarehouseEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.create_warehouse(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on create_warehouse: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn delete_warehouse(&self, event: events::DeleteWarehouseEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.delete_warehouse(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on delete_warehouse: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn set_warehouse_protection(
        &self,
        event: events::SetWarehouseProtectionEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.set_warehouse_protection(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on set_warehouse_protection: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn rename_warehouse(&self, event: events::RenameWarehouseEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.rename_warehouse(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on rename_warehouse: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn update_warehouse_delete_profile(
        &self,
        event: events::UpdateWarehouseDeleteProfileEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.update_warehouse_delete_profile(event.clone())
                .map_err(|e| {
                    tracing::warn!(
                        "Hook '{}' encountered error on update_warehouse_delete_profile: {e:?}",
                        hook.to_string()
                    );
                })
        }))
        .await;
    }

    pub(crate) async fn update_warehouse_storage(
        &self,
        event: events::UpdateWarehouseStorageEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.update_warehouse_storage(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on update_warehouse_storage: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn update_warehouse_storage_credential(
        &self,
        event: events::UpdateWarehouseStorageCredentialEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.update_warehouse_storage_credential(event.clone())
                .map_err(|e| {
                    tracing::warn!(
                        "Hook '{}' encountered error on update_warehouse_storage_credential: {e:?}",
                        hook.to_string()
                    );
                })
        }))
        .await;
    }

    pub(crate) async fn set_namespace_protection(
        &self,
        event: events::SetNamespaceProtectionEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.set_namespace_protection(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on set_namespace_protection: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn create_namespace(&self, event: events::CreateNamespaceEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.create_namespace(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on create_namespace: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn drop_namespace(&self, event: events::DropNamespaceEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.drop_namespace(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on drop_namespace: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn update_namespace_properties(
        &self,
        event: events::UpdateNamespacePropertiesEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.update_namespace_properties(event.clone())
                .map_err(|e| {
                    tracing::warn!(
                        "Hook '{}' encountered error on update_namespace_properties: {e:?}",
                        hook.to_string()
                    );
                })
        }))
        .await;
    }
}

/// `EndpointHook` is a trait that allows for custom hooks to be executed within the context of
/// various endpoints.
///
/// The default implementation of every hook does nothing. Override any function if you want to
/// implement it.
///
/// An implementation should be light-weight, ideally every longer running task is deferred to a
/// background task via a channel or is spawned as a tokio task.
///
/// The `EndpointHook` are passed into the services via the [`EndpointHookCollection`]. If you want
/// to provide your own implementation, you'll have to fork and modify the main function to include
/// your hooks.
///
/// If the hook fails, it will be logged, but the request will continue to process. This is to ensure
/// that the request is not blocked by a hook failure.
#[async_trait::async_trait]
pub trait EndpointHook: Send + Sync + Debug + Display {
    async fn commit_transaction(
        &self,
        _event: events::CommitTransactionEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn drop_table(&self, _event: events::DropTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn register_table(&self, _event: events::RegisterTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_table(&self, _event: events::CreateTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn rename_table(&self, _event: events::RenameTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_view(&self, _event: events::CreateViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn commit_view(&self, _event: events::CommitViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn drop_view(&self, _event: events::DropViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn rename_view(&self, _event: events::RenameViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn undrop_tabular(&self, _event: events::UndropTabularEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_warehouse(&self, _event: events::CreateWarehouseEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_warehouse(&self, _event: events::DeleteWarehouseEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn set_warehouse_protection(
        &self,
        _event: events::SetWarehouseProtectionEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn rename_warehouse(&self, _event: events::RenameWarehouseEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_warehouse_delete_profile(
        &self,
        _event: events::UpdateWarehouseDeleteProfileEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_warehouse_storage(
        &self,
        _event: events::UpdateWarehouseStorageEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_warehouse_storage_credential(
        &self,
        _event: events::UpdateWarehouseStorageCredentialEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn set_namespace_protection(
        &self,
        _event: events::SetNamespaceProtectionEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_namespace(&self, _event: events::CreateNamespaceEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn drop_namespace(&self, _event: events::DropNamespaceEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_namespace_properties(
        &self,
        _event: events::UpdateNamespacePropertiesEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
