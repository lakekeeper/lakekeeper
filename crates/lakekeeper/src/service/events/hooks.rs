use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::TryFutureExt;

use super::types;

/// Collection of endpoint hooks that are invoked after successful operations
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

impl EndpointHookCollection {
    pub(crate) async fn transaction_committed(&self, event: types::CommitTransactionEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.transaction_committed(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on transaction_committed: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn table_dropped(&self, event: types::DropTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.table_dropped(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on table_dropped: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn table_registered(&self, event: types::RegisterTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.table_registered(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on table_registered: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn table_created(&self, event: types::CreateTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.table_created(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on table_created: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn table_renamed(&self, event: types::RenameTableEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.table_renamed(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on table_renamed: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn view_created(&self, event: types::CreateViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.view_created(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on view_created: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn view_committed(&self, event: types::CommitViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.view_committed(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on view_committed: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn view_dropped(&self, event: types::DropViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.view_dropped(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on view_dropped: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn view_renamed(&self, event: types::RenameViewEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.view_renamed(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on view_renamed: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn tabular_undropped(&self, event: types::UndropTabularEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.tabular_undropped(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on tabular_undropped: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn warehouse_created(&self, event: types::CreateWarehouseEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.warehouse_created(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on warehouse_created: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn warehouse_deleted(&self, event: types::DeleteWarehouseEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.warehouse_deleted(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on warehouse_deleted: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn warehouse_protection_set(&self, event: types::SetWarehouseProtectionEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.warehouse_protection_set(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on warehouse_protection_set: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn warehouse_renamed(&self, event: types::RenameWarehouseEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.warehouse_renamed(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on warehouse_renamed: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn warehouse_delete_profile_updated(
        &self,
        event: types::UpdateWarehouseDeleteProfileEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.warehouse_delete_profile_updated(event.clone())
                .map_err(|e| {
                    tracing::warn!(
                        "Hook '{}' encountered error on warehouse_delete_profile_updated: {e:?}",
                        hook.to_string()
                    );
                })
        }))
        .await;
    }

    pub(crate) async fn warehouse_storage_updated(
        &self,
        event: types::UpdateWarehouseStorageEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.warehouse_storage_updated(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on warehouse_storage_updated: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn warehouse_storage_credential_updated(
        &self,
        event: types::UpdateWarehouseStorageCredentialEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.warehouse_storage_credential_updated(event.clone())
                .map_err(|e| {
                    tracing::warn!(
                        "Hook '{}' encountered error on warehouse_storage_credential_updated: {e:?}",
                        hook.to_string()
                    );
                })
        }))
        .await;
    }

    pub(crate) async fn namespace_protection_set(&self, event: types::SetNamespaceProtectionEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.namespace_protection_set(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on namespace_protection_set: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn namespace_created(&self, event: types::CreateNamespaceEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.namespace_created(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on namespace_created: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn namespace_dropped(&self, event: types::DropNamespaceEvent) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.namespace_dropped(event.clone()).map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on namespace_dropped: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn namespace_properties_updated(
        &self,
        event: types::UpdateNamespacePropertiesEvent,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.namespace_properties_updated(event.clone())
                .map_err(|e| {
                    tracing::warn!(
                        "Hook '{}' encountered error on namespace_properties_updated: {e:?}",
                        hook.to_string()
                    );
                })
        }))
        .await;
    }
}

/// `EndpointHook` is a trait that allows for custom hooks to be executed after successful
/// completion of various operations.
///
/// # Naming Convention
///
/// All hook methods use past-tense verbs to indicate they fire after successful operations:
/// - `table_created` - fires after a table has been successfully created
/// - `table_dropped` - fires after a table has been successfully dropped
/// - etc.
///
/// This naming pattern enables future extension with additional lifecycle phases:
/// - Error hooks: `table_create_failed`, `table_drop_failed`
/// - Pre-operation hooks: `before_table_create`, `before_table_drop`
/// - Read hooks: `table_loaded`, `table_listed`
///
/// # Implementation Guidelines
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
    // ===== Table Hooks =====

    /// Invoked after a transaction with multiple table changes has been successfully committed
    async fn transaction_committed(
        &self,
        _event: types::CommitTransactionEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a table has been successfully dropped
    async fn table_dropped(&self, _event: types::DropTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a table has been successfully registered (imported with existing metadata)
    async fn table_registered(&self, _event: types::RegisterTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a table has been successfully created
    async fn table_created(&self, _event: types::CreateTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a table has been successfully renamed
    async fn table_renamed(&self, _event: types::RenameTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== View Hooks =====

    /// Invoked after a view has been successfully created
    async fn view_created(&self, _event: types::CreateViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a view has been successfully committed (updated)
    async fn view_committed(&self, _event: types::CommitViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a view has been successfully dropped
    async fn view_dropped(&self, _event: types::DropViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a view has been successfully renamed
    async fn view_renamed(&self, _event: types::RenameViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Tabular Hooks =====

    /// Invoked after tables or views have been successfully undeleted
    async fn tabular_undropped(&self, _event: types::UndropTabularEvent) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Warehouse Hooks =====

    /// Invoked after a warehouse has been successfully created
    async fn warehouse_created(&self, _event: types::CreateWarehouseEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a warehouse has been successfully deleted
    async fn warehouse_deleted(&self, _event: types::DeleteWarehouseEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after warehouse protection status has been successfully changed
    async fn warehouse_protection_set(
        &self,
        _event: types::SetWarehouseProtectionEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a warehouse has been successfully renamed
    async fn warehouse_renamed(&self, _event: types::RenameWarehouseEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after warehouse delete profile has been successfully updated
    async fn warehouse_delete_profile_updated(
        &self,
        _event: types::UpdateWarehouseDeleteProfileEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after warehouse storage configuration has been successfully updated
    async fn warehouse_storage_updated(
        &self,
        _event: types::UpdateWarehouseStorageEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after warehouse storage credentials have been successfully updated
    async fn warehouse_storage_credential_updated(
        &self,
        _event: types::UpdateWarehouseStorageCredentialEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Namespace Hooks =====

    /// Invoked after namespace protection status has been successfully changed
    async fn namespace_protection_set(
        &self,
        _event: types::SetNamespaceProtectionEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a namespace has been successfully created
    async fn namespace_created(&self, _event: types::CreateNamespaceEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after a namespace has been successfully dropped
    async fn namespace_dropped(&self, _event: types::DropNamespaceEvent) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked after namespace properties have been successfully updated
    async fn namespace_properties_updated(
        &self,
        _event: types::UpdateNamespacePropertiesEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
