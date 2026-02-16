use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::TryFutureExt;

use super::types;

/// Macro to dispatch events to all listeners with error logging.
///
/// Reduces boilerplate by handling the common pattern of:
/// - Cloning the event for each listener
/// - Calling the listener method
/// - Logging errors without propagating them
macro_rules! dispatch_event {
    ($self:ident, $method:ident, $event:expr) => {
        futures::future::join_all($self.0.iter().map(|listener| {
            listener.$method($event.clone()).map_err(|e| {
                tracing::warn!(
                    "Listener '{}' encountered error on {}: {e:?}",
                    listener.to_string(),
                    stringify!($method),
                );
            })
        }))
        .await;
    };
}

/// Collection of event listeners that are invoked after successful operations
#[derive(Clone)]
pub struct EventDispatcher(pub(crate) Vec<Arc<dyn EventListener>>);

impl core::fmt::Debug for EventDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Listeners").field(&self.0.len()).finish()
    }
}

impl EventDispatcher {
    #[must_use]
    pub fn new(listeners: Vec<Arc<dyn EventListener>>) -> Self {
        Self(listeners)
    }

    pub fn append(&mut self, listener: Arc<dyn EventListener>) -> &mut Self {
        self.0.push(listener);
        self
    }
}

impl Display for EventDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventDispatcher with [")?;
        for (idx, hook) in self.0.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{hook}")?;
        }
        write!(f, "]")
    }
}

impl EventDispatcher {
    pub(crate) async fn transaction_committed(&self, event: types::CommitTransactionEvent) {
        dispatch_event!(self, transaction_committed, event);
    }

    pub(crate) async fn table_dropped(&self, event: types::DropTableEvent) {
        dispatch_event!(self, table_dropped, event);
    }

    pub(crate) async fn table_registered(&self, event: types::RegisterTableEvent) {
        dispatch_event!(self, table_registered, event);
    }

    pub(crate) async fn table_created(&self, event: types::CreateTableEvent) {
        dispatch_event!(self, table_created, event);
    }

    pub(crate) async fn table_renamed(&self, event: types::RenameTableEvent) {
        dispatch_event!(self, table_renamed, event);
    }

    pub(crate) async fn table_loaded(&self, event: types::LoadTableEvent) {
        dispatch_event!(self, table_loaded, event);
    }

    pub(crate) async fn view_created(&self, event: types::CreateViewEvent) {
        dispatch_event!(self, view_created, event);
    }

    pub(crate) async fn view_committed(&self, event: types::CommitViewEvent) {
        dispatch_event!(self, view_committed, event);
    }

    pub(crate) async fn view_dropped(&self, event: types::DropViewEvent) {
        dispatch_event!(self, view_dropped, event);
    }

    pub(crate) async fn view_renamed(&self, event: types::RenameViewEvent) {
        dispatch_event!(self, view_renamed, event);
    }

    pub(crate) async fn view_loaded(&self, event: types::LoadViewEvent) {
        dispatch_event!(self, view_loaded, event);
    }

    pub(crate) async fn tabular_undropped(&self, event: types::UndropTabularEvent) {
        dispatch_event!(self, tabular_undropped, event);
    }

    pub(crate) async fn project_created(&self, event: types::CreateProjectEvent) {
        dispatch_event!(self, project_created, event);
    }

    pub(crate) async fn warehouse_created(&self, event: types::CreateWarehouseEvent) {
        dispatch_event!(self, warehouse_created, event);
    }

    pub(crate) async fn warehouse_deleted(&self, event: types::DeleteWarehouseEvent) {
        dispatch_event!(self, warehouse_deleted, event);
    }

    pub(crate) async fn warehouse_protection_set(&self, event: types::SetWarehouseProtectionEvent) {
        dispatch_event!(self, warehouse_protection_set, event);
    }

    pub(crate) async fn warehouse_renamed(&self, event: types::RenameWarehouseEvent) {
        dispatch_event!(self, warehouse_renamed, event);
    }

    pub(crate) async fn warehouse_delete_profile_updated(
        &self,
        event: types::UpdateWarehouseDeleteProfileEvent,
    ) {
        dispatch_event!(self, warehouse_delete_profile_updated, event);
    }

    pub(crate) async fn warehouse_storage_updated(
        &self,
        event: types::UpdateWarehouseStorageEvent,
    ) {
        dispatch_event!(self, warehouse_storage_updated, event);
    }

    pub(crate) async fn warehouse_storage_credential_updated(
        &self,
        event: types::UpdateWarehouseStorageCredentialEvent,
    ) {
        dispatch_event!(self, warehouse_storage_credential_updated, event);
    }

    pub(crate) async fn task_queue_config_set(&self, event: types::SetTaskQueueConfigEvent) {
        dispatch_event!(self, task_queue_config_set, event);
    }

    pub(crate) async fn namespace_protection_set(&self, event: types::SetNamespaceProtectionEvent) {
        dispatch_event!(self, namespace_protection_set, event);
    }

    pub(crate) async fn namespace_created(&self, event: types::CreateNamespaceEvent) {
        dispatch_event!(self, namespace_created, event);
    }

    pub(crate) async fn namespace_dropped(&self, event: types::DropNamespaceEvent) {
        dispatch_event!(self, namespace_dropped, event);
    }

    pub(crate) async fn namespace_properties_updated(
        &self,
        event: types::UpdateNamespacePropertiesEvent,
    ) {
        dispatch_event!(self, namespace_properties_updated, event);
    }

    pub(crate) async fn authorization_failed(&self, event: types::AuthorizationFailedEvent) {
        dispatch_event!(self, authorization_failed, event);
    }

    pub(crate) async fn authorization_succeeded(&self, event: types::AuthorizationSucceededEvent) {
        dispatch_event!(self, authorization_succeeded, event);
    }

    pub(crate) async fn namespace_metadata_loaded(
        &self,
        event: types::NamespaceMetadataLoadedEvent,
    ) {
        dispatch_event!(self, namespace_metadata_loaded, event);
    }
}

/// `EventListener` is a trait that allows for custom listeners to be executed after successful
/// completion of various operations.
///
/// # Naming Convention
///
/// All listener methods use past-tense verbs to indicate they fire after successful operations:
/// - `table_created` - fires after a table has been successfully created
/// - `table_dropped` - fires after a table has been successfully dropped
/// - etc.
///
/// This naming pattern enables future extension with additional lifecycle phases:
/// - Error listeners: `table_create_failed`, `table_drop_failed`
/// - Pre-operation listeners: `before_table_create`, `before_table_drop`
/// - Read listeners: `table_loaded`, `table_listed`
///
/// # Implementation Guidelines
///
/// The default implementation of every listener method does nothing. Override any function if you want to
/// implement it.
///
/// An implementation should be light-weight, ideally every longer running task is deferred to a
/// background task via a channel or is spawned as a tokio task.
///
/// `EventListener` implementations are passed into the services via the [`EventDispatcher`]. If you want
/// to provide your own implementation, you'll have to fork and modify the main function to include
/// your listeners.
///
/// If the listener fails, it will be logged, but the request will continue to process. This is to ensure
/// that the request is not blocked by a listener failure.
#[async_trait::async_trait]
pub trait EventListener: Send + Sync + Debug + Display {
    // ===== Table Events =====

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

    async fn table_loaded(&self, _event: types::LoadTableEvent) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== View Events =====

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

    /// Invoked after a view's metadata has been successfully loaded
    async fn view_loaded(&self, _event: types::LoadViewEvent) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Tabular Events =====

    /// Invoked after tables or views have been successfully undeleted
    async fn tabular_undropped(&self, _event: types::UndropTabularEvent) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Project Events =====

    /// Invoked after a project has been successfully created
    async fn project_created(&self, _event: types::CreateProjectEvent) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Warehouse Events =====

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

    /// Invoked after a warehouse task queue config has been successfully set
    async fn task_queue_config_set(
        &self,
        _event: types::SetTaskQueueConfigEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Namespace Events =====

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

    async fn namespace_metadata_loaded(
        &self,
        _event: types::NamespaceMetadataLoadedEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    // ===== Authorization Hooks =====

    /// Invoked when an authorization check fails during request processing
    ///
    /// This hook enables audit trails for security monitoring and compliance,
    /// capturing who attempted what action and why it was denied. Unlike other
    /// hooks which fire after successful operations, this hook fires when an
    /// operation is denied due to authorization failures.
    ///
    /// # Use Cases
    /// - Security audit logs
    /// - Compliance monitoring (SOC2, GDPR, etc.)
    /// - Anomaly detection (repeated failed access attempts)
    /// - User permission debugging
    async fn authorization_failed(
        &self,
        _event: types::authorization::AuthorizationFailedEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Invoked when an authorization check succeeds during request processing
    ///
    /// This hook enables audit trails for security monitoring and compliance,
    /// capturing who accessed what action.
    ///
    /// # Use Cases
    /// - Security audit logs
    /// - Compliance monitoring (SOC2, GDPR, etc.)
    /// - Anomaly detection (repeated failed access attempts)
    /// - User permission debugging
    async fn authorization_succeeded(
        &self,
        _event: types::authorization::AuthorizationSucceededEvent,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
