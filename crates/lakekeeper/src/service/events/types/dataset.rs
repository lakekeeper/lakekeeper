use std::sync::Arc;

use crate::{
    api::{
        RequestMetadata,
        data::v1::datasets::{CreateDatasetRequest, RenameDatasetRequest},
        iceberg::types::DropParams,
    },
    service::{
        DatasetInfo, NamespaceWithParent,
        authz::{CatalogDatasetAction, CatalogNamespaceAction},
        events::{
            APIEventContext,
            context::{
                AuthzChecked, Resolved, ResolvedDataset, ResolvedNamespace, UserProvidedDataset,
                UserProvidedNamespace,
            },
        },
    },
};

// ===== Dataset Events =====

/// Event emitted when a dataset is created (within a namespace)
#[derive(Clone, Debug)]
pub struct CreateDatasetEvent {
    pub namespace: ResolvedNamespace,
    pub dataset: Arc<DatasetInfo>,
    pub request_metadata: Arc<RequestMetadata>,
    pub request: Arc<CreateDatasetRequest>,
}

/// Event emitted when a dataset is dropped
#[derive(Clone, Debug)]
pub struct DropDatasetEvent {
    pub dataset: ResolvedDataset,
    pub drop_params: DropParams,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a dataset's metadata is loaded
#[derive(Clone, Debug)]
pub struct LoadDatasetEvent {
    pub dataset: ResolvedDataset,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a dataset is renamed, possibly across namespaces
#[derive(Clone, Debug)]
pub struct RenameDatasetEvent {
    pub source_dataset: ResolvedDataset,
    pub destination_namespace: NamespaceWithParent,
    pub request: Arc<RenameDatasetRequest>,
    pub request_metadata: Arc<RequestMetadata>,
}

pub type DatasetEventContext =
    APIEventContext<UserProvidedDataset, Resolved<ResolvedDataset>, CatalogDatasetAction>;

impl
    APIEventContext<
        UserProvidedNamespace,
        Resolved<ResolvedNamespace>,
        CatalogNamespaceAction,
        AuthzChecked,
    >
{
    /// Emit `dataset_created` event
    pub(crate) fn emit_dataset_created_async(
        self,
        dataset: Arc<DatasetInfo>,
        request: Arc<CreateDatasetRequest>,
    ) {
        let event = CreateDatasetEvent {
            namespace: self.resolved_entity.data,
            dataset,
            request_metadata: self.request_metadata,
            request,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_created(event).await;
        });
    }
}

impl
    APIEventContext<
        UserProvidedDataset,
        Resolved<ResolvedDataset>,
        CatalogDatasetAction,
        AuthzChecked,
    >
{
    /// Emit `dataset_loaded` event
    pub(crate) fn emit_dataset_loaded_async(self) {
        let event = LoadDatasetEvent {
            dataset: self.resolved_entity.data,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_loaded(event).await;
        });
    }

    /// Emit `dataset_dropped` event
    pub(crate) fn emit_dataset_dropped_async(self, drop_params: DropParams) {
        let event = DropDatasetEvent {
            dataset: self.resolved_entity.data,
            drop_params,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_dropped(event).await;
        });
    }

    /// Emit `dataset_renamed` event
    pub(crate) fn emit_dataset_renamed_async(
        self,
        destination_namespace: NamespaceWithParent,
        request: Arc<RenameDatasetRequest>,
    ) {
        let event = RenameDatasetEvent {
            source_dataset: self.resolved_entity.data,
            destination_namespace,
            request,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_renamed(event).await;
        });
    }
}
