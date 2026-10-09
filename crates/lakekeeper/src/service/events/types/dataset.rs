use std::sync::Arc;

use crate::{
    api::{
        RequestMetadata,
        data::v1::datasets::{
            CreateDatasetRequest, RenameDatasetRequest, UpdateDatasetSettingsRequest,
        },
        iceberg::types::DropParams,
    },
    service::{
        DatasetInfo, DatasetRef, DatasetSnapshotId, NamespaceWithParent, StagedChanges,
        authz::{CatalogDatasetAction, CatalogNamespaceAction},
        events::{
            APIEventContext,
            context::{
                APIEventActions, AuthzChecked, Resolved, ResolvedDataset, ResolvedNamespace,
                UserProvidedDataset, UserProvidedNamespace,
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

/// A snapshot published onto a branch, as its event announces it.
#[derive(Clone, Debug)]
pub struct DatasetPublished {
    pub branch: String,
    pub snapshot_id: DatasetSnapshotId,
    pub parent_snapshot_id: Option<DatasetSnapshotId>,
    pub summary: Option<serde_json::Value>,
    /// How many manifest entries the snapshot added, modified and removed.
    pub changes: StagedChanges,
}

/// Event emitted when a snapshot is published onto a branch, by a commit or an
/// import
#[derive(Clone, Debug)]
pub struct CommitDatasetEvent {
    pub dataset: ResolvedDataset,
    pub published: DatasetPublished,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a branch or tag of a dataset is created
#[derive(Clone, Debug)]
pub struct CreateDatasetRefEvent {
    pub dataset: ResolvedDataset,
    pub dataset_ref: DatasetRef,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a dataset branch is fast-forwarded or reset
#[derive(Clone, Debug)]
pub struct MoveDatasetRefEvent {
    pub dataset: ResolvedDataset,
    /// The ref as the move left it.
    pub dataset_ref: DatasetRef,
    /// `false` for a reset, which may abandon commits.
    pub fast_forward: bool,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a branch or tag of a dataset is deleted
#[derive(Clone, Debug)]
pub struct DeleteDatasetRefEvent {
    pub dataset: ResolvedDataset,
    pub ref_name: String,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a dataset's settings are changed
#[derive(Clone, Debug)]
pub struct UpdateDatasetSettingsEvent {
    pub dataset: ResolvedDataset,
    pub request: Arc<UpdateDatasetSettingsRequest>,
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

impl<A: APIEventActions>
    APIEventContext<UserProvidedDataset, Resolved<ResolvedDataset>, A, AuthzChecked>
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

    /// Emit `dataset_committed` event
    pub(crate) fn emit_dataset_committed_async(self, published: DatasetPublished) {
        let event = CommitDatasetEvent {
            dataset: self.resolved_entity.data,
            published,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_committed(event).await;
        });
    }

    /// Emit `dataset_ref_created` event
    pub(crate) fn emit_dataset_ref_created_async(self, dataset_ref: DatasetRef) {
        let event = CreateDatasetRefEvent {
            dataset: self.resolved_entity.data,
            dataset_ref,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_ref_created(event).await;
        });
    }

    /// Emit `dataset_ref_moved` event
    pub(crate) fn emit_dataset_ref_moved_async(self, dataset_ref: DatasetRef, fast_forward: bool) {
        let event = MoveDatasetRefEvent {
            dataset: self.resolved_entity.data,
            dataset_ref,
            fast_forward,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_ref_moved(event).await;
        });
    }

    /// Emit `dataset_ref_deleted` event
    pub(crate) fn emit_dataset_ref_deleted_async(self, ref_name: String) {
        let event = DeleteDatasetRefEvent {
            dataset: self.resolved_entity.data,
            ref_name,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_ref_deleted(event).await;
        });
    }

    /// Emit `dataset_settings_updated` event
    pub(crate) fn emit_dataset_settings_updated_async(
        self,
        request: Arc<UpdateDatasetSettingsRequest>,
    ) {
        let event = UpdateDatasetSettingsEvent {
            dataset: self.resolved_entity.data,
            request,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.dataset_settings_updated(event).await;
        });
    }
}
