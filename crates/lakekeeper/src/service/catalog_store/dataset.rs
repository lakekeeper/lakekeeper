use std::{collections::HashMap, sync::LazyLock};

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::Location;
use serde::{Deserialize, Serialize};

use super::{
    AuthZDatasetInfo, BasicTabularInfo, CatalogStore, Transaction, define_simple_error,
    define_transparent_error, impl_error_stack_methods, impl_from_with_detail,
};
use crate::{
    WarehouseId,
    service::{
        CatalogBackendError, ConcurrentUpdateError, DatasetId, DatasetSnapshotId,
        InternalParseLocationError, InvalidNamespaceIdentifier, InvalidPaginationToken,
        LocationAlreadyTaken, NamespaceId, NamespaceVersion, ProtectedTabularDeletionWithoutForce,
        TabularId, WarehouseVersion,
    },
};

/// Whether Lakekeeper owns the dataset's prefix or merely borrows it.
///
/// Gates every destructive behaviour: a managed prefix is Lakekeeper's to purge,
/// an imported one is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum DatasetOwnership {
    /// Lakekeeper created and exclusively owns the prefix.
    Managed,
    /// The prefix pre-existed and is borrowed. Lakekeeper never mutates or
    /// deletes objects under it; dropping the dataset removes catalog rows only.
    Imported,
}

impl DatasetOwnership {
    #[must_use]
    pub fn is_managed(self) -> bool {
        matches!(self, DatasetOwnership::Managed)
    }
}

/// Limits a dataset places on the files it will accept into a commit.
///
/// Enforced at commit, never at write: a holder of storage credentials can always
/// put arbitrary objects under a prefix.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DatasetConstraints {
    /// Content types a file may declare. Unset means anything is accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_content_types: Option<Vec<String>>,
    /// Largest file size in bytes. Unset means unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_file_size: Option<i64>,
}

impl DatasetConstraints {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.allowed_content_types.is_none() && self.max_file_size.is_none()
    }

    /// Check a commit's files against this contract.
    ///
    /// Lives here, not in the handler, so producers that bypass the API are held
    /// to it too. Reports every violation rather than the first.
    pub fn validate(&self, files: &[ManifestEntry]) -> Result<(), DatasetConstraintViolation> {
        let mut violations = Vec::new();

        for file in files {
            if let Some(allowed) = &self.allowed_content_types {
                match &file.content_type {
                    Some(ct) if allowed.contains(ct) => {}
                    Some(ct) => violations.push(format!(
                        "{}: content type '{ct}' is not allowed",
                        file.logical_key
                    )),
                    None => violations.push(format!(
                        "{}: a content type is required by this dataset",
                        file.logical_key
                    )),
                }
            }
            if let Some(max) = self.max_file_size
                && let Some(size) = file.size
                && size > max
            {
                violations.push(format!(
                    "{}: size {size} exceeds the maximum of {max}",
                    file.logical_key
                ));
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(DatasetConstraintViolation::new(violations))
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatasetInfo {
    pub dataset_id: DatasetId,
    pub warehouse_id: WarehouseId,
    pub warehouse_version: WarehouseVersion,
    pub namespace_id: NamespaceId,
    pub namespace_version: NamespaceVersion,
    pub namespace_ident: NamespaceIdent,
    pub name: String,
    pub tabular_ident: TableIdent,
    pub location: Location,
    pub properties: HashMap<String, String>,
    pub protected: bool,
    pub ownership: DatasetOwnership,
    pub constraints: DatasetConstraints,
}

impl BasicTabularInfo for DatasetInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn warehouse_version(&self) -> WarehouseVersion {
        self.warehouse_version
    }

    fn tabular_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn tabular_id(&self) -> TabularId {
        TabularId::Dataset(self.dataset_id)
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn namespace_version(&self) -> NamespaceVersion {
        self.namespace_version
    }
}

impl AuthZDatasetInfo for DatasetInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn dataset_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn is_protected(&self) -> bool {
        self.protected
    }

    fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

#[derive(Debug, Clone)]
pub struct DatasetCreation {
    pub dataset_id: DatasetId,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub name: String,
    pub location: Location,
    pub ownership: DatasetOwnership,
    pub constraints: DatasetConstraints,
}

#[derive(Debug, Clone)]
pub struct DatasetListEntry {
    pub dataset_id: DatasetId,
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub name: String,
    pub tabular_ident: TableIdent,
    pub namespace_ident: NamespaceIdent,
    pub ownership: DatasetOwnership,
    pub protected: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl AuthZDatasetInfo for DatasetListEntry {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn dataset_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn is_protected(&self) -> bool {
        self.protected
    }

    // List entries don't load properties; IncludeInList authz doesn't read them.
    fn properties(&self) -> &HashMap<String, String> {
        static EMPTY: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
        &EMPTY
    }
}

define_simple_error!(DatasetAlreadyExists, "Dataset already exists");
impl From<DatasetAlreadyExists> for ErrorModel {
    fn from(err: DatasetAlreadyExists) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetAlreadyExists")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(DatasetNotFound, "Dataset not found");
impl From<DatasetNotFound> for ErrorModel {
    fn from(err: DatasetNotFound) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum CreateDatasetError,
    stack_message: "Error creating dataset",
    variants: [
        DatasetAlreadyExists,
        CatalogBackendError,
        InternalParseLocationError,
        LocationAlreadyTaken,
        InvalidNamespaceIdentifier,
    ]
}

define_transparent_error! {
    pub enum LoadDatasetError,
    stack_message: "Error loading dataset",
    variants: [
        DatasetNotFound,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum ListDatasetsError,
    stack_message: "Error listing datasets",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken,
    ]
}

define_transparent_error! {
    pub enum DropDatasetError,
    stack_message: "Error dropping dataset",
    variants: [
        DatasetNotFound,
        ProtectedTabularDeletionWithoutForce,
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InternalParseLocationError,
        ConcurrentUpdateError,
    ]
}

#[async_trait::async_trait]
pub trait CatalogDatasetOps
where
    Self: CatalogStore,
{
    async fn create_dataset<'a>(
        creation: DatasetCreation,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetInfo, CreateDatasetError> {
        Self::create_dataset_impl(creation, transaction).await
    }

    async fn load_dataset<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        dataset_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetInfo, LoadDatasetError> {
        Self::load_dataset_impl(warehouse_id, namespace_id, dataset_name, transaction).await
    }

    /// Load a dataset by its stable id. Prefer this over [`Self::load_dataset`]
    /// when the caller already holds an authorized identity (e.g. after a
    /// successful authz check) — using the id closes the TOCTOU window where a
    /// concurrent rename + create-with-same-name between authz and load would
    /// substitute a different row.
    async fn load_dataset_by_id<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetInfo, LoadDatasetError> {
        Self::load_dataset_by_id_impl(warehouse_id, dataset_id, transaction).await
    }

    /// Whether Lakekeeper owns the dataset's prefix, readable after the dataset
    /// has been soft-deleted -- unlike [`Self::load_dataset_by_id`], which skips
    /// deleted rows. Expiry needs it to decide whether a purge may be scheduled.
    async fn load_dataset_ownership<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetOwnership, LoadDatasetError> {
        Self::load_dataset_ownership_impl(warehouse_id, dataset_id, transaction).await
    }

    async fn list_datasets<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        namespace_ident: &NamespaceIdent,
        page_size: Option<i64>,
        page_token: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(Vec<DatasetListEntry>, Option<String>), ListDatasetsError> {
        Self::list_datasets_impl(
            warehouse_id,
            namespace_id,
            namespace_ident,
            page_size,
            page_token,
            transaction,
        )
        .await
    }

    async fn drop_dataset<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        dataset_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetId, DropDatasetError> {
        Self::drop_dataset_impl(warehouse_id, namespace_id, dataset_name, transaction).await
    }

    /// Append a snapshot and move the branch pointer to it, in one transaction.
    ///
    /// The pointer move is a compare-and-swap against
    /// [`DatasetCommit::expected_snapshot_id`]: if the branch moved meanwhile the
    /// commit is refused with the current head rather than overwriting it.
    async fn commit_dataset<'a>(
        commit: DatasetCommit,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetSnapshot, CommitDatasetError> {
        Self::commit_dataset_impl(commit, transaction).await
    }

    /// Open a snapshot for staging. Invisible until `finish_dataset_commit`.
    #[allow(clippy::too_many_arguments)]
    ///
    /// Lets a caller write manifest rows across many transactions, so a scan of
    /// millions of objects is neither one transaction nor one buffer in memory.
    async fn begin_dataset_commit<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        branch: &str,
        snapshot_id: DatasetSnapshotId,
        parent_snapshot_id: Option<DatasetSnapshotId>,
        location: &str,
        summary: Option<serde_json::Value>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::begin_dataset_commit_impl(
            warehouse_id,
            dataset_id,
            branch,
            snapshot_id,
            parent_snapshot_id,
            location,
            summary,
            transaction,
        )
        .await
    }

    /// Append manifest rows to a staging snapshot. Safe to repeat.
    async fn stage_dataset_files<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        added: &[ManifestEntry],
        modified: &[ManifestEntry],
        removed: &[String],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::stage_dataset_files_impl(
            warehouse_id,
            dataset_id,
            snapshot_id,
            added,
            modified,
            removed,
            transaction,
        )
        .await
    }

    /// Make a staged snapshot the branch head. The only step that races.
    ///
    /// Returns whether a checkpoint is now due.
    async fn finish_dataset_commit<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        branch: &str,
        snapshot_id: DatasetSnapshotId,
        expected_snapshot_id: Option<DatasetSnapshotId>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<bool, CommitDatasetError> {
        Self::finish_dataset_commit_impl(
            warehouse_id,
            dataset_id,
            branch,
            snapshot_id,
            expected_snapshot_id,
            transaction,
        )
        .await
    }

    /// Rebase a staged commit onto a different parent after losing the pointer
    /// move. The staged rows survive a lost race; only the chain they hang off
    /// has to move.
    async fn reparent_staging_snapshot<'a>(
        warehouse_id: WarehouseId,
        snapshot_id: DatasetSnapshotId,
        new_parent: Option<DatasetSnapshotId>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::reparent_staging_snapshot_impl(warehouse_id, snapshot_id, new_parent, transaction)
            .await
    }

    /// Drop staging snapshots abandoned before `older_than`, with their rows.
    ///
    /// A commit that dies between begin and finish leaves one behind. It is
    /// invisible and never wrong, but it accumulates. `dataset_id` of `None`
    /// sweeps the whole warehouse.
    async fn expire_staging_snapshots<'a>(
        warehouse_id: WarehouseId,
        dataset_id: Option<DatasetId>,
        older_than: chrono::DateTime<chrono::Utc>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<u64, CommitDatasetError> {
        Self::expire_staging_snapshots_impl(warehouse_id, dataset_id, older_than, transaction).await
    }

    /// Fold a branch's head into a checkpoint if the chain has outgrown the
    /// interval, returning the snapshot folded.
    ///
    /// Runs on the task queue, not in the commit path: the fold restates the whole
    /// file set. Safe to defer because a checkpoint only shortens a walk, never
    /// changes what a read returns. `Ok(None)` means none was due.
    async fn checkpoint_dataset_branch<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        branch: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Option<DatasetSnapshotId>, CommitDatasetError> {
        Self::checkpoint_dataset_branch_impl(warehouse_id, dataset_id, branch, transaction).await
    }

    async fn list_dataset_refs<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<DatasetRef>, ListDatasetRefsError> {
        Self::list_dataset_refs_impl(warehouse_id, dataset_id, transaction).await
    }

    async fn get_dataset_ref<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetRef, ListDatasetRefsError> {
        Self::get_dataset_ref_impl(warehouse_id, dataset_id, name, transaction).await
    }

    /// Create a branch or tag pointing at `snapshot_id`. Zero-copy at any size:
    /// the new ref shares the source snapshot's manifest rows.
    async fn create_dataset_ref<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        name: &str,
        typ: DatasetRefType,
        snapshot_id: DatasetSnapshotId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetRef, CreateDatasetRefError> {
        Self::create_dataset_ref_impl(
            warehouse_id,
            dataset_id,
            name,
            typ,
            snapshot_id,
            transaction,
        )
        .await
    }

    /// Mark a ref protected, refusing direct commits and deletion.
    ///
    /// A structural rule rather than an authorization one: it holds for every
    /// principal, so a protected branch takes changes only by fast-forward no
    /// matter who is asking.
    async fn set_dataset_ref_protection<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        name: &str,
        protected: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetRef, SetDatasetRefProtectionError> {
        Self::set_dataset_ref_protection_impl(
            warehouse_id,
            dataset_id,
            name,
            protected,
            transaction,
        )
        .await
    }

    async fn delete_dataset_ref<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), DeleteDatasetRefError> {
        Self::delete_dataset_ref_impl(warehouse_id, dataset_id, name, transaction).await
    }

    /// Move a branch to `snapshot_id`.
    ///
    /// With `require_descendant` this is a fast-forward: the target must descend
    /// from the current head, so history is extended and never abandoned. Without
    /// it this is a reset, which can move a branch anywhere and is therefore a
    /// separately authorized action.
    async fn move_dataset_ref<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        name: &str,
        snapshot_id: DatasetSnapshotId,
        expected_snapshot_id: Option<DatasetSnapshotId>,
        require_descendant: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetRef, MoveDatasetRefError> {
        Self::move_dataset_ref_impl(
            warehouse_id,
            dataset_id,
            name,
            snapshot_id,
            expected_snapshot_id,
            require_descendant,
            transaction,
        )
        .await
    }

    /// Resolve a ref and reconstruct its file list, paginated by logical key.
    ///
    /// The ref is resolved on the first page only; the returned page token carries
    /// the snapshot so later pages read that same immutable snapshot. Returns the
    /// snapshot actually read, so callers can record what a run saw.
    async fn list_dataset_files<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        ref_name: &str,
        content_type: Option<&str>,
        page_size: Option<i64>,
        page_token: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<
        (
            Option<DatasetSnapshotId>,
            Vec<ManifestEntry>,
            Option<String>,
        ),
        ListManifestEntriesError,
    > {
        Self::list_dataset_files_impl(
            warehouse_id,
            dataset_id,
            ref_name,
            content_type,
            page_size,
            page_token,
            transaction,
        )
        .await
    }
}

impl<T> CatalogDatasetOps for T where T: CatalogStore {}

// ===================== Versioning: refs, snapshots, manifests =====================

/// Whether a ref moves with new commits or is fixed at the snapshot it was created on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum DatasetRefType {
    /// A movable pointer. Commits append to it and fast-forwards advance it.
    Branch,
    /// Fixed at creation. This is what a training run pins to, so it must never
    /// move -- later commits on the branch it was cut from do not affect it.
    Tag,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetRef {
    pub name: String,
    pub typ: DatasetRefType,
    /// `None` only on a branch of a dataset that has never been committed to.
    pub snapshot_id: Option<DatasetSnapshotId>,
    /// Structural rule, not authorization: refuses direct commits and deletion.
    pub protected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetSnapshot {
    pub snapshot_id: DatasetSnapshotId,
    pub parent_snapshot_id: Option<DatasetSnapshotId>,
    /// Copied from the dataset at commit time, so historical resolution never
    /// depends on the dataset's current location.
    pub location: String,
    pub is_checkpoint: bool,
    /// The chain has grown past the checkpoint interval, so the caller should
    /// enqueue a fold. Not persisted -- it is a signal about this commit, not a
    /// property of the snapshot, and it is advisory: the worker re-checks before
    /// doing any work, and a missed enqueue costs read speed, never correctness.
    pub checkpoint_due: bool,
    pub summary: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// How a manifest row relates to the parent snapshot. Manifests are deltas, so a
/// file list is the nearest checkpoint plus the changes since.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ManifestChange {
    Added,
    Removed,
    Modified,
}

/// A file as recorded in a snapshot's manifest. Path-based, not
/// content-addressed: two identical files are two entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    /// What users see, filter and address. Relative to the snapshot's location.
    pub logical_key: String,
    /// Where the bytes are. Identity mapping for declared/imported files.
    pub physical_path: String,
    pub etag: Option<String>,
    pub size: Option<i64>,
    pub content_type: Option<String>,
    /// Authoritative where present: an etag is not a content hash for multipart
    /// or SSE-KMS objects.
    pub checksum: Option<String>,
    /// Captured on versioned buckets so a pinned read targets an exact version.
    pub version_id: Option<String>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// One commit: files to add and logical keys to remove, against an expected parent.
#[derive(Debug, Clone)]
pub struct DatasetCommit {
    pub warehouse_id: WarehouseId,
    pub dataset_id: DatasetId,
    pub branch: String,
    /// The snapshot the caller believes the branch is on. `None` means "the
    /// branch has no commits yet". The compare-and-swap is against this value,
    /// so a stale caller loses rather than overwriting a concurrent commit.
    pub expected_snapshot_id: Option<DatasetSnapshotId>,
    pub snapshot_id: DatasetSnapshotId,
    /// Copied onto the snapshot so its manifest keys resolve against the
    /// location in force when it was committed.
    pub location: String,
    pub added: Vec<ManifestEntry>,
    /// Files whose bytes changed under a key that already existed. Recorded
    /// separately from `added` so a reader can tell a new file from a rewritten
    /// one; both resolve the same way, newest wins.
    pub modified: Vec<ManifestEntry>,
    pub removed: Vec<String>,
    /// Free-form correlation keys (pipeline run, code commit, trace id).
    pub summary: Option<serde_json::Value>,
}

define_simple_error!(DatasetRefNotFound, "Dataset ref not found");
impl From<DatasetRefNotFound> for ErrorModel {
    fn from(err: DatasetRefNotFound) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetRefNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(DatasetRefAlreadyExists, "Dataset ref already exists");
impl From<DatasetRefAlreadyExists> for ErrorModel {
    fn from(err: DatasetRefAlreadyExists) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetRefAlreadyExists")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(DatasetSnapshotNotFound, "Dataset snapshot not found");
impl From<DatasetSnapshotNotFound> for ErrorModel {
    fn from(err: DatasetSnapshotNotFound) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetSnapshotNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(
    DatasetRefProtected,
    "Dataset ref is protected: changes must arrive by fast-forward"
);
impl From<DatasetRefProtected> for ErrorModel {
    fn from(err: DatasetRefProtected) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetRefProtected")
            .code(StatusCode::FORBIDDEN.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(
    DatasetCommitToTag,
    "Cannot commit to a tag: tags are immutable"
);
impl From<DatasetCommitToTag> for ErrorModel {
    fn from(err: DatasetCommitToTag) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetCommitToTag")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(err.stack)
            .build()
    }
}

/// The branch moved between the caller reading it and committing. Carries the
/// current head so the caller can rebase without re-uploading.
#[derive(thiserror::Error, Debug)]
#[error(
    "Branch moved while committing (current head: {}). Rebase onto it and retry.",
    .current_snapshot_id.map_or_else(|| "none".to_string(), |id| id.to_string())
)]
pub struct DatasetCommitConflict {
    pub current_snapshot_id: Option<DatasetSnapshotId>,
    pub stack: Vec<String>,
}

impl DatasetCommitConflict {
    #[must_use]
    pub fn new(current_snapshot_id: Option<DatasetSnapshotId>) -> Self {
        Self {
            current_snapshot_id,
            stack: Vec::new(),
        }
    }
}

impl_error_stack_methods!(DatasetCommitConflict);

impl From<DatasetCommitConflict> for ErrorModel {
    fn from(err: DatasetCommitConflict) -> Self {
        // The head goes in the stack, not only the message: a client rebases by
        // reading it, and `ErrorModel` has no other structured slot to put it in.
        // Parsing prose would be the alternative, so the form here is fixed.
        let mut stack = err.stack.clone();
        if let Some(current) = err.current_snapshot_id {
            stack.push(format!("current-snapshot-id: {current}"));
        }
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetCommitConflict")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(stack)
            .build()
    }
}

// Fast-forward was asked to move a branch to a snapshot that is not a descendant
// of its current head. Moving there would abandon commits, which is what `reset`
// is for -- and reset is separately authorized.
define_simple_error!(
    DatasetSnapshotNotADescendant,
    "Target snapshot is not a descendant of the branch head: use reset to move a branch backwards or sideways"
);
impl From<DatasetSnapshotNotADescendant> for ErrorModel {
    fn from(err: DatasetSnapshotNotADescendant) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetSnapshotNotADescendant")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(err.stack)
            .build()
    }
}

/// A commit declared files the dataset's contract does not accept. The whole
/// commit fails: a partial success would look like success to a pipeline.
#[derive(thiserror::Error, Debug)]
#[error("Commit violates dataset constraints: {}", .violations.join("; "))]
pub struct DatasetConstraintViolation {
    pub violations: Vec<String>,
    pub stack: Vec<String>,
}

impl DatasetConstraintViolation {
    #[must_use]
    pub fn new(violations: Vec<String>) -> Self {
        Self {
            violations,
            stack: Vec::new(),
        }
    }
}

impl_error_stack_methods!(DatasetConstraintViolation);

impl From<DatasetConstraintViolation> for ErrorModel {
    fn from(err: DatasetConstraintViolation) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetConstraintViolation")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum CommitDatasetError,
    stack_message: "Error committing to dataset",
    variants: [
        DatasetNotFound,
        DatasetRefNotFound,
        DatasetCommitConflict,
        DatasetCommitToTag,
        DatasetRefProtected,
        DatasetConstraintViolation,
        DatasetSnapshotNotFound,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum ListDatasetRefsError,
    stack_message: "Error listing dataset refs",
    variants: [
        DatasetRefNotFound,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum CreateDatasetRefError,
    stack_message: "Error creating dataset ref",
    variants: [
        DatasetRefAlreadyExists,
        DatasetRefNotFound,
        DatasetSnapshotNotFound,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum SetDatasetRefProtectionError,
    stack_message: "Error setting dataset ref protection",
    variants: [
        DatasetRefNotFound,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum DeleteDatasetRefError,
    stack_message: "Error deleting dataset ref",
    variants: [
        DatasetRefNotFound,
        DatasetRefProtected,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum MoveDatasetRefError,
    stack_message: "Error moving dataset ref",
    variants: [
        DatasetRefNotFound,
        DatasetSnapshotNotFound,
        DatasetSnapshotNotADescendant,
        DatasetCommitConflict,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum ListManifestEntriesError,
    stack_message: "Error listing dataset files",
    variants: [
        DatasetRefNotFound,
        DatasetSnapshotNotFound,
        CatalogBackendError,
        InvalidPaginationToken,
    ]
}
