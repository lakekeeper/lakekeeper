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
        CatalogBackendError, ConcurrentUpdateError, DatasetAccessGrantId, DatasetId,
        DatasetSnapshotId, InternalParseLocationError, InvalidNamespaceIdentifier,
        InvalidPaginationToken, LocationAlreadyTaken, NamespaceId, NamespaceVersion,
        ProtectedTabularDeletionWithoutForce, TabularId, WarehouseVersion,
        idempotency::IdempotencyKey, tasks::TaskId,
    },
};

/// The branch every dataset is created with, and the default target of commits and
/// imports.
pub const DEFAULT_DATASET_BRANCH: &str = "main";

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
// A misspelt field must not read as no constraint at all.
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct DatasetConstraints {
    /// Content types a file may declare. Unset means anything is accepted; when set,
    /// every file must declare one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_content_types: Option<Vec<String>>,
    /// Largest file size in bytes. Unset means unlimited; when set, every file must
    /// declare its size.
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
    /// to it too. Reports every violation.
    pub fn validate(&self, files: &[ManifestEntry]) -> Result<(), DatasetConstraintViolation> {
        let violations: Vec<String> = files
            .iter()
            .flat_map(|file| {
                self.violations_of(file)
                    .into_iter()
                    .map(|reason| format!("{}: {reason}", file.logical_key))
            })
            .collect();
        if violations.is_empty() {
            Ok(())
        } else {
            Err(DatasetConstraintViolation::new(violations))
        }
    }

    /// Split `files` into those this contract accepts and those it refuses, each
    /// with every reason it is refused.
    #[must_use]
    pub fn partition(&self, files: &[ManifestEntry]) -> (Vec<ManifestEntry>, Vec<SkippedFile>) {
        let mut accepted = Vec::with_capacity(files.len());
        let mut skipped = Vec::new();
        for file in files {
            let violations = self.violations_of(file);
            if violations.is_empty() {
                accepted.push(file.clone());
            } else {
                skipped.push(SkippedFile {
                    logical_key: file.logical_key.clone(),
                    reason: violations.join("; "),
                });
            }
        }
        (accepted, skipped)
    }

    fn violations_of(&self, file: &ManifestEntry) -> Vec<String> {
        let mut violations = Vec::new();
        if let Some(allowed) = &self.allowed_content_types {
            match &file.content_type {
                Some(ct) if allowed.contains(ct) => {}
                Some(ct) => violations.push(format!("content type '{ct}' is not allowed")),
                None => violations.push("a content type is required by this dataset".to_string()),
            }
        }
        if let Some(max) = self.max_file_size {
            match file.size {
                Some(size) if size <= max => {}
                Some(size) => violations.push(format!("size {size} exceeds the maximum of {max}")),
                None => violations.push("a size is required by this dataset".to_string()),
            }
        }
        violations
    }
}

/// How a dataset's snapshots expire. Whatever the mode, a snapshot a ref points
/// at or a live access grant reads does not expire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "mode", deny_unknown_fields)]
pub enum DatasetRetention {
    /// Follow the warehouse's `dataset_snapshot_expiry` task-queue config.
    // A struct variant, so the derive refuses a field beside `mode`: a unit
    // variant would ignore it.
    Inherit {},
    /// Expire nothing automatically; a snapshot expires only when asked to.
    #[serde(rename_all = "kebab-case")]
    Manual {
        /// How long an expired snapshot stays restorable, in ISO 8601 duration
        /// format. Defaults to the warehouse's, or 7 days.
        #[cfg_attr(feature = "open-api", schema(value_type = Option<String>, example = "P7D"))]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::utils::time_conversion::iso8601_option_duration_serde"
        )]
        grace_period: Option<chrono::Duration>,
    },
    /// Expire snapshots older than `max-snapshot-age`, keeping each branch's newest
    /// `min-snapshots-to-keep`, whether or not the warehouse expires anything.
    #[serde(rename_all = "kebab-case")]
    Ttl {
        #[cfg_attr(feature = "open-api", schema(value_type = String, example = "P30D"))]
        #[serde(with = "crate::utils::time_conversion::iso8601_duration_serde")]
        max_snapshot_age: chrono::Duration,
        /// Defaults to 1, the head.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<u32>,
        #[cfg_attr(feature = "open-api", schema(value_type = Option<String>, example = "P7D"))]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::utils::time_conversion::iso8601_option_duration_serde"
        )]
        grace_period: Option<chrono::Duration>,
    },
    /// Keep each branch's newest `max-snapshots`, whatever their age, and expire
    /// the rest.
    #[serde(rename_all = "kebab-case")]
    MaxCount {
        max_snapshots: u32,
        #[cfg_attr(feature = "open-api", schema(value_type = Option<String>, example = "P7D"))]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::utils::time_conversion::iso8601_option_duration_serde"
        )]
        grace_period: Option<chrono::Duration>,
    },
}

impl Default for DatasetRetention {
    fn default() -> Self {
        Self::Inherit {}
    }
}

impl DatasetRetention {
    #[must_use]
    pub fn is_inherit(&self) -> bool {
        matches!(self, Self::Inherit {})
    }
}

/// What a commit does with a file its dataset's constraints refuse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ConstraintViolationPolicy {
    /// Refuse the commit, naming every file that violates.
    #[default]
    Reject,
    /// Record the files that conform and report the ones left out.
    Skip,
}

/// A file a commit or an import left out: one its dataset's constraints refuse,
/// or, in an import, an object stored under a name another file goes by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SkippedFile {
    pub logical_key: String,
    /// Why it was left out: every constraint it fails, or the file whose name it
    /// is stored under.
    pub reason: String,
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
    pub retention: DatasetRetention,
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
    pub enum UpdateDatasetSettingsError,
    stack_message: "Error updating dataset settings",
    variants: [
        DatasetNotFound,
        CatalogBackendError,
    ]
}

/// Settings to change. A field left `None` keeps its current value.
#[derive(Debug, Clone, Default)]
pub struct DatasetSettingsUpdate {
    /// Replaces the dataset's constraints; empty ones lift them.
    pub constraints: Option<DatasetConstraints>,
    /// Replaces the dataset's retention; `inherit` returns it to the warehouse's.
    pub retention: Option<DatasetRetention>,
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
    /// has been soft-deleted — unlike [`Self::load_dataset_by_id`], which skips
    /// deleted rows. Expiry needs it to decide whether a purge may be scheduled.
    async fn load_dataset_ownership<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetOwnership, LoadDatasetError> {
        Self::load_dataset_ownership_impl(warehouse_id, dataset_id, transaction).await
    }

    /// The tasks queued for the dataset. A hard drop cancels them: a retention pass
    /// or a purge is queued for days ahead, and an open task keeps its warehouse
    /// from being deleted.
    async fn list_dataset_task_ids<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<TaskId>, CatalogBackendError> {
        Self::list_dataset_task_ids_impl(warehouse_id, dataset_id, transaction).await
    }

    /// Bring the dataset's scheduled task of `queue_name` forward to `at`, if it is
    /// due later. One that is running is left alone.
    async fn bring_dataset_task_forward<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        queue_name: &str,
        at: chrono::DateTime<chrono::Utc>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CatalogBackendError> {
        Self::bring_dataset_task_forward_impl(warehouse_id, dataset_id, queue_name, at, transaction)
            .await
    }

    /// Whether a commit ever recorded a file named apart from its storage path.
    /// Never cleared: an import that finds none still costs only the walk.
    async fn dataset_has_renamed_files<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<bool, LoadDatasetError> {
        Self::dataset_has_renamed_files_impl(warehouse_id, dataset_id, transaction).await
    }

    /// Apply `update` to the dataset's settings.
    async fn update_dataset_settings<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        update: DatasetSettingsUpdate,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), UpdateDatasetSettingsError> {
        Self::update_dataset_settings_impl(warehouse_id, dataset_id, update, transaction).await
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
    /// commit is refused with the current head, and the branch is left as it is.
    async fn commit_dataset<'a>(
        commit: DatasetCommit,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetCommitOutcome, CommitDatasetError> {
        Self::commit_dataset_impl(commit, transaction).await
    }

    /// Open a snapshot for staging, returning when it was opened by the database's
    /// clock. Invisible until `finish_dataset_commit`.
    ///
    /// Lets a caller write manifest rows across many transactions, so a scan of
    /// millions of objects is neither one transaction nor one buffer in memory.
    #[allow(clippy::too_many_arguments)]
    async fn begin_dataset_commit<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        branch: &str,
        snapshot_id: DatasetSnapshotId,
        parent_snapshot_id: Option<DatasetSnapshotId>,
        location: &str,
        summary: Option<serde_json::Value>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<chrono::DateTime<chrono::Utc>, CommitDatasetError> {
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
    ///
    /// Files the dataset's constraints refuse fail the call under
    /// [`ConstraintViolationPolicy::Reject`], and are left out and reported under
    /// [`ConstraintViolationPolicy::Skip`].
    #[allow(clippy::too_many_arguments)]
    async fn stage_dataset_files<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        added: &[ManifestEntry],
        modified: &[ManifestEntry],
        removed: &[String],
        on_violation: ConstraintViolationPolicy,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<StagedBatch, CommitDatasetError> {
        Self::stage_dataset_files_impl(
            warehouse_id,
            dataset_id,
            snapshot_id,
            added,
            modified,
            removed,
            on_violation,
            transaction,
        )
        .await
    }

    /// Make a staged snapshot the branch head. The only step that races. The
    /// snapshot's creation time becomes the publish's.
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
    /// move. The staged rows survive a lost race, except those for a key a commit
    /// it moves over changed: that commit is newer than the rows, so its entry
    /// stands. Returns the staged rows dropped. A new parent that does not descend
    /// from the old one, as after a reset, is a conflict: the rows were judged
    /// against history the branch has left.
    async fn rebase_staging_snapshot<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        new_parent: Option<DatasetSnapshotId>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<StagedChanges, CommitDatasetError> {
        Self::rebase_staging_snapshot_impl(
            warehouse_id,
            dataset_id,
            snapshot_id,
            new_parent,
            transaction,
        )
        .await
    }

    /// Discard a staging snapshot opened by [`Self::begin_dataset_commit`], with its
    /// rows. A published snapshot is left alone.
    async fn abort_dataset_commit<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::abort_dataset_commit_impl(warehouse_id, dataset_id, snapshot_id, transaction).await
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

    /// The commit a request carrying `key` published, on whichever dataset, and
    /// whatever has become of its snapshot since. `None` when no snapshot carries
    /// the key, as once it is purged.
    async fn get_dataset_snapshot_by_idempotency_key<'a>(
        warehouse_id: WarehouseId,
        key: IdempotencyKey,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Option<RecordedCommit>, CommitDatasetError> {
        Self::get_dataset_snapshot_by_idempotency_key_impl(warehouse_id, key, transaction).await
    }

    /// Every published snapshot of the dataset, expired ones included, as
    /// retention plans over them.
    async fn list_dataset_snapshot_graph<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<DatasetSnapshotNode>, CommitDatasetError> {
        Self::list_dataset_snapshot_graph_impl(warehouse_id, dataset_id, transaction).await
    }

    /// Expire `snapshot_ids`, purgeable from `purge_after`, returning how many
    /// expired. One a ref points at or a live grant reads is skipped, and an
    /// expired one a ref points at is restored: nothing held is ever expired.
    async fn expire_dataset_snapshots<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_ids: &[DatasetSnapshotId],
        purge_after: chrono::DateTime<chrono::Utc>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<u64, CommitDatasetError> {
        Self::expire_dataset_snapshots_impl(
            warehouse_id,
            dataset_id,
            snapshot_ids,
            purge_after,
            transaction,
        )
        .await
    }

    /// Delete expired snapshots past `purge_after`, with their rows.
    ///
    /// A surviving snapshot that reconstructs through a purged one is first made
    /// a checkpoint, so what it reads never changes.
    async fn purge_expired_dataset_snapshots<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetPurge, CommitDatasetError> {
        Self::purge_expired_dataset_snapshots_impl(warehouse_id, dataset_id, transaction).await
    }

    /// Expire one snapshot, purgeable from `purge_after`, and return when it may be
    /// purged: `purge_after`, or the time it was given when it had already expired.
    /// One a ref points at or a live grant reads is refused.
    async fn expire_dataset_snapshot<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        purge_after: chrono::DateTime<chrono::Utc>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<chrono::DateTime<chrono::Utc>, ExpireDatasetSnapshotError> {
        Self::expire_dataset_snapshot_impl(
            warehouse_id,
            dataset_id,
            snapshot_id,
            purge_after,
            transaction,
        )
        .await
    }

    /// Bring an expired snapshot back before it is purged.
    async fn restore_dataset_snapshot<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetSnapshot, RestoreDatasetSnapshotError> {
        Self::restore_dataset_snapshot_impl(warehouse_id, dataset_id, snapshot_id, transaction)
            .await
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
    /// A structural rule, not an authorization one: it holds for every principal,
    /// so a protected branch takes changes only by fast-forward no matter who is
    /// asking.
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

    /// One page of a snapshot's files in key order, starting at `from_key` and
    /// after `after_key`. Returns the key to continue after, `None` at the end.
    /// An expired snapshot reads as not found unless `include_expired`.
    #[allow(clippy::too_many_arguments)]
    async fn list_snapshot_files<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        from_key: Option<&str>,
        after_key: Option<&str>,
        page_size: i64,
        include_expired: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(Vec<ManifestEntry>, Option<String>), ListManifestEntriesError> {
        Self::list_snapshot_files_impl(
            warehouse_id,
            dataset_id,
            snapshot_id,
            from_key,
            after_key,
            page_size,
            include_expired,
            transaction,
        )
        .await
    }

    /// Record objects an import listed into `snapshot_id`, its staging snapshot,
    /// to be read back in key order by [`Self::read_import_listing`].
    async fn spool_import_listing<'a>(
        warehouse_id: WarehouseId,
        snapshot_id: DatasetSnapshotId,
        objects: &[ListedObject],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::spool_import_listing_impl(warehouse_id, snapshot_id, objects, transaction).await
    }

    /// Record an access grant, sweeping the dataset's expired ones.
    async fn create_dataset_access_grant<'a>(
        grant: DatasetAccessGrantCreation,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DatasetAccessGrant, DatasetAccessGrantError> {
        Self::create_dataset_access_grant_impl(grant, transaction).await
    }

    /// The grant a request carrying `key` issued, on whichever dataset and to
    /// whichever caller: the caller judges whether it is theirs. `None` when no
    /// grant carries the key, as once an expired one is swept.
    async fn get_dataset_access_grant_by_idempotency_key<'a>(
        warehouse_id: WarehouseId,
        key: IdempotencyKey,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Option<DatasetAccessGrant>, DatasetAccessGrantError> {
        Self::get_dataset_access_grant_by_idempotency_key_impl(warehouse_id, key, transaction).await
    }

    /// An access grant by id, revoked or expired ones included: the caller judges.
    async fn get_dataset_access_grant<'a>(
        warehouse_id: WarehouseId,
        grant_id: DatasetAccessGrantId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Option<DatasetAccessGrant>, DatasetAccessGrantError> {
        Self::get_dataset_access_grant_impl(warehouse_id, grant_id, transaction).await
    }

    /// Revoke an access grant of `dataset_id`. Revoking twice keeps the first
    /// revocation's time. `None` if the dataset has no such grant.
    async fn revoke_dataset_access_grant<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        grant_id: DatasetAccessGrantId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Option<DatasetAccessGrant>, DatasetAccessGrantError> {
        Self::revoke_dataset_access_grant_impl(warehouse_id, dataset_id, grant_id, transaction)
            .await
    }

    /// The files `keys` name in `snapshot_id`, as it resolves them. A key the
    /// snapshot does not hold is absent from the result.
    async fn get_snapshot_files<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        keys: &[String],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<ManifestEntry>, ListManifestEntriesError> {
        Self::get_snapshot_files_impl(warehouse_id, dataset_id, snapshot_id, keys, transaction)
            .await
    }

    /// The next `limit` spooled objects after `after`, in key order. Fewer than
    /// `limit` means the spool is exhausted.
    async fn read_import_listing<'a>(
        warehouse_id: WarehouseId,
        snapshot_id: DatasetSnapshotId,
        after: Option<&str>,
        limit: i64,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<ListedObject>, CommitDatasetError> {
        Self::read_import_listing_impl(warehouse_id, snapshot_id, after, limit, transaction).await
    }

    /// Mark the objects among `keys` the spool of `snapshot_id` holds as pointed
    /// at by files named apart from their storage path.
    async fn mark_import_objects_referenced<'a>(
        warehouse_id: WarehouseId,
        snapshot_id: DatasetSnapshotId,
        keys: &[String],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::mark_import_objects_referenced_impl(warehouse_id, snapshot_id, keys, transaction)
            .await
    }

    /// The objects among `keys` the spool of `snapshot_id` holds. A key it does
    /// not hold has no entry.
    async fn get_listed_import_objects<'a>(
        warehouse_id: WarehouseId,
        snapshot_id: DatasetSnapshotId,
        keys: &[String],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<ListedObject>, CommitDatasetError> {
        Self::get_listed_import_objects_impl(warehouse_id, snapshot_id, keys, transaction).await
    }

    /// Drop what an import spooled into `snapshot_id`.
    async fn clear_import_listing<'a>(
        warehouse_id: WarehouseId,
        snapshot_id: DatasetSnapshotId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::clear_import_listing_impl(warehouse_id, snapshot_id, transaction).await
    }

    /// Replace what the last materialization check recorded for `snapshot_id`.
    async fn record_snapshot_materialization<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        findings: &MaterializationFindings,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CommitDatasetError> {
        Self::record_snapshot_materialization_impl(
            warehouse_id,
            dataset_id,
            snapshot_id,
            findings,
            transaction,
        )
        .await
    }

    /// The last materialization check of an active snapshot of `dataset_id`, its
    /// degraded files a page at a time. `None` when it was never checked.
    async fn get_snapshot_materialization<'a>(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        page_token: Option<&str>,
        page_size: i64,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Option<SnapshotMaterialization>, ListManifestEntriesError> {
        Self::get_snapshot_materialization_impl(
            warehouse_id,
            dataset_id,
            snapshot_id,
            page_token,
            page_size,
            transaction,
        )
        .await
    }

    /// The last check of each of `snapshot_ids` that was checked.
    async fn get_snapshot_materialization_statuses<'a>(
        warehouse_id: WarehouseId,
        snapshot_ids: &[DatasetSnapshotId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<SnapshotMaterializationStatus>, ListDatasetRefsError> {
        Self::get_snapshot_materialization_statuses_impl(warehouse_id, snapshot_ids, transaction)
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
    /// move — later commits on the branch it was cut from do not affect it.
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
    /// The dataset's location when the snapshot was committed.
    pub location: String,
    pub is_checkpoint: bool,
    pub summary: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// What a commit published, and what it tells its caller. Only the snapshot is
/// persisted; the rest describes this commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetCommitOutcome {
    pub snapshot: DatasetSnapshot,
    /// The chain has grown past the checkpoint interval, so the caller should
    /// enqueue a fold. Advisory: the worker re-checks before doing any work, and
    /// a missed enqueue costs read speed, never correctness.
    pub checkpoint_due: bool,
    /// What the commit recorded, and the files it left out under
    /// [`ConstraintViolationPolicy::Skip`].
    pub staged: StagedBatch,
}

/// A published snapshot as retention sees it: its place in the graph, its age,
/// and whether a reader still holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetSnapshotNode {
    pub snapshot_id: DatasetSnapshotId,
    pub parent_snapshot_id: Option<DatasetSnapshotId>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Expired, and waiting out its grace period.
    pub expired: bool,
    /// Until when a live access grant reads it, if one does.
    pub pinned_until: Option<chrono::DateTime<chrono::Utc>>,
}

/// What one purge deleted, and when the next expired snapshot falls due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatasetPurge {
    pub purged: u64,
    pub next_purge_after: Option<chrono::DateTime<chrono::Utc>>,
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

/// A caller's standing to have one snapshot's files signed, issued after a
/// read authorization on a ref and checked, not re-authorized, on each signing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetAccessGrant {
    pub grant_id: DatasetAccessGrantId,
    pub warehouse_id: WarehouseId,
    pub dataset_id: DatasetId,
    pub snapshot_id: DatasetSnapshotId,
    /// The ref the grant was issued through. The snapshot is what it covers.
    pub ref_name: String,
    /// The actor that obtained the grant; only it may use it.
    pub actor: String,
    /// When set, only files of this content type may be signed.
    pub content_type: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// A grant to record. Everything but the timestamps the store sets.
#[derive(Debug, Clone)]
pub struct DatasetAccessGrantCreation {
    pub grant_id: DatasetAccessGrantId,
    pub warehouse_id: WarehouseId,
    pub dataset_id: DatasetId,
    pub snapshot_id: DatasetSnapshotId,
    pub ref_name: String,
    pub actor: String,
    pub content_type: Option<String>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// The request's `Idempotency-Key`, recorded so a retry is answered with
    /// this grant.
    pub idempotency_key: Option<IdempotencyKey>,
}

/// What a materialization check found wrong with a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum DegradedFileProblem {
    /// The listing did not show its object.
    Missing,
    /// Its object was written again since the file was recorded: its size or
    /// etag differs. Reading the file returns bytes the snapshot never recorded,
    /// unless it records a version.
    Changed,
}

/// A file a materialization check found missing from storage or rewritten there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DegradedFile {
    pub logical_key: String,
    pub physical_path: String,
    pub problem: DegradedFileProblem,
}

/// What an import's materialization check covered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct MaterializationReport {
    /// Snapshots a ref points at, each checked against the listing.
    pub checked_snapshots: i64,
    /// Of those, the ones with files missing or changed.
    pub degraded_snapshots: i64,
}

/// What a materialization check found in one snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaterializationFindings {
    /// Files whose object the listing did not show, in all.
    pub missing_files: i64,
    /// Files whose object was written again since, in all.
    pub changed_files: i64,
    /// The first of both, in key order.
    pub files: Vec<DegradedFile>,
}

/// What a keyed commit recorded: its snapshot, and what its constraints left out,
/// for a retry to be answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCommit {
    pub dataset_id: DatasetId,
    pub snapshot: DatasetSnapshot,
    pub skipped: Vec<SkippedFile>,
}

/// What the last materialization check of a snapshot found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMaterialization {
    pub checked_at: chrono::DateTime<chrono::Utc>,
    /// Files whose object the check did not find, in all.
    pub missing_files: i64,
    /// Files whose object was written again since, in all.
    pub changed_files: i64,
    /// A page of the files the check kept, in key order.
    pub files: Vec<DegradedFile>,
    pub next_page_token: Option<String>,
}

/// The last check of a snapshot, without its files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotMaterializationStatus {
    pub snapshot_id: DatasetSnapshotId,
    pub missing_files: i64,
    pub changed_files: i64,
    pub checked_at: chrono::DateTime<chrono::Utc>,
}

/// An object as a storage listing reported it, keyed as the manifest will be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub logical_key: String,
    pub physical_path: String,
    pub size: Option<i64>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
    pub etag: Option<String>,
    /// The object version the listing reported, where it reports one.
    pub version_id: Option<String>,
    /// A file named apart from its storage path already points at this object.
    /// False as a listing reports it; an import marks it in its spool.
    pub referenced: bool,
}

/// How many staged manifest rows of each kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StagedChanges {
    pub added: u64,
    pub modified: u64,
    pub removed: u64,
}

/// What one call staging files recorded, and what it left out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StagedBatch {
    pub staged: StagedChanges,
    /// Files the constraints refused, under [`ConstraintViolationPolicy::Skip`].
    pub skipped: Vec<SkippedFile>,
}

impl StagedChanges {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.added == 0 && self.modified == 0 && self.removed == 0
    }

    /// These changes less `dropped`, as after a rebase.
    #[must_use]
    pub fn without(self, dropped: Self) -> Self {
        Self {
            added: self.added.saturating_sub(dropped.added),
            modified: self.modified.saturating_sub(dropped.modified),
            removed: self.removed.saturating_sub(dropped.removed),
        }
    }
}

/// One commit: files to add and logical keys to remove, against an expected parent.
#[derive(Debug, Clone)]
pub struct DatasetCommit {
    pub warehouse_id: WarehouseId,
    pub dataset_id: DatasetId,
    pub branch: String,
    /// The snapshot the caller believes the branch is on. `None` means "the
    /// branch has no commits yet". The compare-and-swap is against this value,
    /// so a stale caller loses and a concurrent commit stands.
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
    /// The request's `Idempotency-Key`, recorded on the snapshot so a retry is
    /// answered with it.
    pub idempotency_key: Option<IdempotencyKey>,
    pub on_constraint_violation: ConstraintViolationPolicy,
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
            .code(StatusCode::CONFLICT.as_u16())
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

define_simple_error!(DatasetMoveTag, "Cannot move a tag: tags are immutable");
impl From<DatasetMoveTag> for ErrorModel {
    fn from(err: DatasetMoveTag) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetMoveTag")
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
// is for — and reset is separately authorized.
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

define_simple_error!(
    DatasetSnapshotHeld,
    "The snapshot is held: a ref points at it or a live access grant reads it"
);
impl From<DatasetSnapshotHeld> for ErrorModel {
    fn from(err: DatasetSnapshotHeld) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("DatasetSnapshotHeld")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum ExpireDatasetSnapshotError,
    stack_message: "Error expiring dataset snapshot",
    variants: [
        DatasetSnapshotNotFound,
        DatasetSnapshotHeld,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum RestoreDatasetSnapshotError,
    stack_message: "Error restoring dataset snapshot",
    variants: [
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
        DatasetMoveTag,
        DatasetRefProtected,
        DatasetSnapshotNotFound,
        DatasetSnapshotNotADescendant,
        DatasetCommitConflict,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum DatasetAccessGrantError,
    stack_message: "Error recording a dataset access grant",
    variants: [
        DatasetSnapshotNotFound,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A misspelt field is refused, not dropped: dropped, it would lift every
    /// constraint, or fall back to the warehouse's grace period.
    #[test]
    fn test_settings_refuse_fields_they_do_not_know() {
        assert!(
            serde_json::from_value::<DatasetConstraints>(serde_json::json!({
                "max_file_size": 100
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<DatasetRetention>(serde_json::json!({
                "mode": "manual",
                "grace_period": "P1D"
            }))
            .is_err()
        );
        assert_eq!(
            serde_json::from_value::<DatasetRetention>(serde_json::json!({
                "mode": "manual",
                "grace-period": "P1D"
            }))
            .unwrap(),
            DatasetRetention::Manual {
                grace_period: Some(chrono::Duration::days(1))
            }
        );
        assert_eq!(
            serde_json::from_value::<DatasetRetention>(serde_json::json!({"mode": "inherit"}))
                .unwrap(),
            DatasetRetention::Inherit {}
        );
        assert!(
            serde_json::from_value::<DatasetRetention>(serde_json::json!({
                "mode": "inherit",
                "grace-period": "P1D"
            }))
            .is_err(),
            "a grace period is not the warehouse's"
        );
    }
}
