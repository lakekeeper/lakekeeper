use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    routing::{delete, get, post},
};
use http::{HeaderMap, StatusCode};
#[cfg(feature = "open-api")]
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use iceberg_ext::catalog::rest::StorageCredential;
use serde::{Deserialize, Serialize};

#[cfg(feature = "open-api")]
use crate::api::endpoints::DatasetV1Endpoint;
use crate::{
    api::{
        ApiContext, Result,
        iceberg::{
            types::{DropParams, Prefix},
            v1::{
                DataAccess, DataAccessMode,
                namespace::{NamespaceIdentUrl, NamespaceParameters},
                tables::parse_data_access,
            },
        },
    },
    request_metadata::RequestMetadata,
    service::{
        ConstraintViolationPolicy, DatasetAccessGrantId, DatasetConstraints, DatasetId,
        DatasetInfo, DatasetOwnership, DatasetRefType, DatasetRetention, DatasetSnapshot,
        DatasetSnapshotId, DegradedFile, ManifestEntry, MaterializationReport, SkippedFile,
        SnapshotMaterialization, SnapshotMaterializationStatus, tasks::TaskId,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateDatasetRequest {
    pub name: String,
    /// Location of the dataset: an absolute URI inside the warehouse's storage
    /// profile.
    ///
    /// Omitted, Lakekeeper allocates an exclusive prefix and owns it (managed).
    /// Supplied, the dataset borrows an existing prefix and Lakekeeper never
    /// mutates or deletes objects under it (imported).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<DatasetConstraints>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DatasetData {
    pub name: String,
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub id: DatasetId,
    pub location: String,
    /// Whether Lakekeeper owns the prefix (`managed`) or borrows it (`imported`).
    pub ownership: DatasetOwnership,
    /// Whether the dataset is protected from being deleted.
    pub protected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<DatasetConstraints>,
    /// The dataset's own retention policy. Absent while it follows the
    /// warehouse's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<DatasetRetention>,
}

impl From<&DatasetInfo> for DatasetData {
    fn from(info: &DatasetInfo) -> Self {
        Self {
            name: info.name.clone(),
            id: info.dataset_id,
            location: info.location.to_string(),
            ownership: info.ownership,
            protected: info.protected,
            constraints: (!info.constraints.is_empty()).then(|| info.constraints.clone()),
            retention: (!info.retention.is_inherit()).then(|| info.retention.clone()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadDatasetResponse {
    pub dataset: DatasetData,
}

/// Settings to change. A field left out keeps its current value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct UpdateDatasetSettingsRequest {
    /// Replaces the dataset's constraints; empty constraints lift them. Files already
    /// committed are not re-checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<DatasetConstraints>,
    /// Replaces the dataset's retention policy; `{"mode": "inherit"}` returns it to
    /// the warehouse's. Applies from the next retention pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<DatasetRetention>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DatasetIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub id: DatasetId,
    pub ownership: DatasetOwnership,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListDatasetsResponse {
    pub identifiers: Vec<DatasetIdentifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListDatasetsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
}

impl axum::response::IntoResponse for LoadDatasetResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for ListDatasetsResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CommitFile {
    /// What users see, filter and address. Relative to the dataset location.
    pub logical_key: String,
    /// Where the bytes are. Defaults to `logical-key` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CommitDatasetRequest {
    /// The snapshot the caller believes the branch is on; omit for the first commit.
    /// A mismatch is answered with 409 and the current head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub parent_snapshot_id: Option<DatasetSnapshotId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<CommitFile>,
    /// Logical keys to remove. Their objects stay in storage for older snapshots.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<String>,
    /// Free-form correlation keys recorded on the snapshot (pipeline run id,
    /// code commit, trace id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
    /// What to do with a file the dataset's constraints refuse: `reject` the
    /// commit (the default), or `skip` the file and commit the rest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_constraint_violation: Option<ConstraintViolationPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotResponse {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub snapshot_id: DatasetSnapshotId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub parent_snapshot_id: Option<DatasetSnapshotId>,
    pub location: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Files a `skip` commit left out, with why. A replayed commit reports what
    /// the original left out; a restored snapshot reports none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<SkippedFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DatasetRefResponse {
    pub name: String,
    pub typ: DatasetRefType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub snapshot_id: Option<DatasetSnapshotId>,
    pub protected: bool,
    /// What the last materialization check found for the ref's snapshot. Listed
    /// refs carry it once their snapshot was checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization: Option<RefMaterialization>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListDatasetRefsResponse {
    pub refs: Vec<DatasetRefResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateDatasetRefRequest {
    pub name: String,
    pub typ: DatasetRefType,
    /// Source to branch or tag from: another ref's name, or a snapshot id, resolved
    /// now.
    pub source: DatasetRefSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum DatasetRefSource {
    #[serde(rename_all = "kebab-case")]
    Ref { name: String },
    #[serde(rename_all = "kebab-case")]
    Snapshot {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        snapshot_id: DatasetSnapshotId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct MoveDatasetRefRequest {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub snapshot_id: DatasetSnapshotId,
    /// Compare-and-swap guard: the head the caller believes the branch is on.
    /// Omitted, the branch is expected to have no commits yet, as for a commit's
    /// `parent-snapshot-id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub expected_snapshot_id: Option<DatasetSnapshotId>,
    /// `false` performs a reset: the target need not descend from the current
    /// head, so commits can be abandoned. It takes the `reset` action.
    #[serde(default = "default_true")]
    #[cfg_attr(feature = "open-api", schema(default = true))]
    pub fast_forward: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SetDatasetRefProtectionRequest {
    pub protected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DatasetFile {
    pub logical_key: String,
    pub physical_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListDatasetFilesResponse {
    /// The snapshot the ref resolved to; pagination is pinned to it. Absent on a
    /// branch with no commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub snapshot_id: Option<DatasetSnapshotId>,
    pub files: Vec<DatasetFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    /// How to read the files' bytes. The server chooses.
    pub access_mode: DatasetAccessMode,
}

/// How a reader gets a version's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum DatasetAccessMode {
    /// Credentials for the dataset's prefix, from `.../credentials`. Offered for a
    /// managed dataset whose storage profile vends credentials.
    StorageCredentials,
    /// Per-file signed URLs: obtain an access grant, then sign keys in batches.
    Presigned,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateDatasetAccessGrantRequest {
    /// Sign only files of this content type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DatasetAccessGrantResponse {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub grant_id: DatasetAccessGrantId,
    /// The snapshot the ref resolved to. The grant covers its files only, so a
    /// commit landing on the ref afterwards changes nothing it signs.
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub snapshot_id: DatasetSnapshotId,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// When set, only files of this content type can be signed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// How to read the files' bytes. A grant signs in either mode.
    pub access_mode: DatasetAccessMode,
    /// The most keys one signing call accepts.
    pub max_keys_per_request: usize,
    /// How long a signed URL stays valid, in seconds.
    pub url_validity_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SignDatasetFilesRequest {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub grant_id: DatasetAccessGrantId,
    /// Logical keys to sign. Each must be a file of the grant's snapshot within
    /// its content-type filter.
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SignDatasetFilesResponse {
    pub files: Vec<SignedDatasetFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SignedDatasetFile {
    pub logical_key: String,
    /// A GET of this URL returns the file; HTTP range requests work.
    pub url: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// The etag the snapshot recorded. A download whose `ETag` header differs,
    /// ignoring surrounding quotes, is not the file the snapshot recorded. Absent
    /// when it recorded none, and on GCS, whose downloads report a different etag
    /// than its listings; there a file with a `version-id` is read at that
    /// generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

/// The two versions to compare, each a ref or a snapshot, and the page to read.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct DiffDatasetQuery {
    /// The ref to compare from. Give this or `fromSnapshotId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// The snapshot to compare from. Give this or `from`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", param(value_type = Option<uuid::Uuid>))]
    pub from_snapshot_id: Option<DatasetSnapshotId>,
    /// The ref to compare to. Give this or `toSnapshotId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// The snapshot to compare to. Give this or `to`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", param(value_type = Option<uuid::Uuid>))]
    pub to_snapshot_id: Option<DatasetSnapshotId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
}

/// How a file differs between the two versions compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum DatasetFileChangeKind {
    /// In `to` only.
    Added,
    /// In `from` only.
    Removed,
    /// In both, recorded differently.
    Modified,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DatasetFileChange {
    pub logical_key: String,
    pub change: DatasetFileChangeKind,
    /// The file as `from` records it. Absent when it was added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<DatasetFile>,
    /// The file as `to` records it. Absent when it was removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<DatasetFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct DiffDatasetResponse {
    /// The snapshot `from` resolved to. Pagination is scoped to it. Absent for a
    /// branch with no commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub from_snapshot_id: Option<DatasetSnapshotId>,
    /// The snapshot `to` resolved to. Pagination is scoped to it. Absent for a
    /// branch with no commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub to_snapshot_id: Option<DatasetSnapshotId>,
    /// Changed files in logical-key order. A page can be short, or empty, while
    /// more changes remain; only a missing `next-page-token` ends the diff.
    pub changes: Vec<DatasetFileChange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// A snapshot expired by hand.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ExpireDatasetSnapshotResponse {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub snapshot_id: DatasetSnapshotId,
    /// When the purge may delete it, unless it is restored first.
    pub purge_after: chrono::DateTime<chrono::Utc>,
}

impl axum::response::IntoResponse for ExpireDatasetSnapshotResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

/// Whether the objects behind a snapshot's files were all in storage when last
/// checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum MaterializationStatus {
    /// No import has checked the snapshot.
    Unchecked,
    /// Every file the check judged had its object, as recorded.
    FullyMaterialized,
    /// Some files' objects were gone, or written again since. The snapshot still
    /// resolves; reading those files fails, or returns other bytes.
    PartiallyDegraded,
}

impl MaterializationStatus {
    fn of(missing_files: i64, changed_files: i64) -> Self {
        if missing_files == 0 && changed_files == 0 {
            Self::FullyMaterialized
        } else {
            Self::PartiallyDegraded
        }
    }
}

/// A ref's snapshot, as the last check found it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RefMaterialization {
    pub status: MaterializationStatus,
    pub missing_files: i64,
    pub changed_files: i64,
    pub checked_at: chrono::DateTime<chrono::Utc>,
}

impl From<SnapshotMaterializationStatus> for RefMaterialization {
    fn from(checked: SnapshotMaterializationStatus) -> Self {
        Self {
            status: MaterializationStatus::of(checked.missing_files, checked.changed_files),
            missing_files: checked.missing_files,
            changed_files: checked.changed_files,
            checked_at: checked.checked_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMaterializationQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotMaterializationResponse {
    pub status: MaterializationStatus,
    /// When the last check ran. Absent while `unchecked`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Files whose object the check did not find, in all. Absent while `unchecked`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing_files: Option<i64>,
    /// Files whose object was written again since they were recorded, in all.
    /// Absent while `unchecked`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changed_files: Option<i64>,
    /// A page of the missing and changed files, in key order; the first ten
    /// thousand are kept.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<DegradedFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

impl From<Option<SnapshotMaterialization>> for SnapshotMaterializationResponse {
    fn from(checked: Option<SnapshotMaterialization>) -> Self {
        match checked {
            None => Self {
                status: MaterializationStatus::Unchecked,
                checked_at: None,
                missing_files: None,
                changed_files: None,
                files: Vec::new(),
                next_page_token: None,
            },
            Some(checked) => Self {
                status: MaterializationStatus::of(checked.missing_files, checked.changed_files),
                checked_at: Some(checked.checked_at),
                missing_files: Some(checked.missing_files),
                changed_files: Some(checked.changed_files),
                files: checked.files,
                next_page_token: checked.next_page_token,
            },
        }
    }
}

impl axum::response::IntoResponse for SnapshotMaterializationResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListDatasetFilesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
    /// Return only files declaring this content type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

impl axum::response::IntoResponse for DiffDatasetResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl From<CommitFile> for ManifestEntry {
    fn from(file: CommitFile) -> Self {
        Self {
            physical_path: file
                .physical_path
                .unwrap_or_else(|| file.logical_key.clone()),
            logical_key: file.logical_key,
            etag: file.etag,
            size: file.size,
            content_type: file.content_type,
            checksum: file.checksum,
            version_id: file.version_id,
            last_modified: file.last_modified,
        }
    }
}

impl From<ManifestEntry> for DatasetFile {
    fn from(entry: ManifestEntry) -> Self {
        Self {
            logical_key: entry.logical_key,
            physical_path: entry.physical_path,
            etag: entry.etag,
            size: entry.size,
            content_type: entry.content_type,
            checksum: entry.checksum,
            version_id: entry.version_id,
            last_modified: entry.last_modified,
        }
    }
}

impl From<DatasetSnapshot> for SnapshotResponse {
    fn from(snapshot: DatasetSnapshot) -> Self {
        Self {
            snapshot_id: snapshot.snapshot_id,
            parent_snapshot_id: snapshot.parent_snapshot_id,
            location: snapshot.location,
            summary: snapshot.summary,
            created_at: snapshot.created_at,
            skipped: Vec::new(),
        }
    }
}

impl axum::response::IntoResponse for SnapshotResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for ListDatasetRefsResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for DatasetRefResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for DatasetAccessGrantResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for SignDatasetFilesResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for ListDatasetFilesResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatasetSnapshotParameters {
    pub prefix: Option<Prefix>,
    pub namespace: iceberg::NamespaceIdent,
    pub dataset_name: String,
    pub snapshot_id: DatasetSnapshotId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatasetAccessGrantParameters {
    pub prefix: Option<Prefix>,
    pub namespace: iceberg::NamespaceIdent,
    pub dataset_name: String,
    pub grant_id: DatasetAccessGrantId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatasetRefParameters {
    pub prefix: Option<Prefix>,
    pub namespace: iceberg::NamespaceIdent,
    pub dataset_name: String,
    pub ref_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatasetParameters {
    pub prefix: Option<Prefix>,
    pub namespace: iceberg::NamespaceIdent,
    pub dataset_name: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ImportDatasetRequest {
    /// Run the scan on the task queue. The response then carries a task id and no
    /// snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued: Option<bool>,
    /// `add-only` registers what it finds and touches nothing else. `sync` also
    /// records files whose bytes changed and removes files gone from the prefix,
    /// so the snapshot mirrors the bucket. Defaults to `add-only`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ImportMode>,
    /// Branch to commit the discovered files to. Defaults to `main`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Scan only this path below the dataset's location. Relative; may not escape it.
    /// A sync removes only keys below it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_prefix: Option<String>,
    /// Register only keys ending with this suffix, e.g. `.parquet`. A sync removes
    /// only keys ending with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
    /// Register only keys matching one of these globs, e.g. `**/*.jpg`. Keys are
    /// relative to the dataset's location; `*` and `?` match within one path
    /// segment and `**` across segments. A sync removes only keys they match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    /// Leave out keys matching any of these globs. A sync leaves such keys alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
    /// Also leave out the marker files and scratch directories job writers leave
    /// behind: `**/_SUCCESS`, `**/_temporary/**` and `**/.checkpoint/**`. Defaults
    /// to `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_excludes: Option<bool>,
    /// Record each object's current version, so a signed URL reads exactly the
    /// bytes the snapshot recorded even after the key is written again. On S3 the
    /// bucket needs versioning on, and the warehouse's credential
    /// `s3:ListBucketVersions`; GCS records the generation either way. Refused on
    /// Azure, whose listings carry no version. Defaults to `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_versions: Option<bool>,
    /// After the import, check every snapshot a branch or tag points at against
    /// the listing, and record the files whose object is gone or was written
    /// again since. Needs the whole prefix scanned: no `sub-prefix`, `suffix` or
    /// `include`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_materialization: Option<bool>,
    /// Stop after this many files, reporting `truncated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_files: Option<i64>,
    /// Commit properties, e.g. a pipeline run id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
    /// What to do with an object the dataset's constraints refuse. Defaults to
    /// `skip`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_constraint_violation: Option<ConstraintViolationPolicy>,
}

impl ImportDatasetRequest {
    /// The policy the import applies, `skip` unless the request names one.
    #[must_use]
    pub fn constraint_violation_policy(&self) -> ConstraintViolationPolicy {
        self.on_constraint_violation
            .unwrap_or(ConstraintViolationPolicy::Skip)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ImportMode {
    /// Register newly seen objects only.
    #[default]
    AddOnly,
    /// Make the snapshot mirror the prefix: additions, modifications and removals.
    Sync,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ImportDatasetResponse {
    /// The snapshot the branch is on after the import. Absent when the scan was
    /// queued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub snapshot_id: Option<DatasetSnapshotId>,
    /// The queued scan. Absent when the import ran inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub task_id: Option<TaskId>,
    /// Newly registered files.
    pub imported: i64,
    /// Files whose object changed (its size, modification time, etag or version)
    /// under an existing key, or, for a file named apart from its storage path,
    /// where its bytes are.
    pub modified: i64,
    /// Files removed because they are absent from the prefix. Always zero in
    /// `add-only` mode.
    pub removed: i64,
    /// The scan stopped at `max-files`; more objects remain under the prefix.
    pub truncated: bool,
    /// Objects left out: those the dataset's constraints refused under `skip`,
    /// and those stored under a name a file of the dataset goes by with its bytes
    /// elsewhere.
    pub skipped: i64,
    /// The first of those, with why; at most a hundred.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_files: Vec<SkippedFile>,
    /// What the materialization check found, when one ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization: Option<MaterializationReport>,
}

impl axum::response::IntoResponse for ImportDatasetResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadDatasetCredentialsRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadDatasetCredentialsResponse {
    pub storage_credentials: Vec<StorageCredential>,
    /// Where the caller writes new objects, present when it may commit: a fresh folder
    /// below the location, and the only one its write credential covers. Every request
    /// names a new one. Commit the objects with their `physical-path` under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_prefix: Option<String>,
}

impl axum::response::IntoResponse for LoadDatasetCredentialsResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RenameDatasetTarget {
    pub namespace: Vec<String>,
    pub name: String,
}

impl TryFrom<RenameDatasetTarget> for iceberg::TableIdent {
    type Error = iceberg::Error;

    fn try_from(t: RenameDatasetTarget) -> std::result::Result<Self, Self::Error> {
        let namespace = iceberg::NamespaceIdent::from_vec(t.namespace)?;
        Ok(iceberg::TableIdent::new(namespace, t.name))
    }
}

impl From<iceberg::TableIdent> for RenameDatasetTarget {
    fn from(t: iceberg::TableIdent) -> Self {
        RenameDatasetTarget {
            namespace: t.namespace.inner(),
            name: t.name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RenameDatasetRequest {
    pub source: RenameDatasetTarget,
    pub destination: RenameDatasetTarget,
}

#[async_trait]
pub trait DatasetService<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    async fn create_dataset(
        parameters: NamespaceParameters,
        request: CreateDatasetRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse>;

    async fn load_dataset(
        parameters: DatasetParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse>;

    async fn list_datasets(
        parameters: NamespaceParameters,
        query: ListDatasetsQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetsResponse>;

    async fn drop_dataset(
        parameters: DatasetParameters,
        drop_params: DropParams,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    async fn commit_dataset(
        parameters: DatasetRefParameters,
        request: CommitDatasetRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<SnapshotResponse>;

    async fn list_dataset_refs(
        parameters: DatasetParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetRefsResponse>;

    async fn create_dataset_ref(
        parameters: DatasetParameters,
        request: CreateDatasetRefRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse>;

    async fn move_dataset_ref(
        parameters: DatasetRefParameters,
        request: MoveDatasetRefRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse>;

    async fn delete_dataset_ref(
        parameters: DatasetRefParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    async fn list_dataset_files(
        parameters: DatasetRefParameters,
        query: ListDatasetFilesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetFilesResponse>;

    async fn diff_dataset(
        parameters: DatasetParameters,
        query: DiffDatasetQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<DiffDatasetResponse>;

    async fn restore_dataset_snapshot(
        parameters: DatasetSnapshotParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<SnapshotResponse>;

    async fn get_dataset_snapshot_materialization(
        parameters: DatasetSnapshotParameters,
        query: SnapshotMaterializationQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<SnapshotMaterializationResponse>;

    async fn expire_dataset_snapshot(
        parameters: DatasetSnapshotParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ExpireDatasetSnapshotResponse>;

    async fn set_dataset_ref_protection(
        parameters: DatasetRefParameters,
        request: SetDatasetRefProtectionRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse>;

    async fn create_dataset_access_grant(
        parameters: DatasetRefParameters,
        request: CreateDatasetAccessGrantRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetAccessGrantResponse>;

    async fn revoke_dataset_access_grant(
        parameters: DatasetAccessGrantParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    async fn sign_dataset_files(
        parameters: DatasetSnapshotParameters,
        request: SignDatasetFilesRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<SignDatasetFilesResponse>;

    async fn rename_dataset(
        prefix: Option<Prefix>,
        request: RenameDatasetRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    async fn update_dataset_settings(
        parameters: DatasetParameters,
        request: UpdateDatasetSettingsRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse>;

    async fn load_dataset_credentials(
        parameters: DatasetParameters,
        request: LoadDatasetCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetCredentialsResponse>;

    async fn import_dataset(
        parameters: DatasetParameters,
        request: ImportDatasetRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ImportDatasetResponse>;
}

/// Create a dataset
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::CreateDataset.path(),
    params(("prefix" = String,), ("namespace" = String,)),
    request_body = CreateDatasetRequest,
    responses(
        (status = 200, body = LoadDatasetResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn create_dataset<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CreateDatasetRequest>,
) -> Result<LoadDatasetResponse> {
    I::create_dataset(
        NamespaceParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// List datasets in a namespace
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "dataset",
    path = DatasetV1Endpoint::ListDatasets.path(),
    params(("prefix" = String,), ("namespace" = String,), ListDatasetsQuery),
    responses(
        (status = 200, body = ListDatasetsResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn list_datasets<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
    Query(query): Query<ListDatasetsQuery>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<ListDatasetsResponse> {
    I::list_datasets(
        NamespaceParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
        },
        query,
        api_context,
        metadata,
    )
    .await
}

/// Load a dataset
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "dataset",
    path = DatasetV1Endpoint::LoadDataset.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,)),
    responses(
        (status = 200, body = LoadDatasetResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn load_dataset<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<LoadDatasetResponse> {
    I::load_dataset(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        api_context,
        metadata,
    )
    .await
}

/// Drop a dataset
///
/// A purge is refused for an imported dataset: Lakekeeper borrows the prefix and
/// never deletes objects it does not own. Drop it without one to remove the
/// catalog metadata.
#[cfg_attr(feature = "open-api", utoipa::path(
    delete,
    tag = "dataset",
    path = DatasetV1Endpoint::DropDataset.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), DropParams),
    responses(
        (status = 204, description = "Dataset dropped"),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn drop_dataset<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    Query(drop_params): Query<DropParams>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<StatusCode> {
    I::drop_dataset(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        drop_params,
        api_context,
        metadata,
    )
    .await
    .map(|()| StatusCode::NO_CONTENT)
}

/// Commit to a branch
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::CommitDataset.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("branch" = String,)),
    request_body = CommitDatasetRequest,
    responses(
        (status = 200, body = SnapshotResponse),
        (status = 409, description = "The branch moved: rebase onto the returned head", body = IcebergErrorResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn commit_dataset<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, branch)): Path<(Prefix, NamespaceIdentUrl, String, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CommitDatasetRequest>,
) -> Result<SnapshotResponse> {
    I::commit_dataset(
        DatasetRefParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            ref_name: branch,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// List branches and tags
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "dataset",
    path = DatasetV1Endpoint::ListDatasetRefs.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,)),
    responses(
        (status = 200, body = ListDatasetRefsResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn list_dataset_refs<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<ListDatasetRefsResponse> {
    I::list_dataset_refs(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        api_context,
        metadata,
    )
    .await
}

/// Create a branch or tag
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::CreateDatasetRef.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,)),
    request_body = CreateDatasetRefRequest,
    responses(
        (status = 200, body = DatasetRefResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn create_dataset_ref<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CreateDatasetRefRequest>,
) -> Result<DatasetRefResponse> {
    I::create_dataset_ref(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// Move a branch (fast-forward, or reset)
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::MoveDatasetRef.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("ref" = String,)),
    request_body = MoveDatasetRefRequest,
    responses(
        (status = 200, body = DatasetRefResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn move_dataset_ref<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, ref_name)): Path<(Prefix, NamespaceIdentUrl, String, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<MoveDatasetRefRequest>,
) -> Result<DatasetRefResponse> {
    I::move_dataset_ref(
        DatasetRefParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            ref_name,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// Delete a branch or tag
#[cfg_attr(feature = "open-api", utoipa::path(
    delete,
    tag = "dataset",
    path = DatasetV1Endpoint::DeleteDatasetRef.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("ref" = String,)),
    responses(
        (status = 204, description = "Ref deleted"),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn delete_dataset_ref<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, ref_name)): Path<(Prefix, NamespaceIdentUrl, String, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<StatusCode> {
    I::delete_dataset_ref(
        DatasetRefParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            ref_name,
        },
        api_context,
        metadata,
    )
    .await
    .map(|()| StatusCode::NO_CONTENT)
}

/// List the files of a ref
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "dataset",
    path = DatasetV1Endpoint::ListDatasetFiles.path(),
    params(
        ("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("ref" = String,),
        ListDatasetFilesQuery
    ),
    responses(
        (status = 200, body = ListDatasetFilesResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn list_dataset_files<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, ref_name)): Path<(Prefix, NamespaceIdentUrl, String, String)>,
    Query(query): Query<ListDatasetFilesQuery>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<ListDatasetFilesResponse> {
    I::list_dataset_files(
        DatasetRefParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            ref_name,
        },
        query,
        api_context,
        metadata,
    )
    .await
}

/// Compare two versions of a dataset
///
/// Lists the files that differ between two refs or snapshots, in logical-key
/// order. Pagination is pinned to the snapshots the first page resolved.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "dataset",
    path = DatasetV1Endpoint::DiffDataset.path(),
    params(
        ("prefix" = String,), ("namespace" = String,), ("dataset" = String,),
        DiffDatasetQuery
    ),
    responses(
        (status = 200, body = DiffDatasetResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn diff_dataset<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    Query(query): Query<DiffDatasetQuery>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<DiffDatasetResponse> {
    I::diff_dataset(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        query,
        api_context,
        metadata,
    )
    .await
}

/// Protect a branch, or lift protection
///
/// A protected branch refuses direct commits, resets and deletion for everyone;
/// changes reach it by fast-forward only.
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::SetDatasetRefProtection.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("ref" = String,)),
    request_body = SetDatasetRefProtectionRequest,
    responses(
        (status = 200, body = DatasetRefResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn set_dataset_ref_protection<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, ref_name)): Path<(Prefix, NamespaceIdentUrl, String, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<SetDatasetRefProtectionRequest>,
) -> Result<DatasetRefResponse> {
    I::set_dataset_ref_protection(
        DatasetRefParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            ref_name,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// Rename a dataset
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::RenameDataset.path(),
    params(("prefix" = String,)),
    request_body = RenameDatasetRequest,
    responses(
        (status = 204, description = "Dataset renamed successfully"),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn rename_dataset<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path(prefix): Path<Prefix>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<RenameDatasetRequest>,
) -> Result<StatusCode> {
    I::rename_dataset(Some(prefix), request, api_context, metadata)
        .await
        .map(|()| StatusCode::NO_CONTENT)
}

/// Change a dataset's settings
///
/// Each field sent replaces that setting; one left out keeps its value.
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::UpdateDatasetSettings.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,)),
    request_body = UpdateDatasetSettingsRequest,
    responses(
        (status = 200, body = LoadDatasetResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn update_dataset_settings<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateDatasetSettingsRequest>,
) -> Result<LoadDatasetResponse> {
    I::update_dataset_settings(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// Load storage credentials for a dataset
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "dataset",
    path = DatasetV1Endpoint::LoadDatasetCredentials.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,)),
    responses(
        (status = 200, description = "Storage credentials for the dataset", body = LoadDatasetCredentialsResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn load_dataset_credentials<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    headers: HeaderMap,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<LoadDatasetCredentialsResponse> {
    let data_access = match parse_data_access(&headers) {
        DataAccessMode::ClientManaged => DataAccess::not_specified(),
        DataAccessMode::ServerDelegated(da) => da,
    };

    I::load_dataset_credentials(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        LoadDatasetCredentialsRequest {},
        data_access,
        api_context,
        metadata,
    )
    .await
}

/// Import objects already under a dataset's prefix
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::ImportDataset.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,)),
    request_body = ImportDatasetRequest,
    responses(
        (status = 200, description = "Objects registered as a new snapshot", body = ImportDatasetResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn import_dataset<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<ImportDatasetRequest>,
) -> Result<ImportDatasetResponse> {
    I::import_dataset(
        DatasetParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// Obtain an access grant for the snapshot a ref resolves to
///
/// Authorizes reading the ref once. The grant then lets its holder sign that
/// snapshot's files, a batch at a time, until it expires or is revoked.
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::CreateDatasetAccessGrant.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("ref" = String,)),
    request_body = CreateDatasetAccessGrantRequest,
    responses(
        (status = 200, body = DatasetAccessGrantResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn create_dataset_access_grant<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, ref_name)): Path<(Prefix, NamespaceIdentUrl, String, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CreateDatasetAccessGrantRequest>,
) -> Result<DatasetAccessGrantResponse> {
    I::create_dataset_access_grant(
        DatasetRefParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            ref_name,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// Revoke an access grant
///
/// Signing with it stops at once; URLs already issued expire on their own.
#[cfg_attr(feature = "open-api", utoipa::path(
    delete,
    tag = "dataset",
    path = DatasetV1Endpoint::RevokeDatasetAccessGrant.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("grant" = uuid::Uuid,)),
    responses(
        (status = 204, description = "Grant revoked"),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn revoke_dataset_access_grant<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, grant_id)): Path<(
        Prefix,
        NamespaceIdentUrl,
        String,
        DatasetAccessGrantId,
    )>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<StatusCode> {
    I::revoke_dataset_access_grant(
        DatasetAccessGrantParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            grant_id,
        },
        api_context,
        metadata,
    )
    .await
    .map(|()| StatusCode::NO_CONTENT)
}

/// Restore a snapshot retention expired
///
/// An expired snapshot is hidden from every read until it is purged at the end
/// of its grace period. Restoring it makes it readable again; create a ref to it
/// to keep it, or a later retention pass may expire it again.
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::RestoreDatasetSnapshot.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("snapshot" = uuid::Uuid,)),
    responses(
        (status = 200, body = SnapshotResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn restore_dataset_snapshot<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, snapshot_id)): Path<(
        Prefix,
        NamespaceIdentUrl,
        String,
        DatasetSnapshotId,
    )>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<SnapshotResponse> {
    I::restore_dataset_snapshot(
        DatasetSnapshotParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            snapshot_id,
        },
        api_context,
        metadata,
    )
    .await
}

/// Expire a snapshot by hand
///
/// Hides the snapshot and starts its grace period; it is purged at the end of it
/// unless restored first. Refused with `409` for a snapshot a ref points at or a
/// live access grant reads; one already expired answers with the purge time it
/// was given.
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::ExpireDatasetSnapshot.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("snapshot" = uuid::Uuid,)),
    responses(
        (status = 200, body = ExpireDatasetSnapshotResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn expire_dataset_snapshot<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, snapshot_id)): Path<(
        Prefix,
        NamespaceIdentUrl,
        String,
        DatasetSnapshotId,
    )>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<ExpireDatasetSnapshotResponse> {
    I::expire_dataset_snapshot(
        DatasetSnapshotParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            snapshot_id,
        },
        api_context,
        metadata,
    )
    .await
}

/// Check which of a snapshot's files have their object
///
/// What the last import that ran with `check-materialization` found for this
/// snapshot: `unchecked` until one does, then how many files' objects were gone or
/// written again since, and which.
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "dataset",
    path = DatasetV1Endpoint::GetDatasetSnapshotMaterialization.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("snapshot" = uuid::Uuid,), SnapshotMaterializationQuery),
    responses(
        (status = 200, body = SnapshotMaterializationResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn get_dataset_snapshot_materialization<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, snapshot_id)): Path<(
        Prefix,
        NamespaceIdentUrl,
        String,
        DatasetSnapshotId,
    )>,
    Query(query): Query<SnapshotMaterializationQuery>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<SnapshotMaterializationResponse> {
    I::get_dataset_snapshot_materialization(
        DatasetSnapshotParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            snapshot_id,
        },
        query,
        api_context,
        metadata,
    )
    .await
}

/// Sign files of a snapshot for reading
///
/// Checks the grant — live, held by the caller, covering these keys — and runs no
/// policy, so a long read costs one authorization and one call per batch.
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "dataset",
    path = DatasetV1Endpoint::SignDatasetFiles.path(),
    params(("prefix" = String,), ("namespace" = String,), ("dataset" = String,), ("snapshot" = uuid::Uuid,)),
    request_body = SignDatasetFilesRequest,
    responses(
        (status = 200, body = SignDatasetFilesResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn sign_dataset_files<I: DatasetService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, dataset, snapshot_id)): Path<(
        Prefix,
        NamespaceIdentUrl,
        String,
        DatasetSnapshotId,
    )>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<SignDatasetFilesRequest>,
) -> Result<SignDatasetFilesResponse> {
    I::sign_dataset_files(
        DatasetSnapshotParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            dataset_name: dataset,
            snapshot_id,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

pub fn router<I: DatasetService<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        .route(
            "/{prefix}/namespaces/{namespace}/datasets",
            post(create_dataset::<I, S>).get(list_datasets::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}",
            get(load_dataset::<I, S>).delete(drop_dataset::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/refs",
            get(list_dataset_refs::<I, S>).post(create_dataset_ref::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/refs/{ref}",
            post(move_dataset_ref::<I, S>).delete(delete_dataset_ref::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/refs/{ref}/files",
            get(list_dataset_files::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/refs/{ref}/protection",
            post(set_dataset_ref_protection::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/refs/{ref}/access-grants",
            post(create_dataset_access_grant::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/access-grants/{grant}",
            delete(revoke_dataset_access_grant::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/snapshots/{snapshot}/files/sign",
            post(sign_dataset_files::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/snapshots/{snapshot}/restore",
            post(restore_dataset_snapshot::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/snapshots/{snapshot}/materialization",
            get(get_dataset_snapshot_materialization::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/snapshots/{snapshot}/expire",
            post(expire_dataset_snapshot::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/branches/{branch}/commits",
            post(commit_dataset::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/diff",
            get(diff_dataset::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/import",
            post(import_dataset::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/credentials",
            get(load_dataset_credentials::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/settings",
            post(update_dataset_settings::<I, S>),
        )
        .route("/{prefix}/datasets/rename", post(rename_dataset::<I, S>))
}

#[cfg(feature = "open-api")]
mod openapi;
#[cfg(feature = "open-api")]
pub use openapi::api_doc;
