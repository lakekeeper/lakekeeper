use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
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
    service::{DatasetConstraints, DatasetId, DatasetOwnership, DatasetRefType, DatasetSnapshotId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateDatasetRequest {
    pub name: String,
    /// Location of the dataset, relative to the warehouse storage profile.
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadDatasetResponse {
    pub dataset: DatasetData,
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
    /// The snapshot the caller believes the branch is on. Omit for the first
    /// commit. A mismatch is answered with 409 and the current head, so a stale
    /// writer rebases instead of overwriting a concurrent commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub parent_snapshot_id: Option<DatasetSnapshotId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<CommitFile>,
    /// Logical keys to remove. Metadata-only: the objects stay in storage,
    /// because older snapshots still reference them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<String>,
    /// Free-form correlation keys recorded on the snapshot (pipeline run id,
    /// code commit, trace id). The external-lineage hook.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
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
    /// Source to branch or tag from: another ref's name, or a raw snapshot id.
    /// The new ref points at whatever the source resolves to at this moment;
    /// a tag then never moves again.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub expected_snapshot_id: Option<DatasetSnapshotId>,
    /// `false` performs a reset: the target need not descend from the current
    /// head, so commits can be abandoned. Separately authorized for that reason.
    #[serde(default = "default_true")]
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
    /// The snapshot the ref resolved to. Pagination is scoped to it, so a
    /// concurrent commit cannot shift or tear the page sequence. Absent on a
    /// branch with no commits, which lists no files rather than failing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub snapshot_id: Option<DatasetSnapshotId>,
    pub files: Vec<DatasetFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
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

impl axum::response::IntoResponse for ListDatasetFilesResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
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
    /// Run the scan on the task queue instead of in this request. The response
    /// then carries a task id rather than a snapshot: a prefix with millions of
    /// objects takes minutes, which is longer than a request should live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued: Option<bool>,
    /// `add-only` registers what it finds and touches nothing else. `sync` also
    /// reports files whose bytes changed and files that have since disappeared
    /// from the prefix, so the snapshot mirrors the bucket. Defaults to `add-only`,
    /// because removing manifest entries is not something to do by accident.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ImportMode>,
    /// Branch to commit the discovered files to. Defaults to `main`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Scan only this path below the dataset's location. Relative; may not escape it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_prefix: Option<String>,
    /// Register only keys ending with this suffix, e.g. `.parquet`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
    /// Stop after this many files, reporting `truncated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_files: Option<i64>,
    /// Commit properties, e.g. a pipeline run id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
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
    /// The snapshot the import committed. Absent when the scan was queued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub snapshot_id: Option<DatasetSnapshotId>,
    /// The queued scan. Absent when the import ran inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub task_id: Option<crate::service::tasks::TaskId>,
    /// Newly registered files.
    pub imported: i64,
    /// Files whose size or modification time changed under an existing key.
    pub modified: i64,
    /// Files no longer under the prefix. Always zero in `add-only` mode.
    pub removed: i64,
    /// The scan stopped at `max-files`; more objects remain under the prefix.
    pub truncated: bool,
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

    async fn set_dataset_ref_protection(
        parameters: DatasetRefParameters,
        request: SetDatasetRefProtectionRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse>;

    async fn rename_dataset(
        prefix: Option<Prefix>,
        request: RenameDatasetRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

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
    params(
        ("prefix" = String,),
        ("namespace" = String,),
        ("dataset" = String,),
        ("purgeRequested" = Option<bool>, Query, description = "Delete the dataset's files as well. Defaults to true."),
        ("force" = Option<bool>, Query, description = "Delete immediately, ignoring the warehouse's soft-deletion profile and any protection."),
    ),
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

/// Protect a branch, or lift protection
///
/// A protected branch refuses direct commits and deletion for everyone; changes
/// reach it by fast-forward only. Structural, not authorization: it does not
/// depend on who is asking.
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
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/branches/{branch}/commits",
            post(commit_dataset::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/import",
            post(import_dataset::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/datasets/{dataset}/credentials",
            get(load_dataset_credentials::<I, S>),
        )
        .route("/{prefix}/datasets/rename", post(rename_dataset::<I, S>))
}

#[cfg(feature = "open-api")]
mod openapi;
#[cfg(feature = "open-api")]
pub use openapi::api_doc;
