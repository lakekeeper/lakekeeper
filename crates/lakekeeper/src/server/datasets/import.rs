use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, VecDeque},
    str::FromStr as _,
    sync::Arc,
};

use chrono::SubsecRound as _;
use futures::{StreamExt, stream};
use iceberg::TableIdent;
use lakekeeper_io::{ErrorKind, FileInfo, LakekeeperStorage, Location, StorageBackend};
use uuid::Uuid;

use super::{
    load_ownership,
    manifest::{DEFAULT_EXCLUDES, KeyPatterns, MERGE_PAGE, ManifestCursor, ScanScope},
    physical_location, schedule_after_publish, target_ref, validate_key,
};
use crate::{
    WarehouseId,
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            DatasetParameters, ImportDatasetRequest, ImportDatasetResponse, ImportMode,
        },
        iceberg::v1::Result,
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id, validate_blob_size},
    service::{
        CatalogDatasetOps, CatalogStore, CommitDatasetError, ConstraintViolationPolicy,
        DEFAULT_DATASET_BRANCH, DatasetId, DatasetInfo, DatasetOwnership, DatasetSnapshotId,
        DegradedFile, DegradedFileProblem, ListedObject, ManifestEntry, MaterializationFindings,
        MaterializationReport, NamedEntity, ResolvedWarehouse, SecretStore, SkippedFile,
        StagedChanges, State, TabularListFlags, Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{
            APIEventContext, CommitDatasetEvent, DatasetPublished, EventDispatcher,
            context::{ResolvedDataset, UserProvidedDataset},
        },
        storage::StorageProfile,
        tasks::{
            ScheduleTaskMetadata, TaskCheckState, TaskEntity, WarehouseTaskEntityId,
            dataset_import_queue::{
                DatasetImportExecutionDetails, DatasetImportPayload, DatasetImportTask, ImportPhase,
            },
        },
    },
};

/// Content type inferred from a file extension.
fn content_type_for(key: &str) -> Option<&'static str> {
    let extension = key.rsplit_once('.')?.1.to_ascii_lowercase();
    Some(match extension.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "tif" | "tiff" => "image/tiff",
        "webp" => "image/webp",
        "parquet" => "application/vnd.apache.parquet",
        "json" => "application/json",
        "jsonl" | "ndjson" => "application/x-ndjson",
        "csv" => "text/csv",
        "txt" => "text/plain",
        "pdf" => "application/pdf",
        "wav" => "audio/wav",
        "mp4" => "video/mp4",
        _ => return None,
    })
}

#[allow(clippy::too_many_lines)]
pub(super) async fn import_dataset<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetParameters,
    request: ImportDatasetRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ImportDatasetResponse> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = state.v1_state.authz.clone();

    let params = ImportParams::from(&request);
    let action = CatalogDatasetAction::Commit {
        target_refs: target_ref(&params.branch),
    };
    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        action.clone(),
    );

    let authz_result = authorizer
        .load_and_authorize_dataset_operation::<C>(
            event_ctx.request_metadata(),
            &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
            TabularListFlags::active(),
            action,
            state.v1_state.catalog.clone(),
        )
        .await;
    let (event_ctx, (warehouse, _namespace, info)) = event_ctx.emit_authz(authz_result)?;
    // A managed dataset's files arrive by commit. Its prefix holds the write folders of
    // writers that have not committed yet, which an import would register.
    if load_ownership::<C>(
        warehouse_id,
        info.tabular_id,
        state.v1_state.catalog.clone(),
    )
    .await?
        == DatasetOwnership::Managed
    {
        return Err(ErrorModel::conflict(
            "A managed dataset gets its files by commit. Import into a dataset created with a \
             location.",
            "CannotImportIntoManagedDataset",
            None,
        )
        .into());
    }
    // Before the queued branch, so a request no scan could honour is refused here
    // and never becomes a task that fails later.
    params.validate(&warehouse.storage_profile)?;
    let dataset_entity = TaskEntity::EntityInWarehouse {
        entity_name: dataset_ident.into_name_parts(),
        warehouse_id,
        entity_id: WarehouseTaskEntityId::Dataset {
            dataset_id: info.tabular_id,
        },
    };

    if request.queued.unwrap_or(false) {
        // Authorized against the caller here; the task runs with the server's own
        // authority, as purge does.
        let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        let task_id = DatasetImportTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id: warehouse.project_id.clone(),
                parent_task_id: None,
                scheduled_for: None,
                entity: dataset_entity,
            },
            DatasetImportPayload::from(&request),
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        // `schedule_task` returns None when the dataset already has an active import.
        let Some(task_id) = task_id else {
            return Err(ErrorModel::conflict(
                "An import is already running for this dataset",
                "DatasetImportAlreadyRunning",
                None,
            )
            .into());
        };
        return Ok(ImportDatasetResponse {
            snapshot_id: None,
            task_id: Some(task_id),
            imported: 0,
            modified: 0,
            removed: 0,
            truncated: false,
            skipped: 0,
            skipped_files: Vec::new(),
            materialization: None,
        });
    }

    let outcome = run_import::<C, S>(
        warehouse.as_ref(),
        &state.v1_state.secrets,
        warehouse_id,
        dataset_entity,
        info.location.as_ref(),
        &params,
        &mut ImportHeartbeat::inline(),
        state.v1_state.catalog.clone(),
    )
    .await?;
    if let Some(published) = outcome.published.clone() {
        announce_import::<C>(
            &state.v1_state.events,
            warehouse,
            info.tabular_id,
            published,
            event_ctx.request_metadata_arc(),
            state.v1_state.catalog,
        )
        .await;
    }

    Ok(ImportDatasetResponse {
        snapshot_id: outcome.snapshot_id,
        task_id: None,
        imported: outcome.added,
        modified: outcome.modified,
        removed: outcome.removed,
        truncated: outcome.truncated,
        skipped: outcome.skipped,
        skipped_files: outcome.skipped_files,
        materialization: outcome.materialization,
    })
}

/// Announce the snapshot an import published. Best-effort: failing loses only the
/// event.
pub(crate) async fn announce_import<C: CatalogStore>(
    events: &EventDispatcher,
    warehouse: Arc<ResolvedWarehouse>,
    dataset_id: DatasetId,
    published: DatasetPublished,
    request_metadata: Arc<RequestMetadata>,
    catalog_state: C::State,
) {
    let dataset: Result<DatasetInfo> = async {
        let mut t = C::Transaction::begin_read(catalog_state).await?;
        let dataset =
            C::load_dataset_by_id(warehouse.warehouse_id, dataset_id, t.transaction()).await?;
        t.commit().await?;
        Ok(dataset)
    }
    .await;
    let dataset = match dataset {
        Ok(dataset) => dataset,
        Err(e) => {
            tracing::warn!(
                "Failed to announce the import of dataset {dataset_id}: {}",
                e.error
            );
            return;
        }
    };
    let event = CommitDatasetEvent {
        dataset: ResolvedDataset {
            warehouse,
            dataset: Arc::new(dataset),
        },
        published,
        request_metadata,
    };
    let events = events.clone();
    tokio::spawn(async move {
        let () = events.dataset_committed(event).await;
    });
}

/// What a queued import reports to its task after every batch, learning in return
/// whether it may go on. An inline import has no task.
pub(crate) struct ImportHeartbeat<'t> {
    task: Option<&'t DatasetImportTask>,
    details: DatasetImportExecutionDetails,
    /// Listed objects the merge has taken so far.
    merged: i64,
    /// The task was cancelled once the import was past its publish.
    task_gone: bool,
}

impl<'t> ImportHeartbeat<'t> {
    pub(crate) fn inline() -> Self {
        Self {
            task: None,
            details: DatasetImportExecutionDetails::default(),
            merged: 0,
            task_gone: false,
        }
    }

    pub(crate) fn for_task(task: &'t DatasetImportTask) -> Self {
        Self {
            task: Some(task),
            ..Self::inline()
        }
    }

    fn listed(&mut self, total: usize) {
        self.details.objects_listed = i64::try_from(total).unwrap_or(i64::MAX);
    }

    fn merged(&mut self, taken: i64, staged: StagedChanges, skipped: &Skipped) {
        let count = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
        self.merged = taken;
        self.details.imported = count(staged.added);
        self.details.modified = count(staged.modified);
        self.details.removed = count(staged.removed);
        self.details.skipped = skipped.count;
    }

    /// The share of the listing merged so far, short of the publish.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    fn merge_progress(&self) -> f32 {
        if self.details.objects_listed == 0 {
            return 0.0;
        }
        let share = self.merged as f64 / self.details.objects_listed as f64;
        (share.min(1.0) * 0.99) as f32
    }

    /// Record progress on the task, and fail the import if the task was stopped or
    /// cancelled meanwhile: failing discards what it staged, so nothing publishes.
    async fn beat<C: CatalogStore>(
        &mut self,
        phase: ImportPhase,
        progress: f32,
        catalog_state: C::State,
    ) -> Result<()> {
        let Some(task) = self.task else {
            return Ok(());
        };
        self.details.phase = phase;
        match task
            .heartbeat::<C>(catalog_state, progress, Some(self.details.clone()))
            .await?
        {
            TaskCheckState::Continue => Ok(()),
            TaskCheckState::Stop => Err(ErrorModel::conflict(
                "The import was stopped before it published",
                IMPORT_STOPPED,
                None,
            )
            .into()),
            // The row is gone: cancelled, or already finished by another attempt.
            TaskCheckState::NotActive => Err(ErrorModel::conflict(
                "The import's task was cancelled before it published",
                IMPORT_CANCELLED,
                None,
            )
            .into()),
        }
    }

    /// Record progress once the import is past its publish, when a stop or a cancel
    /// can undo nothing, so neither ends it.
    async fn report<C: CatalogStore>(
        &mut self,
        phase: ImportPhase,
        progress: f32,
        catalog_state: C::State,
    ) {
        let Some(task) = self.task else {
            return;
        };
        if self.task_gone {
            return;
        }
        self.details.phase = phase;
        match task
            .heartbeat::<C>(catalog_state, progress, Some(self.details.clone()))
            .await
        {
            Ok(TaskCheckState::NotActive) => self.task_gone = true,
            Ok(TaskCheckState::Continue | TaskCheckState::Stop) => {}
            Err(e) => tracing::warn!("Failed to report the import's progress: {e}"),
        }
    }
}

/// The error type of an import that met a stop request.
const IMPORT_STOPPED: &str = "DatasetImportStopped";
/// The error type of an import whose task was cancelled under it.
pub(crate) const IMPORT_CANCELLED: &str = "DatasetImportCancelled";

#[derive(Debug, Clone)]
pub(crate) struct ImportParams {
    pub branch: String,
    pub sub_prefix: Option<String>,
    pub suffix: Option<String>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub default_excludes: bool,
    pub record_versions: bool,
    pub check_materialization: bool,
    pub max_files: Option<i64>,
    pub mode: ImportMode,
    pub summary: Option<serde_json::Value>,
    pub on_constraint_violation: ConstraintViolationPolicy,
}

impl From<&ImportDatasetRequest> for ImportParams {
    fn from(request: &ImportDatasetRequest) -> Self {
        Self {
            branch: request
                .branch
                .clone()
                .unwrap_or_else(|| DEFAULT_DATASET_BRANCH.to_string()),
            sub_prefix: request.sub_prefix.clone(),
            suffix: request.suffix.clone(),
            include: request.include.clone().unwrap_or_default(),
            exclude: request.exclude.clone().unwrap_or_default(),
            default_excludes: request.default_excludes.unwrap_or(true),
            record_versions: request.record_versions.unwrap_or(false),
            check_materialization: request.check_materialization.unwrap_or(false),
            max_files: request.max_files,
            mode: request.mode.unwrap_or_default(),
            summary: request.summary.clone(),
            on_constraint_violation: request.constraint_violation_policy(),
        }
    }
}

impl From<&DatasetImportPayload> for ImportParams {
    fn from(payload: &DatasetImportPayload) -> Self {
        Self {
            branch: payload
                .branch
                .clone()
                .unwrap_or_else(|| DEFAULT_DATASET_BRANCH.to_string()),
            sub_prefix: payload.sub_prefix.clone(),
            suffix: payload.suffix.clone(),
            include: payload.include.clone().unwrap_or_default(),
            exclude: payload.exclude.clone().unwrap_or_default(),
            default_excludes: payload.default_excludes.unwrap_or(true),
            record_versions: payload.record_versions.unwrap_or(false),
            check_materialization: payload.check_materialization.unwrap_or(false),
            max_files: payload.max_files,
            mode: payload.mode,
            summary: payload.summary.clone(),
            on_constraint_violation: payload.on_constraint_violation,
        }
    }
}

impl ImportParams {
    /// Refuses parameters no scan of `storage` could honour. The worker checks
    /// again: a queued payload does not pass through the handler.
    fn validate(&self, storage: &StorageProfile) -> Result<()> {
        if self.record_versions && !storage.pins_object_versions() {
            return Err(ErrorModel::bad_request(
                format!(
                    "record-versions needs a storage whose listing reports object versions; {} listings carry none",
                    storage.storage_type()
                ),
                "InvalidRecordVersions",
                None,
            )
            .into());
        }
        if self.max_files.is_some_and(|max| max < 1) {
            return Err(ErrorModel::bad_request(
                "max-files must be positive",
                "InvalidMaxFiles",
                None,
            )
            .into());
        }
        // A trailing `/` names the same directory: the scan root drops it.
        if let Some(sub_prefix) = &self.sub_prefix {
            validate_key(sub_prefix.trim_end_matches('/'), "sub-prefix")?;
        }
        self.patterns()?;
        // A narrowed scan never looked for the files outside it, so it cannot
        // vouch for them.
        if self.check_materialization
            && (self.sub_prefix.is_some() || self.suffix.is_some() || !self.include.is_empty())
        {
            return Err(ErrorModel::bad_request(
                "check-materialization needs the whole prefix scanned: drop sub-prefix, suffix and include",
                "InvalidMaterializationCheck",
                None,
            )
            .into());
        }
        validate_blob_size("Dataset import summary", self.summary.as_ref())
    }

    /// The globs the scan applies: the caller's, and the default excludes unless
    /// turned off.
    fn patterns(&self) -> Result<KeyPatterns> {
        let defaults = self
            .default_excludes
            .then_some(DEFAULT_EXCLUDES)
            .into_iter()
            .flatten();
        KeyPatterns::new(
            self.include.iter().map(String::as_str),
            self.exclude.iter().map(String::as_str).chain(defaults),
        )
    }
}

pub(crate) struct ImportOutcome {
    /// The snapshot the branch is on afterwards.
    pub snapshot_id: Option<DatasetSnapshotId>,
    pub added: i64,
    pub modified: i64,
    pub removed: i64,
    /// Objects the listing found within the scan.
    pub listed: i64,
    pub truncated: bool,
    pub skipped: i64,
    /// The first [`SKIPPED_FILES_REPORTED`] of the skipped objects.
    pub skipped_files: Vec<SkippedFile>,
    /// What the import published, for the event announcing it.
    pub published: Option<DatasetPublished>,
    pub materialization: Option<MaterializationReport>,
}

impl ImportOutcome {
    /// An import that left the branch on `head` without publishing.
    fn unchanged(head: Option<DatasetSnapshotId>, listed: &Listed, skipped: Skipped) -> Self {
        Self {
            snapshot_id: head,
            added: 0,
            modified: 0,
            removed: 0,
            listed: listed.count,
            truncated: listed.truncated,
            skipped: skipped.count,
            skipped_files: skipped.first,
            published: None,
            materialization: None,
        }
    }
}

/// How many files of a checked snapshot are kept by name; the rest are counted.
const DEGRADED_FILES_KEPT: usize = 10_000;

/// The key a listing of `location` reports the object at `physical_path` under, or
/// `None` when it lies outside the location.
fn listed_key_of(location: &str, physical_path: &str) -> Option<String> {
    if physical_path.contains("://") {
        return key_under(location, physical_path);
    }
    // Where `physical_location` reads it: under the location, without empty
    // segments.
    Some(
        physical_path
            .split('/')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("/"),
    )
}

/// The key a listing of `base` reports the object at `uri` under, or `None` when
/// `uri` is not below `base`: one that merely starts with the same characters,
/// `s3://b/ds2/a` against `s3://b/ds`, lies outside it.
fn key_under(base: &str, uri: &str) -> Option<String> {
    let rest = uri.strip_prefix(base.trim_end_matches('/'))?;
    rest.starts_with('/')
        .then(|| rest.trim_start_matches('/').to_string())
}

/// How many pinned versions a check asks storage about at once.
const VERSION_CHECKS_IN_FLIGHT: usize = 16;

/// A materialization check: the snapshots a ref points at, judged against what an
/// import listed.
struct MaterializationCheck<'a> {
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    /// The dataset's location, which the listing's keys are relative to.
    location: &'a str,
    /// The staging snapshot whose spool holds the listing.
    listing: DatasetSnapshotId,
    /// When that snapshot was opened, just before the listing began, by the
    /// database's clock: the clock a snapshot's publish is dated by.
    listed_after: chrono::DateTime<chrono::Utc>,
    scope: &'a ScanScope<'a>,
    file_io: &'a StorageBackend,
    /// Whether an etag a file records compares with the one a listing reports.
    compare_etags: bool,
    /// Whether readers read the version a file records.
    pins_versions: bool,
}

impl MaterializationCheck<'_> {
    /// Run the check. One that cannot finish costs its report, never the import,
    /// which has published by now.
    async fn run<C: CatalogStore>(
        &self,
        published: Option<DatasetSnapshotId>,
        heartbeat: &mut ImportHeartbeat<'_>,
        catalog_state: C::State,
    ) -> Option<MaterializationReport> {
        let checked = async {
            let snapshots = self
                .snapshots_to_check::<C>(published, catalog_state.clone())
                .await?;
            self.check::<C>(snapshots, heartbeat, catalog_state).await
        }
        .await;
        match checked {
            Ok(report) => Some(report),
            Err(e) => {
                tracing::warn!(
                    dataset_id = %self.dataset_id,
                    "The materialization check did not finish: {}",
                    e.error
                );
                None
            }
        }
    }

    /// The ref heads the listing can vouch for: those published before it began.
    /// One published since, even if staged before, may hold a file the listing
    /// passed before it was written. `published` holds exactly the listing,
    /// unless the import was rebased onto a commit, in which case the caller
    /// leaves it out.
    async fn snapshots_to_check<C: CatalogStore>(
        &self,
        published: Option<DatasetSnapshotId>,
        catalog_state: C::State,
    ) -> Result<BTreeSet<DatasetSnapshotId>> {
        // The primary: the snapshot the import just published is a ref's head there.
        let mut t = C::Transaction::begin_write(catalog_state).await?;
        let refs =
            C::list_dataset_refs(self.warehouse_id, self.dataset_id, t.transaction()).await?;
        let created: HashMap<DatasetSnapshotId, chrono::DateTime<chrono::Utc>> =
            C::list_dataset_snapshot_graph(self.warehouse_id, self.dataset_id, t.transaction())
                .await?
                .into_iter()
                .map(|node| (node.snapshot_id, node.created_at))
                .collect();
        t.commit().await?;
        Ok(refs
            .into_iter()
            .filter_map(|r| r.snapshot_id)
            .filter(|snapshot| {
                Some(*snapshot) == published
                    || created
                        .get(snapshot)
                        .is_some_and(|at| *at < self.listed_after)
            })
            .collect())
    }

    /// Judge `snapshots`, recording on each the files whose object the listing did
    /// not show and those whose object was written again since. A file pinned to a
    /// version readers read is judged by that version, which the listing does not
    /// show, so it is looked up.
    async fn check<C: CatalogStore>(
        &self,
        snapshots: BTreeSet<DatasetSnapshotId>,
        heartbeat: &mut ImportHeartbeat<'_>,
        catalog_state: C::State,
    ) -> Result<MaterializationReport> {
        let base = Location::from_str(self.location).map_err(|e| {
            ErrorModel::internal(
                format!("The dataset's location does not parse: {e}"),
                "InvalidDatasetLocation",
                None,
            )
        })?;
        // The scan's patterns apply to the keys it lists, not to what files are
        // called, so every file is read and judged by its listed key.
        let every_file = ScanScope::new(None, None);
        let mut report = MaterializationReport::default();
        let mut since_beat = 0usize;
        for snapshot in snapshots {
            let mut files = ManifestCursor::new(self.warehouse_id, self.dataset_id, Some(snapshot))
                .on_primary();
            let mut findings = MaterializationFindings::default();
            loop {
                files.fill::<C>(&every_file, catalog_state.clone()).await?;
                if files.page.is_empty() {
                    break;
                }
                let judged: Vec<(String, ManifestEntry)> = files
                    .page
                    .drain(..)
                    .filter_map(|file| {
                        listed_key_of(self.location, &file.physical_path).map(|k| (k, file))
                    })
                    .filter(|(key, _)| self.scope.covers(key))
                    .collect();
                since_beat += judged.len();
                let degraded = self
                    .judge_page::<C>(&base, judged, catalog_state.clone())
                    .await?;
                for (file, problem) in degraded {
                    match problem {
                        DegradedFileProblem::Missing => findings.missing_files += 1,
                        DegradedFileProblem::Changed => findings.changed_files += 1,
                    }
                    if findings.files.len() < DEGRADED_FILES_KEPT {
                        findings.files.push(DegradedFile {
                            logical_key: file.logical_key,
                            physical_path: file.physical_path,
                            problem,
                        });
                    }
                }
                if since_beat >= STAGE_BATCH {
                    since_beat = 0;
                    heartbeat
                        .report::<C>(ImportPhase::Checking, 0.99, catalog_state.clone())
                        .await;
                }
            }
            let mut t = C::Transaction::begin_write(catalog_state.clone()).await?;
            C::record_snapshot_materialization(
                self.warehouse_id,
                self.dataset_id,
                snapshot,
                &findings,
                t.transaction(),
            )
            .await?;
            t.commit().await?;
            report.checked_snapshots += 1;
            if findings.missing_files > 0 || findings.changed_files > 0 {
                report.degraded_snapshots += 1;
            }
        }
        Ok(report)
    }

    /// Judge one page of a snapshot's files, each with the key its bytes list
    /// under, against the listing: the missing and the changed, in key order.
    async fn judge_page<C: CatalogStore>(
        &self,
        base: &Location,
        judged: Vec<(String, ManifestEntry)>,
        catalog_state: C::State,
    ) -> Result<Vec<(ManifestEntry, DegradedFileProblem)>> {
        let keys: Vec<String> = judged.iter().map(|(key, _)| key.clone()).collect();
        // The primary: a replica holds no unlogged table.
        let mut t = C::Transaction::begin_write(catalog_state).await?;
        let listed: HashMap<String, ListedObject> =
            C::get_listed_import_objects(self.warehouse_id, self.listing, &keys, t.transaction())
                .await?
                .into_iter()
                .map(|object| (object.logical_key.clone(), object))
                .collect();
        t.commit().await?;

        let mut degraded: Vec<(ManifestEntry, DegradedFileProblem)> = Vec::new();
        let mut unlisted_versions: Vec<(ManifestEntry, Location, String)> = Vec::new();
        for (key, file) in judged {
            let object = listed.get(&key);
            match file.version_id.clone().filter(|_| self.pins_versions) {
                Some(version) => {
                    let listed_as_pinned = object.is_some_and(|o| {
                        o.version_id.as_deref() == Some(&version)
                            || still_the_pinned_object(&file, o, self.compare_etags)
                    });
                    if !listed_as_pinned {
                        let path = physical_location(base, &file.physical_path).map_err(|e| {
                            ErrorModel::internal(
                                format!("A manifest records an invalid physical path: {e}"),
                                "InvalidPhysicalPath",
                                None,
                            )
                        })?;
                        unlisted_versions.push((file, path, version));
                    }
                }
                None => match object {
                    None => degraded.push((file, DegradedFileProblem::Missing)),
                    Some(object) if rewritten(&file, object, self.compare_etags) => {
                        degraded.push((file, DegradedFileProblem::Changed));
                    }
                    Some(_) => {}
                },
            }
        }
        let looked_up: Vec<_> = stream::iter(unlisted_versions)
            .map(|(file, path, version)| async move {
                let stored = self.file_io.version_exists(path.as_str(), &version).await;
                (file, path, stored)
            })
            .buffer_unordered(VERSION_CHECKS_IN_FLIGHT)
            .collect()
            .await;
        for (file, path, stored) in looked_up {
            let stored = stored.map_err(|e| {
                ErrorModel::internal(
                    format!("Cannot tell whether {path} still holds its version: {e}"),
                    "DatasetMaterializationCheckFailed",
                    None,
                )
            })?;
            if !stored {
                degraded.push((file, DegradedFileProblem::Missing));
            }
        }
        degraded.sort_by(|a, b| a.0.logical_key.cmp(&b.0.logical_key));
        Ok(degraded)
    }
}

/// How many skipped objects an import names; the rest are counted.
const SKIPPED_FILES_REPORTED: usize = 100;

/// The objects an import left out, as the report carries them.
#[derive(Debug, Default)]
struct Skipped {
    count: i64,
    first: Vec<SkippedFile>,
}

impl Skipped {
    fn record(&mut self, files: Vec<SkippedFile>) {
        self.count += i64::try_from(files.len()).unwrap_or(i64::MAX);
        let room = SKIPPED_FILES_REPORTED.saturating_sub(self.first.len());
        self.first.extend(files.into_iter().take(room));
    }
}

/// Scan the prefix and commit the result. The caller has authorized it: an API
/// request, or the request that enqueued the task.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) async fn run_import<C: CatalogStore, S: SecretStore>(
    warehouse: &ResolvedWarehouse,
    secrets: &S,
    warehouse_id: WarehouseId,
    dataset: TaskEntity,
    location: &str,
    params: &ImportParams,
    heartbeat: &mut ImportHeartbeat<'_>,
    catalog_state: C::State,
) -> Result<ImportOutcome> {
    let dataset_id = match &dataset {
        TaskEntity::EntityInWarehouse {
            entity_id: WarehouseTaskEntityId::Dataset { dataset_id },
            ..
        } => *dataset_id,
        _ => {
            return Err(ErrorModel::internal(
                "An import runs on a dataset",
                "UnexpectedImportEntity",
                None,
            )
            .into());
        }
    };
    let storage_secret = maybe_get_secret(warehouse.storage_secret_id, secrets).await?;
    let file_io = warehouse
        .storage_profile
        .file_io(storage_secret.as_deref())
        .await?;
    let compare_etags = warehouse.storage_profile.has_one_etag_per_object();

    // Before the conversion below: `usize::try_from` would fail on a negative and
    // fall back to unbounded, turning a limit into its opposite.
    params.validate(&warehouse.storage_profile)?;
    let scan_root = resolve_scan_root(location, params.sub_prefix.as_deref())?;
    let scope = ScanScope::new(params.sub_prefix.as_deref(), params.suffix.as_deref())
        .with_patterns(params.patterns()?);

    let mode = params.mode;
    let max_files = params
        .max_files
        .map_or(usize::MAX, |max| usize::try_from(max).unwrap_or(usize::MAX));

    // The scan is compared against `head`'s manifest and publishes onto it, so a
    // commit landing after this read fails the pointer move.
    let (head, has_renamed_files) = {
        let mut t = C::Transaction::begin_read(catalog_state.clone()).await?;
        let head = C::get_dataset_ref(warehouse_id, dataset_id, &params.branch, t.transaction())
            .await?
            .snapshot_id;
        let has_renamed_files =
            C::dataset_has_renamed_files(warehouse_id, dataset_id, t.transaction()).await?;
        t.commit().await?;
        (head, has_renamed_files)
    };

    // Rows are flushed as they are found, so peak memory is one batch and no
    // transaction spans the listing.
    let snapshot_id = DatasetSnapshotId::from(Uuid::now_v7());
    let opened_at = {
        let mut t = C::Transaction::begin_write(catalog_state.clone()).await?;
        // An import that died between begin and finish left a snapshot no ref
        // points at. Swept here because imports are the only thing that creates
        // them; the age floor keeps a concurrent import's snapshot safe.
        C::expire_staging_snapshots(
            warehouse_id,
            Some(dataset_id),
            chrono::Utc::now() - ABANDONED_STAGING_AFTER,
            t.transaction(),
        )
        .await?;
        let opened_at = C::begin_dataset_commit(
            warehouse_id,
            dataset_id,
            &params.branch,
            snapshot_id,
            head,
            location,
            params.summary.clone(),
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        opened_at
    };

    // Everything below stages into `snapshot_id`. A failure publishes nothing, so
    // what it staged is discarded.
    let staged = StagedSnapshot::<C> {
        warehouse_id,
        dataset_id,
        snapshot_id,
        catalog_state: Some(catalog_state.clone()),
    };
    let imported: Result<ImportOutcome> = async {
        let materialization_check = MaterializationCheck {
            warehouse_id,
            dataset_id,
            location,
            listing: snapshot_id,
            listed_after: opened_at,
            scope: &scope,
            file_io: &file_io,
            compare_etags,
            pins_versions: warehouse.storage_profile.pins_object_versions(),
        };
        let listed = spool_listing::<C>(
            &file_io,
            params.record_versions,
            scan_root.as_ref(),
            location,
            &scope,
            max_files,
            warehouse_id,
            snapshot_id,
            heartbeat,
            catalog_state.clone(),
        )
        .await?;

        // Absence is only evidence of deletion if the whole prefix was seen.
        if mode == ImportMode::Sync && listed.truncated {
            return Err(ErrorModel::bad_request(
                "sync mode cannot run against a truncated scan: raise max-files or narrow sub-prefix",
                "DatasetImportTruncatedSync",
                None,
            )
            .into());
        }

        let (staged, skipped) = merge_into_snapshot::<C>(
            mode,
            params.on_constraint_violation,
            &scope,
            location,
            compare_etags,
            head,
            has_renamed_files,
            warehouse_id,
            dataset_id,
            snapshot_id,
            heartbeat,
            catalog_state.clone(),
        )
        .await?;

        // Checked against the spool, which goes with the staging snapshot. A scan
        // that stopped at `max-files` saw too little to vouch for anything.
        let check = params.check_materialization && !listed.truncated;

        // An empty snapshot would still count towards the next checkpoint, which
        // restates every file, so a scan that changed nothing publishes nothing.
        if staged.is_empty() {
            let materialization = if check {
                materialization_check
                    .run::<C>(None, heartbeat, catalog_state.clone())
                    .await
            } else {
                None
            };
            let mut t = C::Transaction::begin_write(catalog_state).await?;
            C::abort_dataset_commit(warehouse_id, dataset_id, snapshot_id, t.transaction())
                .await?;
            t.commit().await?;
            return Ok(ImportOutcome {
                materialization,
                ..ImportOutcome::unchanged(head, &listed, skipped)
            });
        }

        // The last chance to stop: past the pointer move the snapshot is published.
        heartbeat
            .beat::<C>(ImportPhase::Publishing, 0.99, catalog_state.clone())
            .await?;
        // The only step that races. Everything above is invisible, so losing costs
        // the pointer move, not the scan.
        let finished = finish_with_retry::<C>(
            warehouse,
            dataset_id,
            dataset,
            &params.branch,
            snapshot_id,
            head,
            staged,
            catalog_state.clone(),
        )
        .await?;

        // Nothing past this point fails the import: it has published.
        let materialization = if let Finished::Published { parent, .. } = &finished {
            let materialization = if check {
                // Rebased onto a commit, the snapshot holds files the listing never saw.
                let unrebased = (*parent == head).then_some(snapshot_id);
                materialization_check
                    .run::<C>(unrebased, heartbeat, catalog_state.clone())
                    .await
            } else {
                None
            };
            if let Err(e) = clear_import_listing::<C>(warehouse_id, snapshot_id, catalog_state).await
            {
                // The staging sweep clears what is left behind.
                tracing::warn!(%dataset_id, "Failed to clear an import's listing: {}", e.error);
            }
            materialization
        } else {
            None
        };

        Ok(match finished {
            Finished::Published { changes, parent } => ImportOutcome {
                materialization,
                snapshot_id: Some(snapshot_id),
                added: i64::try_from(changes.added).unwrap_or(i64::MAX),
                modified: i64::try_from(changes.modified).unwrap_or(i64::MAX),
                removed: i64::try_from(changes.removed).unwrap_or(i64::MAX),
                listed: listed.count,
                truncated: listed.truncated,
                skipped: skipped.count,
                skipped_files: skipped.first,
                published: Some(DatasetPublished {
                    branch: params.branch.clone(),
                    snapshot_id,
                    parent_snapshot_id: parent,
                    summary: params.summary.clone(),
                    changes,
                }),
            },
            Finished::Superseded(head) => {
                ImportOutcome::unchanged(head, &listed, skipped)
            }
        })
    }
    .await;
    if imported.is_ok() {
        staged.disarm();
    } else {
        staged.discard().await;
    }
    imported
}

/// An import's staging snapshot, discarded unless the import ran to its end. A
/// handler is dropped at `max-request-time` and when the client hangs up, and what
/// it staged would otherwise hold every purge on the dataset until the sweep.
struct StagedSnapshot<C: CatalogStore> {
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    /// Taken once the snapshot is published or discarded.
    catalog_state: Option<C::State>,
}

impl<C: CatalogStore> StagedSnapshot<C> {
    /// The import published the snapshot, or discarded it itself.
    fn disarm(mut self) {
        self.catalog_state = None;
    }

    async fn discard(mut self) {
        if let Some(catalog_state) = self.catalog_state.take() {
            discard_staging::<C>(
                self.warehouse_id,
                self.dataset_id,
                self.snapshot_id,
                catalog_state,
            )
            .await;
        }
    }
}

impl<C: CatalogStore> Drop for StagedSnapshot<C> {
    fn drop(&mut self) {
        let Some(catalog_state) = self.catalog_state.take() else {
            return;
        };
        // Dropped mid-import. Outside a runtime the staging sweep is the backstop.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(discard_staging::<C>(
            self.warehouse_id,
            self.dataset_id,
            self.snapshot_id,
            catalog_state,
        ));
    }
}

async fn clear_import_listing<C: CatalogStore>(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    catalog_state: C::State,
) -> Result<()> {
    let mut t = C::Transaction::begin_write(catalog_state).await?;
    C::clear_import_listing(warehouse_id, snapshot_id, t.transaction()).await?;
    t.commit().await?;
    Ok(())
}

/// Best-effort: the staging sweep is the backstop if this fails too.
async fn discard_staging<C: CatalogStore>(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    catalog_state: C::State,
) {
    let discarded: Result<()> = async {
        let mut t = C::Transaction::begin_write(catalog_state).await?;
        C::abort_dataset_commit(warehouse_id, dataset_id, snapshot_id, t.transaction()).await?;
        t.commit().await?;
        Ok(())
    }
    .await;
    if let Err(e) = discarded {
        tracing::warn!(
            "Failed to discard staging snapshot {snapshot_id}: {}",
            e.error
        );
    }
}

/// Where the scan starts: the dataset's location, optionally narrowed by a
/// `sub_prefix` that [`ImportParams::validate`] has kept inside it.
fn resolve_scan_root(location: &str, sub_prefix: Option<&str>) -> Result<Location> {
    let mut root = Location::from_str(location)
        .map_err(|e| ErrorModel::internal(e.to_string(), "InvalidDatasetLocation", None))?;
    if let Some(sub_prefix) = sub_prefix {
        root.extend(sub_prefix.split('/').filter(|s| !s.is_empty()));
    }
    Ok(root)
}

/// How long a staging snapshot must sit untouched to count as abandoned.
pub(crate) const ABANDONED_STAGING_AFTER: chrono::TimeDelta = chrono::TimeDelta::hours(24);

/// How many times to re-aim the pointer move at a moved branch.
const FINISH_ATTEMPTS: usize = 3;

/// Where an import's pointer move left the branch.
enum Finished {
    /// On the staged snapshot, which carries these changes, published onto this
    /// parent.
    Published {
        changes: StagedChanges,
        parent: Option<DatasetSnapshotId>,
    },
    /// On this snapshot: every staged change was to a key a commit that landed
    /// first had changed, so the staged snapshot was discarded.
    Superseded(Option<DatasetSnapshotId>),
}

/// Move the branch to the staged snapshot, rebasing the staged rows onto a branch
/// that moved underneath. A key a commit that landed first changed keeps that
/// commit's entry: it is newer than the listing.
#[allow(clippy::too_many_arguments)]
async fn finish_with_retry<C: CatalogStore>(
    warehouse: &ResolvedWarehouse,
    dataset_id: DatasetId,
    dataset: TaskEntity,
    branch: &str,
    snapshot_id: DatasetSnapshotId,
    mut expected: Option<DatasetSnapshotId>,
    mut staged: StagedChanges,
    catalog_state: C::State,
) -> Result<Finished> {
    let warehouse_id = warehouse.warehouse_id;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let mut t = C::Transaction::begin_write(catalog_state.clone()).await?;
        match C::finish_dataset_commit(
            warehouse_id,
            dataset_id,
            branch,
            snapshot_id,
            expected,
            t.transaction(),
        )
        .await
        {
            Ok(checkpoint_due) => {
                // As a commit does: a dataset kept current only by imports must
                // still have its chain folded.
                schedule_after_publish::<C>(
                    warehouse.project_id.clone(),
                    dataset,
                    branch,
                    checkpoint_due,
                    &mut t,
                )
                .await?;
                t.commit().await?;
                return Ok(Finished::Published {
                    changes: staged,
                    parent: expected,
                });
            }
            Err(CommitDatasetError::DatasetCommitConflict(conflict)) => {
                // Roll back before re-reading: the failed attempt's transaction
                // holds nothing worth keeping.
                drop(t);
                if attempt == FINISH_ATTEMPTS {
                    return Err(CommitDatasetError::DatasetCommitConflict(conflict).into());
                }
                expected = conflict.current_snapshot_id;

                let mut t = C::Transaction::begin_write(catalog_state.clone()).await?;
                let dropped = C::rebase_staging_snapshot(
                    warehouse_id,
                    dataset_id,
                    snapshot_id,
                    expected,
                    t.transaction(),
                )
                .await?;
                staged = staged.without(dropped);
                if staged.is_empty() {
                    C::abort_dataset_commit(warehouse_id, dataset_id, snapshot_id, t.transaction())
                        .await?;
                    t.commit().await?;
                    return Ok(Finished::Superseded(expected));
                }
                t.commit().await?;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Rows per staging transaction, and per spool write.
const STAGE_BATCH: usize = 5_000;

/// How a listing ended.
struct Listed {
    /// Objects found within the scan.
    count: i64,
    /// Stopped at `max-files` with objects left unlisted.
    truncated: bool,
}

/// List the prefix into the import's spool, a batch at a time as pages arrive,
/// with each object's current version when `record_versions`.
///
/// Storage lists in its own order — ADLS depth-first, not by key — so nothing is
/// compared here; the spool hands the listing back in key order for the merge.
#[allow(clippy::too_many_arguments)]
async fn spool_listing<C: CatalogStore>(
    file_io: &StorageBackend,
    record_versions: bool,
    root: &str,
    base: &str,
    scope: &ScanScope<'_>,
    max_files: usize,
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    heartbeat: &mut ImportHeartbeat<'_>,
    catalog_state: C::State,
) -> Result<Listed> {
    let pages = if record_versions {
        file_io.list_current_versions(root, None).await
    } else {
        file_io.list(root, None).await
    };
    let mut pages = pages.map_err(|e| {
        ErrorModel::bad_request(
            format!("Cannot list {root}: {e}"),
            "DatasetImportListFailed",
            None,
        )
    })?;

    let mut batch: Vec<ListedObject> = Vec::new();
    let mut total = 0usize;
    let mut seen = 0usize;
    let mut truncated = false;
    'pages: while let Some(page) = pages.next().await {
        let page = page.map_err(|e| {
            let message = format!("Failed listing {root}: {e}");
            // A prefix storage refuses or does not have is the request's to fix.
            match e.kind() {
                ErrorKind::PermissionDenied | ErrorKind::NotFound | ErrorKind::ConfigInvalid => {
                    ErrorModel::bad_request(message, "DatasetImportListFailed", None)
                }
                _ => ErrorModel::internal(message, "DatasetImportListFailed", None),
            }
        })?;
        for file in page {
            seen += 1;
            if let Some(object) = listed_object_for(&file, base, scope) {
                if total >= max_files {
                    truncated = true;
                    break 'pages;
                }
                total += 1;
                batch.push(object);
                if batch.len() >= STAGE_BATCH {
                    spool::<C>(warehouse_id, snapshot_id, &mut batch, catalog_state.clone())
                        .await?;
                }
            }
            // Counted over everything listed: a scope that admits little still
            // hears a stop on time.
            if seen.is_multiple_of(STAGE_BATCH) {
                heartbeat.listed(total);
                heartbeat
                    .beat::<C>(ImportPhase::Listing, 0.0, catalog_state.clone())
                    .await?;
            }
        }
    }
    spool::<C>(warehouse_id, snapshot_id, &mut batch, catalog_state).await?;
    heartbeat.listed(total);
    Ok(Listed {
        count: i64::try_from(total).unwrap_or(i64::MAX),
        truncated,
    })
}

async fn spool<C: CatalogStore>(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    batch: &mut Vec<ListedObject>,
    catalog_state: C::State,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut t = C::Transaction::begin_write(catalog_state).await?;
    C::spool_import_listing(warehouse_id, snapshot_id, batch, t.transaction()).await?;
    t.commit().await?;
    batch.clear();
    Ok(())
}

/// Diff the spooled listing against `head`'s manifest and stage the difference
/// into `snapshot_id`. Both sides arrive in byte order a page at a time, so this
/// is a sorted merge whose memory does not grow with the dataset.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn merge_into_snapshot<C: CatalogStore>(
    mode: ImportMode,
    on_violation: ConstraintViolationPolicy,
    scope: &ScanScope<'_>,
    location: &str,
    compare_etags: bool,
    head: Option<DatasetSnapshotId>,
    has_renamed_files: bool,
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    heartbeat: &mut ImportHeartbeat<'_>,
    catalog_state: C::State,
) -> Result<(StagedChanges, Skipped)> {
    let mut listing = ListingCursor::new(warehouse_id, snapshot_id);
    let mut recorded = ManifestCursor::new(warehouse_id, dataset_id, head).including_expired();
    let mut batch = StagingBatch::new(on_violation);
    let mut staged = StagedChanges::default();
    let mut skipped = Skipped::default();
    if let Some(head) = head.filter(|_| has_renamed_files) {
        merge_renamed_files::<C>(
            mode,
            scope,
            location,
            compare_etags,
            head,
            warehouse_id,
            dataset_id,
            snapshot_id,
            &mut batch,
            &mut staged,
            &mut skipped,
            heartbeat,
            catalog_state.clone(),
        )
        .await?;
    }
    let mut steps = 0usize;
    loop {
        listing.fill::<C>(catalog_state.clone()).await?;
        recorded.fill::<C>(scope, catalog_state.clone()).await?;
        let order = match (listing.page.front(), recorded.page.front()) {
            (None, None) => break,
            // Past the listing, add-only has nothing left to do.
            (None, Some(_)) if mode == ImportMode::AddOnly => break,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(listed), Some(existing)) => listed.logical_key.cmp(&existing.logical_key),
        };
        match order {
            // Listed, not recorded under its storage path: new, unless a file
            // named apart from it already points at the object.
            Ordering::Less => {
                if let Some(listed) = listing.page.pop_front()
                    && !listed.referenced
                {
                    batch.added.push(entry_for(listed));
                }
            }
            // Recorded, not listed: gone, and a sync records the removal. A file
            // named apart from its storage path was judged where its bytes are.
            Ordering::Greater => {
                if let Some(gone) = recorded.page.pop_front()
                    && mode == ImportMode::Sync
                    && !is_renamed(location, &gone)
                {
                    batch.removed.push(gone.logical_key);
                }
            }
            Ordering::Equal => {
                if let (Some(listed), Some(existing)) =
                    (listing.page.pop_front(), recorded.page.pop_front())
                {
                    merge_same_key(
                        listed,
                        &existing,
                        location,
                        compare_etags,
                        mode,
                        &mut batch,
                        &mut skipped,
                    );
                }
            }
        }
        batch
            .flush_when_full::<C>(
                warehouse_id,
                dataset_id,
                snapshot_id,
                &mut staged,
                &mut skipped,
                catalog_state.clone(),
            )
            .await?;
        // Counted over every key compared: a sync that changes nothing still hears
        // a stop on time.
        steps += 1;
        if steps.is_multiple_of(STAGE_BATCH) {
            heartbeat.merged(listing.taken, staged, &skipped);
            heartbeat
                .beat::<C>(
                    ImportPhase::Merging,
                    heartbeat.merge_progress(),
                    catalog_state.clone(),
                )
                .await?;
        }
    }
    batch
        .flush::<C>(
            warehouse_id,
            dataset_id,
            snapshot_id,
            &mut staged,
            &mut skipped,
            catalog_state,
        )
        .await?;
    Ok((staged, skipped))
}

/// Why an object stored under a name a file already goes by is left out.
const NAME_TAKEN: &str = "a file of the dataset goes by this name, with its bytes elsewhere";

/// A listed object and a recorded file under one key: the same file, modified if
/// the object changed. Unless the file's bytes are elsewhere: then another object
/// is stored under its name, and the file keeps it.
fn merge_same_key(
    listed: ListedObject,
    existing: &ManifestEntry,
    location: &str,
    compare_etags: bool,
    mode: ImportMode,
    batch: &mut StagingBatch,
    skipped: &mut Skipped,
) {
    if is_renamed(location, existing) {
        if !listed.referenced {
            skipped.record(vec![SkippedFile {
                logical_key: listed.logical_key,
                reason: NAME_TAKEN.to_string(),
            }]);
        }
    } else if is_modified(&listed, existing, mode) {
        batch
            .modified
            .push(rewritten_entry(listed, existing, compare_etags));
    }
}

/// Whether a manifest entry is named apart from where its bytes are: its listed
/// key, the one a listing of `location` reports its object under, is not its name.
fn is_renamed(location: &str, entry: &ManifestEntry) -> bool {
    listed_key_of(location, &entry.physical_path).as_deref() != Some(entry.logical_key.as_str())
}

/// Judge the files of `head` named apart from their storage path by where their
/// bytes are. The objects they point at are marked in the spool, so the merge by
/// name neither registers them again nor leaves them out.
#[allow(clippy::too_many_arguments)]
async fn merge_renamed_files<C: CatalogStore>(
    mode: ImportMode,
    scope: &ScanScope<'_>,
    location: &str,
    compare_etags: bool,
    head: DatasetSnapshotId,
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    batch: &mut StagingBatch,
    staged: &mut StagedChanges,
    skipped: &mut Skipped,
    heartbeat: &mut ImportHeartbeat<'_>,
    catalog_state: C::State,
) -> Result<()> {
    // Whatever a file is called, it is judged by the key its bytes list under.
    let every_file = ScanScope::new(None, None);
    let mut files = ManifestCursor::new(warehouse_id, dataset_id, Some(head)).including_expired();
    let mut since_beat = 0usize;
    loop {
        files.fill::<C>(&every_file, catalog_state.clone()).await?;
        if files.page.is_empty() {
            break;
        }
        since_beat += files.page.len();
        let judged: Vec<(String, ManifestEntry)> = files
            .page
            .drain(..)
            .filter(|file| is_renamed(location, file))
            .filter_map(|file| listed_key_of(location, &file.physical_path).map(|k| (k, file)))
            .filter(|(key, _)| scope.covers(key))
            .collect();
        if !judged.is_empty() {
            judge_renamed_files::<C>(
                mode,
                compare_etags,
                judged,
                warehouse_id,
                snapshot_id,
                batch,
                catalog_state.clone(),
            )
            .await?;
        }
        batch
            .flush_when_full::<C>(
                warehouse_id,
                dataset_id,
                snapshot_id,
                staged,
                skipped,
                catalog_state.clone(),
            )
            .await?;
        if since_beat >= STAGE_BATCH {
            since_beat = 0;
            heartbeat
                .beat::<C>(ImportPhase::Merging, 0.0, catalog_state.clone())
                .await?;
        }
    }
    Ok(())
}

/// Judge one page of renamed files, each with the key its bytes list under,
/// against the spool, and mark the objects they keep or take up.
async fn judge_renamed_files<C: CatalogStore>(
    mode: ImportMode,
    compare_etags: bool,
    judged: Vec<(String, ManifestEntry)>,
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    batch: &mut StagingBatch,
    catalog_state: C::State,
) -> Result<()> {
    let mut keys: Vec<String> = judged.iter().map(|(key, _)| key.clone()).collect();
    if mode == ImportMode::Sync {
        keys.extend(judged.iter().map(|(_, file)| file.logical_key.clone()));
    }
    // The primary: a replica holds no unlogged table.
    let mut t = C::Transaction::begin_write(catalog_state).await?;
    let listed: HashMap<String, ListedObject> =
        C::get_listed_import_objects(warehouse_id, snapshot_id, &keys, t.transaction())
            .await?
            .into_iter()
            .map(|object| (object.logical_key.clone(), object))
            .collect();
    let mut referenced = Vec::new();
    for (key, file) in judged {
        if let Some(object) = listed.get(&key) {
            if is_modified(object, &file, mode) {
                batch.modified.push(ManifestEntry {
                    logical_key: file.logical_key.clone(),
                    physical_path: file.physical_path.clone(),
                    ..rewritten_entry(object.clone(), &file, compare_etags)
                });
            }
            referenced.push(key);
        } else if mode == ImportMode::Sync {
            // Gone from where it was; an object under its own name now holds it.
            if let Some(own) = listed.get(&file.logical_key) {
                batch
                    .modified
                    .push(rewritten_entry(own.clone(), &file, compare_etags));
                referenced.push(file.logical_key);
            } else {
                batch.removed.push(file.logical_key);
            }
        }
    }
    C::mark_import_objects_referenced(warehouse_id, snapshot_id, &referenced, t.transaction())
        .await?;
    t.commit().await?;
    Ok(())
}

/// The spooled listing, drained in key order a page at a time.
struct ListingCursor {
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    page: VecDeque<ListedObject>,
    /// The last key read: the spool is read, not consumed, so a materialization
    /// check can consult it once the merge is done.
    after: Option<String>,
    drained: bool,
    /// Objects read from the spool so far.
    taken: i64,
}

impl ListingCursor {
    fn new(warehouse_id: WarehouseId, snapshot_id: DatasetSnapshotId) -> Self {
        Self {
            warehouse_id,
            snapshot_id,
            page: VecDeque::new(),
            after: None,
            drained: false,
            taken: 0,
        }
    }

    async fn fill<C: CatalogStore>(&mut self, catalog_state: C::State) -> Result<()> {
        if !self.page.is_empty() || self.drained {
            return Ok(());
        }
        // The primary: a replica holds no unlogged table.
        let mut t = C::Transaction::begin_write(catalog_state).await?;
        let objects = C::read_import_listing(
            self.warehouse_id,
            self.snapshot_id,
            self.after.as_deref(),
            MERGE_PAGE,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        let fetched = i64::try_from(objects.len()).unwrap_or(i64::MAX);
        self.drained = fetched < MERGE_PAGE;
        self.taken += fetched;
        self.after = objects.last().map(|o| o.logical_key.clone());
        self.page.extend(objects);
        Ok(())
    }
}

/// Staged rows not yet written.
struct StagingBatch {
    on_violation: ConstraintViolationPolicy,
    added: Vec<ManifestEntry>,
    modified: Vec<ManifestEntry>,
    removed: Vec<String>,
}

impl StagingBatch {
    fn new(on_violation: ConstraintViolationPolicy) -> Self {
        Self {
            on_violation,
            added: Vec::new(),
            modified: Vec::new(),
            removed: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.added.len() + self.modified.len() + self.removed.len()
    }

    /// [`Self::flush`], once the batch holds [`STAGE_BATCH`] rows.
    async fn flush_when_full<C: CatalogStore>(
        &mut self,
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        staged: &mut StagedChanges,
        skipped: &mut Skipped,
        catalog_state: C::State,
    ) -> Result<()> {
        if self.len() < STAGE_BATCH {
            return Ok(());
        }
        self.flush::<C>(
            warehouse_id,
            dataset_id,
            snapshot_id,
            staged,
            skipped,
            catalog_state,
        )
        .await
    }

    async fn flush<C: CatalogStore>(
        &mut self,
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        snapshot_id: DatasetSnapshotId,
        staged: &mut StagedChanges,
        skipped: &mut Skipped,
        catalog_state: C::State,
    ) -> Result<()> {
        if self.len() == 0 {
            return Ok(());
        }
        let mut t = C::Transaction::begin_write(catalog_state).await?;
        let batch = C::stage_dataset_files(
            warehouse_id,
            dataset_id,
            snapshot_id,
            &self.added,
            &self.modified,
            &self.removed,
            self.on_violation,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        staged.added += batch.staged.added;
        staged.modified += batch.staged.modified;
        staged.removed += batch.staged.removed;
        skipped.record(batch.skipped);
        self.added.clear();
        self.modified.clear();
        self.removed.clear();
        Ok(())
    }
}

/// Whether a listed object differs from the file the manifest records for its key:
/// its size, modification time, etag where both have one, or a version the listing
/// reports. Add-only leaves a key it already has alone.
fn is_modified(listed: &ListedObject, recorded: &ManifestEntry, mode: ImportMode) -> bool {
    mode != ImportMode::AddOnly
        && (recorded.size != listed.size
            || recorded.last_modified != listed.last_modified
            || etags_differ(recorded.etag.as_deref(), listed.etag.as_deref())
            || (listed.version_id.is_some() && recorded.version_id != listed.version_id))
}

/// Whether an object a listing reports without versions is the version a file is
/// pinned to: its key was not written since, as its modification time shows, and
/// its size, and its etag where etags compare, are the ones the file records.
fn still_the_pinned_object(
    file: &ManifestEntry,
    listed: &ListedObject,
    compare_etags: bool,
) -> bool {
    listed.version_id.is_none()
        && file.last_modified.is_some()
        && file.last_modified == listed.last_modified
        && !rewritten(file, listed, compare_etags)
}

/// Whether two etags, both known, name different bytes. Storage quotes an etag in
/// some responses and not in others, and may mark it weak (`W/`).
fn etags_differ(a: Option<&str>, b: Option<&str>) -> bool {
    fn bare(etag: &str) -> &str {
        etag.trim().trim_start_matches("W/").trim_matches('"')
    }
    matches!((a, b), (Some(a), Some(b)) if bare(a) != bare(b))
}

/// Whether the object at a file's key is not the one the file records, by size or,
/// where `compare_etags`, etag.
fn rewritten(recorded: &ManifestEntry, listed: &ListedObject, compare_etags: bool) -> bool {
    matches!((recorded.size, listed.size), (Some(a), Some(b)) if a != b)
        || (compare_etags && etags_differ(recorded.etag.as_deref(), listed.etag.as_deref()))
}

/// Whether a listed object holds the bytes a file records, as far as it shows.
/// Where nothing tells them apart the bytes count as the same, so a producer's
/// checksum stands: kept wrongly, it fails the next read that verifies it.
fn same_bytes(recorded: &ManifestEntry, listed: &ListedObject, compare_etags: bool) -> bool {
    if rewritten(recorded, listed, compare_etags) {
        return false;
    }
    let etags_compared = compare_etags && recorded.etag.is_some() && listed.etag.is_some();
    etags_compared
        || match (&recorded.version_id, &listed.version_id) {
            (Some(recorded), Some(listed)) => recorded == listed,
            _ => true,
        }
}

/// One listed object, keyed as the manifest will be, or `None` if outside the
/// scan.
fn listed_object_for(file: &FileInfo, base: &str, scope: &ScanScope<'_>) -> Option<ListedObject> {
    let physical_path = file.location().to_string();
    // Keys are recorded relative to the dataset, so a dataset whose prefix later
    // moves keeps the same logical keys.
    let logical_key = key_under(base, &physical_path)?;
    // A key ending in `/` is a directory, which ADLS lists, or the placeholder an
    // S3 or GCS console writes for one: never a file.
    if logical_key.is_empty() || logical_key.ends_with('/') || !scope.covers(&logical_key) {
        return None;
    }
    Some(ListedObject {
        logical_key,
        physical_path,
        size: file.size().and_then(|s| i64::try_from(s).ok()),
        // At the precision Postgres stores, so the next scan compares equal.
        last_modified: file.last_modified().map(|t| t.trunc_subsecs(6)),
        etag: file.e_tag().map(ToString::to_string),
        version_id: file.version().map(ToString::to_string),
        referenced: false,
    })
}

/// The entry a modified object gets. What a listing cannot know is kept: the
/// content type a producer set, and the checksum unless the object shows other
/// bytes, as when only a version was taken up.
fn rewritten_entry(
    listed: ListedObject,
    existing: &ManifestEntry,
    compare_etags: bool,
) -> ManifestEntry {
    let same_bytes = same_bytes(existing, &listed, compare_etags);
    let mut entry = entry_for(listed);
    if existing.content_type.is_some() {
        entry.content_type.clone_from(&existing.content_type);
    }
    if same_bytes {
        entry.checksum.clone_from(&existing.checksum);
    }
    entry
}

fn entry_for(listed: ListedObject) -> ManifestEntry {
    ManifestEntry {
        content_type: content_type_for(&listed.logical_key).map(ToString::to_string),
        logical_key: listed.logical_key,
        physical_path: listed.physical_path,
        etag: listed.etag,
        size: listed.size,
        checksum: None,
        version_id: listed.version_id,
        last_modified: listed.last_modified,
    }
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};

    use super::{
        DEFAULT_EXCLUDES, FileInfo, ImportMode, KeyPatterns, ScanScope, content_type_for,
        entry_for, etags_differ, is_modified, listed_key_of, listed_object_for, rewritten,
        rewritten_entry, still_the_pinned_object,
    };
    use crate::service::{ListedObject, ManifestEntry};

    fn listed(key: &str, size: i64, modified_secs: i64) -> ListedObject {
        ListedObject {
            logical_key: key.to_string(),
            physical_path: format!("s3://b/{key}"),
            size: Some(size),
            last_modified: Some(Utc.timestamp_opt(modified_secs, 0).unwrap()),
            etag: None,
            version_id: None,
            referenced: false,
        }
    }

    fn entry(key: &str, size: i64, modified_secs: i64) -> ManifestEntry {
        ManifestEntry {
            logical_key: key.to_string(),
            physical_path: format!("s3://b/{key}"),
            etag: None,
            size: Some(size),
            content_type: None,
            checksum: None,
            version_id: None,
            last_modified: Some(Utc.timestamp_opt(modified_secs, 0).unwrap()),
        }
    }

    #[test]
    fn rescanning_an_unchanged_prefix_modifies_nothing() {
        // Re-importing must not rewrite every row; otherwise a nightly sync would
        // grow the manifest without anything having changed.
        for (key, size, modified) in [("a", 10, 100), ("b", 20, 200)] {
            assert!(
                !is_modified(
                    &listed(key, size, modified),
                    &entry(key, size, modified),
                    ImportMode::Sync
                ),
                "{key}"
            );
        }
    }

    #[test]
    fn a_changed_size_or_timestamp_is_a_modification() {
        let recorded = entry("f", 10, 100);
        // Either signal alone counts: the listing gives us both and neither is
        // reliable on its own.
        assert!(is_modified(
            &listed("f", 99, 100),
            &recorded,
            ImportMode::Sync
        ));
        assert!(is_modified(
            &listed("f", 10, 999),
            &recorded,
            ImportMode::Sync
        ));
    }

    #[test]
    fn a_listed_timestamp_is_kept_at_the_precision_the_manifest_stores() {
        // Postgres keeps microseconds. Finer timestamps from a listing would never
        // compare equal to what was stored, and every sync would report every file
        // as modified.
        let listed = Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap();
        let file = FileInfo::new(Some(listed), "s3://b/p/f".parse().unwrap(), Some(1));
        let object = listed_object_for(&file, "s3://b/p", &ScanScope::new(None, None)).unwrap();
        assert_eq!(
            object.last_modified,
            Some(Utc.timestamp_opt(1_700_000_000, 123_456_000).unwrap())
        );
    }

    #[test]
    fn a_scan_covers_only_keys_under_its_sub_prefix_with_its_suffix() {
        // A sync removes what its scan covers and did not see, so a key the scan
        // never looked at must not count as covered.
        let scope = ScanScope::new(Some("shard=00/"), Some(".parquet"));
        assert!(scope.covers("shard=00/a.parquet"));
        assert!(!scope.covers("shard=00/a.csv"));
        assert!(!scope.covers("shard=001/a.parquet"));
        assert!(!scope.covers("a.parquet"));
        assert!(ScanScope::new(None, None).covers("any/key"));
        // A walk in key order starts at the sub-prefix and ends past it.
        assert_eq!(scope.first_key(), Some("shard=00/"));
        assert!(scope.within_prefix("shard=00/a.csv"));
        assert_eq!(ScanScope::new(None, None).first_key(), None);
    }

    #[test]
    fn the_default_excludes_leave_out_job_debris_only() {
        let scope = ScanScope::new(None, None)
            .with_patterns(KeyPatterns::new([], DEFAULT_EXCLUDES).unwrap());
        for debris in [
            "_SUCCESS",
            "out/day=1/_SUCCESS",
            "_temporary/0/part-0000.parquet",
            "out/_temporary/attempt/part-0001.parquet",
            "out/.checkpoint/state",
        ] {
            assert!(!scope.covers(debris), "{debris} is excluded");
        }
        for data in [
            "out/day=1/part-0000.parquet",
            "out/_SUCCESS.md",
            "out/temporary/part-0000.parquet",
            "out/checkpoint/state",
        ] {
            assert!(scope.covers(data), "{data} is kept");
        }
    }

    #[test]
    fn a_star_stays_within_a_segment_and_a_double_star_crosses() {
        let top =
            ScanScope::new(None, None).with_patterns(KeyPatterns::new(["*.jpg"], []).unwrap());
        assert!(top.covers("a.jpg"));
        assert!(!top.covers("train/a.jpg"));
        let any = ScanScope::new(None, None)
            .with_patterns(KeyPatterns::new(["**/*.jpg", "**/*.png"], ["**/thumbs/**"]).unwrap());
        assert!(any.covers("a.jpg"));
        assert!(any.covers("train/cats/a.png"));
        assert!(!any.covers("train/a.txt"));
        assert!(
            !any.covers("train/thumbs/a.jpg"),
            "an exclude beats an include"
        );
    }

    #[test]
    fn a_glob_that_does_not_parse_is_refused() {
        let err = KeyPatterns::new(["train/[a"], []).unwrap_err();
        assert_eq!(err.error.r#type, "InvalidGlob");
    }

    #[test]
    fn a_changed_etag_or_version_is_a_modification() {
        let recorded = ManifestEntry {
            etag: Some("\"a\"".to_string()),
            version_id: Some("1".to_string()),
            ..entry("f", 10, 100)
        };
        // Same size and time, rewritten: only the etag tells.
        let rewritten = ListedObject {
            etag: Some("\"b\"".to_string()),
            ..listed("f", 10, 100)
        };
        assert!(is_modified(&rewritten, &recorded, ImportMode::Sync));
        let new_version = ListedObject {
            etag: Some("\"a\"".to_string()),
            version_id: Some("2".to_string()),
            ..listed("f", 10, 100)
        };
        assert!(is_modified(&new_version, &recorded, ImportMode::Sync));
        // A backend that reports no etag says nothing about it.
        assert!(!is_modified(
            &listed("f", 10, 100),
            &recorded,
            ImportMode::Sync
        ));
    }

    /// Recording versions over a dataset imported without them pins its files:
    /// each takes up the version the listing reports, and keeps it after.
    #[test]
    fn a_version_listed_where_none_is_recorded_is_taken_up() {
        let versioned = ListedObject {
            version_id: Some("7".to_string()),
            ..listed("f", 10, 100)
        };
        assert!(is_modified(
            &versioned,
            &entry("f", 10, 100),
            ImportMode::Sync
        ));
        let pinned = ManifestEntry {
            version_id: Some("7".to_string()),
            ..entry("f", 10, 100)
        };
        assert!(!is_modified(&versioned, &pinned, ImportMode::Sync));
    }

    /// A rewritten object keeps the content type its producer set, but not the
    /// checksum, which described the old bytes.
    #[test]
    fn a_rewritten_entry_drops_the_checksum_of_the_old_bytes() {
        let recorded = ManifestEntry {
            etag: Some("\"a\"".to_string()),
            checksum: Some("sha256:old".to_string()),
            content_type: Some("application/x-custom".to_string()),
            ..entry("f", 10, 100)
        };
        for listed in [
            listed("f", 11, 100),
            ListedObject {
                etag: Some("\"b\"".to_string()),
                ..listed("f", 10, 100)
            },
        ] {
            let entry = rewritten_entry(listed, &recorded, true);
            assert_eq!(entry.checksum, None, "{entry:?}");
            assert_eq!(entry.content_type.as_deref(), Some("application/x-custom"));
        }
        let same_bytes = rewritten_entry(listed("f", 10, 200), &recorded, true);
        assert_eq!(same_bytes.checksum.as_deref(), Some("sha256:old"));
    }

    /// Where etags do not compare, as on GCS, an etag tells nothing about the bytes:
    /// the checksum stays unless the size or the version shows other bytes. Where
    /// they do, an equal etag shows the same bytes under a new version.
    #[test]
    fn a_rewritten_entry_judges_the_bytes_by_what_compares() {
        let recorded = ManifestEntry {
            etag: Some("\"producer\"".to_string()),
            version_id: Some("1".to_string()),
            checksum: Some("sha256:old".to_string()),
            ..entry("f", 10, 100)
        };
        let other_etag = ListedObject {
            etag: Some("CJ3v".to_string()),
            version_id: Some("1".to_string()),
            ..listed("f", 10, 200)
        };
        assert_eq!(
            rewritten_entry(other_etag.clone(), &recorded, false)
                .checksum
                .as_deref(),
            Some("sha256:old"),
            "an etag that does not compare is no evidence"
        );
        let other_version = ListedObject {
            version_id: Some("2".to_string()),
            ..other_etag
        };
        assert_eq!(
            rewritten_entry(other_version, &recorded, false).checksum,
            None,
            "another generation is other bytes"
        );
        let copied_in_place = ListedObject {
            etag: Some("producer".to_string()),
            version_id: Some("2".to_string()),
            ..listed("f", 10, 200)
        };
        assert_eq!(
            rewritten_entry(copied_in_place, &recorded, true)
                .checksum
                .as_deref(),
            Some("sha256:old"),
            "an equal etag shows the same bytes"
        );
    }

    #[test]
    fn etags_compare_without_their_quotes() {
        assert!(!etags_differ(Some("\"abc\""), Some("abc")));
        assert!(!etags_differ(Some("W/\"abc\""), Some("\"abc\"")));
        assert!(etags_differ(Some("\"abc\""), Some("\"abd\"")));
        assert!(
            !etags_differ(None, Some("abc")),
            "an etag counts only where both sides have one"
        );
    }

    #[test]
    fn a_rewritten_object_shows_in_its_size_or_where_etags_compare_its_etag() {
        let recorded = ManifestEntry {
            etag: Some("\"a\"".to_string()),
            ..entry("f", 10, 100)
        };
        assert!(rewritten(&recorded, &listed("f", 11, 100), false));
        let other_etag = ListedObject {
            etag: Some("\"b\"".to_string()),
            ..listed("f", 10, 100)
        };
        assert!(rewritten(&recorded, &other_etag, true));
        assert!(
            !rewritten(&recorded, &other_etag, false),
            "where etags do not compare, as on GCS"
        );
        assert!(
            !rewritten(&recorded, &listed("f", 10, 200), true),
            "a newer timestamp alone is no rewrite"
        );
    }

    /// A key a listing shows untouched since a file was pinned to it still holds
    /// that version, so no lookup is needed; one written since may not.
    #[test]
    fn an_untouched_key_still_holds_the_pinned_version() {
        let pinned = ManifestEntry {
            etag: Some("\"a\"".to_string()),
            version_id: Some("v1".to_string()),
            ..entry("f", 10, 100)
        };
        let untouched = ListedObject {
            etag: Some("a".to_string()),
            ..listed("f", 10, 100)
        };
        assert!(still_the_pinned_object(&pinned, &untouched, true));
        let written_again = ListedObject {
            last_modified: listed("f", 10, 200).last_modified,
            ..untouched.clone()
        };
        assert!(!still_the_pinned_object(&pinned, &written_again, true));
        let other_bytes = ListedObject {
            etag: Some("b".to_string()),
            ..untouched.clone()
        };
        assert!(!still_the_pinned_object(&pinned, &other_bytes, true));
        let unknown_time = ManifestEntry {
            last_modified: None,
            ..pinned
        };
        assert!(
            !still_the_pinned_object(&unknown_time, &untouched, true),
            "without a recorded time nothing shows the key untouched"
        );
    }

    /// A file is judged by the key its bytes list under: where a read finds them,
    /// and nothing for bytes a listing of the location cannot show.
    #[test]
    fn a_file_lists_under_the_key_a_read_finds_its_bytes_at() {
        assert_eq!(
            listed_key_of("s3://b/ds", "s3://b/ds/raw/x").as_deref(),
            Some("raw/x")
        );
        assert_eq!(
            listed_key_of("s3://b/ds/", "s3://b/ds/raw/x").as_deref(),
            Some("raw/x")
        );
        assert_eq!(
            listed_key_of("s3://b/ds", "s3://b/ds2/raw/x"),
            None,
            "a sibling prefix is outside the location"
        );
        assert_eq!(listed_key_of("s3://b/ds", "s3://other/raw/x"), None);
        assert_eq!(
            listed_key_of("s3://b/ds", "raw//x").as_deref(),
            Some("raw/x"),
            "a relative path reads without its empty segments"
        );
    }

    /// ADLS lists directories, and S3 and GCS consoles write a placeholder object
    /// for one: neither is a file of the dataset.
    #[test]
    fn a_directory_is_not_a_listed_object() {
        let scope = ScanScope::new(None, None);
        for directory in [
            "s3://b/p/photos/",
            "abfss://fs@acc.dfs.core.windows.net/p/a/b/",
        ] {
            let base = directory.split("/p/").next().unwrap().to_string() + "/p";
            let file = FileInfo::new(None, directory.parse().unwrap(), None);
            assert!(
                listed_object_for(&file, &base, &scope).is_none(),
                "{directory}"
            );
        }
        let file = FileInfo::new(None, "s3://b/p/photos/a.jpg".parse().unwrap(), Some(1));
        assert!(listed_object_for(&file, "s3://b/p", &scope).is_some());
    }

    #[test]
    fn a_listed_object_keeps_the_etag_and_version_the_backend_reported() {
        let file = FileInfo::new(None, "gs://b/p/f".parse().unwrap(), Some(1))
            .with_e_tag(Some("CJ3v".to_string()))
            .with_version(Some("1700000000123456".to_string()));
        let object = listed_object_for(&file, "gs://b/p", &ScanScope::new(None, None)).unwrap();
        assert_eq!(object.etag.as_deref(), Some("CJ3v"));
        assert_eq!(object.version_id.as_deref(), Some("1700000000123456"));
        let entry = entry_for(object);
        assert_eq!(entry.etag.as_deref(), Some("CJ3v"));
        assert_eq!(entry.version_id.as_deref(), Some("1700000000123456"));
    }

    #[test]
    fn add_only_never_reports_a_modification() {
        // The default mode must not rewrite an existing row, however the object
        // changed — it only ever adds.
        assert!(!is_modified(
            &listed("f", 99, 999),
            &entry("f", 10, 100),
            ImportMode::AddOnly
        ));
    }

    #[test]
    fn content_type_is_inferred_from_the_extension_case_insensitively() {
        assert_eq!(content_type_for("a/b/c.JPG"), Some("image/jpeg"));
        assert_eq!(
            content_type_for("x.parquet"),
            Some("application/vnd.apache.parquet")
        );
        assert_eq!(content_type_for("notes.txt"), Some("text/plain"));
        // Unknown and extensionless keys carry no type: the value is advisory and a
        // wrong one is worse than none.
        assert_eq!(content_type_for("archive.xyz"), None);
        assert_eq!(content_type_for("README"), None);
        // A dot in a directory must not be mistaken for the file's extension.
        assert_eq!(content_type_for("v1.2/data"), None);
    }
}
