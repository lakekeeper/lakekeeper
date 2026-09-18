use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures::StreamExt;
use lakekeeper_io::LakekeeperStorage;
use uuid::Uuid;

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            DatasetParameters, ImportDatasetRequest, ImportDatasetResponse, ImportMode,
        },
        iceberg::v1::Result,
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id},
    service::{
        CatalogDatasetOps, CatalogStore, CommitDatasetError, DatasetSnapshotId, ManifestEntry,
        NamedEntity, ResolvedWarehouse, SecretStore, State, TabularListFlags, Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{APIEventContext, context::UserProvidedDataset},
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            dataset_import_queue::{DatasetImportPayload, DatasetImportTask},
        },
    },
};

/// Content type inferred from a file extension. Advisory: the catalog never
/// opens the object.
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

/// Register the objects already under a dataset's location as a new snapshot.
///
/// Nothing is copied: the listing is the source of truth, so this works on
/// prefixes too large to put in a request body.
// A sequence of phases; the parts worth naming are already extracted.
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

    let dataset_ident = iceberg::TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        CatalogDatasetAction::Commit,
    );

    let authz_result = authorizer
        .load_and_authorize_dataset_operation::<C>(
            event_ctx.request_metadata(),
            &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
            TabularListFlags::active_and_staged(),
            CatalogDatasetAction::Commit,
            state.v1_state.catalog.clone(),
        )
        .await;
    let (_event_ctx, (warehouse, _namespace, info)) = event_ctx.emit_authz(authz_result)?;

    if request.queued.unwrap_or(false) {
        // Authorized against the caller here; the task then runs with the
        // server's own authority, as purge does. The task table admits one
        // active import per dataset, so firing twice enqueues once.
        let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        let task_id = DatasetImportTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id: warehouse.project_id.clone(),
                parent_task_id: None,
                scheduled_for: None,
                entity: TaskEntity::EntityInWarehouse {
                    entity_name: dataset_ident.into_name_parts(),
                    warehouse_id,
                    entity_id: WarehouseTaskEntityId::Dataset {
                        dataset_id: info.tabular_id,
                    },
                },
            },
            DatasetImportPayload {
                branch: request.branch.clone(),
                sub_prefix: request.sub_prefix.clone(),
                suffix: request.suffix.clone(),
                max_files: request.max_files,
                mode: request.mode.unwrap_or_default(),
                summary: request.summary.clone(),
            },
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        // `schedule_task` returns None when the dataset already has an active
        // import. Reporting success with neither id would read as "queued
        // nothing".
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
        });
    }

    let outcome = run_import::<C, S>(
        warehouse.as_ref(),
        &state.v1_state.secrets,
        warehouse_id,
        info.tabular_id,
        info.location.as_ref(),
        &ImportParams::from(&request),
        state.v1_state.catalog,
    )
    .await?;

    Ok(ImportDatasetResponse {
        snapshot_id: Some(outcome.snapshot_id),
        task_id: None,
        imported: outcome.added,
        modified: outcome.modified,
        removed: outcome.removed,
        truncated: outcome.truncated,
    })
}

/// Everything an import needs that is not the dataset itself.
#[derive(Debug, Clone)]
pub(crate) struct ImportParams {
    pub branch: String,
    pub sub_prefix: Option<String>,
    pub suffix: Option<String>,
    pub max_files: Option<i64>,
    pub mode: ImportMode,
    pub summary: Option<serde_json::Value>,
}

impl From<&ImportDatasetRequest> for ImportParams {
    fn from(request: &ImportDatasetRequest) -> Self {
        Self {
            branch: request.branch.clone().unwrap_or_else(|| "main".to_string()),
            sub_prefix: request.sub_prefix.clone(),
            suffix: request.suffix.clone(),
            max_files: request.max_files,
            mode: request.mode.unwrap_or_default(),
            summary: request.summary.clone(),
        }
    }
}

/// What an import did.
pub(crate) struct ImportOutcome {
    pub snapshot_id: DatasetSnapshotId,
    pub added: i64,
    pub modified: i64,
    pub removed: i64,
    pub truncated: bool,
}

/// Scan the prefix and commit the result. No authorization happens here: the
/// caller has already established it, whether that was an API request or the
/// request that enqueued a task.
// A linear sequence of phases, each already a named call; splitting further
// would separate steps that only make sense in order.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run_import<C: CatalogStore, S: SecretStore>(
    warehouse: &ResolvedWarehouse,
    secrets: &S,
    warehouse_id: crate::WarehouseId,
    dataset_id: crate::service::DatasetId,
    location: &str,
    params: &ImportParams,
    catalog_state: C::State,
) -> Result<ImportOutcome> {
    let storage_secret = maybe_get_secret(warehouse.storage_secret_id, secrets).await?;
    let file_io = warehouse
        .storage_profile
        .file_io(storage_secret.as_deref())
        .await?;

    let scan_root = resolve_scan_root(location, params.sub_prefix.as_deref())?;

    let mode = params.mode;
    let max_files = match params.max_files {
        // `usize::try_from` would fail on a negative and fall back to unbounded,
        // turning a limit into its opposite.
        Some(max) if max < 1 => {
            return Err(ErrorModel::bad_request(
                "max-files must be positive",
                "InvalidMaxFiles",
                None,
            )
            .into());
        }
        Some(max) => usize::try_from(max).unwrap_or(usize::MAX),
        None => usize::MAX,
    };

    // Read before staging opens, so a long scan holds no read transaction.
    let current = if mode == ImportMode::Sync {
        current_manifest::<C>(
            warehouse_id,
            dataset_id,
            &params.branch,
            catalog_state.clone(),
        )
        .await?
    } else {
        HashMap::new()
    };

    let head = {
        let mut t = C::Transaction::begin_read(catalog_state.clone()).await?;
        let head = C::get_dataset_ref(warehouse_id, dataset_id, &params.branch, t.transaction())
            .await?
            .snapshot_id;
        t.commit().await?;
        head
    };

    // Rows are flushed as they are found, so peak memory is one batch and no
    // transaction spans the listing.
    let snapshot_id = DatasetSnapshotId::from(Uuid::now_v7());
    {
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
        C::begin_dataset_commit(
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
    }

    let outcome = stream_scan_into_snapshot::<C>(
        &file_io,
        scan_root.as_ref(),
        location,
        params.suffix.as_deref(),
        max_files,
        mode,
        &current,
        warehouse_id,
        dataset_id,
        snapshot_id,
        catalog_state.clone(),
    )
    .await?;

    // Absence is only evidence of deletion if the whole prefix was seen.
    if mode == ImportMode::Sync && outcome.truncated {
        return Err(ErrorModel::bad_request(
            "sync mode cannot run against a truncated scan: raise max-files or narrow sub-prefix",
            "DatasetImportTruncatedSync",
            None,
        )
        .into());
    }

    let removed_count = if mode == ImportMode::Sync {
        stage_vanished::<C>(
            warehouse_id,
            dataset_id,
            snapshot_id,
            &current,
            &outcome.seen,
            catalog_state.clone(),
        )
        .await?
    } else {
        0
    };

    // The only step that races. Everything above is invisible, so losing costs
    // the pointer move, not the scan.
    finish_with_retry::<C>(
        warehouse_id,
        dataset_id,
        &params.branch,
        snapshot_id,
        head,
        mode,
        &outcome.seen,
        catalog_state,
    )
    .await?;

    Ok(ImportOutcome {
        snapshot_id,
        added: i64::try_from(outcome.added).unwrap_or(i64::MAX),
        modified: i64::try_from(outcome.modified).unwrap_or(i64::MAX),
        removed: i64::try_from(removed_count).unwrap_or(i64::MAX),
        truncated: outcome.truncated,
    })
}

/// Where the scan starts: the dataset's location, optionally narrowed.
///
/// `sub_prefix` narrows within the location rather than replacing it -- an import
/// must not reach outside the prefix the dataset was created against.
fn resolve_scan_root(location: &str, sub_prefix: Option<&str>) -> Result<lakekeeper_io::Location> {
    let mut root = <lakekeeper_io::Location as std::str::FromStr>::from_str(location)
        .map_err(|e| ErrorModel::internal(e.to_string(), "InvalidDatasetLocation", None))?;
    if let Some(sub_prefix) = sub_prefix {
        if sub_prefix.starts_with('/') || sub_prefix.split('/').any(|s| s == "..") {
            return Err(ErrorModel::bad_request(
                "sub-prefix must be relative and must not escape the dataset location",
                "InvalidSubPrefix",
                None,
            )
            .into());
        }
        root.extend(sub_prefix.split('/').filter(|s| !s.is_empty()));
    }
    Ok(root)
}

/// Stage a removal for every key the manifest has that the scan did not see.
async fn stage_vanished<C: CatalogStore>(
    warehouse_id: crate::WarehouseId,
    dataset_id: crate::service::DatasetId,
    snapshot_id: DatasetSnapshotId,
    current: &HashMap<String, ManifestEntry>,
    seen: &HashSet<String>,
    catalog_state: C::State,
) -> Result<usize> {
    let removed: Vec<String> = current
        .keys()
        .filter(|key| !seen.contains(*key))
        .cloned()
        .collect();
    if removed.is_empty() {
        return Ok(0);
    }
    let count = removed.len();
    let mut t = C::Transaction::begin_write(catalog_state).await?;
    C::stage_dataset_files(
        warehouse_id,
        dataset_id,
        snapshot_id,
        &[],
        &[],
        &removed,
        t.transaction(),
    )
    .await?;
    t.commit().await?;
    Ok(count)
}

/// How long a staging snapshot must sit untouched to count as abandoned. Long
/// enough that a slow but live import is never swept.
const ABANDONED_STAGING_AFTER: chrono::TimeDelta = chrono::TimeDelta::hours(24);

/// How many times to re-aim the pointer move at a moved branch.
const FINISH_ATTEMPTS: usize = 3;

/// Move the branch to the staged snapshot, re-aiming if it moved underneath.
///
/// A lost compare-and-swap invalidates only the parent the staged rows hang off,
/// so a retry re-parents rather than repeating the scan. Sync also recomputes
/// removals: a file another writer added is in the manifest but was not in our
/// listing, and removing it would delete a file that is there.
#[allow(clippy::too_many_arguments)]
async fn finish_with_retry<C: CatalogStore>(
    warehouse_id: crate::WarehouseId,
    dataset_id: crate::service::DatasetId,
    branch: &str,
    snapshot_id: DatasetSnapshotId,
    mut expected: Option<DatasetSnapshotId>,
    mode: ImportMode,
    seen: &HashSet<String>,
    catalog_state: C::State,
) -> Result<()> {
    for attempt in 0..FINISH_ATTEMPTS {
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
            Ok(_checkpoint_due) => {
                t.commit().await?;
                return Ok(());
            }
            Err(CommitDatasetError::DatasetCommitConflict(conflict)) => {
                // Roll back before re-reading: the failed attempt's transaction
                // holds nothing worth keeping.
                drop(t);
                if attempt + 1 == FINISH_ATTEMPTS {
                    return Err(CommitDatasetError::DatasetCommitConflict(conflict).into());
                }
                expected = conflict.current_snapshot_id;

                let mut t = C::Transaction::begin_write(catalog_state.clone()).await?;
                C::reparent_staging_snapshot(warehouse_id, snapshot_id, expected, t.transaction())
                    .await?;
                t.commit().await?;

                if mode == ImportMode::Sync {
                    restage_removals::<C>(
                        warehouse_id,
                        dataset_id,
                        branch,
                        snapshot_id,
                        seen,
                        catalog_state.clone(),
                    )
                    .await?;
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Which keys the new head has that our listing did not see. Rows are keyed by
/// `(snapshot, logical_key)`, so re-staging an existing removal is a no-op.
async fn restage_removals<C: CatalogStore>(
    warehouse_id: crate::WarehouseId,
    dataset_id: crate::service::DatasetId,
    branch: &str,
    snapshot_id: DatasetSnapshotId,
    seen: &HashSet<String>,
    catalog_state: C::State,
) -> Result<()> {
    let current =
        current_manifest::<C>(warehouse_id, dataset_id, branch, catalog_state.clone()).await?;
    let removed: Vec<String> = current
        .keys()
        .filter(|key| !seen.contains(*key))
        .cloned()
        .collect();
    if removed.is_empty() {
        return Ok(());
    }
    let mut t = C::Transaction::begin_write(catalog_state).await?;
    C::stage_dataset_files(
        warehouse_id,
        dataset_id,
        snapshot_id,
        &[],
        &[],
        &removed,
        t.transaction(),
    )
    .await?;
    t.commit().await?;
    Ok(())
}

/// Rows per staging transaction. Bounds peak memory and transaction size, so a
/// prefix of any size costs a fixed amount of either.
const STAGE_BATCH: usize = 5_000;

struct ScanOutcome {
    added: usize,
    modified: usize,
    truncated: bool,
    /// Keys the scan saw, needed to work out what has since disappeared.
    /// Keys only -- holding whole entries would defeat the batching above.
    seen: HashSet<String>,
}

/// List the prefix, classifying each object and flushing batches as it goes.
#[allow(clippy::too_many_arguments)]
async fn stream_scan_into_snapshot<C: CatalogStore>(
    file_io: &impl LakekeeperStorage,
    root: &str,
    base: &str,
    suffix: Option<&str>,
    max_files: usize,
    mode: ImportMode,
    current: &HashMap<String, ManifestEntry>,
    warehouse_id: crate::WarehouseId,
    dataset_id: crate::service::DatasetId,
    snapshot_id: DatasetSnapshotId,
    catalog_state: C::State,
) -> Result<ScanOutcome> {
    let mut pages = file_io.list(root, None).await.map_err(|e| {
        ErrorModel::bad_request(
            format!("Cannot list {root}: {e}"),
            "DatasetImportListFailed",
            None,
        )
    })?;

    let mut outcome = ScanOutcome {
        added: 0,
        modified: 0,
        truncated: false,
        seen: HashSet::new(),
    };
    let mut added_batch: Vec<ManifestEntry> = Vec::new();
    let mut modified_batch: Vec<ManifestEntry> = Vec::new();
    let mut total = 0usize;

    'pages: while let Some(page) = pages.next().await {
        let page = page.map_err(|e| {
            ErrorModel::internal(
                format!("Failed listing {root}: {e}"),
                "DatasetImportListFailed",
                None,
            )
        })?;
        for file in page {
            let Some(entry) = manifest_entry_for(&file, base, suffix) else {
                continue;
            };
            if total >= max_files {
                outcome.truncated = true;
                break 'pages;
            }
            total += 1;

            if mode == ImportMode::Sync {
                outcome.seen.insert(entry.logical_key.clone());
            }
            match classify(&entry, mode, current) {
                Classification::Added => {
                    outcome.added += 1;
                    added_batch.push(entry);
                }
                Classification::Modified => {
                    outcome.modified += 1;
                    modified_batch.push(entry);
                }
                Classification::Unchanged => {}
            }

            if added_batch.len() + modified_batch.len() >= STAGE_BATCH {
                flush::<C>(
                    warehouse_id,
                    dataset_id,
                    snapshot_id,
                    &mut added_batch,
                    &mut modified_batch,
                    catalog_state.clone(),
                )
                .await?;
            }
        }
    }

    flush::<C>(
        warehouse_id,
        dataset_id,
        snapshot_id,
        &mut added_batch,
        &mut modified_batch,
        catalog_state,
    )
    .await?;

    Ok(outcome)
}

async fn flush<C: CatalogStore>(
    warehouse_id: crate::WarehouseId,
    dataset_id: crate::service::DatasetId,
    snapshot_id: DatasetSnapshotId,
    added: &mut Vec<ManifestEntry>,
    modified: &mut Vec<ManifestEntry>,
    catalog_state: C::State,
) -> Result<()> {
    if added.is_empty() && modified.is_empty() {
        return Ok(());
    }
    let mut t = C::Transaction::begin_write(catalog_state).await?;
    C::stage_dataset_files(
        warehouse_id,
        dataset_id,
        snapshot_id,
        added,
        modified,
        &[],
        t.transaction(),
    )
    .await?;
    t.commit().await?;
    added.clear();
    modified.clear();
    Ok(())
}

/// A scanned object, relative to the manifest the branch already has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Classification {
    /// Not in the manifest.
    Added,
    /// Present, but the object differs.
    Modified,
    /// Present and indistinguishable from what is recorded.
    Unchanged,
}

/// Size and last-modified are all a listing gives, so they are the whole
/// comparison -- which is what makes re-importing an untouched prefix an empty
/// commit rather than a full rewrite.
fn classify(
    entry: &ManifestEntry,
    mode: ImportMode,
    current: &HashMap<String, ManifestEntry>,
) -> Classification {
    if mode == ImportMode::AddOnly {
        return Classification::Added;
    }
    match current.get(&entry.logical_key) {
        None => Classification::Added,
        Some(existing) => {
            if existing.size == entry.size && existing.last_modified == entry.last_modified {
                Classification::Unchanged
            } else {
                Classification::Modified
            }
        }
    }
}

/// One listed object as a manifest entry, or `None` if filtered out.
fn manifest_entry_for(
    file: &lakekeeper_io::FileInfo,
    base: &str,
    suffix: Option<&str>,
) -> Option<ManifestEntry> {
    let physical_path = file.location().to_string();
    // Keys are recorded relative to the dataset, so a dataset whose prefix later
    // moves keeps the same logical keys.
    let logical_key = physical_path
        .strip_prefix(base)
        .map(|k| k.trim_start_matches('/').to_string())?;
    if logical_key.is_empty() {
        return None;
    }
    if let Some(suffix) = suffix
        && !logical_key.ends_with(suffix)
    {
        return None;
    }
    Some(ManifestEntry {
        logical_key: logical_key.clone(),
        physical_path,
        etag: None,
        size: file.size().and_then(|s| i64::try_from(s).ok()),
        content_type: content_type_for(&logical_key).map(ToString::to_string),
        checksum: None,
        version_id: None,
        last_modified: file.last_modified(),
    })
}

/// Every file the branch currently resolves to, keyed by logical key.
///
/// A branch with no commits lists no files, so the first import sees an empty
/// manifest rather than an error.
async fn current_manifest<C: CatalogStore>(
    warehouse_id: crate::WarehouseId,
    dataset_id: crate::service::DatasetId,
    branch: &str,
    catalog_state: C::State,
) -> Result<HashMap<String, ManifestEntry>> {
    let mut current = HashMap::new();
    let mut page_token = None;
    loop {
        let mut t = C::Transaction::begin_read(catalog_state.clone()).await?;
        let (_snapshot, entries, next) = C::list_dataset_files(
            warehouse_id,
            dataset_id,
            branch,
            None,
            None,
            page_token.as_deref(),
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        for entry in entries {
            current.insert(entry.logical_key.clone(), entry);
        }
        // A page may be short or empty while files remain, so the token is what
        // ends the walk -- not an empty page.
        match next {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }
    Ok(current)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};

    use super::{Classification, ImportMode, classify, content_type_for};
    use crate::service::ManifestEntry;

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

    fn manifest(entries: Vec<ManifestEntry>) -> HashMap<String, ManifestEntry> {
        entries
            .into_iter()
            .map(|e| (e.logical_key.clone(), e))
            .collect()
    }

    #[test]
    fn rescanning_an_unchanged_prefix_classifies_everything_unchanged() {
        // Re-importing must not rewrite every row; otherwise a nightly sync would
        // grow the manifest without anything having changed.
        let current = manifest(vec![entry("a", 10, 100), entry("b", 20, 200)]);
        for scanned in [entry("a", 10, 100), entry("b", 20, 200)] {
            assert_eq!(
                classify(&scanned, ImportMode::Sync, &current),
                Classification::Unchanged,
                "{}",
                scanned.logical_key
            );
        }
    }

    #[test]
    fn a_changed_size_or_timestamp_is_a_modification() {
        let current = manifest(vec![entry("f", 10, 100)]);
        // Either signal alone counts: the listing gives us both and neither is
        // reliable on its own.
        assert_eq!(
            classify(&entry("f", 99, 100), ImportMode::Sync, &current),
            Classification::Modified
        );
        assert_eq!(
            classify(&entry("f", 10, 999), ImportMode::Sync, &current),
            Classification::Modified
        );
        assert_eq!(
            classify(&entry("new", 1, 1), ImportMode::Sync, &current),
            Classification::Added
        );
    }

    #[test]
    fn add_only_never_reports_a_modification() {
        // The default mode must not rewrite an existing row, however the object
        // changed -- it only ever adds.
        let current = manifest(vec![entry("f", 10, 100)]);
        assert_eq!(
            classify(&entry("f", 99, 999), ImportMode::AddOnly, &current),
            Classification::Added
        );
    }

    #[test]
    fn content_type_is_inferred_from_the_extension_case_insensitively() {
        assert_eq!(content_type_for("a/b/c.JPG"), Some("image/jpeg"));
        assert_eq!(
            content_type_for("x.parquet"),
            Some("application/vnd.apache.parquet")
        );
        assert_eq!(content_type_for("notes.txt"), Some("text/plain"));
        // Unknown and extensionless keys carry no type rather than a guessed one:
        // the value is advisory and a wrong one is worse than none.
        assert_eq!(content_type_for("archive.xyz"), None);
        assert_eq!(content_type_for("README"), None);
        // A dot in a directory must not be mistaken for the file's extension.
        assert_eq!(content_type_for("v1.2/data"), None);
    }
}
