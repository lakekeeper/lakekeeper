//! Dataset versioning: snapshots, refs, and manifest reconstruction.
//!
//! Concurrency is one conditional update: a commit stages a snapshot and its
//! manifest rows, then moves the branch pointer with a compare-and-swap against
//! the snapshot the caller expected.
use std::borrow::Cow;

use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper::{
    CONFIG, WarehouseId,
    service::{
        CatalogBackendError, CommitDatasetError, ConstraintViolationPolicy, CreateDatasetRefError,
        DatasetCommit, DatasetCommitConflict, DatasetCommitOutcome, DatasetCommitToTag,
        DatasetConstraints, DatasetId, DatasetMoveTag, DatasetNotFound, DatasetPurge, DatasetRef,
        DatasetRefAlreadyExists, DatasetRefNotFound, DatasetRefProtected, DatasetRefType,
        DatasetSnapshot, DatasetSnapshotHeld, DatasetSnapshotId, DatasetSnapshotNode,
        DatasetSnapshotNotADescendant, DatasetSnapshotNotFound, DegradedFile, DegradedFileProblem,
        DeleteDatasetRefError, ExpireDatasetSnapshotError, InvalidPaginationToken,
        ListDatasetRefsError, ListManifestEntriesError, ListedObject, ManifestEntry,
        MaterializationFindings, MoveDatasetRefError, RecordedCommit, RestoreDatasetSnapshotError,
        SetDatasetRefProtectionError, SnapshotMaterialization, SnapshotMaterializationStatus,
        StagedBatch, StagedChanges, idempotency::IdempotencyKey,
    },
};
use uuid::Uuid;

use super::super::dbutils::DBErrorHandler;

/// The snapshot the first page resolved to, plus the last key returned, so a commit
/// landing mid-pagination cannot tear the sequence. The ref rides too, encoded as
/// it may contain `&`; the key is last so `splitn(4, '&')` keeps it intact.
struct FilesPageToken {
    snapshot_id: DatasetSnapshotId,
    ref_name: String,
    after_key: String,
}

impl FilesPageToken {
    fn encode(snapshot_id: DatasetSnapshotId, ref_name: &str, after_key: &str) -> String {
        let ref_name = BASE64_URL_SAFE_NO_PAD.encode(ref_name);
        let raw = format!("1&{snapshot_id}&{ref_name}&{after_key}");
        BASE64_URL_SAFE_NO_PAD.encode(raw)
    }

    fn decode(token: &str) -> Result<Self, InvalidPaginationToken> {
        let decoded = BASE64_URL_SAFE_NO_PAD
            .decode(token)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(|| {
                InvalidPaginationToken::new("Invalid dataset files page token encoding", token)
            })?;

        let parts = decoded.splitn(4, '&').collect::<Vec<_>>();
        match parts.as_slice() {
            ["1", snapshot, ref_name, key] => Ok(Self {
                snapshot_id: snapshot
                    .parse::<Uuid>()
                    .map_err(|_| {
                        InvalidPaginationToken::new(
                            "Invalid dataset files page token snapshot",
                            token,
                        )
                    })?
                    .into(),
                ref_name: BASE64_URL_SAFE_NO_PAD
                    .decode(ref_name)
                    .ok()
                    .and_then(|b| String::from_utf8(b).ok())
                    .ok_or_else(|| {
                        InvalidPaginationToken::new("Invalid dataset files page token ref", token)
                    })?,
                after_key: (*key).to_string(),
            }),
            _ => Err(InvalidPaginationToken::new(
                "Invalid dataset files page token structure",
                token,
            )),
        }
    }
}

/// How a manifest row changes its file. The file listing drops removals in Rust,
/// which lets the paging bound stay inside the DISTINCT ON.
#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "dataset_manifest_change", rename_all = "lowercase")]
enum DatasetManifestChange {
    Added,
    Modified,
    Removed,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "dataset_ref_type", rename_all = "lowercase")]
enum DbDatasetRefType {
    Branch,
    Tag,
}

impl From<DatasetRefType> for DbDatasetRefType {
    fn from(typ: DatasetRefType) -> Self {
        match typ {
            DatasetRefType::Branch => DbDatasetRefType::Branch,
            DatasetRefType::Tag => DbDatasetRefType::Tag,
        }
    }
}

impl From<DbDatasetRefType> for DatasetRefType {
    fn from(typ: DbDatasetRefType) -> Self {
        match typ {
            DbDatasetRefType::Branch => DatasetRefType::Branch,
            DbDatasetRefType::Tag => DatasetRefType::Tag,
        }
    }
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "dataset_degraded_file_problem", rename_all = "lowercase")]
enum DbDegradedFileProblem {
    Missing,
    Changed,
}

impl From<DegradedFileProblem> for DbDegradedFileProblem {
    fn from(problem: DegradedFileProblem) -> Self {
        match problem {
            DegradedFileProblem::Missing => DbDegradedFileProblem::Missing,
            DegradedFileProblem::Changed => DbDegradedFileProblem::Changed,
        }
    }
}

impl From<DbDegradedFileProblem> for DegradedFileProblem {
    fn from(problem: DbDegradedFileProblem) -> Self {
        match problem {
            DbDegradedFileProblem::Missing => DegradedFileProblem::Missing,
            DbDegradedFileProblem::Changed => DegradedFileProblem::Changed,
        }
    }
}

async fn fetch_constraints(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetConstraints, CommitDatasetError> {
    let row = sqlx::query!(
        r#"SELECT constraints FROM dataset
           WHERE warehouse_id = $1 AND dataset_id = $2"#,
        *warehouse_id,
        *dataset_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| CommitDatasetError::from(DatasetNotFound::new()))?;

    match row.constraints {
        Some(value) => serde_json::from_value(value)
            .map_err(|e| CommitDatasetError::from(CatalogBackendError::new_unexpected(e))),
        None => Ok(DatasetConstraints::default()),
    }
}

/// A manifest row as the listing reads it: newest record per key, removals
/// included until they are dropped.
struct ManifestRow {
    logical_key: String,
    physical_path: String,
    change: DatasetManifestChange,
    etag: Option<String>,
    size: Option<i64>,
    content_type: Option<String>,
    checksum: Option<String>,
    version_id: Option<String>,
    last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<ManifestRow> for ManifestEntry {
    fn from(row: ManifestRow) -> Self {
        ManifestEntry {
            logical_key: row.logical_key,
            physical_path: row.physical_path,
            etag: row.etag,
            size: row.size,
            content_type: row.content_type,
            checksum: row.checksum,
            version_id: row.version_id,
            last_modified: row.last_modified,
        }
    }
}

struct SnapshotRow {
    snapshot_id: Uuid,
    parent_snapshot_id: Option<Uuid>,
    location: String,
    is_checkpoint: bool,
    summary: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<SnapshotRow> for DatasetSnapshot {
    fn from(row: SnapshotRow) -> Self {
        DatasetSnapshot {
            snapshot_id: row.snapshot_id.into(),
            parent_snapshot_id: row.parent_snapshot_id.map(Into::into),
            location: row.location,
            is_checkpoint: row.is_checkpoint,
            summary: row.summary,
            created_at: row.created_at,
        }
    }
}

struct RefRow {
    name: String,
    typ: DbDatasetRefType,
    snapshot_id: Option<Uuid>,
    protected: bool,
}

impl From<RefRow> for DatasetRef {
    fn from(row: RefRow) -> Self {
        DatasetRef {
            name: row.name,
            typ: row.typ.into(),
            snapshot_id: row.snapshot_id.map(Into::into),
            protected: row.protected,
        }
    }
}

async fn fetch_ref(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DatasetRef>, CatalogBackendError> {
    let row = sqlx::query_as!(
        RefRow,
        r#"
        SELECT name, typ as "typ: DbDatasetRefType", snapshot_id, protected
        FROM dataset_ref
        WHERE warehouse_id = $1 AND dataset_id = $2 AND name = $3
        "#,
        *warehouse_id,
        *dataset_id,
        name,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(row.map(Into::into))
}

/// Walk the parent chain from `from` looking for `ancestor`.
async fn is_descendant_of(
    warehouse_id: WarehouseId,
    from: DatasetSnapshotId,
    ancestor: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool, CatalogBackendError> {
    let found = sqlx::query_scalar!(
        r#"
        WITH RECURSIVE ancestry(snapshot_id, parent_snapshot_id) AS (
            SELECT snapshot_id, parent_snapshot_id
            FROM dataset_snapshot
            WHERE warehouse_id = $1 AND snapshot_id = $2
          UNION ALL
            SELECT s.snapshot_id, s.parent_snapshot_id
            FROM dataset_snapshot s
            INNER JOIN ancestry a ON s.snapshot_id = a.parent_snapshot_id
            WHERE s.warehouse_id = $1
        )
        SELECT EXISTS (SELECT 1 FROM ancestry WHERE snapshot_id = $3) as "found!"
        "#,
        *warehouse_id,
        *from,
        *ancestor,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(found)
}

/// How many snapshots may accumulate past a checkpoint before one is due. Trades
/// the fold, which restates the whole file set, against the length of the ancestry
/// walk a read performs; `dataset_manifest_scale.rs` measures both.
const CHECKPOINT_INTERVAL: i32 = 20;

/// Seeds the advisory lock that [`lock_chain`] takes.
const CHAIN_LOCK_SEED: i64 = 0x6473_6368_6169_6E73;

/// Keep a purge apart from the work that reads a dataset's chain to rewrite it: a
/// checkpoint and a rebase. Each reads the ancestry in one statement and the rows
/// it names in another; a purge committing between the two would fold a snapshot
/// into a checkpoint and delete the rows below it, and the stale ancestry would
/// then read past the checkpoint to rows the purge cut off. A purge takes the lock
/// `exclusive`; the others share it, as they cut nothing. Taken first, before any
/// row lock, so it never waits while holding one.
async fn lock_chain(
    dataset_id: DatasetId,
    exclusive: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CatalogBackendError> {
    let statement = if exclusive {
        "SELECT pg_advisory_xact_lock(hashtextextended($1, $2))"
    } else {
        "SELECT pg_advisory_xact_lock_shared(hashtextextended($1, $2))"
    };
    sqlx::query(statement)
        .bind(dataset_id.to_string())
        .bind(CHAIN_LOCK_SEED)
        .execute(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;
    Ok(())
}

/// Distance from `snapshot_id` back to the nearest checkpoint, or to the root.
async fn depth_since_checkpoint(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i32, CommitDatasetError> {
    let depth = sqlx::query_scalar!(
        r#"
        WITH RECURSIVE ancestry(snapshot_id, parent_snapshot_id, depth, is_checkpoint) AS (
            SELECT snapshot_id, parent_snapshot_id, 0, is_checkpoint
            FROM dataset_snapshot
            WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
          UNION ALL
            SELECT s.snapshot_id, s.parent_snapshot_id, a.depth + 1, s.is_checkpoint
            FROM dataset_snapshot s
            INNER JOIN ancestry a ON s.snapshot_id = a.parent_snapshot_id
            WHERE s.warehouse_id = $1 AND NOT a.is_checkpoint
        )
        SELECT COALESCE(MAX(depth), 0) as "depth!" FROM ancestry
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(depth)
}

/// Materialise the full file set onto `snapshot_id` as `added` rows. Keys this
/// commit already wrote are left alone: they are newest.
async fn write_checkpoint(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    include_expired: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    let ancestry = snapshot_ancestry(
        warehouse_id,
        dataset_id,
        snapshot_id,
        include_expired,
        transaction,
    )
    .await?;
    // Flagged a checkpoint with no rows, the snapshot would read as empty.
    if ancestry.is_empty() {
        return Err(DatasetSnapshotNotFound::new().into());
    }
    sqlx::query!(
        r#"
        INSERT INTO dataset_manifest_entry
            (warehouse_id, snapshot_id, logical_key, physical_path, change,
             etag, size, content_type, checksum, version_id, last_modified)
        SELECT $1, $2, logical_key, physical_path, 'added',
               etag, size, content_type, checksum, version_id, last_modified
        FROM (
            SELECT DISTINCT ON (t.logical_key)
                t.logical_key, t.physical_path, t.change, t.etag, t.size,
                t.content_type, t.checksum, t.version_id, t.last_modified
            FROM unnest($3::uuid[]) WITH ORDINALITY AS a(snapshot_id, depth)
            CROSS JOIN LATERAL (
                SELECT m.logical_key, m.physical_path, m.change, m.etag, m.size,
                       m.content_type, m.checksum, m.version_id, m.last_modified,
                       a.depth AS depth
                FROM dataset_manifest_entry m
                WHERE m.warehouse_id = $1 AND m.snapshot_id = a.snapshot_id
            ) t
            ORDER BY t.logical_key, t.depth
        ) newest
        WHERE change <> 'removed'
        ON CONFLICT DO NOTHING
        "#,
        *warehouse_id,
        *snapshot_id,
        &ancestry,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(())
}

/// Fold a branch's current head into a checkpoint, if one is still warranted. The
/// head is resolved here: by the time the worker runs, the branch has usually moved.
pub(crate) async fn checkpoint_dataset_branch(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    branch: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DatasetSnapshotId>, CommitDatasetError> {
    lock_chain(dataset_id, false, transaction).await?;
    let Some(snapshot_id) = fetch_ref(warehouse_id, dataset_id, branch, transaction)
        .await?
        .and_then(|r| r.snapshot_id)
    else {
        return Ok(None);
    };

    let depth = depth_since_checkpoint(warehouse_id, dataset_id, snapshot_id, transaction).await?;
    if depth < CHECKPOINT_INTERVAL {
        return Ok(None);
    }

    write_checkpoint(warehouse_id, dataset_id, snapshot_id, false, transaction).await?;

    sqlx::query!(
        r#"UPDATE dataset_snapshot SET is_checkpoint = true
           WHERE warehouse_id = $1 AND snapshot_id = $2"#,
        *warehouse_id,
        *snapshot_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(Some(snapshot_id))
}

/// Open a snapshot for staging. No ref points at it until [`finish_dataset_commit`]
/// moves the pointer.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn begin_dataset_commit(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    branch: &str,
    snapshot_id: DatasetSnapshotId,
    parent_snapshot_id: Option<DatasetSnapshotId>,
    location: &str,
    summary: Option<serde_json::Value>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<chrono::DateTime<chrono::Utc>, CommitDatasetError> {
    // Checked here to fail a doomed commit before the caller does any work, and
    // again at finish: a ref can be protected, or replaced by a tag, while a long
    // scan is in flight.
    ensure_branch_writable(warehouse_id, dataset_id, branch, transaction).await?;

    let created = sqlx::query!(
        r#"
        INSERT INTO dataset_snapshot
            (warehouse_id, dataset_id, snapshot_id, parent_snapshot_id, location, status, summary)
        VALUES ($1, $2, $3, $4, $5, 'staging', $6)
        RETURNING created_at
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
        parent_snapshot_id.map(|id| *id),
        location,
        summary as Option<serde_json::Value>,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(created.created_at)
}

/// A branch that accepts commits: it exists, is not a tag, and is not protected.
async fn ensure_branch_writable(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    branch: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetRef, CommitDatasetError> {
    let existing = fetch_ref(warehouse_id, dataset_id, branch, transaction)
        .await?
        .ok_or_else(|| CommitDatasetError::from(DatasetRefNotFound::new()))?;
    if existing.typ == DatasetRefType::Tag {
        return Err(DatasetCommitToTag::new().into());
    }
    if existing.protected {
        return Err(DatasetRefProtected::new().into());
    }
    Ok(existing)
}

/// Append manifest rows to a staging snapshot. Constraints are checked per batch,
/// so under `reject` a violation stops the scan at the batch that holds it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stage_dataset_files(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    added: &[ManifestEntry],
    modified: &[ManifestEntry],
    removed: &[String],
    on_violation: ConstraintViolationPolicy,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<StagedBatch, CommitDatasetError> {
    // A published snapshot is immutable, and the inserts below key on the snapshot
    // alone. The lock holds a concurrent publish off until these rows are in.
    let staging = sqlx::query_scalar!(
        r#"
        SELECT true AS "staging!" FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
          AND status = 'staging'
        FOR SHARE
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    if staging.is_none() {
        return Err(DatasetSnapshotNotFound::new()
            .append_detail("No staging snapshot of this dataset to add files to")
            .into());
    }

    let mut skipped = Vec::new();
    let (added, modified, removed) = if added.is_empty() && modified.is_empty() {
        (
            Cow::Borrowed(added),
            Cow::Borrowed(modified),
            Cow::Borrowed(removed),
        )
    } else {
        let constraints = fetch_constraints(warehouse_id, dataset_id, transaction).await?;
        match on_violation {
            ConstraintViolationPolicy::Reject => {
                constraints.validate(added)?;
                constraints.validate(modified)?;
                (
                    Cow::Borrowed(added),
                    Cow::Borrowed(modified),
                    Cow::Borrowed(removed),
                )
            }
            ConstraintViolationPolicy::Skip => {
                let (added, skipped_added) = constraints.partition(added);
                let (modified, skipped_modified) = constraints.partition(modified);
                // A key whose new bytes are refused is removed: kept, its entry
                // would describe bytes storage has replaced.
                let mut removed = removed.to_vec();
                removed.extend(skipped_modified.iter().map(|f| f.logical_key.clone()));
                skipped.extend(skipped_added);
                skipped.extend(skipped_modified);
                (Cow::Owned(added), Cow::Owned(modified), Cow::Owned(removed))
            }
        }
    };

    insert_manifest_rows(
        warehouse_id,
        snapshot_id,
        &added,
        DatasetManifestChange::Added,
        transaction,
    )
    .await?;
    insert_manifest_rows(
        warehouse_id,
        snapshot_id,
        &modified,
        DatasetManifestChange::Modified,
        transaction,
    )
    .await?;

    if !removed.is_empty() {
        let removed_keys: Vec<&str> = removed.iter().map(String::as_str).collect();
        sqlx::query!(
            r#"
            INSERT INTO dataset_manifest_entry
                (warehouse_id, snapshot_id, logical_key, physical_path, change)
            SELECT $1, $2, k, k, 'removed'
            FROM UNNEST($3::text[]) AS t(k)
            ON CONFLICT DO NOTHING
            "#,
            *warehouse_id,
            *snapshot_id,
            &removed_keys as &[&str],
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    }

    let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    Ok(StagedBatch {
        staged: StagedChanges {
            added: count(added.len()),
            modified: count(modified.len()),
            removed: count(removed.len()),
        },
        skipped,
    })
}

/// The add/modify insert; one statement serves both, with the change kind bound.
async fn insert_manifest_rows(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    entries: &[ManifestEntry],
    change: DatasetManifestChange,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    if entries.is_empty() {
        return Ok(());
    }

    let logical_keys: Vec<&str> = entries.iter().map(|f| f.logical_key.as_str()).collect();
    let physical_paths: Vec<&str> = entries.iter().map(|f| f.physical_path.as_str()).collect();
    let etags: Vec<Option<String>> = entries.iter().map(|f| f.etag.clone()).collect();
    let sizes: Vec<Option<i64>> = entries.iter().map(|f| f.size).collect();
    let content_types: Vec<Option<String>> =
        entries.iter().map(|f| f.content_type.clone()).collect();
    let checksums: Vec<Option<String>> = entries.iter().map(|f| f.checksum.clone()).collect();
    let version_ids: Vec<Option<String>> = entries.iter().map(|f| f.version_id.clone()).collect();
    let last_modified: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        entries.iter().map(|f| f.last_modified).collect();

    sqlx::query!(
        r#"
        INSERT INTO dataset_manifest_entry
            (warehouse_id, snapshot_id, logical_key, physical_path, change,
             etag, size, content_type, checksum, version_id, last_modified)
        SELECT $1, $2, k, p, $11::dataset_manifest_change,
               e, s, c, ck, v, lm
        FROM UNNEST(
            $3::text[], $4::text[], $5::text[], $6::bigint[],
            $7::text[], $8::text[], $9::text[], $10::timestamptz[]
        ) AS t(k, p, e, s, c, ck, v, lm)
        ON CONFLICT DO NOTHING
        "#,
        *warehouse_id,
        *snapshot_id,
        &logical_keys as &[&str],
        &physical_paths as &[&str],
        &etags as &[Option<String>],
        &sizes as &[Option<i64>],
        &content_types as &[Option<String>],
        &checksums as &[Option<String>],
        &version_ids as &[Option<String>],
        &last_modified as &[Option<chrono::DateTime<chrono::Utc>>],
        change as DatasetManifestChange,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(())
}

/// Make a staged snapshot the branch head. The only step that races: losing costs
/// the pointer move alone, and the rows can be finished once rebased.
pub(crate) async fn finish_dataset_commit(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    branch: &str,
    snapshot_id: DatasetSnapshotId,
    expected_snapshot_id: Option<DatasetSnapshotId>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool, CommitDatasetError> {
    // Re-checked: the ref may have been protected or replaced while staging ran.
    ensure_branch_writable(warehouse_id, dataset_id, branch, transaction).await?;

    let moved = sqlx::query!(
        r#"
        UPDATE dataset_ref
        SET snapshot_id = $4
        WHERE warehouse_id = $1
          AND dataset_id = $2
          AND name = $3
          AND snapshot_id IS NOT DISTINCT FROM $5
          -- Re-checked in the statement: the ref can be protected after the check
          -- above read it.
          AND typ = 'branch' AND NOT protected
          -- The snapshot must be staged on the expected head, or the branch would
          -- skip every commit between the two.
          AND EXISTS (
              SELECT 1 FROM dataset_snapshot s
              WHERE s.warehouse_id = $1 AND s.dataset_id = $2 AND s.snapshot_id = $4
                AND s.status = 'staging'
                AND s.parent_snapshot_id IS NOT DISTINCT FROM $5
          )
        "#,
        *warehouse_id,
        *dataset_id,
        branch,
        *snapshot_id,
        expected_snapshot_id.map(|id| *id),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    if moved.rows_affected() == 0 {
        let current = fetch_ref(warehouse_id, dataset_id, branch, transaction).await?;
        return Err(match current {
            Some(r) if r.typ == DatasetRefType::Tag => DatasetCommitToTag::new().into(),
            Some(r) if r.protected => DatasetRefProtected::new().into(),
            // The pointer matched, so the snapshot was the problem. Not a conflict:
            // a rebase and retry would fail the same way.
            Some(r) if r.snapshot_id == expected_snapshot_id => DatasetSnapshotNotFound::new()
                .append_detail("No staged snapshot on the expected parent")
                .into(),
            current => DatasetCommitConflict::new(current.and_then(|r| r.snapshot_id)).into(),
        });
    }

    // Measured in snapshots, not rows: chain length is what bounds reconstruction.
    // The caller enqueues the fold; a late checkpoint is never wrong.
    let depth = depth_since_checkpoint(warehouse_id, dataset_id, snapshot_id, transaction).await?;

    // A snapshot dates from its publish, as a commit's does: an import that staged
    // it over hours published none of it before.
    sqlx::query!(
        r#"UPDATE dataset_snapshot SET status = 'active', created_at = now()
           WHERE warehouse_id = $1 AND snapshot_id = $2"#,
        *warehouse_id,
        *snapshot_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(depth >= CHECKPOINT_INTERVAL)
}

/// Move a staging snapshot onto `new_parent` after a lost pointer move, dropping
/// every staged row for a key whose entry differs between the old parent's
/// manifest and the new one's: those commits are newer than the rows. Refuses a
/// `new_parent` that does not descend from the old one, and an active snapshot.
pub(crate) async fn rebase_staging_snapshot(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    new_parent: Option<DatasetSnapshotId>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<StagedChanges, CommitDatasetError> {
    lock_chain(dataset_id, false, transaction)
        .await
        .map_err(CommitDatasetError::from)?;
    let old_parent = sqlx::query_scalar!(
        r#"
        SELECT parent_snapshot_id FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
          AND status = 'staging'
        FOR UPDATE
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| CommitDatasetError::from(DatasetSnapshotNotFound::new()))?;

    // Only commits that land on top carry over: after a reset the staged rows were
    // judged against history the branch has left.
    if let Some(old_parent) = old_parent.map(DatasetSnapshotId::from) {
        let on_top = match new_parent {
            Some(new_parent) => is_descendant_of(warehouse_id, new_parent, old_parent, transaction)
                .await
                .map_err(CommitDatasetError::from)?,
            None => false,
        };
        if !on_top {
            return Err(DatasetCommitConflict::new(new_parent).into());
        }
    }

    // Either parent may have expired since it was read: the old one since the
    // staging snapshot was built on it, the new one since a lost pointer move named
    // it. An expired snapshot keeps its rows until purged, and purge keeps them
    // while anything stages on top, so both chains still read.
    let mut chains = [Vec::new(), Vec::new()];
    for (chain, parent) in chains
        .iter_mut()
        .zip([old_parent.map(DatasetSnapshotId::from), new_parent])
    {
        if let Some(parent) = parent {
            *chain = snapshot_ancestry(warehouse_id, dataset_id, parent, true, transaction)
                .await
                .map_err(CommitDatasetError::from)?;
        }
    }
    let [old_chain, new_chain] = chains;

    // Each side resolves a staged key's entry the way the file listing does: the
    // newest row along its chain, a removal meaning absent.
    let dropped = sqlx::query_scalar!(
        r#"
        WITH staged AS (
            SELECT logical_key FROM dataset_manifest_entry
            WHERE warehouse_id = $1 AND snapshot_id = $2
        ),
        before AS (
            SELECT DISTINCT ON (m.logical_key)
                m.logical_key, m.change, m.physical_path, m.etag, m.size,
                m.content_type, m.checksum, m.version_id, m.last_modified
            FROM unnest($3::uuid[]) WITH ORDINALITY AS a(snapshot_id, depth)
            INNER JOIN dataset_manifest_entry m
                ON m.warehouse_id = $1 AND m.snapshot_id = a.snapshot_id
            INNER JOIN staged k ON k.logical_key = m.logical_key
            ORDER BY m.logical_key, a.depth
        ),
        after AS (
            SELECT DISTINCT ON (m.logical_key)
                m.logical_key, m.change, m.physical_path, m.etag, m.size,
                m.content_type, m.checksum, m.version_id, m.last_modified
            FROM unnest($4::uuid[]) WITH ORDINALITY AS a(snapshot_id, depth)
            INNER JOIN dataset_manifest_entry m
                ON m.warehouse_id = $1 AND m.snapshot_id = a.snapshot_id
            INNER JOIN staged k ON k.logical_key = m.logical_key
            ORDER BY m.logical_key, a.depth
        ),
        changed AS (
            SELECT k.logical_key
            FROM staged k
            LEFT JOIN before b ON b.logical_key = k.logical_key AND b.change <> 'removed'
            LEFT JOIN after f ON f.logical_key = k.logical_key AND f.change <> 'removed'
            WHERE (b.physical_path, b.etag, b.size, b.content_type, b.checksum,
                   b.version_id, b.last_modified)
                IS DISTINCT FROM
                  (f.physical_path, f.etag, f.size, f.content_type, f.checksum,
                   f.version_id, f.last_modified)
        )
        DELETE FROM dataset_manifest_entry d
        USING changed c
        WHERE d.warehouse_id = $1 AND d.snapshot_id = $2 AND d.logical_key = c.logical_key
        RETURNING d.change AS "change!: DatasetManifestChange"
        "#,
        *warehouse_id,
        *snapshot_id,
        &old_chain,
        &new_chain,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    sqlx::query!(
        r#"
        UPDATE dataset_snapshot
        SET parent_snapshot_id = $4
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
          AND status = 'staging'
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
        new_parent.map(|id| *id),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    let mut changes = StagedChanges::default();
    for change in dropped {
        match change {
            DatasetManifestChange::Added => changes.added += 1,
            DatasetManifestChange::Modified => changes.modified += 1,
            DatasetManifestChange::Removed => changes.removed += 1,
        }
    }
    Ok(changes)
}

/// Delete staging snapshots older than `older_than`; rows go by cascade. The age
/// bound keeps this from deleting a snapshot an import is still filling.
pub(crate) async fn expire_staging_snapshots(
    warehouse_id: WarehouseId,
    dataset_id: Option<DatasetId>,
    older_than: chrono::DateTime<chrono::Utc>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<u64, CommitDatasetError> {
    let deleted = sqlx::query!(
        r#"
        DELETE FROM dataset_snapshot
        WHERE warehouse_id = $1
          AND ($2::uuid IS NULL OR dataset_id = $2)
          AND status = 'staging'
          AND created_at < $3
          -- Defensive: a ref should never point at a staging snapshot, but
          -- deleting one that something resolves through would lose history.
          AND NOT EXISTS (
              SELECT 1 FROM dataset_ref r
              WHERE r.warehouse_id = dataset_snapshot.warehouse_id
                AND r.snapshot_id = dataset_snapshot.snapshot_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM dataset_snapshot child
              WHERE child.warehouse_id = dataset_snapshot.warehouse_id
                AND child.parent_snapshot_id = dataset_snapshot.snapshot_id
          )
        "#,
        *warehouse_id,
        dataset_id.map(|id| *id) as Option<Uuid>,
        older_than,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    // A published import clears its spool itself; one that died after publishing
    // left it behind.
    sqlx::query!(
        r#"
        DELETE FROM dataset_import_listing l
        USING dataset_snapshot s
        WHERE l.warehouse_id = $1
          AND s.warehouse_id = l.warehouse_id AND s.snapshot_id = l.snapshot_id
          AND ($2::uuid IS NULL OR s.dataset_id = $2)
          AND s.status <> 'staging'
          AND s.created_at < $3
        "#,
        *warehouse_id,
        dataset_id.map(|id| *id) as Option<Uuid>,
        older_than,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(deleted.rows_affected())
}

pub(crate) async fn list_dataset_snapshot_graph(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<DatasetSnapshotNode>, CommitDatasetError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            s.snapshot_id,
            s.parent_snapshot_id,
            s.created_at,
            s.status = 'expired' AS "expired!",
            (
                SELECT max(g.expires_at) FROM dataset_access_grant g
                WHERE g.warehouse_id = s.warehouse_id AND g.snapshot_id = s.snapshot_id
                  AND g.revoked_at IS NULL AND g.expires_at > now()
            ) AS pinned_until
        FROM dataset_snapshot s
        WHERE s.warehouse_id = $1 AND s.dataset_id = $2 AND s.status <> 'staging'
        "#,
        *warehouse_id,
        *dataset_id,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(rows
        .into_iter()
        .map(|r| DatasetSnapshotNode {
            snapshot_id: r.snapshot_id.into(),
            parent_snapshot_id: r.parent_snapshot_id.map(Into::into),
            created_at: r.created_at,
            expired: r.expired,
            pinned_until: r.pinned_until,
        })
        .collect())
}

pub(crate) async fn expire_dataset_snapshots(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_ids: &[DatasetSnapshotId],
    purge_after: chrono::DateTime<chrono::Utc>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<u64, CommitDatasetError> {
    let ids: Vec<Uuid> = snapshot_ids.iter().map(|id| **id).collect();
    // Waits out any ref or grant being pointed at a candidate, and holds off new
    // ones; see `lock_active_snapshot`. In id order, so two expiries cannot
    // deadlock.
    sqlx::query!(
        r#"
        SELECT snapshot_id FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = ANY($3)
          AND status = 'active'
        ORDER BY snapshot_id
        FOR NO KEY UPDATE
        "#,
        *warehouse_id,
        *dataset_id,
        &ids,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    let expired = sqlx::query!(
        r#"
        UPDATE dataset_snapshot s
        SET status = 'expired', expired_at = now(), purge_after = $4
        WHERE s.warehouse_id = $1 AND s.dataset_id = $2 AND s.snapshot_id = ANY($3)
          AND s.status = 'active'
          -- Checked after the lock above, in a statement of its own, so it sees a
          -- ref moved onto it, or a grant made on it, since the plan was made.
          AND NOT EXISTS (
              SELECT 1 FROM dataset_ref r
              WHERE r.warehouse_id = s.warehouse_id AND r.snapshot_id = s.snapshot_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM dataset_access_grant g
              WHERE g.warehouse_id = s.warehouse_id AND g.snapshot_id = s.snapshot_id
                AND g.revoked_at IS NULL AND g.expires_at > now()
          )
        "#,
        *warehouse_id,
        *dataset_id,
        &ids,
        purge_after,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(expired.rows_affected())
}

pub(crate) async fn purge_expired_dataset_snapshots(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetPurge, CommitDatasetError> {
    lock_chain(dataset_id, true, transaction).await?;
    // Due, and held by nothing: no ref, no live grant, and no import staging into
    // the dataset, whose rebase tells commits landed on top from a reset by
    // walking the chain a purge cuts. Locked so a restore waits for the purge.
    let due = sqlx::query_scalar!(
        r#"
        SELECT s.snapshot_id FROM dataset_snapshot s
        WHERE s.warehouse_id = $1 AND s.dataset_id = $2
          AND s.status = 'expired' AND s.purge_after <= now()
          AND NOT EXISTS (
              SELECT 1 FROM dataset_ref r
              WHERE r.warehouse_id = s.warehouse_id AND r.snapshot_id = s.snapshot_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM dataset_access_grant g
              WHERE g.warehouse_id = s.warehouse_id AND g.snapshot_id = s.snapshot_id
                AND g.revoked_at IS NULL AND g.expires_at > now()
          )
          AND NOT EXISTS (
              SELECT 1 FROM dataset_snapshot c
              WHERE c.warehouse_id = s.warehouse_id AND c.dataset_id = s.dataset_id
                AND c.status = 'staging'
          )
        FOR UPDATE OF s
        "#,
        *warehouse_id,
        *dataset_id,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    if !due.is_empty() {
        // A survivor whose parent goes reconstructs through rows about to be
        // deleted. Fold it into a checkpoint first, then cut it loose.
        let boundaries = sqlx::query!(
            r#"
            SELECT snapshot_id, is_checkpoint FROM dataset_snapshot
            WHERE warehouse_id = $1 AND parent_snapshot_id = ANY($2)
              AND NOT (snapshot_id = ANY($2))
            "#,
            *warehouse_id,
            &due,
        )
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
        for boundary in boundaries.iter().filter(|b| !b.is_checkpoint) {
            write_checkpoint(
                warehouse_id,
                dataset_id,
                boundary.snapshot_id.into(),
                true,
                transaction,
            )
            .await?;
        }
        let boundary_ids: Vec<Uuid> = boundaries.iter().map(|b| b.snapshot_id).collect();
        sqlx::query!(
            r#"
            UPDATE dataset_snapshot SET is_checkpoint = true, parent_snapshot_id = NULL
            WHERE warehouse_id = $1 AND snapshot_id = ANY($2)
            "#,
            *warehouse_id,
            &boundary_ids,
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

        sqlx::query!(
            r#"DELETE FROM dataset_manifest_entry WHERE warehouse_id = $1 AND snapshot_id = ANY($2)"#,
            *warehouse_id,
            &due,
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
        sqlx::query!(
            r#"DELETE FROM dataset_snapshot WHERE warehouse_id = $1 AND snapshot_id = ANY($2)"#,
            *warehouse_id,
            &due,
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    }

    let next_purge_after = sqlx::query_scalar!(
        r#"
        SELECT min(purge_after) FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND status = 'expired'
        "#,
        *warehouse_id,
        *dataset_id,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(DatasetPurge {
        purged: u64::try_from(due.len()).unwrap_or(u64::MAX),
        next_purge_after,
    })
}

pub(crate) async fn expire_dataset_snapshot(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    purge_after: chrono::DateTime<chrono::Utc>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<chrono::DateTime<chrono::Utc>, ExpireDatasetSnapshotError> {
    // Waits out any ref or grant being pointed at it, and holds off new ones; see
    // `lock_active_snapshot`.
    let current = sqlx::query!(
        r#"
        SELECT status::text AS "status!", purge_after
        FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
        FOR NO KEY UPDATE
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| ExpireDatasetSnapshotError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| ExpireDatasetSnapshotError::from(DatasetSnapshotNotFound::new()))?;
    match (current.status.as_str(), current.purge_after) {
        ("expired", Some(already)) => return Ok(already),
        ("active", _) => {}
        // Staging: half-written, not a snapshot yet.
        _ => return Err(DatasetSnapshotNotFound::new().into()),
    }
    // A statement of its own, after the lock: it sees what the lock waited for.
    let held = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM dataset_ref
            WHERE warehouse_id = $1 AND snapshot_id = $2
        ) OR EXISTS (
            SELECT 1 FROM dataset_access_grant
            WHERE warehouse_id = $1 AND snapshot_id = $2
              AND revoked_at IS NULL AND expires_at > now()
        ) AS "held!"
        "#,
        *warehouse_id,
        *snapshot_id,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| ExpireDatasetSnapshotError::from(e.into_catalog_backend_error()))?;
    if held {
        return Err(DatasetSnapshotHeld::new().into());
    }
    // The stored value, at the database's precision, so a retry answers the same.
    sqlx::query_scalar!(
        r#"
        UPDATE dataset_snapshot
        SET status = 'expired', expired_at = now(), purge_after = $4
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
        RETURNING purge_after AS "purge_after!"
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
        purge_after,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| ExpireDatasetSnapshotError::from(e.into_catalog_backend_error()))
}

pub(crate) async fn restore_dataset_snapshot(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetSnapshot, RestoreDatasetSnapshotError> {
    sqlx::query_as!(
        SnapshotRow,
        r#"
        UPDATE dataset_snapshot
        SET status = 'active', expired_at = NULL, purge_after = NULL
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
          AND status = 'expired'
        RETURNING snapshot_id, parent_snapshot_id, location, is_checkpoint, summary, created_at
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| RestoreDatasetSnapshotError::from(e.into_catalog_backend_error()))?
    .map(Into::into)
    .ok_or_else(|| RestoreDatasetSnapshotError::from(DatasetSnapshotNotFound::new()))
}

pub(crate) async fn get_dataset_snapshot_by_idempotency_key(
    warehouse_id: WarehouseId,
    key: IdempotencyKey,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<RecordedCommit>, CommitDatasetError> {
    let Some(row) = sqlx::query!(
        r#"
        SELECT dataset_id, snapshot_id, parent_snapshot_id, location, is_checkpoint, summary,
               created_at, skipped_files
        FROM dataset_snapshot
        WHERE warehouse_id = $1 AND idempotency_key = $2 AND status <> 'staging'
        "#,
        *warehouse_id,
        key.as_uuid(),
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?
    else {
        return Ok(None);
    };
    let skipped = row
        .skipped_files
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| CommitDatasetError::from(CatalogBackendError::new_unexpected(e)))?
        .unwrap_or_default();
    Ok(Some(RecordedCommit {
        dataset_id: row.dataset_id.into(),
        snapshot: SnapshotRow {
            snapshot_id: row.snapshot_id,
            parent_snapshot_id: row.parent_snapshot_id,
            location: row.location,
            is_checkpoint: row.is_checkpoint,
            summary: row.summary,
            created_at: row.created_at,
        }
        .into(),
        skipped,
    }))
}

/// Discard one staging snapshot and its rows; a published one is never matched.
pub(crate) async fn abort_dataset_commit(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    sqlx::query!(
        r#"
        DELETE FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
          AND status = 'staging'
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(())
}

/// Stage and finish in one transaction: the whole-delta-in-memory path.
pub(crate) async fn commit_dataset(
    commit: DatasetCommit,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetCommitOutcome, CommitDatasetError> {
    let DatasetCommit {
        warehouse_id,
        dataset_id,
        branch,
        expected_snapshot_id,
        snapshot_id,
        location,
        added,
        modified,
        removed,
        summary,
        idempotency_key,
        on_constraint_violation,
    } = commit;

    // A parent other than the head fails the pointer move below; one that does not
    // exist, as after a purge, would first fail the insert, on its foreign key.
    let head = ensure_branch_writable(warehouse_id, dataset_id, &branch, transaction)
        .await?
        .snapshot_id;
    if head != expected_snapshot_id {
        return Err(DatasetCommitConflict::new(head).into());
    }

    let created_at = begin_dataset_commit(
        warehouse_id,
        dataset_id,
        &branch,
        snapshot_id,
        expected_snapshot_id,
        &location,
        summary.clone(),
        transaction,
    )
    .await?;

    let staged = stage_dataset_files(
        warehouse_id,
        dataset_id,
        snapshot_id,
        &added,
        &modified,
        &removed,
        on_constraint_violation,
        transaction,
    )
    .await?;

    // A file named apart from where its bytes are: neither its key, as a commit
    // without a physical path records it, nor its key under the location. Over the
    // commit's own rows only, which the primary key indexes.
    sqlx::query!(
        r#"
        UPDATE dataset SET has_renamed_files = true
        WHERE warehouse_id = $1 AND dataset_id = $2 AND NOT has_renamed_files
          AND EXISTS (
              SELECT 1 FROM dataset_manifest_entry m
              WHERE m.warehouse_id = $1 AND m.snapshot_id = $3 AND m.change <> 'removed'
                AND m.physical_path <> m.logical_key
                AND m.physical_path <> $4 || m.logical_key
          )
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
        format!("{}/", location.trim_end_matches('/')),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    let checkpoint_due = finish_dataset_commit(
        warehouse_id,
        dataset_id,
        &branch,
        snapshot_id,
        expected_snapshot_id,
        transaction,
    )
    .await?;

    if let Some(key) = idempotency_key {
        let skipped = (!staged.skipped.is_empty())
            .then(|| serde_json::to_value(&staged.skipped))
            .transpose()
            .map_err(|e| CommitDatasetError::from(CatalogBackendError::new_unexpected(e)))?;
        sqlx::query!(
            r#"
            UPDATE dataset_snapshot SET idempotency_key = $3, skipped_files = $4
            WHERE warehouse_id = $1 AND snapshot_id = $2
            "#,
            *warehouse_id,
            *snapshot_id,
            key.as_uuid(),
            skipped as Option<serde_json::Value>,
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    }

    Ok(DatasetCommitOutcome {
        snapshot: DatasetSnapshot {
            snapshot_id,
            parent_snapshot_id: expected_snapshot_id,
            location,
            // A commit never produces a checkpoint; the queued worker flips this on
            // the snapshot it folds.
            is_checkpoint: false,
            summary,
            created_at,
        },
        checkpoint_due,
        staged,
    })
}

pub(crate) async fn list_dataset_refs(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<DatasetRef>, ListDatasetRefsError> {
    let rows = sqlx::query_as!(
        RefRow,
        r#"
        SELECT name, typ as "typ: DbDatasetRefType", snapshot_id, protected
        FROM dataset_ref
        WHERE warehouse_id = $1 AND dataset_id = $2
        ORDER BY name ASC
        "#,
        *warehouse_id,
        *dataset_id,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListDatasetRefsError::from(e.into_catalog_backend_error()))?;

    Ok(rows.into_iter().map(Into::into).collect())
}

pub(crate) async fn get_dataset_ref(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetRef, ListDatasetRefsError> {
    fetch_ref(warehouse_id, dataset_id, name, transaction)
        .await
        .map_err(ListDatasetRefsError::from)?
        .ok_or_else(|| ListDatasetRefsError::from(DatasetRefNotFound::new()))
}

/// Whether `snapshot_id` is `active`, locked `FOR SHARE` until the transaction ends
/// so a ref or grant can be pointed at it.
///
/// A staging row is a half-written manifest, and pointing a ref at it would expose
/// a torn file list. The lock is what keeps an expiry out: expiring takes
/// `FOR NO KEY UPDATE` on the snapshot before it looks for what holds it, so it
/// either waits for this transaction and then sees its ref or grant, or expires
/// first and this check, waiting on it, finds the snapshot expired.
pub(super) async fn lock_active_snapshot(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool, CatalogBackendError> {
    let active = sqlx::query_scalar!(
        r#"
        SELECT true AS "active!" FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
          AND status = 'active'
        FOR SHARE
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(active.is_some())
}

pub(crate) async fn create_dataset_ref(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    typ: DatasetRefType,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetRef, CreateDatasetRefError> {
    if !lock_active_snapshot(warehouse_id, dataset_id, snapshot_id, transaction)
        .await
        .map_err(CreateDatasetRefError::from)?
    {
        return Err(DatasetSnapshotNotFound::new().into());
    }

    let typ_db = DbDatasetRefType::from(typ);
    let inserted = sqlx::query!(
        r#"
        INSERT INTO dataset_ref (warehouse_id, dataset_id, name, typ, snapshot_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        "#,
        *warehouse_id,
        *dataset_id,
        name,
        typ_db as DbDatasetRefType,
        *snapshot_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CreateDatasetRefError::from(e.into_catalog_backend_error()))?;

    if inserted.rows_affected() == 0 {
        return Err(DatasetRefAlreadyExists::new().into());
    }

    Ok(DatasetRef {
        name: name.to_string(),
        typ,
        snapshot_id: Some(snapshot_id),
        protected: false,
    })
}

pub(crate) async fn set_dataset_ref_protection(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    protected: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetRef, SetDatasetRefProtectionError> {
    let row = sqlx::query_as!(
        RefRow,
        r#"
        UPDATE dataset_ref SET protected = $4
        WHERE warehouse_id = $1 AND dataset_id = $2 AND name = $3
        RETURNING name, typ as "typ: DbDatasetRefType", snapshot_id, protected
        "#,
        *warehouse_id,
        *dataset_id,
        name,
        protected,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| SetDatasetRefProtectionError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| SetDatasetRefProtectionError::from(DatasetRefNotFound::new()))?;

    Ok(row.into())
}

pub(crate) async fn delete_dataset_ref(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), DeleteDatasetRefError> {
    let existing = fetch_ref(warehouse_id, dataset_id, name, transaction)
        .await
        .map_err(DeleteDatasetRefError::from)?
        .ok_or_else(|| DeleteDatasetRefError::from(DatasetRefNotFound::new()))?;

    if existing.protected {
        return Err(DatasetRefProtected::new().into());
    }

    let deleted = sqlx::query!(
        r#"DELETE FROM dataset_ref
           WHERE warehouse_id = $1 AND dataset_id = $2 AND name = $3
             -- Re-checked in the statement: the ref can be protected after the check
             -- above read it.
             AND NOT protected"#,
        *warehouse_id,
        *dataset_id,
        name,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| DeleteDatasetRefError::from(e.into_catalog_backend_error()))?;

    if deleted.rows_affected() == 0 {
        let current = fetch_ref(warehouse_id, dataset_id, name, transaction)
            .await
            .map_err(DeleteDatasetRefError::from)?;
        return Err(match current {
            Some(r) if r.protected => DatasetRefProtected::new().into(),
            _ => DatasetRefNotFound::new().into(),
        });
    }

    Ok(())
}

pub(crate) async fn move_dataset_ref(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    snapshot_id: DatasetSnapshotId,
    expected_snapshot_id: Option<DatasetSnapshotId>,
    require_descendant: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetRef, MoveDatasetRefError> {
    let existing = fetch_ref(warehouse_id, dataset_id, name, transaction)
        .await
        .map_err(MoveDatasetRefError::from)?
        .ok_or_else(|| MoveDatasetRefError::from(DatasetRefNotFound::new()))?;

    if existing.typ == DatasetRefType::Tag {
        return Err(DatasetMoveTag::new().into());
    }
    // The checks below judge the head read here; one the caller did not expect
    // would let a fast-forward pass from a head the move then never replaces.
    if existing.snapshot_id != expected_snapshot_id {
        return Err(DatasetCommitConflict::new(existing.snapshot_id).into());
    }
    // A protected branch moves only forward.
    if existing.protected && !require_descendant {
        return Err(DatasetRefProtected::new().into());
    }
    if !lock_active_snapshot(warehouse_id, dataset_id, snapshot_id, transaction)
        .await
        .map_err(MoveDatasetRefError::from)?
    {
        return Err(DatasetSnapshotNotFound::new().into());
    }

    // Abandoning commits takes `reset`.
    if require_descendant
        && let Some(head) = existing.snapshot_id
        && !is_descendant_of(warehouse_id, snapshot_id, head, transaction)
            .await
            .map_err(MoveDatasetRefError::from)?
    {
        return Err(DatasetSnapshotNotADescendant::new().into());
    }

    let moved = sqlx::query_as!(
        RefRow,
        r#"
        UPDATE dataset_ref
        SET snapshot_id = $4
        WHERE warehouse_id = $1
          AND dataset_id = $2
          AND name = $3
          AND snapshot_id IS NOT DISTINCT FROM $5
          -- Re-checked in the statement: the ref can change after the checks above.
          AND typ = 'branch' AND (NOT protected OR $6)
        RETURNING name, typ as "typ: DbDatasetRefType", snapshot_id, protected
        "#,
        *warehouse_id,
        *dataset_id,
        name,
        *snapshot_id,
        expected_snapshot_id.map(|id| *id),
        require_descendant,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| MoveDatasetRefError::from(e.into_catalog_backend_error()))?;

    let Some(moved) = moved else {
        let current = fetch_ref(warehouse_id, dataset_id, name, transaction)
            .await
            .map_err(MoveDatasetRefError::from)?;
        return Err(match current {
            Some(r) if r.typ == DatasetRefType::Tag => DatasetMoveTag::new().into(),
            Some(r) if r.protected && !require_descendant => DatasetRefProtected::new().into(),
            current => DatasetCommitConflict::new(current.and_then(|r| r.snapshot_id)).into(),
        });
    };

    Ok(moved.into())
}

/// The snapshots whose rows make up `snapshot_id`'s manifest, newest first: the
/// snapshot itself back to the nearest checkpoint. Empty if `snapshot_id` is not a
/// published snapshot of this dataset. An expired snapshot counts only when
/// `include_expired`: readers never see one, while purge folds through it and a
/// rebase compares across it.
///
/// Resolved in its own query: Postgres will not push a snapshot_id equality through
/// a join against a recursive CTE, and planned the combined query as a seq scan
/// plus a full sort of the manifest. Bounded by the checkpoint interval, so the list
/// is small.
async fn snapshot_ancestry(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    include_expired: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<Uuid>, CatalogBackendError> {
    sqlx::query_scalar!(
        r#"
        WITH RECURSIVE ancestry(snapshot_id, parent_snapshot_id, depth, is_checkpoint) AS (
            SELECT snapshot_id, parent_snapshot_id, 0, is_checkpoint
            FROM dataset_snapshot
            -- A page token names its snapshot directly, so the anchor must refuse
            -- one still staging: its manifest is half-written.
            WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
              AND (status = 'active' OR ($4 AND status = 'expired'))
          UNION ALL
            SELECT s.snapshot_id, s.parent_snapshot_id, a.depth + 1, s.is_checkpoint
            FROM dataset_snapshot s
            INNER JOIN ancestry a ON s.snapshot_id = a.parent_snapshot_id
            WHERE s.warehouse_id = $1
              -- Recurse past a snapshot only when it is not a checkpoint: the
              -- checkpoint itself is included, everything older is unreachable.
              AND NOT a.is_checkpoint
        )
        SELECT snapshot_id as "snapshot_id!" FROM ancestry ORDER BY depth ASC
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
        include_expired,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| e.into_catalog_backend_error())
}

pub(crate) async fn list_dataset_files(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    ref_name: &str,
    content_type: Option<&str>,
    page_size: Option<i64>,
    page_token: Option<&str>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<
    (
        Option<DatasetSnapshotId>,
        Vec<ManifestEntry>,
        Option<String>,
    ),
    ListManifestEntriesError,
> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    // Later pages take the snapshot from the token, never re-reading the ref.
    let (snapshot_id, after_key) = match page_token {
        Some(token) => {
            let parsed = FilesPageToken::decode(token)?;
            if parsed.ref_name != ref_name {
                return Err(InvalidPaginationToken::new(
                    "Dataset files page token was issued for another ref",
                    token,
                )
                .into());
            }
            (parsed.snapshot_id, Some(parsed.after_key))
        }
        None => {
            let resolved = fetch_ref(warehouse_id, dataset_id, ref_name, transaction)
                .await?
                .ok_or_else(|| ListManifestEntriesError::from(DatasetRefNotFound::new()))?;
            // A branch with no commits resolves to no files, not a 404.
            let Some(snapshot_id) = resolved.snapshot_id else {
                return Ok((None, Vec::new(), None));
            };
            (snapshot_id, None)
        }
    };
    let (entries, last_key) = list_snapshot_files(
        warehouse_id,
        dataset_id,
        snapshot_id,
        None,
        after_key.as_deref(),
        page_size,
        false,
        transaction,
    )
    .await?;
    let next_page_token = last_key.map(|key| FilesPageToken::encode(snapshot_id, ref_name, &key));
    let entries = entries
        .into_iter()
        .filter(|e| match content_type {
            Some(wanted) => e.content_type.as_deref() == Some(wanted),
            None => true,
        })
        .collect();

    Ok((Some(snapshot_id), entries, next_page_token))
}

/// One page of `snapshot_id`'s files in key order: keys from `from_key` on, and
/// after `after_key`. Returns the files and, if the page was full, the last key it
/// scanned, to continue after.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn list_snapshot_files(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    from_key: Option<&str>,
    after_key: Option<&str>,
    page_size: i64,
    include_expired: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(Vec<ManifestEntry>, Option<String>), ListManifestEntriesError> {
    for _ in 0..CHAIN_READ_ATTEMPTS {
        let ancestry = snapshot_ancestry(
            warehouse_id,
            dataset_id,
            snapshot_id,
            include_expired,
            transaction,
        )
        .await
        .map_err(ListManifestEntriesError::from)?;
        if ancestry.is_empty() {
            return Err(DatasetSnapshotNotFound::new().into());
        }
        let page = list_chain_files(
            warehouse_id,
            &ancestry,
            from_key,
            after_key,
            page_size,
            transaction,
        )
        .await?;
        if chain_unchanged(
            warehouse_id,
            dataset_id,
            snapshot_id,
            include_expired,
            &ancestry,
            transaction,
        )
        .await?
        {
            return Ok(page);
        }
    }
    Err(chain_kept_changing().into())
}

/// How many times a read retries a chain a purge cut under it.
const CHAIN_READ_ATTEMPTS: usize = 3;

/// Whether `snapshot_id`'s chain is still `ancestry`. A reader reads the chain in
/// one statement and its rows in another; a purge committing between them folds a
/// snapshot into a checkpoint and deletes the rows below, so rows read against the
/// old chain may be ones the purge cut off. Read again after the rows, a changed
/// chain says to read once more. Readers take no chain lock: they run on replicas,
/// which the primary's purge does not wait for.
async fn chain_unchanged(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    include_expired: bool,
    ancestry: &[Uuid],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool, ListManifestEntriesError> {
    let now = snapshot_ancestry(
        warehouse_id,
        dataset_id,
        snapshot_id,
        include_expired,
        transaction,
    )
    .await
    .map_err(ListManifestEntriesError::from)?;
    Ok(now == ancestry)
}

fn chain_kept_changing() -> CatalogBackendError {
    CatalogBackendError::new_unexpected(ErrorModel::internal(
        "A snapshot's chain kept changing while its files were read",
        "DatasetChainChanged",
        None,
    ))
}

/// One page of the files `ancestry` resolves to, and the key to continue after.
async fn list_chain_files(
    warehouse_id: WarehouseId,
    ancestry: &[Uuid],
    from_key: Option<&str>,
    after_key: Option<&str>,
    page_size: i64,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(Vec<ManifestEntry>, Option<String>), ListManifestEntriesError> {
    // One ordered index range per ancestor, merged. Taking `page_size` keys from
    // each is enough for the global first `page_size`: a key with fewer than that
    // many below it overall has fewer below it in any single ancestor. The key
    // bounds may ride inside the fold, since DISTINCT ON groups by logical_key;
    // `change` may not — an older record can be live where the newest removed
    // it — so removals are dropped after it.
    let scanned = sqlx::query_as!(
        ManifestRow,
        r#"
        SELECT DISTINCT ON (t.logical_key)
            t.logical_key,
            t.physical_path,
            t.change AS "change: DatasetManifestChange",
            t.etag,
            t.size,
            t.content_type,
            t.checksum,
            t.version_id,
            t.last_modified
        FROM unnest($2::uuid[]) WITH ORDINALITY AS a(snapshot_id, depth)
        CROSS JOIN LATERAL (
            SELECT m.logical_key, m.physical_path, m.change, m.etag, m.size,
                   m.content_type, m.checksum, m.version_id, m.last_modified,
                   a.depth AS depth
            FROM dataset_manifest_entry m
            WHERE m.warehouse_id = $1
              AND m.snapshot_id = a.snapshot_id
              AND ($3::text IS NULL OR m.logical_key > $3)
              AND ($5::text IS NULL OR m.logical_key >= $5)
            ORDER BY m.logical_key ASC
            LIMIT $4
        ) t
        ORDER BY t.logical_key ASC, t.depth ASC
        LIMIT $4
        "#,
        *warehouse_id,
        ancestry,
        after_key,
        page_size,
        from_key,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListManifestEntriesError::from(e.into_catalog_backend_error()))?;

    // The continuation follows the scan, not the surviving rows: a page filtered
    // down to nothing must still continue, or the caller reads it as the end. Pages
    // may be short; only a missing continuation means the end.
    let last_key = (i64::try_from(scanned.len()).unwrap_or(i64::MAX) >= page_size)
        .then(|| scanned.last().map(|r| r.logical_key.clone()))
        .flatten();

    let entries = scanned
        .into_iter()
        .filter(|r| r.change != DatasetManifestChange::Removed)
        .map(Into::into)
        .collect();

    Ok((entries, last_key))
}

/// The live files `keys` name in `snapshot_id`, resolved as the file listing does.
pub(crate) async fn get_snapshot_files(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    keys: &[String],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<ManifestEntry>, ListManifestEntriesError> {
    for _ in 0..CHAIN_READ_ATTEMPTS {
        let ancestry = snapshot_ancestry(warehouse_id, dataset_id, snapshot_id, false, transaction)
            .await
            .map_err(ListManifestEntriesError::from)?;
        if ancestry.is_empty() {
            return Err(DatasetSnapshotNotFound::new().into());
        }
        let files = get_chain_files(warehouse_id, &ancestry, keys, transaction).await?;
        if chain_unchanged(
            warehouse_id,
            dataset_id,
            snapshot_id,
            false,
            &ancestry,
            transaction,
        )
        .await?
        {
            return Ok(files);
        }
    }
    Err(chain_kept_changing().into())
}

async fn get_chain_files(
    warehouse_id: WarehouseId,
    ancestry: &[Uuid],
    keys: &[String],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<ManifestEntry>, ListManifestEntriesError> {
    let rows = sqlx::query_as!(
        ManifestRow,
        r#"
        SELECT DISTINCT ON (t.logical_key)
            t.logical_key,
            t.physical_path,
            t.change AS "change: DatasetManifestChange",
            t.etag,
            t.size,
            t.content_type,
            t.checksum,
            t.version_id,
            t.last_modified
        FROM unnest($2::uuid[]) WITH ORDINALITY AS a(snapshot_id, depth)
        CROSS JOIN LATERAL (
            SELECT m.logical_key, m.physical_path, m.change, m.etag, m.size,
                   m.content_type, m.checksum, m.version_id, m.last_modified,
                   a.depth AS depth
            FROM dataset_manifest_entry m
            WHERE m.warehouse_id = $1
              AND m.snapshot_id = a.snapshot_id
              AND m.logical_key = ANY($3::text[])
        ) t
        ORDER BY t.logical_key ASC, t.depth ASC
        "#,
        *warehouse_id,
        ancestry,
        keys,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListManifestEntriesError::from(e.into_catalog_backend_error()))?;

    Ok(rows
        .into_iter()
        .filter(|r| r.change != DatasetManifestChange::Removed)
        .map(Into::into)
        .collect())
}

/// Record objects an import listed, keyed by its staging snapshot. A key listed
/// twice is kept once.
pub(crate) async fn spool_import_listing(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    objects: &[ListedObject],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    let keys: Vec<&str> = objects.iter().map(|o| o.logical_key.as_str()).collect();
    let paths: Vec<&str> = objects.iter().map(|o| o.physical_path.as_str()).collect();
    let sizes: Vec<Option<i64>> = objects.iter().map(|o| o.size).collect();
    let modified: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        objects.iter().map(|o| o.last_modified).collect();
    let etags: Vec<Option<&str>> = objects.iter().map(|o| o.etag.as_deref()).collect();
    let versions: Vec<Option<&str>> = objects.iter().map(|o| o.version_id.as_deref()).collect();
    sqlx::query!(
        r#"
        INSERT INTO dataset_import_listing
            (warehouse_id, snapshot_id, logical_key, physical_path, size, last_modified, etag,
             version_id)
        SELECT $1, $2, k, p, s, m, e, v
        FROM unnest($3::text[], $4::text[], $5::bigint[], $6::timestamptz[], $7::text[], $8::text[])
            AS t(k, p, s, m, e, v)
        ON CONFLICT DO NOTHING
        "#,
        *warehouse_id,
        *snapshot_id,
        &keys as &[&str],
        &paths as &[&str],
        &sizes as &[Option<i64>],
        &modified as &[Option<chrono::DateTime<chrono::Utc>>],
        &etags as &[Option<&str>],
        &versions as &[Option<&str>],
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    Ok(())
}

pub(crate) async fn read_import_listing(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    after: Option<&str>,
    limit: i64,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<ListedObject>, CommitDatasetError> {
    let objects = sqlx::query!(
        r#"
        SELECT logical_key, physical_path, size, last_modified, etag, version_id, referenced
        FROM dataset_import_listing
        WHERE warehouse_id = $1 AND snapshot_id = $2
          AND ($3::text IS NULL OR logical_key > $3::text COLLATE "C")
        ORDER BY logical_key ASC
        LIMIT $4
        "#,
        *warehouse_id,
        *snapshot_id,
        after,
        limit,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?
    .into_iter()
    .map(|r| ListedObject {
        logical_key: r.logical_key,
        physical_path: r.physical_path,
        size: r.size,
        last_modified: r.last_modified,
        etag: r.etag,
        version_id: r.version_id,
        referenced: r.referenced,
    })
    .collect();
    Ok(objects)
}

/// Mark the objects among `keys` the spool of `snapshot_id` holds as pointed at
/// by files named apart from their storage path.
pub(crate) async fn mark_import_objects_referenced(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    keys: &[String],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    sqlx::query!(
        r#"
        UPDATE dataset_import_listing SET referenced = true
        WHERE warehouse_id = $1 AND snapshot_id = $2 AND logical_key = ANY($3::text[])
        "#,
        *warehouse_id,
        *snapshot_id,
        &keys as &[&str],
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    Ok(())
}

pub(crate) async fn get_listed_import_objects(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    keys: &[String],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<ListedObject>, CommitDatasetError> {
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let objects = sqlx::query!(
        r#"
        SELECT logical_key, physical_path, size, last_modified, etag, version_id, referenced
        FROM dataset_import_listing
        WHERE warehouse_id = $1 AND snapshot_id = $2 AND logical_key = ANY($3::text[])
        "#,
        *warehouse_id,
        *snapshot_id,
        &keys as &[&str],
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?
    .into_iter()
    .map(|r| ListedObject {
        logical_key: r.logical_key,
        physical_path: r.physical_path,
        size: r.size,
        last_modified: r.last_modified,
        etag: r.etag,
        version_id: r.version_id,
        referenced: r.referenced,
    })
    .collect();
    Ok(objects)
}

pub(crate) async fn clear_import_listing(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    sqlx::query!(
        "DELETE FROM dataset_import_listing WHERE warehouse_id = $1 AND snapshot_id = $2",
        *warehouse_id,
        *snapshot_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    Ok(())
}

pub(crate) async fn record_snapshot_materialization(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    findings: &MaterializationFindings,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    let updated = sqlx::query!(
        r#"
        UPDATE dataset_snapshot
        SET missing_files = $4, changed_files = $5, materialization_checked_at = now()
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
        findings.missing_files,
        findings.changed_files,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    if updated.rows_affected() == 0 {
        return Err(DatasetSnapshotNotFound::new().into());
    }
    sqlx::query!(
        "DELETE FROM dataset_degraded_file WHERE warehouse_id = $1 AND snapshot_id = $2",
        *warehouse_id,
        *snapshot_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    let keys: Vec<&str> = findings
        .files
        .iter()
        .map(|f| f.logical_key.as_str())
        .collect();
    let paths: Vec<&str> = findings
        .files
        .iter()
        .map(|f| f.physical_path.as_str())
        .collect();
    let problems: Vec<DbDegradedFileProblem> =
        findings.files.iter().map(|f| f.problem.into()).collect();
    sqlx::query!(
        r#"
        INSERT INTO dataset_degraded_file
            (warehouse_id, snapshot_id, logical_key, physical_path, problem)
        SELECT $1, $2, k, p, q
        FROM unnest($3::text[], $4::text[], $5::dataset_degraded_file_problem[]) AS t(k, p, q)
        ON CONFLICT DO NOTHING
        "#,
        *warehouse_id,
        *snapshot_id,
        &keys as &[&str],
        &paths as &[&str],
        &problems as &[DbDegradedFileProblem],
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;
    Ok(())
}

pub(crate) async fn get_snapshot_materialization(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    page_token: Option<&str>,
    page_size: i64,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<SnapshotMaterialization>, ListManifestEntriesError> {
    let checked = sqlx::query!(
        r#"
        SELECT missing_files, changed_files, materialization_checked_at
        FROM dataset_snapshot
        WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3 AND status = 'active'
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| ListManifestEntriesError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| ListManifestEntriesError::from(DatasetSnapshotNotFound::new()))?;
    let (Some(missing_files), Some(changed_files), Some(checked_at)) = (
        checked.missing_files,
        checked.changed_files,
        checked.materialization_checked_at,
    ) else {
        return Ok(None);
    };

    let after = page_token
        .map(|token| {
            BASE64_URL_SAFE_NO_PAD
                .decode(token)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .ok_or_else(|| {
                    ListManifestEntriesError::from(InvalidPaginationToken::new(
                        "Invalid materialization page token",
                        token,
                    ))
                })
        })
        .transpose()?;
    let mut files: Vec<DegradedFile> = sqlx::query!(
        r#"
        SELECT logical_key, physical_path, problem AS "problem: DbDegradedFileProblem"
        FROM dataset_degraded_file
        WHERE warehouse_id = $1 AND snapshot_id = $2
          AND ($3::text IS NULL OR logical_key > $3::text COLLATE "C")
        ORDER BY logical_key ASC
        LIMIT $4
        "#,
        *warehouse_id,
        *snapshot_id,
        after,
        page_size + 1,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListManifestEntriesError::from(e.into_catalog_backend_error()))?
    .into_iter()
    .map(|r| DegradedFile {
        logical_key: r.logical_key,
        physical_path: r.physical_path,
        problem: r.problem.into(),
    })
    .collect();
    let next_page_token = if i64::try_from(files.len()).unwrap_or(i64::MAX) > page_size {
        files.truncate(usize::try_from(page_size).unwrap_or(usize::MAX));
        files
            .last()
            .map(|f| BASE64_URL_SAFE_NO_PAD.encode(f.logical_key.as_bytes()))
    } else {
        None
    };
    Ok(Some(SnapshotMaterialization {
        checked_at,
        missing_files,
        changed_files,
        files,
        next_page_token,
    }))
}

pub(crate) async fn get_snapshot_materialization_statuses(
    warehouse_id: WarehouseId,
    snapshot_ids: &[DatasetSnapshotId],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<SnapshotMaterializationStatus>, ListDatasetRefsError> {
    let ids: Vec<Uuid> = snapshot_ids.iter().map(|id| **id).collect();
    let rows = sqlx::query!(
        r#"
        SELECT snapshot_id, missing_files AS "missing_files!",
               changed_files AS "changed_files!",
               materialization_checked_at AS "checked_at!"
        FROM dataset_snapshot
        WHERE warehouse_id = $1 AND snapshot_id = ANY($2)
          AND missing_files IS NOT NULL AND changed_files IS NOT NULL
          AND materialization_checked_at IS NOT NULL
        "#,
        *warehouse_id,
        &ids,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListDatasetRefsError::from(e.into_catalog_backend_error()))?;
    Ok(rows
        .into_iter()
        .map(|r| SnapshotMaterializationStatus {
            snapshot_id: r.snapshot_id.into(),
            missing_files: r.missing_files,
            changed_files: r.changed_files,
            checked_at: r.checked_at,
        })
        .collect())
}
