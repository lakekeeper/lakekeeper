//! Dataset versioning: snapshots, refs, and manifest reconstruction.
//!
//! Concurrency is one conditional update: a commit stages a snapshot and its
//! manifest rows, then moves the branch pointer with a compare-and-swap against
//! the snapshot the caller expected.
use base64::Engine;
use lakekeeper::{
    CONFIG, WarehouseId,
    service::{
        CatalogBackendError, CommitDatasetError, CreateDatasetRefError, DatasetCommit,
        DatasetCommitConflict, DatasetCommitToTag, DatasetConstraints, DatasetId, DatasetNotFound,
        DatasetRef, DatasetRefAlreadyExists, DatasetRefNotFound, DatasetRefProtected,
        DatasetRefType, DatasetSnapshot, DatasetSnapshotId, DatasetSnapshotNotADescendant,
        DatasetSnapshotNotFound, DeleteDatasetRefError, InvalidPaginationToken,
        ListDatasetRefsError, ListManifestEntriesError, ManifestEntry, MoveDatasetRefError,
        SetDatasetRefProtectionError,
    },
};

use super::super::dbutils::DBErrorHandler;

/// The snapshot the first page resolved to, plus the last key returned.
///
/// A ref is a moving target, so the snapshot rides in the token: a commit landing
/// mid-pagination cannot tear the sequence. The key is last so `splitn(3, '&')`
/// keeps it intact when it contains `&`.
struct FilesPageToken {
    snapshot_id: DatasetSnapshotId,
    after_key: String,
}

impl FilesPageToken {
    fn encode(snapshot_id: DatasetSnapshotId, after_key: &str) -> String {
        let raw = format!("1&{snapshot_id}&{after_key}");
        base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(raw)
    }

    fn decode(token: &str) -> Result<Self, InvalidPaginationToken> {
        let decoded = base64::prelude::BASE64_URL_SAFE_NO_PAD
            .decode(token)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(|| {
                InvalidPaginationToken::new("Invalid dataset files page token encoding", token)
            })?;

        let parts = decoded.splitn(3, '&').collect::<Vec<_>>();
        match parts.as_slice() {
            ["1", snapshot, key] => Ok(Self {
                snapshot_id: snapshot
                    .parse::<uuid::Uuid>()
                    .map_err(|_| {
                        InvalidPaginationToken::new(
                            "Invalid dataset files page token snapshot",
                            token,
                        )
                    })?
                    .into(),
                after_key: (*key).to_string(),
            }),
            _ => Err(InvalidPaginationToken::new(
                "Invalid dataset files page token structure",
                token,
            )),
        }
    }
}

/// Read back only. Exists so the file listing drops removals in Rust, which lets
/// the paging bound stay inside the DISTINCT ON.
#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "dataset_manifest_change", rename_all = "lowercase")]
enum DatasetManifestChange {
    Added,
    Modified,
    Removed,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "dataset_ref_type", rename_all = "lowercase")]
enum DatasetRefTypeDb {
    Branch,
    Tag,
}

impl From<DatasetRefType> for DatasetRefTypeDb {
    fn from(typ: DatasetRefType) -> Self {
        match typ {
            DatasetRefType::Branch => DatasetRefTypeDb::Branch,
            DatasetRefType::Tag => DatasetRefTypeDb::Tag,
        }
    }
}

impl From<DatasetRefTypeDb> for DatasetRefType {
    fn from(typ: DatasetRefTypeDb) -> Self {
        match typ {
            DatasetRefTypeDb::Branch => DatasetRefType::Branch,
            DatasetRefTypeDb::Tag => DatasetRefType::Tag,
        }
    }
}

/// Read a dataset's declared constraints inside the current transaction.
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

/// Read a ref inside the current transaction.
async fn fetch_ref(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DatasetRef>, CatalogBackendError> {
    let row = sqlx::query!(
        r#"
        SELECT name, typ as "typ: DatasetRefTypeDb", snapshot_id, protected
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

    Ok(row.map(|r| DatasetRef {
        name: r.name,
        typ: r.typ.into(),
        snapshot_id: r.snapshot_id.map(Into::into),
        protected: r.protected,
    }))
}

/// Walk the parent chain from `from` looking for `ancestor`.
///
/// Fast-forward uses this to prove the target extends the branch rather than
/// abandoning commits on it.
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

/// How many snapshots may accumulate past a checkpoint before one is due.
///
/// Trades write amplification -- a checkpoint restates the whole file set --
/// against the length of the ancestry walk a read performs. Gate A measured the
/// read side as flat in manifest size and ~0.065ms per level, so read cost alone
/// would tolerate a much longer chain; the fold is the expensive half, at ~13s
/// per million files. Re-measure with `dataset_gate_a.rs`.
const CHECKPOINT_INTERVAL: i32 = 20;

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

/// Materialise the full file set onto `snapshot_id` as `added` rows.
///
/// Reconstruction stops at a checkpoint, so the snapshot then answers on its own
/// rows alone. Keys this commit already wrote are left alone: they are newest.
async fn write_checkpoint(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    sqlx::query!(
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
        ),
        newest AS (
            SELECT DISTINCT ON (m.logical_key)
                m.logical_key, m.physical_path, m.change, m.etag, m.size,
                m.content_type, m.checksum, m.version_id, m.last_modified
            FROM dataset_manifest_entry m
            INNER JOIN ancestry a ON a.snapshot_id = m.snapshot_id
            WHERE m.warehouse_id = $1
            ORDER BY m.logical_key, a.depth ASC
        )
        INSERT INTO dataset_manifest_entry
            (warehouse_id, snapshot_id, logical_key, physical_path, change,
             etag, size, content_type, checksum, version_id, last_modified)
        SELECT $1, $3, logical_key, physical_path, 'added',
               etag, size, content_type, checksum, version_id, last_modified
        FROM newest
        WHERE change <> 'removed'
        ON CONFLICT DO NOTHING
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

/// Fold a branch's current head into a checkpoint, if one is still warranted.
///
/// The head is resolved here rather than carried in the payload: by the time the
/// worker runs the branch has usually moved, and folding the snapshot that
/// tripped the threshold would spend the run on a chain nobody reads. The
/// threshold is re-checked, so a task that lost a race exits cheaply.
pub(crate) async fn checkpoint_dataset_branch(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    branch: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DatasetSnapshotId>, CommitDatasetError> {
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

    write_checkpoint(warehouse_id, dataset_id, snapshot_id, transaction).await?;

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

/// Open a snapshot for staging.
///
/// Referenced by no ref, so nothing observes it until [`finish_dataset_commit`]
/// moves the pointer -- which is what lets a caller write manifest rows across
/// many transactions.
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

/// Append manifest rows to a staging snapshot.
///
/// Safe to repeat in separate transactions. Constraints are checked per batch, so
/// a violation stops the scan rather than surfacing after millions of rows.
pub(crate) async fn stage_dataset_files(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    added: &[ManifestEntry],
    modified: &[ManifestEntry],
    removed: &[String],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    if !added.is_empty() || !modified.is_empty() {
        let constraints = fetch_constraints(warehouse_id, dataset_id, transaction).await?;
        constraints.validate(added)?;
        constraints.validate(modified)?;
    }

    insert_manifest_rows(warehouse_id, snapshot_id, added, "added", transaction).await?;
    insert_manifest_rows(warehouse_id, snapshot_id, modified, "modified", transaction).await?;

    // A removal is metadata-only: the object stays, older snapshots still
    // reference it.
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

    Ok(())
}

/// The add/modify insert; the change kind is bound rather than written twice.
async fn insert_manifest_rows(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    entries: &[ManifestEntry],
    change: &str,
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
        change as _,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(())
}

/// Make a staged snapshot the branch head.
///
/// The only step that races. Everything staged before it is invisible, so losing
/// costs the pointer move alone: the rows remain and can be finished against the
/// new head once rebased.
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
        let current = fetch_ref(warehouse_id, dataset_id, branch, transaction)
            .await?
            .and_then(|r| r.snapshot_id);
        return Err(DatasetCommitConflict::new(current).into());
    }

    // Measured in snapshots, not rows: chain length is what bounds reconstruction.
    // The fold itself is left to the caller to enqueue -- it costs ~13s per million
    // files, and a checkpoint only shortens a walk, so a late one is never wrong.
    let depth = depth_since_checkpoint(warehouse_id, dataset_id, snapshot_id, transaction).await?;

    sqlx::query!(
        r#"UPDATE dataset_snapshot SET status = 'active'
           WHERE warehouse_id = $1 AND snapshot_id = $2"#,
        *warehouse_id,
        *snapshot_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(depth >= CHECKPOINT_INTERVAL)
}

/// Point a staging snapshot at a different parent, to rebase after a lost
/// pointer move. Refuses an active snapshot: history something already resolves
/// through must not be re-parented under it.
pub(crate) async fn reparent_staging_snapshot(
    warehouse_id: WarehouseId,
    snapshot_id: DatasetSnapshotId,
    new_parent: Option<DatasetSnapshotId>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CommitDatasetError> {
    let updated = sqlx::query!(
        r#"
        UPDATE dataset_snapshot
        SET parent_snapshot_id = $3
        WHERE warehouse_id = $1 AND snapshot_id = $2 AND status = 'staging'
        "#,
        *warehouse_id,
        *snapshot_id,
        new_parent.map(|id| *id),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    if updated.rows_affected() == 0 {
        return Err(DatasetSnapshotNotFound::new().into());
    }
    Ok(())
}

/// Delete staging snapshots older than `older_than`; rows go by cascade.
///
/// A commit that dies between begin and finish leaves one behind. It is invisible
/// and never wrong, it just accumulates. The age bound is what keeps this from
/// deleting a snapshot an import is still filling.
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
        dataset_id.map(|id| *id) as Option<uuid::Uuid>,
        older_than,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CommitDatasetError::from(e.into_catalog_backend_error()))?;

    Ok(deleted.rows_affected())
}

/// Stage and finish in one transaction: the whole-delta-in-memory path.
pub(crate) async fn commit_dataset(
    commit: DatasetCommit,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetSnapshot, CommitDatasetError> {
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
    } = commit;

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

    stage_dataset_files(
        warehouse_id,
        dataset_id,
        snapshot_id,
        &added,
        &modified,
        &removed,
        transaction,
    )
    .await?;

    let checkpoint_due = finish_dataset_commit(
        warehouse_id,
        dataset_id,
        &branch,
        snapshot_id,
        expected_snapshot_id,
        transaction,
    )
    .await?;

    Ok(DatasetSnapshot {
        snapshot_id,
        parent_snapshot_id: expected_snapshot_id,
        location,
        // A commit never produces a checkpoint any more; the queued worker flips
        // this on the snapshot it folds.
        is_checkpoint: false,
        checkpoint_due,
        summary,
        created_at,
    })
}

pub(crate) async fn list_dataset_refs(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<DatasetRef>, ListDatasetRefsError> {
    let rows = sqlx::query!(
        r#"
        SELECT name, typ as "typ: DatasetRefTypeDb", snapshot_id, protected
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

    Ok(rows
        .into_iter()
        .map(|r| DatasetRef {
            name: r.name,
            typ: r.typ.into(),
            snapshot_id: r.snapshot_id.map(Into::into),
            protected: r.protected,
        })
        .collect())
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

/// A snapshot is addressable only once it is `active`: a staging row is a
/// half-written manifest, and pointing a ref at it would expose a torn file list.
async fn require_active_snapshot(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CatalogBackendError> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM dataset_snapshot
            WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
              AND status = 'active'
        ) as "found!"
        "#,
        *warehouse_id,
        *dataset_id,
        *snapshot_id,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    if exists {
        Ok(())
    } else {
        Err(CatalogBackendError::new_unexpected(
            DatasetSnapshotNotFound::new(),
        ))
    }
}

pub(crate) async fn create_dataset_ref(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    name: &str,
    typ: DatasetRefType,
    snapshot_id: DatasetSnapshotId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetRef, CreateDatasetRefError> {
    if require_active_snapshot(warehouse_id, dataset_id, snapshot_id, transaction)
        .await
        .is_err()
    {
        return Err(DatasetSnapshotNotFound::new().into());
    }

    let typ_db = DatasetRefTypeDb::from(typ);
    let inserted = sqlx::query!(
        r#"
        INSERT INTO dataset_ref (warehouse_id, dataset_id, name, typ, snapshot_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        "#,
        *warehouse_id,
        *dataset_id,
        name,
        typ_db as DatasetRefTypeDb,
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
    let row = sqlx::query!(
        r#"
        UPDATE dataset_ref SET protected = $4
        WHERE warehouse_id = $1 AND dataset_id = $2 AND name = $3
        RETURNING name, typ as "typ: DatasetRefTypeDb", snapshot_id, protected
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

    Ok(DatasetRef {
        name: row.name,
        typ: row.typ.into(),
        snapshot_id: row.snapshot_id.map(Into::into),
        protected: row.protected,
    })
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

    sqlx::query!(
        r#"DELETE FROM dataset_ref
           WHERE warehouse_id = $1 AND dataset_id = $2 AND name = $3"#,
        *warehouse_id,
        *dataset_id,
        name,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| DeleteDatasetRefError::from(e.into_catalog_backend_error()))?;

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

    if require_active_snapshot(warehouse_id, dataset_id, snapshot_id, transaction)
        .await
        .is_err()
    {
        return Err(DatasetSnapshotNotFound::new().into());
    }

    // Fast-forward only extends history; abandoning commits is what `reset` is
    // for, under its own permission.
    if require_descendant
        && let Some(head) = existing.snapshot_id
        && !is_descendant_of(warehouse_id, snapshot_id, head, transaction)
            .await
            .map_err(MoveDatasetRefError::from)?
    {
        return Err(DatasetSnapshotNotADescendant::new().into());
    }

    let moved = sqlx::query!(
        r#"
        UPDATE dataset_ref
        SET snapshot_id = $4
        WHERE warehouse_id = $1
          AND dataset_id = $2
          AND name = $3
          AND snapshot_id IS NOT DISTINCT FROM $5
        "#,
        *warehouse_id,
        *dataset_id,
        name,
        *snapshot_id,
        expected_snapshot_id.map(|id| *id),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| MoveDatasetRefError::from(e.into_catalog_backend_error()))?;

    if moved.rows_affected() == 0 {
        let current = fetch_ref(warehouse_id, dataset_id, name, transaction)
            .await
            .map_err(MoveDatasetRefError::from)?
            .and_then(|r| r.snapshot_id);
        return Err(DatasetCommitConflict::new(current).into());
    }

    Ok(DatasetRef {
        name: name.to_string(),
        typ: existing.typ,
        snapshot_id: Some(snapshot_id),
        protected: existing.protected,
    })
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
            (parsed.snapshot_id, Some(parsed.after_key))
        }
        None => {
            let resolved = fetch_ref(warehouse_id, dataset_id, ref_name, transaction)
                .await?
                .ok_or_else(|| ListManifestEntriesError::from(DatasetRefNotFound::new()))?;
            // A branch with no commits resolves to no files. The ref exists, so
            // this is not a 404: reporting one would make every caller special-case
            // a dataset nothing has been committed to yet.
            let Some(snapshot_id) = resolved.snapshot_id else {
                return Ok((None, Vec::new(), None));
            };
            (snapshot_id, None)
        }
    };
    let after_key = after_key.as_deref();

    // Resolved separately rather than joined in: Postgres will not push a
    // snapshot_id equality through a join against a recursive CTE, and planned the
    // combined query as a seq scan plus a full sort of the manifest. Bounded by the
    // checkpoint interval, so the list is small.
    let ancestry = sqlx::query_scalar!(
        r#"
        WITH RECURSIVE ancestry(snapshot_id, parent_snapshot_id, depth, is_checkpoint) AS (
            SELECT snapshot_id, parent_snapshot_id, 0, is_checkpoint
            FROM dataset_snapshot
            WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $3
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
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListManifestEntriesError::from(e.into_catalog_backend_error()))?;

    if ancestry.is_empty() {
        return Err(DatasetSnapshotNotFound::new().into());
    }

    // One ordered index range per ancestor, merged. Taking `page_size` keys from
    // each is enough for the global first `page_size`: a key with fewer than that
    // many below it overall has fewer below it in any single ancestor. The key
    // bound may ride inside the fold, since DISTINCT ON groups by logical_key;
    // `change` and content_type may not -- an older record can carry a
    // content_type the newest does not -- so they are applied after it.
    let scanned = sqlx::query!(
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
            ORDER BY m.logical_key ASC
            LIMIT $4
        ) t
        ORDER BY t.logical_key ASC, t.depth ASC
        LIMIT $4
        "#,
        *warehouse_id,
        &ancestry,
        after_key,
        page_size,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListManifestEntriesError::from(e.into_catalog_backend_error()))?;

    // The token follows the scan, not the surviving rows: a page filtered down to
    // nothing must still continue, or the caller reads it as the end. Pages may be
    // short; only a null token means the end.
    let next_page_token = (i64::try_from(scanned.len()).unwrap_or(i64::MAX) >= page_size)
        .then(|| scanned.last())
        .flatten()
        .map(|r| FilesPageToken::encode(snapshot_id, &r.logical_key));

    let rows = scanned
        .into_iter()
        .filter(|r| r.change != DatasetManifestChange::Removed)
        .filter(|r| match content_type {
            Some(wanted) => r.content_type.as_deref() == Some(wanted),
            None => true,
        })
        .collect::<Vec<_>>();

    let entries = rows
        .into_iter()
        .map(|r| ManifestEntry {
            logical_key: r.logical_key,
            physical_path: r.physical_path,
            etag: r.etag,
            size: r.size,
            content_type: r.content_type,
            checksum: r.checksum,
            version_id: r.version_id,
            last_modified: r.last_modified,
        })
        .collect();

    Ok((Some(snapshot_id), entries, next_page_token))
}
