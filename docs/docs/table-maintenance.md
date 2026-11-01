---
description: "How Lakekeeper maintains Iceberg tables: metadata file cleanup, delete-after-commit behaviour and retention of previous metadata versions."
---

# Table Maintenance

## Metadata File Cleanup

Lakekeeper honors the Iceberg table properties `write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max`. Starting with Lakekeeper v0.10.0, `delete-after-commit` is enabled by default (it was disabled in earlier versions). On each table commit, when `delete-after-commit` is enabled, Lakekeeper keeps the current table metadata file plus up to `write.metadata.previous-versions-max` previous metadata files (default: 100) and deletes the oldest tracked metadata file from the metadata log once that limit is exceeded. This cleanup applies only to metadata files tracked in the metadata log; it does not remove orphaned metadata files.

For example: if `write.metadata.previous-versions-max=20`, Lakekeeper retains 21 files in total (the current plus 20 previous); committing a 22nd version deletes the oldest tracked metadata file.

Link to [Expire Snapshots](#expire-snapshots)

## Expire Snapshots { #expire-snapshots .lkp }

Lakekeeper automatically expires old table snapshots based on configurable age and retention policies. This helps manage storage costs and performance by removing outdated snapshot metadata and associated data files.

Expire snapshots can be configured per warehouse and optionally overridden at the table level using Iceberg table properties.

### Configuration

Configuration can be set via the Management UI or REST API endpoints:

- **GET** `/management/v1/warehouse/{warehouse_id}/task-queue/expire_snapshots/config`
- **POST** `/management/v1/warehouse/{warehouse_id}/task-queue/expire_snapshots/config`

| Parameter                 | Type    | Default                          | Description |
|---------------------------|---------|----------------------------------|-----|
| `enable-expire-snapshots` | boolean | `false`                          | Enable automatic snapshot expiration for all tables in the warehouse. Can be overridden per table with `lakekeeper.history.expire.enabled` |
| `max-snapshot-age-ms`     | integer | `432000000` (5 days)             | Maximum age of snapshots in milliseconds before expiration. Override per table with `history.expire.max-snapshot-age-ms` |
| `min-snapshots-to-keep`   | integer | `1`                              | Minimum snapshots to retain on each table branch. Override per table with `history.expire.min-snapshots-to-keep` |
| `min-snapshots-to-expire` | integer | `20`                             | Minimum snapshots required before expiration job is scheduled (prevents expensive jobs for few snapshots). Override per table with `lakekeeper.history.expire.min-snapshots-to-expire` |
| `max-ref-age-ms`          | integer | `9223372036854775807` (no limit) | Maximum age for snapshot references (except main branch). Main branch references never expire |

### Table-Level Overrides

Individual tables can override warehouse settings using these Iceberg table properties:

- `lakekeeper.history.expire.enabled` - Enable/disable for specific table
- `history.expire.max-snapshot-age-ms` - Custom max age for table snapshots  
- `history.expire.min-snapshots-to-keep` - Custom minimum retention for table
- `lakekeeper.history.expire.min-snapshots-to-expire` - Custom threshold for table

!!! note
    Tables with `gc.enabled=false` are excluded from automatic expiration regardless of other settings.

!!! note
    Tables using Iceberg native encryption (`encryption.key-id` set) are skipped. Expiration must read manifest lists and manifests to determine which files to remove, and Lakekeeper cannot decrypt them. Skipped tables are recorded as a successful (skipped) run, not a failure.

### Production Deployment

For production workloads, we recommend running expire snapshots workers in dedicated pods to avoid impacting REST API performance. This can be achieved by:

1. **API pods**: Set `LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS=0` to disable workers
2. **Worker pods**: Use default worker configuration (2 workers) to handle expire snapshots tasks or set `LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS` to desired number of workers

### Task Scheduling

Expire snapshots tasks are intelligently scheduled immediately after table commits when needed, eliminating the overhead of cron-based polling. This ensures timely cleanup while maintaining optimal performance.

## Remove Orphan Files { #remove-orphan-files .lkp }

Lakekeeper can detect and remove orphan files — files in a table's storage location that are no longer referenced by any snapshot, manifest, statistics file, or metadata log entry. Orphans typically come from failed writes (optimistic-concurrency conflicts) or incomplete maintenance jobs.

The queue is **opt-in per warehouse** and disabled by default. Once enabled, tables are scheduled adaptively: the next run is timed to the observed rate at which orphans accumulate, with a 1-day floor and a configurable ceiling. Idle tables get a periodic safety check at the ceiling; busy tables get reclaimed more often. No cron-based polling is involved.

### How It Works

1. **Scan referenced files**: All files referenced by the current table metadata are collected (manifest lists, manifests, data files, statistics files, partition statistics files, and metadata log entries).
2. **List storage**: The table's storage location is listed recursively.
3. **Identify orphans**: Files present in storage but not in the referenced set are orphan candidates.
4. **Age filter**: Only files older than the configured threshold are deleted. Recently created files are preserved to avoid deleting data from in-progress writes.
5. **Delete**: Orphans are deleted in micro-batches (using cloud-native batch-delete APIs where available).
6. **Reschedule**: The worker self-schedules the next run based on the observed orphan-bytes-per-second rate.

### Configuration

Configuration can be set via the Management UI or REST API endpoints:

- **GET** `/management/v1/warehouse/{warehouse_id}/task-queue/remove_orphan_files/config`
- **POST** `/management/v1/warehouse/{warehouse_id}/task-queue/remove_orphan_files/config`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable-remove-orphan-files` | boolean | `false` | Master switch for the warehouse. When `false`, no tables are scheduled unless they set `lakekeeper.remove-orphan-files.enabled=true`. |
| `default-older-than-ms` | integer | `604800000` (7 days) | Minimum file age before deletion. Must be at least 24 hours, unless `disable-min-older-than-check` is `true` (see below). Override per table with `lakekeeper.remove-orphan-files.older-than-ms`. |
| `target-reclaim-bytes` | integer | `1073741824` (1 GiB) | Adaptive scheduler target: the next run is timed to reclaim roughly this many bytes based on the last run's observed rate. Clamped to [1 day, `maximum-interval-seconds`]. |
| `maximum-interval-seconds` | integer | `7776000` (90 days) | Upper bound on the adaptive next-run interval. Idle tables get a safety check at this cadence. Must be ≥ 86400 (1 day). |
| `max-run-time-seconds` | integer | `3600` (1 hour) | Wall-clock timeout for a single task attempt. Must be ≥ 60. |
| `dry-run` | boolean | `false` | Identify orphans but do not delete. Useful for previewing what a run would remove. |
| `disable-min-older-than-check` | boolean | `false` | Allow a `default-older-than-ms` below 24 hours. Set it only for development and test setups. It does not apply to the table property `lakekeeper.remove-orphan-files.older-than-ms`. |

### Table-Level Overrides

Individual tables can override warehouse settings using these Iceberg table properties:

- `lakekeeper.remove-orphan-files.enabled` — `true` includes a table even when the warehouse master switch is off; `false` excludes it.
- `lakekeeper.remove-orphan-files.older-than-ms` — per-table deletion-age threshold. A value below 24 hours is raised to 24 hours, unless the table also sets `lakekeeper.remove-orphan-files.disable-min-older-than-check=true`.
- `lakekeeper.remove-orphan-files.disable-min-older-than-check` — `true` lets this table use an `older-than-ms` below 24 hours. Remove orphan files then also deletes files that writers uploaded but did not commit yet, if they are older than `older-than-ms`. We recommend setting it only while nothing writes to the table, and removing both properties after the cleanup.
- `lakekeeper.remove-orphan-files.dry-run` — `true` makes the runs of this table dry runs, `false` lets them delete. Any other value also gives a dry run, so a typo never deletes files. If the warehouse config sets `dry-run`, a table cannot turn it off.

### Safety Mechanisms

- **`gc.enabled` check**: Tables with `gc.enabled=false` are excluded. Attempting to remove orphan files on such a table returns an error immediately.
- **Encrypted tables skipped**: Tables using Iceberg native encryption (`encryption.key-id` set) are skipped. Identifying the referenced-file set requires reading manifests, which Lakekeeper cannot decrypt; running anyway risks deleting live data. Skipped tables are recorded as a successful (skipped) run; manually scheduling one returns an error immediately.
- **24-hour minimum retention**: Saving a queue config with a `default-older-than-ms` below 24 hours fails with `RemoveOrphanFilesRetentionTooShort`, unless the same request sets `disable-min-older-than-check: true`. A typo like `default-older-than-ms: 6000` (six seconds) would otherwise delete files that writers uploaded more than six seconds ago but did not commit yet. A table value of `lakekeeper.remove-orphan-files.older-than-ms` below 24 hours is raised to 24 hours, unless the table sets `lakekeeper.remove-orphan-files.disable-min-older-than-check=true`. The run result shows the retention that the run used in `older-than-ms`.
- **Unknown age preservation**: Files where the storage backend does not report a last-modified timestamp are skipped (counted as `skipped-unknown-age-count` in the result).

### Recommended Usage

Run remove orphan files **after** expire snapshots. Expiring snapshots first ensures that files from removed snapshots are no longer referenced, so they can be correctly identified as orphans.

### Production Deployment

For production workloads, we recommend running remove orphan files workers in dedicated pods, similar to expire snapshots:

1. **API pods**: Set `LAKEKEEPER__TASK_REMOVE_ORPHAN_FILES_WORKERS=0` to disable workers.
2. **Worker pods**: Use the default (2 workers) or set `LAKEKEEPER__TASK_REMOVE_ORPHAN_FILES_WORKERS` to the desired number.

!!! warning
    Every run performs a full recursive listing of the table's storage location, which can be expensive for tables with many files. The adaptive scheduler stretches the cadence on tables that produce few orphans, but each run still pays the listing cost.

## Transparent Commit { #transparent-commit .lkp }

Iceberg optimistic concurrency is symmetric: whoever commits second loses. When a compaction (a `REPLACE` snapshot) lands first, a concurrent writer's commit fails validation even though the compaction changed no logical table data — the compaction, being lower value, wins over the user's write. Transparent commit reverses that priority at the catalog: the writer wins, and the conflicting compaction is silently rolled back.

The catalog uses a lazy *tag-and-reap* model. A compaction commit lands as an ordinary, durable snapshot; it merely carries a marker in its snapshot summary declaring it rollbackable. If no conflicting writer ever appears, it stays as a perfectly valid compaction result and nothing further happens. Only when a writer's commit would otherwise fail optimistic-concurrency validation does the catalog act: if *every* snapshot between the writer's expected base and the current branch tip is a rollbackable compaction snapshot, the catalog excises those snapshots, rebases the writer onto its expected base, and commits — folding the revert into the writer's own commit. The pre-compaction data files still physically exist, so restoring the earlier snapshot leaves the writer's positional deletes referencing live files again.

If any genuine data commit sits between the writer's base and the tip, the catalog does **not** roll back — the writer takes a normal concurrency failure and retries, and the compaction snapshot becomes permanent. A compaction is rolled back only while it is younger than the warehouse's maximum compaction age, 24 hours by default. After that, a conflicting writer takes a normal concurrency failure. This is a yield primitive, not a rebase — the catalog drops the compaction snapshot rather than replaying it, which is what makes it safe on V2 positional-delete tables.

### Two-part opt-in

Transparent commit is disabled by default and requires two independent opt-ins:

1. **Warehouse flag.** Enable it per warehouse:

    ```bash
    curl -X POST ".../management/v1/warehouse/{warehouse_id}/rollback-compaction-policy" \
      -H "Content-Type: application/json" \
      -d '{"enabled": true}'
    ```

    The optional `max-compaction-age-seconds` sets how old a compaction may be and still be rolled back. It is measured from the compaction snapshot's timestamp. The optional `snapshot-removal-fence-seconds` sets the window described in [Snapshot expiration](#transparent-commit-snapshot-expiration). Each request sets the whole policy, so a request without them sets them back to 86400 and 60 seconds.

    Changing the flag needs the `set_rollback_compaction_policy` permission on the warehouse. With OpenFGA, `modify` on the warehouse grants it.

2. **Per-snapshot marker.** The compaction engine must stamp its `REPLACE` snapshot summary with either `lakekeeper.rollbackable=true` (native) or Apache Polaris's `polaris.internal.conflict-resolution.by-operation-type.replace=rollback` (honoured for compatibility with engines already integrated against Polaris). Because the opt-in is a snapshot-summary marker, external compaction engines participate without any Lakekeeper-specific integration beyond stamping the marker.

### Snapshot expiration { #transparent-commit-snapshot-expiration }

Lakekeeper keeps rollbacks and snapshot expiration apart on each table, for a window of 60 seconds by default. For the window after a compaction of a table was rolled back, a commit that removes snapshots from that table, such as an `expire_snapshots` run, is rejected with a conflict. Iceberg clients retry such a commit with freshly loaded table metadata, so the run normally succeeds on a later attempt or a later run. For the window after a commit removed snapshots, no compaction of that table is rolled back, and a conflicting writer retries as usual. Set the window with `snapshot-removal-fence-seconds`.

The window protects files that a table still reads. Iceberg's Java `ExpireSnapshots` loads the table before and after its commit, and deletes the files of every snapshot that is missing from the second load as if it had expired that snapshot itself. A compaction rolled back between the two loads is such a snapshot, and the files it replaced are the files the table reads again after the rollback. The window must be longer than the time an expiration run needs from its first load to its second load, which is normally less than a second.

### Notes and limitations

A rolled-back compaction leaves its data and manifest files behind; they are reclaimed by [Remove Orphan Files](#remove-orphan-files), not synchronously during the commit. Rollback is attempted only for a writer commit that adds one `overwrite` or `delete` snapshot to one branch, such as a row-level delete, update or merge. Such a commit can depend on files the compaction rewrote. An `append` or `replace` commit gets a normal concurrency failure and succeeds when the client retries on top of the compaction. Snapshots referenced by another branch or tag are never excised. On V3 (row-lineage) tables, rollback is skipped when a reaped snapshot carries row ranges.
