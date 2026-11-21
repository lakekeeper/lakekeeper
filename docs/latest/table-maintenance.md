---
title: "Table Maintenance"
description: "How Lakekeeper maintains Iceberg tables: metadata file cleanup, and in Lakekeeper Plus expire snapshots, optimize and remove orphan files."
source: "https://docs.lakekeeper.io/docs/latest/table-maintenance/"
---

# Table Maintenance

## Metadata File Cleanup

Lakekeeper honors the Iceberg table properties `write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max`. Starting with Lakekeeper v0.10.0, `delete-after-commit` is enabled by default (it was disabled in earlier versions). On each table commit, when `delete-after-commit` is enabled, Lakekeeper keeps the current table metadata file plus up to `write.metadata.previous-versions-max` previous metadata files (default: 100) and deletes the oldest tracked metadata file from the metadata log once that limit is exceeded. This cleanup applies only to metadata files tracked in the metadata log; it does not remove orphaned metadata files.

For example: if `write.metadata.previous-versions-max=20`, Lakekeeper retains 21 files in total (the current plus 20 previous); committing a 22nd version deletes the oldest tracked metadata file.

If a commit does not change a table, Lakekeeper writes no new metadata file and deletes none. Such a commit has no updates, or only updates that leave the metadata as it is, for example removing statistics that the table does not have. Lakekeeper checks the requirements of the commit and returns the current metadata. The table keeps its metadata file, its metadata log and its last-updated time, and Lakekeeper publishes no `updateTable` event. A commit to a staged table always writes a metadata file. In a transaction across several tables, Lakekeeper writes nothing only if no table changes and no table is staged. Otherwise, it writes new metadata for every table of the transaction.

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
| `max-snapshot-age-ms`     | integer | `432000000` (5 days)             | Maximum age of snapshots in milliseconds before expiration. Must be at least 24 hours, unless `disable-min-age-check` is `true` (see below). Override per table with `history.expire.max-snapshot-age-ms` |
| `min-snapshots-to-keep`   | integer | `1`                              | Minimum snapshots to retain on each table branch. Override per table with `history.expire.min-snapshots-to-keep` |
| `min-snapshots-to-expire` | integer | `20`                             | Minimum snapshots required before expiration job is scheduled (prevents expensive jobs for few snapshots). Override per table with `lakekeeper.history.expire.min-snapshots-to-expire` |
| `max-ref-age-ms`          | integer | `9223372036854775807` (no limit) | Maximum age for snapshot references (except main branch). Main branch references never expire. Must be at least 24 hours, unless `disable-min-age-check` is `true`. Override per table with `history.expire.max-ref-age-ms` |
| `disable-min-age-check`   | boolean | `false`                          | Allow a `max-snapshot-age-ms` or `max-ref-age-ms` below 24 hours. We recommend it only for development and test setups. It does not apply to values that a table sets in its properties or on its branches and tags. For those, see `lakekeeper.history.expire.disable-min-age-check` |

### Table-Level Overrides

Individual tables can override warehouse settings using these Iceberg table properties:

- `lakekeeper.history.expire.enabled` - Enable/disable for specific table
- `history.expire.max-snapshot-age-ms` - Custom max age for table snapshots
- `history.expire.max-ref-age-ms` - Custom max age for branches and tags of this table, except main
- `lakekeeper.history.expire.disable-min-age-check` - `true` lets this table use ages below 24 hours in the two properties above and in the retention that a branch or tag sets for itself.
- `history.expire.min-snapshots-to-keep` - Custom minimum retention for table
- `lakekeeper.history.expire.min-snapshots-to-expire` - Custom threshold for table

A value of `history.expire.max-snapshot-age-ms`, `history.expire.max-ref-age-ms` or `history.expire.min-snapshots-to-keep` that is not a whole number, such as `30d` or `2592000000L`, makes Lakekeeper skip the table instead of using the default. Lakekeeper logs a warning that names the property and its value.

**24-hour minimum**:

- Saving a queue config with `max-snapshot-age-ms` or `max-ref-age-ms` below 24 hours fails with `ExpireSnapshotsRetentionTooShort`, unless the same request sets `disable-min-age-check: true`. This stops a typo such as `max-snapshot-age-ms: 6000` (six seconds) from removing almost all snapshots.
- A config with an age below 24 hours that was saved before Lakekeeper Plus 0.15 makes every run fail. Save it again with a larger value, or with `disable-min-age-check: true`.
- A table value below 24 hours is raised to 24 hours, unless the table sets `lakekeeper.history.expire.disable-min-age-check=true`. Lakekeeper then logs a warning that names the property and its value, and the run result shows `retention-adjustment: raised-to-minimum`.

!!! note
    Tables with `gc.enabled=false` are excluded from automatic expiration regardless of other settings.

!!! note
    Tables using Iceberg native encryption (`encryption.key-id` set) are skipped. Expiration must read manifest lists and manifests to determine which files to remove, and Lakekeeper cannot decrypt them. Skipped tables are recorded as a successful (skipped) run, not a failure.

!!! note
    Tables that Apache Paimon may still use are skipped, so expiration does not delete data files that Paimon needs. Paimon 2.1 can register a mirror at its own storage path. A table is skipped while it keeps any snapshot that Paimon 2.1 or later wrote. To include it again, expire these snapshots with the engine that owns the table. A table is also skipped while its location holds a Paimon table of any version. Setting `gc.enabled=false` on a mirror protects it as well. With Paimon 2.1 or later, you can set it through the Paimon table option `metadata.iceberg.table-properties.gc.enabled=false`.

### Production Deployment

For production workloads, we recommend running expire snapshots workers in dedicated pods to avoid impacting REST API performance. This can be achieved by:

1. **API pods**: Set `LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS=0` to disable workers
2. **Worker pods**: Use default worker configuration (2 workers) to handle expire snapshots tasks or set `LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS` to desired number of workers

### Task Scheduling

Expire snapshots tasks are intelligently scheduled immediately after table commits when needed, eliminating the overhead of cron-based polling. This ensures timely cleanup while maintaining optimal performance.

## Optimize { #optimize .lkp }

Optimize keeps tables fast to read. After writes, it compacts small data files into larger ones, applies delete files and merges small manifests. It is off by default and can be turned on per warehouse or per table. See [Optimize](./optimize.md), and [Optimize Workers](./optimize-workers.md) for the workers that run it.

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
| `disable-min-older-than-check` | boolean | `false` | Allow a `default-older-than-ms` below 24 hours. We recommend it only for development and test setups. It does not apply to the table property `lakekeeper.remove-orphan-files.older-than-ms`. |

### Table-Level Overrides

Individual tables can override warehouse settings using these Iceberg table properties:

- `lakekeeper.remove-orphan-files.enabled` — `true` includes a table even when the warehouse master switch is off; `false` excludes it.
- `lakekeeper.remove-orphan-files.older-than-ms` — per-table deletion-age threshold. A value below 24 hours is raised to 24 hours, unless the table also sets `lakekeeper.remove-orphan-files.disable-min-older-than-check=true`. A value that is not a whole number makes Lakekeeper skip the table and log a warning.
- `lakekeeper.remove-orphan-files.disable-min-older-than-check` — `true` lets this table use an `older-than-ms` below 24 hours. Remove orphan files then also deletes files that writers uploaded but did not commit yet, if they are older than `older-than-ms`. We recommend setting it only while nothing writes to the table, and removing both properties after the cleanup.
- `lakekeeper.remove-orphan-files.dry-run` — `true` makes the runs of this table dry runs, `false` lets them delete. Any other value also gives a dry run, so a typo never deletes files. If the warehouse config sets `dry-run`, a table cannot turn it off.

### Safety Mechanisms

- **`gc.enabled` check**: Tables with `gc.enabled=false` are excluded. Attempting to remove orphan files on such a table returns an error immediately.
- **Encrypted tables skipped**: Tables using Iceberg native encryption (`encryption.key-id` set) are skipped. Identifying the referenced-file set requires reading manifests, which Lakekeeper cannot decrypt; running anyway risks deleting live data. Skipped tables are recorded as a successful (skipped) run; manually scheduling one returns an error immediately.
- **24-hour minimum retention**: Saving a queue config with a `default-older-than-ms` below 24 hours fails with `RemoveOrphanFilesRetentionTooShort`, unless the same request sets `disable-min-older-than-check: true`. A typo like `default-older-than-ms: 6000` (six seconds) would otherwise delete files that writers uploaded more than six seconds ago but did not commit yet. A table value of `lakekeeper.remove-orphan-files.older-than-ms` below 24 hours is raised to 24 hours, unless the table sets `lakekeeper.remove-orphan-files.disable-min-older-than-check=true`. The run result shows the retention that the run used in `older-than-ms`.
- **Paimon mirrors skipped**: Tables that Apache Paimon may still use are skipped, so Paimon's own files are not removed as orphans. As for [expiration](#expire-snapshots), a table is skipped while it keeps any snapshot that Paimon 2.1 or later wrote, and while its location holds a Paimon table of any version. Setting `gc.enabled=false` on a mirror protects it as well.
- **Unknown age preservation**: Files where the storage backend does not report a last-modified timestamp are skipped (counted as `skipped-unknown-age-count` in the result).

### Recommended Usage

Run remove orphan files **after** expire snapshots. Expiring snapshots first ensures that files from removed snapshots are no longer referenced, so they can be correctly identified as orphans.

### Production Deployment

For production workloads, we recommend running remove orphan files workers in dedicated pods, similar to expire snapshots:

1. **API pods**: Set `LAKEKEEPER__TASK_REMOVE_ORPHAN_FILES_WORKERS=0` to disable workers.
2. **Worker pods**: Use the default (2 workers) or set `LAKEKEEPER__TASK_REMOVE_ORPHAN_FILES_WORKERS` to the desired number.

!!! warning
    Every run performs a full recursive listing of the table's storage location, which can be expensive for tables with many files. The adaptive scheduler stretches the cadence on tables that produce few orphans, but each run still pays the listing cost.
