# Table Maintenance

## Metadata File Cleanup
Lakekeeper honors the Iceberg table properties `write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max`. Starting with Lakekeeper v0.10.0, `delete-after-commit` is enabled by default (it was disabled in earlier versions). On each table commit, when `delete-after-commit` is enabled, Lakekeeper keeps the current table metadata file plus up to `write.metadata.previous-versions-max` previous metadata files (default: 100) and deletes the oldest tracked metadata file from the metadata log once that limit is exceeded. This cleanup applies only to metadata files tracked in the metadata log; it does not remove orphaned metadata files.

For example: if `write.metadata.previous-versions-max=20`, Lakekeeper retains 21 files in total (the current plus 20 previous); committing a 22nd version deletes the oldest tracked metadata file.

Link to [Expire Snapshots](#expire-snapshots)

## Expire Snapshots <span class="lkp"></span> {#expire-snapshots}

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

### Production Deployment

For production workloads, we recommend running expire snapshots workers in dedicated pods to avoid impacting REST API performance. This can be achieved by:

1. **API pods**: Set `LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS=0` to disable workers
2. **Worker pods**: Use default worker configuration (2 workers) to handle expire snapshots tasks or set `LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS` to desired number of workers

### Task Scheduling

Expire snapshots tasks are intelligently scheduled immediately after table commits when needed, eliminating the overhead of cron-based polling. This ensures timely cleanup while maintaining optimal performance.
