---
title: "Optimize"
description: "Lakekeeper Plus optimize keeps Iceberg tables fast to read: after writes it compacts small data files, applies delete files and merges small manifests, per warehouse or per table."
source: "https://docs.lakekeeper.io/docs/latest/optimize/"
---

# Optimize { #optimize .lkp }

Optimize keeps your tables fast to read. Streaming jobs and frequent small writes leave many small files behind, and every query then opens more files than it needs. Optimize rewrites them in the background into fewer, larger files, so you do not need to run your own compaction jobs.

An optimize run has two steps:

- **Compaction** rewrites small data files into files of the target size (512 MiB by default) and applies delete files to the data. If the table has a sort order, compaction writes the rows in that order, unless the table sets `lakekeeper.compaction.strategy=binpack`.
- **Manifest rewrite** merges small manifests, so that query engines read fewer files when they plan a query.

Optimize is off by default. The server must offer it (`LAKEKEEPER__OPTIMIZE__AVAILABLE=true`), and it needs optimize workers, which no Lakekeeper process runs by default. The Helm chart sets up both when you set `optimizeWorkers.enabled: true`. See [Optimize Workers](./optimize-workers.md).

## When It Runs

After each commit, Lakekeeper checks whether the table has collected small files or delete files, or had many commits since its last manifest rewrite. If so, it schedules a run for about one minute later, so that a burst of writes leads to one run.

Automatic compaction of a table runs at most once per hour. A run that could not finish all the work it found continues right away.

After a change of the partition spec, compaction moves the data files that are much smaller or much larger than the target size into the new partitions. A second run follows about one minute later and merges the small files that the first run wrote.

## Turn It On

Turn optimize on for a warehouse with the management API:

- **GET** `/management/v1/warehouse/{warehouse_id}/task-queue/optimize/config`
- **POST** `/management/v1/warehouse/{warehouse_id}/task-queue/optimize/config`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enable-optimize` | boolean | `false` | Optimize every table of the warehouse. |
| `enable-compaction` | boolean | unset | Turn compaction on or off for the warehouse. If it is unset, `enable-optimize` decides. |
| `enable-rewrite-manifests` | boolean | unset | Turn manifest rewrite on or off for the warehouse. If it is unset, `enable-optimize` decides. |
| `target-file-size-bytes` | integer | `536870912` (512 MiB) | Size of the files that compaction writes, for tables that do not set `write.target-file-size-bytes`. Allowed values: 32 MiB to 2 GiB. |
| `target-manifest-size-bytes` | integer | `8388608` (8 MiB) | Size of the manifests that manifest rewrite writes, for tables that do not set `commit.manifest.target-size-bytes`. Allowed values: 1 MiB to 256 MiB. |

For example, send `{"queue-config": {"enable-optimize": true}}` to the POST endpoint. The POST replaces the whole configuration, so always send every field you want to keep.

Lakekeeper checks each table at its next commit. To optimize a table that gets no writes, see [Run It Now](#run-it-now).

### Table-Level Overrides

A table can override the warehouse with these table properties:

- `lakekeeper.optimize.enabled`: `true` optimizes the table even if the warehouse has optimize turned off. `false` turns optimize off for the table.
- `lakekeeper.compaction.enabled` and `lakekeeper.rewrite-manifests.enabled`: turn one step on or off for this table. They take precedence over `lakekeeper.optimize.enabled`.
- `lakekeeper.compaction.strategy`: `sort` (the default) writes the rows in the table's sort order. `binpack` writes them unsorted, which is faster. Tables without a sort order are written unsorted in both cases. With any other value, compaction does not rewrite the table, and the task log names the property.
- `write.target-file-size-bytes` and `commit.manifest.target-size-bytes`: the Iceberg properties for the size of data files and manifests. Compaction keeps the data file size between 32 MiB and 2 GiB.

Write `true` or `false` for the switches. A value that Lakekeeper does not understand turns the step off for the table, and the Lakekeeper log shows a warning that names the property.

## Tables That Optimize Skips

Optimize does not run on these tables:

- Tables with format version 3.
- Tables that use Iceberg native encryption (`encryption.key-id` is set).
- Tables that use write-audit-publish (`write.wap.enabled=true`), because a rewrite would make the next publish fail. Set `lakekeeper.optimize.enabled=true` on such a table to optimize it anyway.
- Tables that Apache Paimon 2.1 or later mirrors. [Snapshot expiry](./table-maintenance.md#expire-snapshots) and [orphan-file removal](./table-maintenance.md#remove-orphan-files) skip them too. Paimon recreates such a table after another writer adds a snapshot, and it can register the table at its own storage path, where expiry and orphan-file removal would delete Paimon's files. Optimize cannot detect tables that earlier Paimon versions mirror, so set `lakekeeper.optimize.enabled=false` on them.
- Tables partitioned by the value of a `uuid` column (`identity` transform) in any of their partition specs, including earlier ones.

Compaction skips these tables, but manifest rewrite still runs on them:

- Tables whose default write format is not Parquet.
- Tables stored on Azure Data Lake Storage (ADLS).
- Tables with a column that compaction cannot write back unchanged. The task log names the column.
- Tables whose data location (`write.data.path`) is outside the table location.

Compaction fails on tables that reference data files outside the table location, for example files added with Spark's `add_files` or `snapshot` procedure. The task log names the file. To stop these failures, set `lakekeeper.compaction.enabled=false` on the table. Manifest rewrite keeps running.

Compaction skips data files that are too large to sort in one run (larger than 3 GiB). The task log says how many files it skipped and how to compact them.

## Run It Now

To optimize a table now, send this to **POST** `/management/v1/warehouse/{warehouse_id}/task-queue/optimize/schedule`, with the table's UUID (`table-uuid` in its metadata):

```json
{"entity": {"type": "table", "table-id": "<table uuid>"}}
```

This runs compaction and manifest rewrite even if they are not due. A step that is turned off for the warehouse or the table does not run. To run one step only, name it in `payload`:

```json
{"entity": {"type": "table", "table-id": "<table uuid>"}, "payload": {"steps": ["compaction"]}}
```

For manifest rewrite only, write `rewrite-manifests`. It then rewrites all manifests, not only the small ones.

## See the Results

Each run writes one line per step to the task log of the table: what the step rewrote, why it skipped the table, or why it failed. You find the runs on the table's **Tasks** tab in the Lakekeeper UI, or with the task endpoints of the management API. The task details also hold the full report of each run, for example the number of files and bytes it rewrote.

To see whether runs wait too long for a worker, use the [task queue metrics](./monitoring.md#task-queue-metrics) of the queue `optimize`.

A task that fails is tried again, up to three attempts in total. The task log says why it failed. The most common causes:

- **The worker ran out of memory.** Add a larger [size class](./optimize-workers.md#size-classes).
- **The spill disk is full.** Give the workers more [spill disk](./optimize-workers.md#memory-cores-and-spill-disk).
- **The table references data files outside the table location.** See [Tables That Optimize Skips](#tables-that-optimize-skips).

## Turn Optimize Off for a Server

To remove optimize from a deployment, remove `LAKEKEEPER__OPTIMIZE__AVAILABLE` or set it to `false`. See [Turning Optimize On and Off](./configuration.md#turning-optimize-on-and-off).
