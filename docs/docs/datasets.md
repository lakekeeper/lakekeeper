---
description: "Version collections of files with Lakekeeper Datasets: immutable snapshots, branches and tags, compare-and-swap commits, and zero-copy import of objects already in storage."
---

# Datasets

A **Dataset** is a versioned collection of files. It lives in a Namespace next to Iceberg tables, views and generic tables, and every change to it is an immutable **snapshot**. **Branches** move as you commit; **tags** pin a snapshot for good. A tag cut for a training run resolves to exactly the same files next year, however far `main` has moved on.

Lakekeeper stores only metadata. A snapshot's manifest lists each file's logical key, where its bytes are, and what is known about them (size, etag, checksum, content type, object version). Lakekeeper never writes, copies or moves the files themselves: producers write objects to storage, then commit a manifest that points at them.

!!! note "Not the generic table `dataset` format"
    [Generic tables](generic-tables.md) can carry a free-form `format` such as `dataset` for a raw file drop. That registers the drop once, with no history. A Dataset is a different resource: it versions the file list itself.

## When to use datasets

- **Reproducible training data** — tag the exact file set a model was trained on, and resolve that tag later to the same files.
- **Unstructured and multimodal data** — images, audio, documents and other files that have no table format of their own.
- **Write-audit-publish** — commit to a branch, check it, then fast-forward a protected `main` to it.
- **Registering data already in a bucket** — [import](#importing-objects-already-in-storage) a prefix without copying it, and keep the dataset in step with the bucket as it changes.

## Concepts

### Managed and imported datasets

A dataset created **without** a `location` is *managed*: Lakekeeper allocates an exclusive prefix inside the warehouse, and producers write under it. A dataset created **with** a `location` is *imported*: it borrows an existing prefix, and Lakekeeper never mutates or deletes objects there. Purging an imported dataset is refused; dropping it removes only the catalog metadata.

The location is fixed at creation. It must lie within the warehouse's storage profile.

### Snapshots, branches and tags

Every commit produces a snapshot whose parent is the snapshot the branch was on. A new dataset has one branch, `main`, pointing at nothing until the first commit.

| Ref | Moves on commit | Can be moved | Notes |
|-----|:---:|:---:|-------|
| Branch | :white_check_mark: | Fast-forward or reset | Created from any snapshot or ref |
| Tag | :x: | :x: | Fixed at creation; always resolves to the same snapshot |

A **fast-forward** moves a branch to a snapshot that descends from its current head, so no commit is abandoned. A **reset** moves it anywhere, abandoning commits; it is authorized separately (see [Authorization model](#authorization-model)).

A **protected** branch refuses direct commits, resets and deletion for everyone, including owners, with `409 Conflict` and error type `DatasetRefProtected`. Changes reach it only by fast-forward, which is what makes it the publish step of write-audit-publish. Protection is a structural rule on the branch, not a permission; setting it takes `manage_refs`.

### Constraints

A dataset can declare `allowed-content-types` and `max-file-size` at creation. A file that does not declare the property a constraint checks — its content type, or its size — fails that constraint. Constraints are enforced by the catalog on the manifest, not by storage at upload time.

`POST .../datasets/{dataset}/settings` with `{"constraints": {...}}` replaces them; `{"constraints": {}}` lifts them. New constraints bound commits and imports from then on; files already committed are not re-checked. Changing them takes `update_settings`, which derives from `modify`, as a table's property update does.

`on-constraint-violation` says what happens to a file that fails them. Under `reject`, the default for a commit, the whole request is refused with `400 DatasetConstraintViolation` and the error lists every violation. Under `skip`, the default for an [import](#importing-objects-already-in-storage), the conforming files are recorded and the rest are reported in `skipped`, each with its reasons; nothing is left out silently. A `sync` that finds a file's new bytes refused removes the file, since its entry would otherwise describe bytes storage has replaced.

## Committing

A commit names the files to add and the logical keys to remove, and the snapshot the caller believes the branch is on:

```bash
curl -X POST "$LAKEKEEPER/lakekeeper/v1/$WAREHOUSE/namespaces/ml/datasets/images/branches/main/commits" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "parent-snapshot-id": "0199a0b4-...",
    "added": [
      {"logical-key": "train/0001.jpg", "physical-path": "raw/2026-09/0001.jpg",
       "size": 48213, "content-type": "image/jpeg", "etag": "\"9b2cf5...\""}
    ],
    "removed": ["train/0000.jpg"],
    "summary": {"pipeline-run": "run-4711", "code-commit": "a1b2c3d"}
  }'
```

`physical-path` is where the bytes are: a path relative to the dataset's location, or a full object URI inside it. It defaults to the logical key; imports record full URIs. Either way the bytes must lie below the dataset's location, or the commit is refused with `400 PhysicalPathOutsideDataset`. A key is relative, at most 1024 characters, with no empty, `.` or `..` segment (percent-encoded or not), no `\`, `#` or `?`, and no control or invisible formatting character, and a commit names each key once; anything else is refused with `400 InvalidKey` or `400 DuplicateKey`. A producer writing a managed dataset uploads into the `write-prefix` its [credentials](#storage-credentials) name and commits each file with its `physical-path` there, so no object a snapshot records is ever written again and every version keeps its bytes, on any bucket. `summary` is free-form and is recorded on the snapshot — use it to correlate a snapshot with the pipeline run or code commit that produced it. Omit `parent-snapshot-id` only for the first commit on a branch. A commit is one request, so `LAKEKEEPER__MAX_REQUEST_BODY_SIZE` (32 MB by default, see [configuration](./configuration.md)) bounds how many files it can name; a producer with more commits them in several, or [imports](#importing-objects-already-in-storage) them.

The branch pointer moves by **compare-and-swap**. If another writer committed first, the request fails with `409 Conflict` and the error stack carries the branch's current head as `current-snapshot-id: <uuid>`. Nothing was written, so the writer can rebase its change onto that head and retry.

### Retrying safely

A commit whose response was lost may have been applied. Retrying it as it stands answers `409 Conflict`, since the branch has moved, and rebasing it would commit the change a second time. Send an `Idempotency-Key` header, a fresh UUID per logical commit, and a retry with the same key is answered with the snapshot the commit published — even when later commits have landed on top of it. A request that failed recorded nothing, so its key is still unspent: the rebased retry after a `409` may reuse it.

Creating, moving and deleting a ref, and requesting an [access grant](#signed-urls), take the key too. A replayed grant request answers with the same grant, so no second grant is left live that its caller never learned of; a replayed ref operation answers with the ref as it stands. A commit's retry also reports the files the commit's constraints left out. The snapshot or grant carries the key for as long as it lives, so a retry after the key's record has lapsed is answered from it too. A commit's or grant's key spent on another dataset, or a grant's spent by another caller, is refused with `400 IdempotencyKeyReused`; a retry whose grant has expired and been swept, or whose snapshot has been purged, answers `404`. A ref operation's key is bound to its endpoint only, as a table's is: its replay reports the named ref of the dataset it names. A commit's retry is authorized as the commit was, since it tells what the commit published; a grant's and a ref operation's are authorized as a read of the dataset, and a drop's, a rename's and a ref deletion's replay without one, as they reveal nothing. Keys are kept for `LAKEKEEPER__IDEMPOTENCY__LIFETIME`; see [Idempotency](./configuration.md#idempotency).

## Reading a version

`GET .../datasets/{dataset}/refs/{ref}/files` lists the files a branch or tag resolves to, in byte order of their logical keys — the order object stores list in — with an optional `contentType` filter. To read a snapshot no ref points at, tag it first. Pagination is pinned to the snapshot the first page resolved, so a commit landing mid-walk cannot shift or tear the sequence.

!!! warning "A page can be short or empty while files remain"
    Each page is bounded by a scan over the key range; removed files and the `contentType` filter are applied after it. A page can therefore come back with fewer files than requested — or none — and still carry a `next-page-token`. Only the **absence** of a token ends the listing. A client that stops at an empty page silently drops files.

To read the bytes, check `access-mode` on the file listing. It is `storage-credentials` when the dataset is managed and the warehouse's storage profile vends credentials (S3 or GCS with STS, Azure with SAS enabled), and `presigned` for every other dataset. An imported dataset is always `presigned`: its prefix is borrowed and may hold files that are not the dataset's, and a credential for the prefix would reach them too.

### Storage credentials

`GET .../datasets/{dataset}/credentials` with the `X-Iceberg-Access-Delegation: vended-credentials` header returns storage credentials for the dataset. A caller who may read receives read-only credentials for the dataset's prefix. A caller who may commit also receives `write-prefix`, a fresh, empty folder `data/<id>/` below the location, with read, write and delete credentials for that folder alone: it can upload new objects there, and cannot overwrite or delete an object any snapshot records. Each request names a new folder. A caller with neither is refused. An imported dataset vends none, since its prefix may hold objects that are not the dataset's: the request is refused with `409 CannotVendCredentialsForImportedDataset`, and its files are read through [signed URLs](#signed-urls).

### Signed URLs

Signed URLs work on every storage profile, in either access mode. A reader obtains an access grant once, then asks for URLs in batches:

1. `POST .../datasets/{dataset}/refs/{ref}/access-grants` authorizes `read_data` on the dataset and pins the snapshot the ref points at now. The body may set `content-type` to narrow the grant to files of that type. The response carries `grant-id`, `snapshot-id`, `expires-at`, `max-keys-per-request` and `url-validity-seconds`. A ref with no commits has nothing to grant and is refused with `409`.
2. `POST .../datasets/{dataset}/snapshots/{snapshot-id}/files/sign` with `{"grant-id": "…", "keys": ["…"]}` returns one signed GET URL per key, in the order asked, each with its `expires-at`. A call takes at most `max-keys-per-request` keys.

A signing call runs no policy; it checks the grant. The grant must exist, belong to this dataset and to the caller — the same principal, acting as the same role (matched by id) if it assumed one — be neither revoked nor expired, and pin the snapshot named in the path. Another caller's grant reads as `404`. Every key must name a live file of that snapshot, of the grant's content type if it has one; otherwise nothing is signed and the call fails with `403 DatasetFilesOutsideGrant`. A commit to the ref after the grant was issued changes nothing it signs. The grant is read from the primary database, so a revocation stops the next call.

A URL is a plain HTTPS GET against the object store and needs no other credential; range requests work. A file that records an object version (`version-id`) is signed at that version on S3 and STACKIT (`versionId`) and on GCS (`generation`): the URL reads exactly the bytes the snapshot pinned, never bytes written to the key since, and fails once that version is deleted. On a GCS bucket without object versioning, overwriting a pinned object deletes the pinned generation, so its URL then fails. Azure URLs read the current blob, whatever version a file records.

A file that records no version is read from whatever the key holds when it is fetched, so a key written again since serves other bytes. Each signed file carries the `etag` the snapshot recorded: a download whose `ETag` header differs from it, ignoring surrounding quotes, is not the file the snapshot recorded, and a reader should refuse it. GCS signed files carry no `etag`, because GCS reports a different etag to a download than to a listing; a GCS file read at its generation needs no check, so commit GCS files with their generation as `version-id`.

URLs stay valid for `url-validity-seconds` (15 minutes by default) and a grant for 12 hours; see [Configuration](./configuration.md#datasets). Sign ahead of the reader in batches, and sign again as URLs near expiry; a snapshot need not be signed up front. Reads go straight to the object store, so its request-rate limits apply: a snapshot of many small files costs one request per file, and packing them into larger shards is the usual remedy.

`DELETE .../datasets/{dataset}/access-grants/{grant-id}` revokes a grant. Its holder may revoke it without further permission; revoking another caller's takes `revoke_access_grants`, which derives from grant authority. URLs signed before the revocation keep working until they expire.

Each signing call that reaches its grant leaves one [`dataset_files_signed`](./logging.md#operational-audit-events) audit record naming the grant, the snapshot and the number of keys, never the URLs; one refused for an empty or oversized batch leaves none.

How each backend signs:

- **S3 and S3-compatible storage** sign locally with the warehouse's credential.
- **GCS** signs locally with a service-account key. A system identity signs through the IAM `signBlob` API, one call per URL, and needs `iam.serviceAccounts.signBlob` on itself; see [GCP System Identity](./storage-gcs.md#gcp-system-identity). A bearer-token credential cannot sign.
- **Azure and OneLake** mint a read-only SAS per file, from the account key or from a user delegation key that is cached per dataset like a table's SAS, and need SAS enabled on the storage profile.

## Comparing versions

`GET .../datasets/{dataset}/diff` lists the files that differ between two versions: the review step before a promote. Name each side once, as a ref (`from`, `to`) or a snapshot (`fromSnapshotId`, `toSnapshotId`); naming neither or both is refused with `400`. It is authorized as `read_data` on the dataset, naming the refs given.

Changes come in logical-key order. `added` is a file only `to` has, `removed` one only `from` has, and `modified` one both have but record differently; each change carries the file as `from` and `to` record it. A ref with no commits has no files, so everything on the other side is added.

Pagination is pinned to the snapshots the first page resolved, and a page token continues only the comparison it came from. Each request compares at most 10,000 keys, so two large, mostly equal versions return short or empty pages while changes remain: as with listing, only a missing `next-page-token` ends the diff.

## Importing objects already in storage

`POST .../datasets/{dataset}/import` lists the dataset's prefix and commits what it finds. Nothing is copied: each manifest entry points at the object that was already there, with its size and modification time taken from the listing, and the content type inferred from the extension. The etag is recorded where the listing reports one (S3, GCS and ADLS), and on GCS the object's generation as its `version-id`, so a [signed URL](#signed-urls) reads that generation. An S3 listing carries no version id; an import with `record-versions` lists object versions, and a producer that commits its files sets `version-id` itself.

| Field | Meaning |
|-------|---------|
| `mode` | `add-only` (default) registers objects under keys the dataset does not have yet, and never modifies or removes an entry. `sync` makes the snapshot mirror the prefix: changed objects are recorded as modified and vanished ones as removed. An object counts as changed when its size or modification time differs, or its etag where both the entry and the listing have one, or its version where the listing reports one: a version where the entry records none counts too, which pins the file from then on. GCS listings report every object's generation, so a `sync` there pins committed files that set no `version-id`. |
| `sub-prefix`, `suffix` | Narrow the scan within the dataset's location. A `sync` judges only the keys the narrowed scan covers, and leaves every other file alone. A `sub-prefix` that escapes the location is refused. |
| `include`, `exclude` | Globs over keys relative to the dataset's location: a key is registered if it matches one `include`, when any is given, and no `exclude`. `*` and `?` match within one path segment and `**` across segments, so `**/*.jpg` matches at any depth and `*.jpg` only at the top. A `sync` judges only the keys they admit. A glob that does not parse is refused with `400 InvalidGlob`. |
| `default-excludes` | `true` (default) also leaves out what job writers leave behind: `**/_SUCCESS`, `**/_temporary/**` and `**/.checkpoint/**`. |
| `record-versions` | `true` records each object's current version as its `version-id`, so a signed URL reads exactly those bytes, even after the key is written again. On S3 the bucket needs versioning on, and the warehouse's credential `s3:ListBucketVersions`; listing versions is slower on a bucket that keeps many old ones. GCS records the generation either way. Refused with `400 InvalidRecordVersions` on Azure, whose listings carry no version. A `sync` with it pins files imported without it: a version where none was recorded counts as a modification. |
| `check-materialization` | After the import, check every snapshot a branch or tag points at against the listing; see [Checking that the files are still there](#checking-that-the-files-are-still-there). Needs the whole prefix scanned, so it is refused with `sub-prefix`, `suffix` or `include`. A scan cut short by `max-files`, or an import superseded by a concurrent commit, checks nothing and reports no `materialization`. |
| `max-files` | Stop after this many objects; the response then reports `truncated: true`. A truncated scan cannot run in `sync` mode, because an unseen object is not evidence of deletion. |
| `queued` | Run the scan on the `dataset_import` task queue and return a `task-id`. At most one queued import per dataset is active; queuing a second is refused with `409`. |
| `on-constraint-violation` | `skip` (default) leaves out objects the dataset's [constraints](#constraints) refuse; `reject` fails the import on the first batch that holds one. The response counts the skipped objects in `skipped` and names the first hundred in `skipped-files`. |

An import that finds nothing to change publishes no snapshot and returns the head it compared the listing against, so a scheduled `sync` over an unchanged bucket grows no history.

An import writes what it lists to a scratch table in the catalog database, then merges it against the branch's manifest in key order, a page at a time. Its memory stays fixed however large the prefix or the dataset. The database work grows with the listing, and for a `sync` with the files under the scanned prefix; a dataset whose commits ever named a file apart from its storage path (below) is also walked whole once per import.

An import recognises a file by where its bytes are. A file a commit named apart from its storage path — `train/cat.jpg` with its bytes at `raw/0001.jpg` — is that object: the import does not register `raw/0001.jpg` a second time, records the object's new bytes under `train/cat.jpg` when it changed, and a `sync` removes `train/cat.jpg` once its object is gone. While the file's own object is there, an object stored under the name it goes by is another object; it is left out and reported in `skipped`, and the file keeps its name. Once the file's own object is gone, a `sync` points the file at the object stored under its name.

A commit that lands while an import runs is kept. The import leaves alone every file that commit changed, and a `sync` removes only files the branch had when the import began and the scan did not find. The next import judges whatever the commit changed. A branch reset while an import runs fails the import with `409 Conflict` and publishes nothing: its listing was merged against history the branch has left.

### Checking that the files are still there

A snapshot is immutable, but the objects it points at live in a bucket other tools write to. An imported dataset's objects can be deleted under a tag, and nothing in the catalog would show it until a read failed. An object can also be written again under a tag, so the tag reads bytes it never recorded. An import run with `"check-materialization": true` looks for both: it compares every snapshot a branch or tag points at against the listing it just made, and records on each the files whose object is gone (`missing`) and those whose object differs from what the file records (`changed`): its size differs, or its etag, except on GCS, whose listings report a different etag than uploads do.

`GET .../datasets/{dataset}/snapshots/{snapshot-id}/materialization` reports the result: `unchecked` until an import checks the snapshot, then `fully-materialized` or `partially-degraded`, with `checked-at`, the counts of `missing-files` and `changed-files`, and the files themselves a page at a time, each with its `problem` — the first ten thousand of them. Listing refs shows each ref's status too. Reading the report takes `read_data`, like listing the files.

A snapshot that is `partially-degraded` still resolves, and every file in it keeps its manifest entry; reading a missing one fails, and reading a changed one returns other bytes. The check judges only the files its scan covers, by the key their bytes list under: a file whose bytes an `exclude` leaves out, or that lie outside the dataset's location, is not judged. A snapshot published after the import's listing began is left as it was, even one another import staged before, since the listing may have passed a key before the file under it was written. A snapshot's `created-at` is when it was published. On S3 and GCS a file pinned to an object version is judged by that version: as recorded while storage holds it, though the key was deleted or written again, and `missing` once the version is gone; a version the listing does not show is looked up, unless the key's object is the one recorded, not written since. On Azure and other storage that pins no versions, a version means nothing to a reader, so the file is judged by its key. A check that cannot finish costs its report, never the import: the response then carries no `materialization`. The status is what the last check saw; run another to refresh it.

### Keeping a dataset in step with a bucket

Datasets have no built-in event ingestion. A scheduled job — cron, a Kubernetes `CronJob`, or a Lambda on a timer — that runs a queued sync is enough to keep a dataset current:

```bash
curl -X POST "$LAKEKEEPER/lakekeeper/v1/$WAREHOUSE/namespaces/ml/datasets/images/import" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"mode": "sync", "queued": true}'
```

The task's outcome — files imported, modified, removed and skipped — is recorded in its execution details at `GET /management/v1/warehouse/{warehouse_id}/task/by-id/{task_id}`. While it runs, the same details show its `phase` — `listing`, `merging`, `publishing`, `checking` when it checks materialization, then `done` — and `objects_listed`, updated every 5,000 objects the listing returns. A listing has no known total, so `progress` stays at zero until the merge begins and then follows the share of the listing merged. A producer that knows exactly which files it wrote should commit them directly: a commit costs nothing proportional to the size of the prefix.

Authorization is checked when the import is queued. The task then runs with the catalog's own authority, as a purge does, so revoking the caller's access afterwards does not stop it. `POST /management/v1/warehouse/{warehouse_id}/task/control` does: `stop` ends the import and queues it to run again, and `cancel` ends it for good. Either reaches a running import within 5,000 objects listed or merged, and neither publishes anything: what the import staged is discarded, and the branch stays where it was. Once the import has published, neither undoes it: it runs its materialization check to the end, and a cancelled task leaves no record of the outcome.

## Retention

Retention expires old snapshots, keeps them restorable for a grace period, then purges them. A warehouse sets the policy its datasets follow, and a dataset may [set its own](#a-datasets-own-policy). The warehouse's is **off** until enabled, through the task-queue config of `dataset_snapshot_expiry`:

- **GET** `/management/v1/warehouse/{warehouse_id}/task-queue/dataset_snapshot_expiry/config`
- **POST** `/management/v1/warehouse/{warehouse_id}/task-queue/dataset_snapshot_expiry/config`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enabled` | boolean | `false` | Expire snapshots automatically in this warehouse |
| `max-snapshot-age` | ISO 8601 duration | `P5D` | How old a snapshot must be to expire |
| `min-snapshots-to-keep` | integer | `1` | The newest snapshots of each branch that never expire, whatever their age; below 1 it is 1 |
| `grace-period` | ISO 8601 duration | `P7D` | How long an expired snapshot stays restorable before it is purged |

What retention never expires: a snapshot a tag points at (tags protect their snapshots outright), the newest `min-snapshots-to-keep` of each branch and any younger than `max-snapshot-age`, anything a ref points at, and anything a live [access grant](#signed-urls) reads. A snapshot no ref reaches — a deleted branch's or tag's — expires once it is older than `max-snapshot-age`.

A commit, an import, a ref deletion or a policy change queues a retention pass an hour later, and the writes of that hour share it. A pass that keeps a snapshot only because it is younger than `max-snapshot-age`, or because a live grant reads it, queues the next pass for an hour after that changes, so the history a quiet dataset already has still ages out. A change to the warehouse's policy reaches a dataset at its next write or policy change.

An expired snapshot is hidden: no ref points at it, so nothing lists it or grants access to it, and naming it by id — in a diff, or to create a ref — answers `404`. `POST .../datasets/{dataset}/snapshots/{snapshot}/restore` brings it back before the grace period ends; it takes `restore_snapshots`, which derives from grant authority. Create a ref to the restored snapshot to keep it, or a later pass may expire it again. Recreating a deleted branch works the same way: restore its last snapshot if it expired, then create the branch at it.

`POST .../datasets/{dataset}/snapshots/{snapshot}/expire` expires one snapshot now, whatever the policy, and answers with its `purge-after`: now plus the dataset's grace period, the earliest the purge deletes it. Asking again for a snapshot already expired answers the same `purge-after`. A snapshot a ref points at, or a live access grant reads, is refused with `409 DatasetSnapshotHeld`; delete the ref or revoke the grant first. It takes `expire_snapshots`, which derives from grant authority.

Once the grace period is over, the `dataset_snapshot_purge` queue deletes the snapshot and its manifest rows. A purge already pending keeps its time, so a snapshot expired with a shorter grace period than it waits for it. A surviving snapshot that reconstructed through a purged one is first folded into a checkpoint, so every tag and branch reads exactly the files it read before. A snapshot that a live grant reads waits for a later purge, and so does every snapshot of a dataset an import is staging into. A purge that leaves expired snapshots for later runs again a day later at the soonest, so one fold covers a day of expiries.

Purge cuts history: a surviving snapshot keeps no link to the purged ones below it. A fast-forward whose path crosses purged history is refused as not a descendant; a reset still moves the branch.

### A dataset's own policy

`POST .../datasets/{dataset}/settings` with `{"retention": {"mode": "…", …}}` gives a dataset a policy of its own, which applies whether or not the warehouse's is enabled:

| `mode` | Fields | Expires automatically |
|---|---|---|
| `inherit` | — | What the warehouse's policy expires. The default; setting it drops the dataset's own policy |
| `manual` | `grace-period` | Nothing: a snapshot expires only when [expired by hand](#retention) |
| `ttl` | `max-snapshot-age`, `min-snapshots-to-keep`, `grace-period` | Snapshots older than `max-snapshot-age`, beyond the newest `min-snapshots-to-keep` (default `1`) of each branch |
| `max-count` | `max-snapshots`, `grace-period` | Every snapshot beyond the newest `max-snapshots` of each branch, whatever its age; `max-snapshots` must be at least `1` |

A `grace-period` left out is the warehouse's. In every mode, a snapshot a ref points at or a live grant reads does not expire. Changing a policy queues a retention pass an hour out, as a commit does, so it applies to a dataset nobody writes to as well. The dataset's load response carries the policy under `retention`, absent while the dataset inherits. A policy that keeps no head (`max-snapshots` or `min-snapshots-to-keep` of `0`), or that names a duration over 100 years, is refused with `400 InvalidRetention`.

Setting a policy takes `update_retention`, which derives from grant authority, as expiring by hand does: plain write access would let anyone who can commit set a policy that purges the dataset's history. Changing `constraints` takes `update_settings`, and a request that changes both takes both.

## Events

Datasets publish CloudEvents to the configured event stream — [NATS](./configuration.md#nats) or [Kafka](./configuration.md#kafka) — as tables and views do, so a pipeline can start when a new version lands, without polling. Each event names the dataset, its namespace and warehouse, and the actor.

| Type | Published when | Data |
|------|----------------|------|
| `createDataset`, `renameDataset` | The dataset is created or renamed | The request |
| `dropDataset` | The dataset is dropped | None |
| `commitDataset` | A commit or an import publishes a snapshot | `branch`, `snapshot-id`, `parent-snapshot-id`, the `added`, `modified` and `removed` counts, and the commit's `summary` |
| `createDatasetRef` | A branch or tag is created | `name`, `type`, `snapshot-id` |
| `updateDatasetRef` | A branch is fast-forwarded or reset | `name`, `snapshot-id`, and `fast-forward`, which is `false` for a reset |
| `deleteDatasetRef` | A branch or tag is deleted | `name` |
| `updateDatasetSettings` | The dataset's [constraints](#constraints) or [retention policy](#a-datasets-own-policy) change | The request |

`commitDataset` carries counts, never the file list; read the files through the snapshot id. An import that changes nothing publishes no snapshot and no event. A queued import's event names Lakekeeper itself as the actor, since the task runs with the catalog's own authority.

## Capabilities

Datasets are a subtype of the same tabular resource as tables, views and generic tables, so most of Lakekeeper's resource machinery applies unchanged:

| Feature | Datasets | Notes |
|---------|:--------:|-------|
| Credentials vending (S3, GCS, Azure) | :white_check_mark: | Scoped to the dataset's prefix |
| Signed URLs (S3, GCS, Azure) | :white_check_mark: | Per file, under a revocable access grant; see [Signed URLs](#signed-urls) |
| Remote signing | :x: | Readers get [signed URLs](#signed-urls) |
| [Soft-deletion](./concepts.md#soft-deletion) + undrop | :white_check_mark: | Respects the warehouse's soft-delete settings |
| [Protection](./concepts.md#protection) flag | :white_check_mark: | `GET/POST /management/v1/warehouse/{wh}/dataset/{id}/protection` |
| Rename, including across namespaces | :white_check_mark: | `POST /lakekeeper/v1/{prefix}/datasets/rename` |
| [Governance tags](./tags.md) | :white_check_mark: | `/management/v1/warehouse/{wh}/dataset/{id}/tags` |
| [Grants](./grants.md) | :white_check_mark: | `/management/v1/warehouse/{wh}/dataset/{id}/grants` |
| Permission checks | :white_check_mark: | `GET /management/v1/warehouse/{wh}/dataset/{id}/actions`, or a `dataset` operation in `POST /management/v1/action/batch-check` |
| Name uniqueness across types | :white_check_mark: | A dataset cannot share a name with a table, view or generic table in the same Namespace |
| Versioned history | :white_check_mark: | Snapshots, branches, tags, fast-forward, reset, [diff](#comparing-versions) |
| [Retention](#retention) | :white_check_mark: | Off by default; per-dataset policies, expire by hand, restore within a grace period, purge |
| Server-side import | :white_check_mark: | Inline or on the task queue |

## Authorization model

Datasets have their own OpenFGA type, `lakekeeper_dataset`, parallel to `lakekeeper_table` and `lakekeeper_generic_table`. Privileges inherit from the parent Namespace and Warehouse and can be granted through the [Grants API](./grants.md). Beside the usual metadata, data and lifecycle actions, datasets add four versioning actions:

| Action | Derived from | Allows |
|--------|--------------|--------|
| `commit` | `modify` | Append a snapshot and move a branch forward |
| `manage_refs` | `manage_refs` | Create, delete and protect branches and tags |
| `promote` | `modify` | Fast-forward a branch — the only way changes reach a protected branch |
| `reset` | `manage_refs` | Move a branch to a snapshot that does not descend from its head, abandoning commits |

`manage_refs` is a privilege of its own, granted apart from `modify`: a writer can commit, but can neither delete a tag and recreate it elsewhere nor unprotect a branch to commit to it directly. It inherits down the hierarchy, so one grant on a namespace, beside `describe` to see the datasets, lets a fleet of agents create and delete their own branches in every dataset below it; it lets them reset any branch, `main` included, as well. The dataset's owner holds it, as does a project's `data_admin`.

`reset` derives from `manage_refs`: it is ref management, like creating, deleting and protecting refs, and it reaches `main` too, which cannot be deleted but can be reset. Protection is a guard against mistakes, as it is for a table's deletion protection, not a permission: whoever holds `manage_refs` can lift it. A rule such as "nobody rewinds `main`" belongs in a policy-based authorizer, which sees the branch in `target-refs`, as it does for an Iceberg table's commits. Dropping a dataset is a write, as for every tabular: a writer can lift its deletion protection and drop it. What takes grant authority is losing history silently while the dataset lives on: a retention policy, or expiring and restoring snapshots by hand.

`revoke_access_grants`, also from `manage_grants`, revokes an access grant another caller obtained; see [Signed URLs](#signed-urls). `restore_snapshots`, from `manage_grants` too, brings back a snapshot retention expired, and `expire_snapshots` expires one by hand; see [Retention](#retention). `update_settings`, from `modify`, changes the dataset's [constraints](#constraints). `update_retention`, from `manage_grants`, sets its [retention policy](#a-datasets-own-policy).

Refs are not authorization objects: in the OpenFGA model the versioning actions apply to every ref of the dataset, and a branch's `protected` flag is the only per-branch control. Creating and deleting branches leaves no tuples to clean up. Each versioning action still names the ref it acts on, in the same `target-refs` field a table commit carries, so a policy-based authorizer can decide per branch or tag, and the [audit log](./logging.md) records which ref was touched. Snapshot ids within a dataset are not an authorization boundary: a diff and a materialization report name a snapshot directly, under `read_data` with no ref, and a file listing's page token re-reads the snapshot its first page resolved, under the ref the request names. A policy that decides per ref decides what a read naming no ref allows too.

## Limits

- **Retention deletes records, not files.** Purging an expired snapshot removes it and its manifest from the catalog; its objects stay in storage.
- **Revoking a grant does not recall URLs it already signed.** They work until they expire. Lower `LAKEKEEPER__DATASET_SIGNED_URL_VALIDITY_SECONDS` if that window is too long.
- **Removing a file does not erase it.** A removal applies to later snapshots; earlier snapshots, and tags on them, still resolve to the file, and its bytes stay in storage. Do not store data you may be obliged to erase — personal data in particular — in a dataset.
- **The API is new.** Endpoints and payloads under `/lakekeeper/v1/{prefix}/.../datasets` may still change.
