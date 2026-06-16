# Lakekeeper Plus Release Notes

## v0.12.2 (2026-05-26)

### Highlights
- Orphan-file cleanup now schedules itself adaptively per table — running more often where files accumulate and backing off where they don't — with a new dry-run mode.
- The LDAP role provider can resolve groups via subtree **Search** and conditional **Branching**, not just the `memberOf` attribute.

### Features
- **Adaptive orphan-file scheduling.** The remove-orphan-files worker now self-tunes its cadence based on how fast reclaimable data builds up, and adds a dry-run mode that reports what it *would* delete. Orphan removal is now opt-in via `enable-remove-orphan-files`; default retention raised from 3 to 7 days. See the Table Maintenance docs for full config.
- **LDAP group resolution modes.** Resolve group memberships via `Search` (paged subtree) or `Branching` (per-user-DN rules) in addition to the `memberOf` attribute; the resolution mode is recorded in audit logs.
- **Build metadata in Server Info.** Server Info now reports Lakekeeper, Enterprise, and Console versions and commit SHAs, so deployed builds are easy to identify.

### Breaking Changes
- The orphan-files task-queue API was renamed `remove_orphaned_files` → `remove_orphan_files` (paths, config schemas, and worker/enable fields).

### Upgrade Notes
- Update any automation/IaC to the new `remove_orphan_files` task-queue path and schema names.
- Orphan removal is now **opt-in** (it ran by default in 0.12.1): set `enable-remove-orphan-files=true` to keep it active. Default retention is now 7 days.

### Upstream Lakekeeper changes (bump to v0.12.3)
- Core and extension database migrations now apply atomically — no partial-migration state ([lakekeeper#1768](https://github.com/lakekeeper/lakekeeper/pull/1768)).
- Fixed table property removal being lost when no properties remained ([lakekeeper#1767](https://github.com/lakekeeper/lakekeeper/pull/1767)).
- New read-only maintenance mode for the server ([lakekeeper#1765](https://github.com/lakekeeper/lakekeeper/pull/1765)).
- Users may now share an email address — the unique-email constraint was dropped ([lakekeeper#1755](https://github.com/lakekeeper/lakekeeper/pull/1755)).
- New `LAKEKEEPER__UI__ENABLE_SURVEYS` flag to opt out of in-console surveys ([lakekeeper#1750](https://github.com/lakekeeper/lakekeeper/pull/1750)).

## v0.12.1 (2026-05-10)

### Features
- **Remove Orphan Files.** New maintenance capability that reclaims storage by deleting data, manifest, and metadata files no longer referenced by any snapshot — available as a server background worker and as a `remove-orphan-files` subcommand (with dry-run). Enabled by default; set `LAKEKEEPER__TASK_REMOVE_ORPHANED_FILES_WORKERS=0` to disable. Respects `gc.enabled` and per-table opt-out properties. See the Table Maintenance docs for full config.
- **Bounded orphan-files runtime.** Cap how long a single orphan-files run may take with a configurable max run time.

### Bug Fixes
- Warehouse rename no longer leaves a stale name in the UI table preview (bundled UI 0.7.12).

### Upgrade Notes
- The orphan-files worker is **enabled by default** (2 workers); set `LAKEKEEPER__TASK_REMOVE_ORPHANED_FILES_WORKERS=0` to disable. By default it only deletes files older than 3 days and honors `gc.enabled` / per-table opt-out.

### Upstream Lakekeeper changes (bump to v0.12.2)
- OpenFGA: rebuild/reconcile authorization tuples from the catalog, and support switching an existing server to OpenFGA ([lakekeeper#1731](https://github.com/lakekeeper/lakekeeper/pull/1731), [lakekeeper#1733](https://github.com/lakekeeper/lakekeeper/pull/1733)).
- OPA Trino batch authorization gains a broad-access fast path for warehouses/namespaces ([lakekeeper#1727](https://github.com/lakekeeper/lakekeeper/pull/1727)).
- Storage: dropped the opendal dependency and now validates vended credentials via `lakekeeper_io` ([lakekeeper#1737](https://github.com/lakekeeper/lakekeeper/pull/1737)).
- ADLS fixes: correct SAS-token key removal and `%`-encoding in blob names ([lakekeeper#1746](https://github.com/lakekeeper/lakekeeper/pull/1746)).

## v0.12.0 (2026-04-21)

### Highlights
- **Cedar authorization matured** into a configurable, inspectable system: derive user attributes from identity fields, reference roles by global ID, and use a new resolve-entities API + Console tabs to see exactly what drives a decision.
- **Role providers** resolve user roles from external sources — including LDAP groups and table properties — with caching, metrics, and audit.
- **Console**: a visual Cedar Policy Builder (beta) with a Cedar-aware editor, authorization-inspection tabs, and new statistics dashboards.
- **Container images now default to `ubi10`** (breaking — see below).

### Features
- **Cedar user identity derivations.** Extract attributes from identity fields with named-capture regex rules (optional `lowercase`/`uppercase` transform) and match policies on the derived values.
- **Global role IDs in policies.** Reference provider-scoped global role IDs as Cedar property values, and use short-form roles without a default provider.
- **Resolve-entities API.** `POST /management/v1/permissions/cedar/resolve-entities` returns the Cedar entities for any resource — for debugging why a decision was reached.
- **SelectView action.** Adds `select` / `SelectView` (and `grant_select`) for views, aligning with the upstream data-plane authorization split.
- **Role-provider subsystem.** LDAP Group Provider + token-provider chain, roles parsed from table properties, caching with stale-fallback and metrics, and an opt-in audit event for resolved roles — wired into the Cedar authorizer. Configure via `ROLE_PROVIDER_FILE` (TOML), overridable per-field by env vars.
- **Richer permission-introspection audit.** `introspect_permissions` logs now include the inner check tuples and their individual decisions.
- **Console: Cedar Policy Builder (beta).** Visual editor/builder with a CodeMirror Cedar editor (highlighting, autocomplete, inline diagnostics, format/validate via cedar-wasm) and live Evaluate.
- **Console: authorization inspection + dashboards.** Tabs for entity/policy sources, schema, and resolve-entities; new Home and Warehouse statistics dashboards; storage-layout configuration.

### Bug Fixes
- Cedar: correctness fixes around short-form role tags, per-request provider-ID derivation, Role subjects, and resolve-entities server gating.
- Maintenance: expire-snapshots now removes statistics / partition-statistics from metadata, avoiding dangling references to deleted files.
- TLS: added webpki and native root certs to the S3 client and UBI images, fixing handshake failures in some environments.
- Console: correct handling of sub-namespaces containing dots; per-tab 403 keeps the navigation rail visible.

### Breaking Changes
- Container images now default to **`ubi10`**; the `ubi9`-based image remains available under a separate tag.

### Upgrade Notes
- If you pin the `ubi9` base image (e.g. FIPS/compliance), switch to the dedicated `ubi9` tag — the default is now `ubi10`.
- Role-provider config can be supplied via `ROLE_PROVIDER_FILE` (TOML), with env vars overriding per field — review precedence if you set both.

### Upstream Lakekeeper changes
- **Instance Admins** — server-wide admin role independent of project membership ([lakekeeper#1716](https://github.com/lakekeeper/lakekeeper/pull/1716)).
- **Idempotency keys** for safely retrying mutating requests ([lakekeeper#1671](https://github.com/lakekeeper/lakekeeper/pull/1671)).
- **`referenced-by`** to discover views referencing a table/view ([lakekeeper#1627](https://github.com/lakekeeper/lakekeeper/pull/1627)).
- Configurable **trusted engines** in request metadata for authorization ([lakekeeper#1629](https://github.com/lakekeeper/lakekeeper/pull/1629)).
- **Protect immutable table properties** (e.g. `encryption.key-id`) during commits ([lakekeeper#1700](https://github.com/lakekeeper/lakekeeper/pull/1700)).
- Faster list namespaces/tables/views ([lakekeeper#1618](https://github.com/lakekeeper/lakekeeper/pull/1618)).
