# Lakekeeper Plus Release Notes

## v0.13.1 (2026-07-20)

_Based on Lakekeeper OSS v0.13.3._

### Highlights
- **Okta role provider.** Resolve a user's Okta group memberships to Lakekeeper roles via the Okta management API — private-key-JWT client auth with DPoP (RFC 9449) proofs enabled by default.
- **Provider-synced roles are now protected.** Roles owned by a configured role provider (Okta, Entra, LDAP, or token IdP) can no longer be mutated through the management API, so the next provider sync can't silently clobber manual edits.

### Features
- **Okta role provider (with DPoP).** Group memberships resolved via `GET /users/{id}/groups` (Link-header pagination, keyed by immutable group id). OAuth2 client-credentials + private-key-JWT (JWK or PEM key); DPoP on by default with ephemeral P-256 proofs and nonce challenge/replay handling — opt out to Bearer. Requires the `okta.users.read` scope; wrapped in the shared role cache. See the Okta role-provider docs.
- **Managed-role write protection.** The Cedar authorizer now reports its configured provider namespaces to the management-API guard, so create / update / delete / source-system rebind / member (un)assignment on a provider-owned role is rejected with `400 ManagedRoleImmutable`. Native `lakekeeper` roles and the reserved `system` namespace are never included, so API-native and catalog-managed roles stay writable. Active only when a role provider is configured.
- **Post-logout redirect controls.** Two new UI env vars — `LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_URL` and `LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_DISABLED`.
- **Static-asset caching in the UI server.** Per-class `Cache-Control` plus weak `ETag`/`304` on bundled assets: content-hashed `assets/*` are cached immutably and the DuckDB WASM is no longer re-downloaded on every load, while `index.html` stays uncached so runtime config placeholders remain fresh.

### Bug Fixes
- **Maintenance page for warehouse-only permissions.** Console bump to 0.16.1 (console-components 0.17.1) fixes the maintenance view for users who hold only warehouse-level permissions.

### Upgrade Notes
- **Provider-role edits now return `400 ManagedRoleImmutable`.** If you previously edited provider-synced roles (Okta/Entra/LDAP/token) through the management API, those calls are now rejected — such edits were overwritten by the next sync anyway. Manage those roles at the source. No migration.
- **Building Plus from source:** the Kubernetes client stack moved to k8s-openapi 0.28 / kube 4.0 (pulled in by the upstream limes 0.4.2 bump). Prebuilt binaries and images are unaffected.

### Upstream Lakekeeper changes (up to Lakekeeper v0.13.3)
Notable for Plus users:
- **Configurable Kubernetes subject source.** `LAKEKEEPER__KUBERNETES_AUTHENTICATION_SUBJECT_SOURCE=username` derives a service account's Lakekeeper user id from `system:serviceaccount:<namespace>:<name>` (stable across clusters) instead of the per-cluster `uid` (default, unchanged), so Kubernetes roles and instance admins can be pre-provisioned ([lakekeeper#1899](https://github.com/lakekeeper/lakekeeper/pull/1899)).
- **Reject role writes in provider-managed namespaces** — the upstream API guard behind the managed-role protection above ([lakekeeper#1891](https://github.com/lakekeeper/lakekeeper/pull/1891)).
- Stop evaluating a discarded `Select` on target-view load, avoiding a spurious authorization check ([lakekeeper#1886](https://github.com/lakekeeper/lakekeeper/pull/1886)).

## v0.13.0 (2026-07-02)

_Based on Lakekeeper OSS v0.13.1._

### Highlights
- **Microsoft Entra ID (Graph) role provider.** Resolve a user's transitive Entra group memberships into Lakekeeper roles — with secret, certificate, managed-identity, and workload-identity credentials, sovereign-cloud support, and built-in throttling/retry.
- **External admission gate.** A new post-authentication seam can ask your control plane whether an already-authenticated caller may use this instance — for IdPs that issue broad, non-instance-scoped tokens — and contribute the caller's resolved roles.
- **Console overhaul.** The bundled UI jumps to v0.13.2: a Files/storage explorer with in-browser Parquet/Avro/CSV preview, per-entity action menus, datasets as a first-class entity, redesigned view and table-health pages, a Role Members tab, and an enterprise usage-report builder.

### Features
- **Entra ID / Microsoft Graph role provider.** Paged `transitiveMemberOf` resolution; credential methods secret / certificate / managed-identity / workload-identity; public, US-gov, and China clouds; retries on 429 (honoring `Retry-After`) and transient 5xx.
- **AD range retrieval for LDAP attribute-mode groups.** Active Directory returns >1500 group values under a ranged `memberOf;range=…` key; attribute mode now walks the range windows, so users in many groups are no longer silently truncated. OpenLDAP/389-DS behavior is unchanged.
- **External enforce-endpoint admission gate** (`lakekeeper-admission-enforce`). Configurable named checks POST to your endpoint; the HTTP status is the decision (2xx allows and grants the check's role, `403` denies, anything else fails closed with `503` + `Retry-After`). Allow and deny are both cached; caller bearer-token relay is opt-in and never logged.
- **Persist OIDC token roles for DEFINER views.** Opt-in via `LAKEKEEPER__ROLE_PROVIDER_CHAIN__PERSIST_TOKEN_ROLES` (default off) — mirrors a user's OIDC-token roles into the catalog so authorization can evaluate them when the user isn't the live caller, e.g. a DEFINER view running as its owner. Write-gated; no migration.
- **Generic-table parity in Cedar authorization.** Non-Iceberg generic tables (e.g. Lance, Delta) now resolve and authorize through the Cedar surface exactly like tables and views.
- **Destructive-delete context for Cedar policies.** `force` / `purge` / `recursive` and a warehouse `soft_delete_enabled` attribute are now in the Cedar request context, so a policy can forbid hard deletes that would bypass configured soft-deletion.
- **Role-membership actions in Cedar.** The new manage/read role-assignment actions map to dedicated fine-grained Cedar actions, so policy authors control their bundling.
- **Destination-aware role source-system rebind.** A dedicated `update_source_system` Cedar action exposes the target provider/source, so rebinds can be gated by destination — something the coarse upstream OpenFGA relation cannot express.
- **Per-decision policy trace in authorization audit.** Audit events and the `/check` endpoint now record which Cedar policies determined each allow/deny outcome.
- **Schedule maintenance directly.** `expire_snapshots` and `remove_orphan_files` can be triggered per table via the task-queue schedule endpoint, without waiting for a commit hook.

### Bug Fixes
- **Corrupt-manifest orphan-files task no longer retries forever.** A permanent failure (e.g. a corrupt Avro manifest) is now classified permanent and not requeued, instead of failing silently and re-running every day. Maintenance workers (`remove_orphan_files`, `expire_snapshots`) also persist a readable failure reason, surfaced in the task-details API — no server-log access required.

### Breaking Changes
- **Default storage layout is now flat** (inherited from upstream Lakekeeper 0.13): new namespaces use `<base>/<tabular-uuid>` instead of nesting tabulars under the parent-namespace UUID. Not retroactive — existing namespaces and paths are unchanged — so explicitly configure the full-hierarchy layout if you need the old behavior for new namespaces ([lakekeeper#1853](https://github.com/lakekeeper/lakekeeper/pull/1853)).

### Upgrade Notes
- **Encrypted tables are skipped by maintenance.** `expire_snapshots` and `remove_orphan_files` now detect Iceberg native encryption (format v3) via the immutable `encryption.key-id` property and skip such tables — Lakekeeper cannot read their encrypted manifests, and processing anyway risked deleting live data. Manually scheduling either task on an encrypted table returns `400`.
- **Downgrade protection** (upstream): `serve` refuses to start against a database already migrated by a newer binary. After a rollback, start the older binary with `serve --force-start`, accepting the schema-incompatibility risk ([lakekeeper#1861](https://github.com/lakekeeper/lakekeeper/pull/1861)).
- **Docker base images** moved from Debian 12 (bookworm) to Debian 13 (trixie).
- **Building Plus from source:** the catalog Postgres backend and the NATS/Kafka event backends are now separate upstream crates (`lakekeeper-storage-postgres`, `lakekeeper-events-nats`, `lakekeeper-events-kafka`). Prebuilt binaries and images are unaffected ([lakekeeper#1812](https://github.com/lakekeeper/lakekeeper/pull/1812), [lakekeeper#1814](https://github.com/lakekeeper/lakekeeper/pull/1814)).

### Upstream Lakekeeper changes (up to Lakekeeper v0.13.1)
Rolls up OSS **v0.12.4**, **v0.13.0**, and **v0.13.1** (full list in the [Lakekeeper release notes](https://docs.lakekeeper.io/about/release-notes/)). Notable for Plus users:
- **Generic Table API** — register non-Iceberg tables (Lance, Delta) as first-class generic tables with credential vending and full authorization ([lakekeeper#1673](https://github.com/lakekeeper/lakekeeper/pull/1673), [lakekeeper#1813](https://github.com/lakekeeper/lakekeeper/pull/1813)); surfaced in Plus through the Cedar generic-table parity above.
- **Operator-owned warehouses.** A `managed_by` marker locks warehouse spec mutations (delete, rename, (de)activate, storage profile, protection, format-version policy) to instance admins ([lakekeeper#1828](https://github.com/lakekeeper/lakekeeper/pull/1828)).
- **Authorizer-independent role-membership API** — one management surface to list/add/remove a role's members regardless of the configured authorizer ([lakekeeper#1829](https://github.com/lakekeeper/lakekeeper/pull/1829)).
- **Multiple OIDC providers** at once via `LAKEKEEPER__OPENID_PROVIDERS` (e.g. Okta for users + a cloud issuer for service accounts) ([lakekeeper#1760](https://github.com/lakekeeper/lakekeeper/pull/1760)).
- **Microsoft OneLake / Fabric storage** profile, including workspace private-link endpoints ([lakekeeper#1852](https://github.com/lakekeeper/lakekeeper/pull/1852)).
- **Per-warehouse table format-version policy** — allowed Iceberg format versions and an optional default per warehouse ([lakekeeper#1786](https://github.com/lakekeeper/lakekeeper/pull/1786)).
- **Customer-managed KMS encryption** — warehouses with `aws-kms-key-arn` advertise `s3.sse.type=kms`, so vended-credential writes use your KMS key ([lakekeeper#1847](https://github.com/lakekeeper/lakekeeper/pull/1847)).
- **Cache hardening for large fleets** — single-flight read-throughs and TTL jitter cut thundering-herd load on the database and on rate-limited STS/SAS endpoints ([lakekeeper#1833](https://github.com/lakekeeper/lakekeeper/pull/1833), [lakekeeper#1837](https://github.com/lakekeeper/lakekeeper/pull/1837)).
- `/health` now returns `503` (not `200`) when unhealthy, so Kubernetes HTTP probes detect it ([lakekeeper#1802](https://github.com/lakekeeper/lakekeeper/pull/1802)).
- Postgres migration locks are transaction-scoped, so a failed migration can't leak an advisory lock that blocks future migrations ([lakekeeper#1790](https://github.com/lakekeeper/lakekeeper/pull/1790)).

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
