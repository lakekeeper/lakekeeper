# Release Notes

Highlights for each Lakekeeper release. For the full commit-level
changelog, see the [GitHub Releases](https://github.com/lakekeeper/lakekeeper/releases)
or [`CHANGELOG.md`](https://github.com/lakekeeper/lakekeeper/blob/main/CHANGELOG.md).

For Lakekeeper Plus releases, see the [Lakekeeper Plus Release Notes](enterprise-release-notes.md).

<!-- Maintainers: how to update this page at release → .github/RELEASING.md -->

## v0.12.3 (2026-05-26)

### Features
- **Read-only maintenance mode.** Set `LAKEKEEPER__MAINTENANCE_MODE=read-only` to reject mutating requests with `503` + `Retry-After` during planned maintenance ([#1765](https://github.com/lakekeeper/lakekeeper/pull/1765)).
- **Atomic core + extension migrations.** Schema migrations now apply in a single transaction, so an interrupted upgrade can't leave a half-migrated database ([aa734bf](https://github.com/lakekeeper/lakekeeper/commit/aa734bffcacd98566aa670f62341a4455833496c)).
- **Users may share an email address.** The unique-email constraint was dropped, so multiple users can have the same email ([#1755](https://github.com/lakekeeper/lakekeeper/pull/1755)).
- **Survey opt-out.** New `LAKEKEEPER__UI__ENABLE_SURVEYS` flag disables in-console surveys and their third-party requests ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf)).
- **Reserved `system` role provider** for catalog-managed roles ([#1776](https://github.com/lakekeeper/lakekeeper/pull/1776)).
- **Console.** New "Export for GitHub" support bundle (server info + UI config, no tokens), a Feedback button, role-provider IDs in the overview, and a two-column Server Settings layout ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf)).

### Bug Fixes
- Fixed table property removal being lost when no properties remained ([#1767](https://github.com/lakekeeper/lakekeeper/pull/1767)).
- Views now preserve protection (`protected=true`) across commits — previously lost on update (thanks @fallintoplace) ([#1770](https://github.com/lakekeeper/lakekeeper/pull/1770)).
- `force=true` is now respected when dropping soft-deletion warehouses that contain views ([#1779](https://github.com/lakekeeper/lakekeeper/pull/1779)).
- Fixed a memory leak from stale Vault (KV2) health status (thanks @fallintoplace) ([#1773](https://github.com/lakekeeper/lakekeeper/pull/1773)).
- Postgres: rewrote the namespace trigger so `pg_restore` can replay it ([#1781](https://github.com/lakekeeper/lakekeeper/pull/1781)).

### Upgrade Notes
- Minimum supported Rust version (MSRV) raised to 1.94 — affects building from source.

## v0.12.2 (2026-05-10)

### Features
- Storage locations are canonicalised at parse time to avoid path aliases ([#1743](https://github.com/lakekeeper/lakekeeper/pull/1743)).
- Object size is now exposed on `FileInfo` ([#1741](https://github.com/lakekeeper/lakekeeper/pull/1741)).

### Bug Fixes
- **Security:** hardened S3 STS/CEL credential-vending policies against path injection ([#1740](https://github.com/lakekeeper/lakekeeper/pull/1740)).
- Azure (ADLS): pre-encode `%` in blob names so the SDK no longer collapses distinct paths onto the same alias ([#1746](https://github.com/lakekeeper/lakekeeper/pull/1746)).
- Postgres: apply `pg_acquire_timeout` to all connection-pool initialisations ([#1744](https://github.com/lakekeeper/lakekeeper/pull/1744)).

## v0.12.1 (2026-05-04)

### Highlights
- **Instance Admins.** Designate break-glass principals that bypass control-plane authorization for management actions via `LAKEKEEPER__INSTANCE_ADMINS` (a list of `<idp_id>~<subject>` IDs). The bypass excludes data-plane operations and role-assumed requests ([#1716](https://github.com/lakekeeper/lakekeeper/pull/1716)).
- **Safe switch to OpenFGA.** Existing deployments can adopt or rebuild OpenFGA: `openfga reconcile` rebuilds hierarchy tuples from the catalog (with dry-run and drift-deletion), and `reopen-bootstrap` re-enables bootstrap for recovery ([#1731](https://github.com/lakekeeper/lakekeeper/pull/1731), [#1733](https://github.com/lakekeeper/lakekeeper/pull/1733)).
- **OpenDAL dropped.** Storage I/O now goes exclusively through the hyperscaler-native backends wrapped by `lakekeeper-io`, including vended-credential validation ([#1737](https://github.com/lakekeeper/lakekeeper/pull/1737)).
- **Security.** Upgraded `rustls-webpki` to 0.103.12 for RUSTSEC-2026-0098 ([#1713](https://github.com/lakekeeper/lakekeeper/pull/1713)).

### Features
- **Trusted engines for views (`referenced-by`).** Validates the `referenced-by` parameter and resolves view-on-view chains for batch authorization, enabling secure DEFINER-style execution for trusted query engines — configured under `LAKEKEEPER__TRUSTED_ENGINES__<NAME>` ([#1647](https://github.com/lakekeeper/lakekeeper/pull/1647)).
- **Protected security-relevant properties.** Only a matched trusted engine may set or remove view owner / run-as properties (case variants rejected), and commits can no longer overwrite immutable table properties such as `encryption.key-id` ([#1700](https://github.com/lakekeeper/lakekeeper/pull/1700), [#1724](https://github.com/lakekeeper/lakekeeper/pull/1724)).
- **Richer audit.** `introspect_permission` events now include the inner check tuples and their decisions, and events record whether access was granted internally, via an instance admin, or by the authorizer ([#1697](https://github.com/lakekeeper/lakekeeper/pull/1697)).
- **Data-plane `Select` action for views**, plus a public `resolve_principal` API for downstream API-to-authz `UserOrRole` conversion ([#1721](https://github.com/lakekeeper/lakekeeper/pull/1721), [#1703](https://github.com/lakekeeper/lakekeeper/pull/1703)).
- **OPA bridge.** View-on-view queries via `CreateViewWithSelectFromColumns`, the Trino `ADD_FILES` operation, and a warehouse/namespace broad-access fast path for batch authorization ([#1712](https://github.com/lakekeeper/lakekeeper/pull/1712), [#1727](https://github.com/lakekeeper/lakekeeper/pull/1727)).
- **Extended Server Info** with console information and commit SHAs ([#1725](https://github.com/lakekeeper/lakekeeper/pull/1725)).

### Bug Fixes
- Added `webpki_root_certs` / UBI native certs to the S3 client to fix TLS trust issues ([#1720](https://github.com/lakekeeper/lakekeeper/pull/1720)).
- Namespace/table case handling: allow renaming a table to a different case of its own name; lookups return the caller's case, ID lookups the canonical case ([7c26309](https://github.com/lakekeeper/lakekeeper/commit/7c263091f255b75ed5d66024b5bc6b29ef553508)).
- Pinned `gcloud-storage` / `gcloud-auth` to `~1.2` to avoid a `reqwest-middleware` conflict ([#1701](https://github.com/lakekeeper/lakekeeper/pull/1701)).
- ADLS: remove the actual matched SAS token key rather than its prefix ([76a091b](https://github.com/lakekeeper/lakekeeper/commit/76a091b9b01ba507cc448a56241d54f526c19a14)).
- Console: fixed base-URL trailing slash, Vite 8 authentication breakage, and a stale warehouse name after rename ([#1729](https://github.com/lakekeeper/lakekeeper/pull/1729), [#1723](https://github.com/lakekeeper/lakekeeper/pull/1723)).

### Upgrade Notes
- **Switching to OpenFGA on an existing instance is now safe:** run `openfga reconcile` (dry-run first; drift-deletion mode to also remove stale tuples), and `reopen-bootstrap` to re-enter bootstrap if needed. The minimum required OpenFGA version was raised.
- **OpenDAL removed** — storage now relies solely on the native S3/GCS/ADLS backends in `lakekeeper-io`; re-verify storage and vended-credential config after upgrade.
- Deploying into a custom Postgres schema is now supported and documented ([#1714](https://github.com/lakekeeper/lakekeeper/pull/1714)).

## v0.12.0 (2026-04-01)

### Highlights
- **Audit Event System.** Authorization decisions and catalog operations emit dedicated audit events with exactly-once-per-call delivery, giving a reliable trail of who did what ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5)).
- **Idempotency keys for safe retries.** Send an `Idempotency-Key` header on mutating requests and Lakekeeper replays the original response instead of applying the change twice — on by default, scoped per warehouse, with a 30-minute key lifetime ([#1671](https://github.com/lakekeeper/lakekeeper/pull/1671)).
- **Customizable storage layouts.** Choose how namespace/table paths are templated on S3/GCS/ADLS using `{uuid}` / `{name}` placeholders ([#1615](https://github.com/lakekeeper/lakekeeper/pull/1615), [#1628](https://github.com/lakekeeper/lakekeeper/pull/1628)).
- **Structured JSON logs.** Log output is now structured JSON with objects as field values, ready for log-pipeline ingestion ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5)).

### Features
- **Configurable STS endpoint.** Set a separate `sts-endpoint` on an S3 storage profile when your S3-compatible storage exposes STS on a different host ([#1653](https://github.com/lakekeeper/lakekeeper/pull/1653)).
- **Fallback subject claims.** OpenID subject-claim config accepts a comma-separated list; the first matching claim in the token wins, easing varied-IdP integration ([#1646](https://github.com/lakekeeper/lakekeeper/pull/1646)).
- **Request size/time limits.** New `LAKEKEEPER__MAX_REQUEST_BODY_SIZE` (default 2 MB) and `LAKEKEEPER__MAX_REQUEST_TIME` (default `30s`) guard against oversized and slow requests ([#1583](https://github.com/lakekeeper/lakekeeper/pull/1583)).
- **Trusted engines configuration.** Declare trusted engines (e.g. Trino) with a selectable Invoker/Definer security model; the engine is auto-detected from token audience and recorded in request metadata ([#1629](https://github.com/lakekeeper/lakekeeper/pull/1629)).
- **Role assignment store and cache** with provider-scoped role identifiers, plus a roles cache for faster authorization ([#1638](https://github.com/lakekeeper/lakekeeper/pull/1638), [#1623](https://github.com/lakekeeper/lakekeeper/pull/1623)).
- **Iceberg V3 Variant datatype** support, validated against Spark 4 integration tests ([daa7947](https://github.com/lakekeeper/lakekeeper/commit/daa7947333097b25e09a91281a6057d334db599c)).
- **Tokio runtime metrics** exported for runtime observability ([#1664](https://github.com/lakekeeper/lakekeeper/pull/1664)).
- **Reduced memory footprint** by switching the allocator to jemalloc ([0eaeedc](https://github.com/lakekeeper/lakekeeper/commit/0eaeedc8411120f18ec9229b4dd08c36dd294d23)).
- **OPA bridge improvements:** batch-authorization optimization, configurable admin users, system-schema handling, and request-context forwarding ([#1674](https://github.com/lakekeeper/lakekeeper/pull/1674), [#1662](https://github.com/lakekeeper/lakekeeper/pull/1662)).
- **Faster listing** of namespaces, tables, and views ([#1618](https://github.com/lakekeeper/lakekeeper/pull/1618)).
- **Console.** New Home dashboard with usage statistics and API-call charts; branch operations (create, rename, delete, rollback, fast-forward); a Properties dialog for tables/views/namespaces; storage-layout configuration; and a local query engine with memory management ([#1621](https://github.com/lakekeeper/lakekeeper/pull/1621), [#1634](https://github.com/lakekeeper/lakekeeper/pull/1634)).

### Bug Fixes
- Fixed duplicate results when paginating `list_tabulars` ([#1682](https://github.com/lakekeeper/lakekeeper/pull/1682), [#1684](https://github.com/lakekeeper/lakekeeper/pull/1684)).
- Fixed a memory leak in the S3 identity cache ([0eaeedc](https://github.com/lakekeeper/lakekeeper/commit/0eaeedc8411120f18ec9229b4dd08c36dd294d23)).
- Allowed updating the storage-profile region when an S3 endpoint is set ([#1678](https://github.com/lakekeeper/lakekeeper/pull/1678)).
- Patched security advisories in crypto dependencies (`aws-lc-sys` / `rustls-webpki`) ([#1672](https://github.com/lakekeeper/lakekeeper/pull/1672)) and an `lz4_flex` memory-leak advisory ([#1665](https://github.com/lakekeeper/lakekeeper/pull/1665)).

### Breaking Changes
- **Cache metrics unified.** Per-cache metric names are replaced by shared names distinguished by a `cache_type` label ([#1641](https://github.com/lakekeeper/lakekeeper/pull/1641)).
- **Structured log format.** Logs now emit structured JSON with objects as field values instead of flat text ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5)).

### Upgrade Notes
- **Cache metrics:** migrate dashboards/alerts from the old per-cache metric names to the unified `lakekeeper_cache_hits_total` / `lakekeeper_cache_misses_total` / `lakekeeper_cache_size`, filtering by the `cache_type` label (`role`, `warehouse`, `namespace`, `secrets`, `stc`).
- **Structured logs:** log consumers must parse JSON rather than plain text. Set `LAKEKEEPER__DEBUG__EXTENDED_LOGS=true` to include `filename`/`line_number` fields.
- **S3 credential fields** dropped their `aws_` prefix; the old names remain accepted as aliases, but update to the new names ([#1685](https://github.com/lakekeeper/lakekeeper/pull/1685)).
