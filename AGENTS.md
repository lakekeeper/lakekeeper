# AGENTS.md

## Meta-rules for this file

- Keep this file concise. For each line, ask: would removing it cause mistakes? If not, cut it.
- Write commands and rules, not prose. Be imperative.
- Don't repeat what's in Cargo.toml, CI configs, or code comments.
- Update this file like code — review changes in PRs.

## Project

Lakekeeper — open-source Apache Iceberg REST catalog, written in Rust.

Repository: <https://github.com/lakekeeper/lakekeeper>

## Build & Test

Uses [just](https://github.com/casey/just) as task runner. See `justfile` for all available recipes.

Key commands:

- Build: `cargo build`
- Test all: `just test` (includes doc tests)
- Unit tests only: `just unit-test`
- Test one: `cargo test -p <crate> <test_name>`
- Lint: `just check` (runs clippy with multiple feature combinations, format check, cargo-sort)
- Format: `just fix-format` (requires `cargo sort`)
- Auto-fix: `just fix`

Clippy runs with multiple feature flag combinations — don't just run `cargo clippy --all-features`. Use `just check-clippy`.

## Workspace Crates

| Crate | Path | Purpose |
|-------|------|---------|
| lakekeeper | crates/lakekeeper | Core catalog logic |
| lakekeeper-alloc | crates/alloc | Allocator configuration and observability |
| lakekeeper-bin | crates/lakekeeper-bin | Server binary |
| lakekeeper-io | crates/io | Storage I/O (S3, GCS, Azure, etc.) |
| iceberg-ext | crates/iceberg-ext | Iceberg format extensions |
| lakekeeper-authz-openfga | crates/authz-openfga | OpenFGA authorization |
| lakekeeper-audit-macros | crates/audit-macros | `#[audit_part]` attribute for audit types |
| lakekeeper-storage-postgres | crates/lakekeeper-storage-postgres | PostgreSQL catalog backend and migrations |
| lakekeeper-events-kafka | crates/lakekeeper-events-kafka | Kafka cloud-events publisher |
| lakekeeper-events-nats | crates/lakekeeper-events-nats | NATS cloud-events publisher |
| lakekeeper-secrets-kv2 | crates/lakekeeper-secrets-kv2 | Vault KV2 secrets backend |
| lakekeeper-integration-tests | crates/lakekeeper-integration-tests | Service-layer tests against a storage backend (Postgres) |

## Authz

- OpenFGA model: `authz/openfga/` — validate with `just test-openfga`, update JSON with `just update-openfga`
- OPA policies: `authz/opa-bridge/` — check with `just check-opa` (requires `opa` and `regal` CLIs)

## Code Style

- Follow existing patterns in adjacent files.
- Fix the code, don't silence the lint. `#[allow(clippy::…)]` needs a structural reason the clean fix is wrong, in a trailing comment. `too_many_arguments` → a `typed-builder` spec struct; `too_many_lines` → extract functions.
- An existing `allow` is not permission to grow what it covers. Adding an argument, branch, or line under one means fixing the underlying issue in the same change.
- Use `thiserror` for error types, `tracing` for logging.
- Use `typed-builder` for struct construction.
- Share a type through `Arc` on many call sites with `pub type ArcX = Arc<X>;` next to `X`, documented "Reference to [`X`] that can be cheaply cloned and shared." Where an alias exists, use it — never write `Arc<X>`.
- Use workspace dependencies (`{ workspace = true }`) — don't add versions directly.
- All crate versions use `version.workspace = true`.
- Minimize new dependencies — justify additions.
- Describe current behavior in comments: no "rather than", "instead of", "no longer", "previously", and no plan or task labels. Version changelogs are exempt — stating the delta is their purpose.

## Docs

Applies to `docs/docs/*.md` and `site/docs/`. Release notes: also follow `.github/RELEASING.md`.

- Readers are operators, many not native English speakers. Write plain, full sentences with common words.
- Lead with what the feature does for the operator, then explain the mechanism roughly.
- Leave protocol detail (headers, JSON shapes, endpoint paths, error-type lists, formulas) to the API reference, unless the reader must act on it.
- For a standard feature, link the upstream spec. Don't map which endpoint implements which part of it.
- Never refer to something the reader can't know ("the endpoint current clients use", "the earlier meaning"). Name it or drop it.
- No caveats about what might get wrong.
- Avoid ambiguous words: write "if it did not change", not "if not"; write "upper and lower case", not "case".
- Verify every claim against the merged code, not the PR description.
- Trim after fact-checking. Never delete still-true text without saying so.
- One line per paragraph, no hard wrapping.
- Indent nested lists and blocks under a list item by 4 spaces. After an indented paragraph or code block, leave a blank line before the next item, or MkDocs merges the items.

## Architecture

- Before adding new code, check if existing crates already solve the problem. Reuse over reinvention.
- Challenge duplication — if similar logic exists elsewhere, refactor to share it.
- New features should extend existing traits/interfaces where possible rather than introducing parallel abstractions.
- Cold path (management/admin routes): bypass per-process in-memory caches; read authoritative data from the DB.
- Hot authz path: may tolerate cache lag.
- After any write: invalidate the local replica's in-memory cache immediately.
- Never rely on per-process caches for cross-replica correctness — caches have no cross-replica invalidation.
- Never retrofit a behavior-narrowing query param onto an existing route (worst case: scoping a DELETE). Released servers ignore unknown query params, so under version skew a new client gets the un-scoped operation. Add a new route instead — old servers reject it.

## Authorization & audit in handlers

Follow `crates/lakekeeper/src/api/management/v1/lakekeeper_actions.rs` as the reference.

- Validate request inputs (query parsing, `require_project_id(None)`, request shape) with `?` before authorizing; such errors emit no event. From the first authorization step on, audit every failure: role resolution, catalog fetches, authz and serialization go inside the single `Result` passed to one `event_ctx.emit_authz(...)?`. A handler that emits after doing its work reports a denial with `event_ctx.emit_early_authz_failure(...)`.
- Use `require_*_presence` to fold `Result<Option<T>, CatalogError>` into `AuthZError`.
- Match `APIEventContext::for_*` to the actual target resource — never default to `for_server`.
- Never format errors into user-facing messages. Attach typed errors via `.source(Some(Box::new(e)))`.

## Audit Log

- Before changing any record carrying `"event_source": "audit"`, read `docs/docs/developer-guide.md` → "I need to change the audit log format".
- `AUDIT_FORMAT` is derived from the last release tag and the fragments. Write a fragment under `audit-format/unreleased/`.
- A `context` value drawn from a fixed set is a vocabulary: put `#[audit_part(field = "<key>")]` on its enum and let the key hold `Wire<ThatEnum>`. A value derived from the request is data.
- Never build a wire value from a bare string: emit `Variant::as_wire()` of a vocabulary enum. Never hand-write an `as_str` on one.
- Everything this log names is `lower_snake_case`. The attribute checks values and keys; record and part field names are checked in review.
- Never add a `_ =>` arm to an `action_descriptor` match: a new action would emit no context.
- A fixture must describe a record the server can produce: build it from `action_descriptor()` or the handler's `event_actions()`. Add one for every new emission path, and extend `crates/lakekeeper-integration-tests/tests/audit_corpus.rs` for every new record shape.
- Never suppress a log line because "the audit record covers it" without asking `crate::audit::enabled()` first.
- After a change: `just update-audit-fixtures` and `just update-audit-schema`, review both diffs, commit, then run `just check-audit-format` (it reads `HEAD`). Never edit `AUDIT_FORMAT`, the fixtures or `docs/docs/audit/schema.json` by hand.

## Rules

- Never skip or disable tests.
- Do not modify generated or vendored files.
- Release versioning is managed by release-please (`release-please/`).
- Write a clear PR description of the user-visible change; optionally add a `## Release notes` section. The docs-site Release Notes page (`site/docs/about/release-notes.md`) is summarised from PR descriptions at release; `CHANGELOG.md` (release-please) stays headlines-only. See `.github/RELEASING.md`.
- Never acquire a nested database connection. If a transaction is active, all subsequent queries must use that transaction — do not check out another connection from the read or write pool. Nested connections cause pool exhaustion and deadlocks.
- To return updated state after a write, read it back **in the same transaction** — a follow-up query may hit a lagging read replica and miss the write.
- Stay on the default isolation level — no `REPEATABLE READ`/`SERIALIZABLE`. Read co-dependent data in ONE statement; guard a read-then-write with a version compare-and-set that reports a mismatch as retryable. Raise the level only where one statement cannot read the data together, with a comment saying why.
- After changing a query, run `just sqlx-prepare` and commit `.sqlx/`.
- Never edit a migration file a release tag carries (`git tag --contains`): it changes the sqlx checksum. Add a new numbered file, or list the version in `get_changed_migration_ids()` (`crates/lakekeeper-storage-postgres/src/migrations/mod.rs`).

## Testing

- Assert exact expected values. Never `assert x in (a, b)` or approximate matches — ambiguity in tests hides real bugs. If you're unsure which value is correct, find out first.

## Downstream: Lakekeeper Plus

Lakekeeper Plus (private repository) builds on this workspace, pinned to a git revision. It uses `lakekeeper`'s public API (`service`, `api`, `audit`, `__private`, the `test-utils` feature) and `lakekeeper-storage-postgres`'s `ExtensionMigrations`.

- Treat every `pub` item of these crates as API. A rename, removal or signature change breaks Plus at its next revision bump: name it in the PR description under `## Downstream follow-up`.
- Table names starting with `ext_` are reserved for downstream extensions.
