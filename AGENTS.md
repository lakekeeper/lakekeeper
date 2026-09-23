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
- Format: `just fix-format` (requires `cargo +nightly fmt` and `cargo sort`)
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
| catalog-error-macros | crates/catalog-error-macros | Error derive macros |

## Authz

- OpenFGA model: `authz/openfga/` — validate with `just test-openfga`, update JSON with `just update-openfga`
- OPA policies: `authz/opa-bridge/` — check with `just check-opa` (requires `opa` and `regal` CLIs)

## Code Style

- Follow existing patterns in adjacent files.
- Use `thiserror` for error types, `tracing` for logging.
- Use `typed-builder` for struct construction.
- Use workspace dependencies (`{ workspace = true }`) — don't add versions directly.
- All crate versions use `version.workspace = true`.
- Minimize new dependencies — justify additions.
- Describe current behavior in comments: no "rather than", "instead of", "no longer", "previously", and no plan or task labels. Version changelogs are exempt — stating the delta is their purpose.
- Docs prose (`docs/docs/*.md`): one line per paragraph — no hard line wrapping. Rely on soft-wrap.

## Architecture

- Before adding new code, check if existing crates already solve the problem. Reuse over reinvention.
- Challenge duplication — if similar logic exists elsewhere, refactor to share it.
- New features should extend existing traits/interfaces where possible rather than introducing parallel abstractions.
- Cold path (management/admin routes): bypass per-process in-memory caches; read authoritative data from the DB.
- Hot authz path: may tolerate cache lag.
- After any write: invalidate the local replica's in-memory cache immediately.
- Never rely on per-process caches for cross-replica correctness — caches have no cross-replica invalidation.

## Audit Log

Before changing any record carrying `"event_source": "audit"`, read `docs/docs/developer-guide.md` → "I need to change the audit log format" — it has the decision table, the registration lists, and what each check does and does not cover.

- `AUDIT_FORMAT` is derived from `audit-format/`. Write a fragment; never edit a version number.
- Never build a wire value from a bare string. Put `#[audit_part(field = "...")]` on a vocabulary enum and emit `Variant::as_wire()`; `WireStr::new` is for the attribute's expansion only and a test enforces that.
- Never add a `_ =>` arm to an `action_descriptor` match. The missing wildcard is the mechanism, and it fails `just check`, not `cargo build`.
- Add a fixture for every new emission path — fixtures pin only the scenarios they cover. Extend `crates/lakekeeper-integration-tests/tests/audit_corpus.rs` for every new record *shape*; that file is meant to grow.
- Run `just update-audit-fixtures`, then `just check-audit-format`, after any change. Review the fixture diff — it is what consumers will see. Run the corpus test with `just test-audit-corpus` (needs the local Postgres).
- The audit type registry (`#[audit_part]`) exists in debug builds only. Registry and schema tests, and `just update-audit-schema`, run in the dev profile; with `--release` the registry is empty and `Registration::require_registry()` fails with that message.
- Every crate that declares audit types commits `audit-schema.json` at its root; `audit-format/schema.json` and `docs/docs/audit/reference-<emitter>.md` are their merge. Run `just update-audit-schema` after touching any audit type or its doc comments. Never edit those three files by hand. Every field of an audit type needs a doc comment.
- One generated reference per emitter, `docs/docs/audit/reference-<emitter>.md`. `audit_format` governs the whole record's shape; `emitter.format` governs what that emitter contributes. They are equal only for Lakekeeper's own records.
- `audit-format/schema-baseline.json` (declared types) and `audit-format/records-baseline.json` (top-level record shapes, from the fixtures) record the audit format before the shape change. Never edit them by hand, and never regenerate them once that change has begun. List what changed with `--compare-schemas` against each; `--summarise-records DIR OUT` rebuilds the record half.

## Rules

- Never skip or disable tests.
- Do not modify generated or vendored files.
- Release versioning is managed by release-please (`release-please/`).
- Write a clear PR description of the user-visible change; optionally add a `## Release notes` section. The docs-site Release Notes page (`site/docs/about/release-notes.md`) is summarised from PR descriptions at release; `CHANGELOG.md` (release-please) stays headlines-only. See `.github/RELEASING.md`.
- Never acquire a nested database connection. If a transaction is active, all subsequent queries must use that transaction — do not check out another connection from the read or write pool. Nested connections cause pool exhaustion and deadlocks.
- To return updated state after a write, read it back **in the same transaction** — a follow-up query may hit a lagging read replica and miss the write.
