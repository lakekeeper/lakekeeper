# AGENTS.md

## Meta-rules for this file
- Keep this file concise. For each line, ask: would removing it cause mistakes? If not, cut it.
- Write commands and rules, not prose. Be imperative.
- Don't repeat what's in Cargo.toml, CI configs, or code comments.
- Update this file like code — review changes in PRs.

## Project
Lakekeeper — open-source Apache Iceberg REST catalog, written in Rust.

Repository: https://github.com/lakekeeper/lakekeeper

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

## Architecture
- Before adding new code, check if existing crates already solve the problem. Reuse over reinvention.
- Challenge duplication — if similar logic exists elsewhere, refactor to share it.
- New features should extend existing traits/interfaces where possible rather than introducing parallel abstractions.

## Rules
- Never skip or disable tests.
- Do not modify generated or vendored files.
- Release versioning is managed by release-please (`release-please/`).
- Never acquire a nested database connection. If a transaction is active, all subsequent queries must use that transaction — do not check out another connection from the read or write pool. Nested connections cause pool exhaustion and deadlocks.
