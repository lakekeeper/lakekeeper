# Developer Guide

All commits to main go through a PR. CI checks have to pass before merging the PR. Keep in mind that CI checks include lints. Before merge, commits are squashed, but GitHub is taking care of this, so don't worry. PR titles should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). We encourage small and orthogonal PRs. If you want to work on a bigger feature, please open an issue and discuss it with us first.

If you want to work on something but don't know what, take a look at our issues tagged with `help wanted`. If you're still unsure, please reach out to us via the [Lakekeeper Discord](https://discord.gg/jkAGG8p93B). If you have questions while working on something, please use the GitHub issue or our Discord. We are happy to guide you!

## Foundation & CLA

We hate red tape. Currently, all committers need to sign the CLA in GitHub. To ensure the future of Lakekeeper, we want to donate the project to a foundation. We are not sure yet if this is going to be Apache, Linux, a Lakekeeper foundation or something else. Currently, we prefer to spend our time on adding cool new features to Lakekeeper, but we will revisit this topic during 2026.

## Initial Setup

To work on small and self-contained features, it is usually enough to have a Postgres database running while setting a few envs. The code block below should get you started up to running most unit tests as well as clippy.

```bash
# start postgres
docker run -d --name postgres-16 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17
# set envs
echo 'export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres' > .env
echo 'export ICEBERG_REST__PG_ENCRYPTION_KEY="abc"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_READ="postgresql://postgres:postgres@localhost/postgres"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgresql://postgres:postgres@localhost/postgres"' >> .env
source .env

# Migrate db (make sure you have sqlx installed `cargo install sqlx-cli`).
# sqlx-cli auto-loads `.env` from the workspace root, so DATABASE_URL is picked up.
sqlx database create
sqlx migrate run --source crates/lakekeeper-storage-postgres/migrations

# Run tests (make sure you have cargo nextest installed, `cargo install cargo-nextest`)
cargo nextest run --all-features

# run clippy
just check-clippy
# formatting the code (make sure you have cargo-sort installed, `cargo install cargo-sort`)
# You may have to install nightly rust toolchain
just fix-format
```

Keep in mind that some tests are excluded by the `default-filter` in `.config/nextest.toml`. You can find a list of them in the [Testing section](#test-cloud-storage-profiles) below or by searching for modules whose name contains `_integration_tests` within files ending with `.rs`.
There are a few cargo commands we run on CI. You may install [just](https://crates.io/crates/just) to run them conveniently.
If you made any changes to SQL queries, please follow [Working with SQLx](#working-with-sqlx) before submitting your PR.

### Required tools for OpenAPI regeneration

The `just update-management-openapi` and `just update-generic-table-openapi` recipes — plus several `add-*-to-rest-openapi` recipes — require **Go yq** ([mikefarah/yq](https://github.com/mikefarah/yq)).

The Python `yq` (kislyuk) shipped via `pip install yq` is **not compatible**: it uses different flags (`-y -i` instead of `-i`) and its YAML emitter formats lists differently, which produces large whitespace-only diffs.

Install Go yq:

```bash
# macOS
brew install yq

# Linux (download the static binary)
curl -L "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64" \
  -o ~/.local/bin/yq && chmod +x ~/.local/bin/yq

# Verify (must say "mikefarah" in the version output)
yq --version
```

## Code structure

### What is where?

We have three crates, `lakekeeper`, `lakekeeper-bin` and `iceberg-ext`. The bulk of the code is in `lakekeeper`. The `lakekeeper-bin` crate contains the main entry point for the catalog. The `iceberg-ext` crate contains extensions to `iceberg-rust`.

**lakekeeper**

The `lakekeeper` crate contains the core of the catalog. It is structured into several modules:

1. `api` - contains the implementation of the REST API handlers as well as the `axum` router instantiation.
2. `catalog` - contains the core business logic of the REST catalog
3. `service` - contains various function blocks that make up the whole service, e.g., authn, authz and implementations of specific cloud storage backends.
4. `tests` - contains integration tests and some common test helpers, see below for more information.
5. `implementations` - contains the concrete implementation of the catalog backend, currently there's only a Postgres implementation and an alternative for Postgres as secret-store, `kv2`.

**lakekeeper-bin**

The main function branches out into multiple commands, amongst others, there's a health-check, migrations, but also serve which is likely the most relevant to you. In case you are forking us to implement your own AuthZ backend, you'll want to change the `serve` command to use your own implementation, just follow the call-chain.

### Where to put tests?

We try to keep unit-tests close to the code they are testing. E.g., all tests for the database module of tables are located in `crates/lakekeeper/src/implementations/postgres/tabular/table/mod.rs`. While working on more complex features we noticed a lot of repetition within tests and started to put commonly used functions into `crates/lakekeeper/src/tests/mod.rs`. Within the `tests` module, there are also some higher-level tests that cannot be easily mapped to a single module or require a non-trivial setup. Depending on what you are working on, you may want to put your tests there.

### I need to add an endpoint

You'll start at `api` and add the endpoint function to either `management` or `iceberg` depending on whether the endpoint belongs to official iceberg REST specification. The likely next step is to extend the respective `Service` trait so that there's a function to be called from the REST handler. Within the trait function, depending on your feature, you may need to store or fetch something from the storage backend. Depending on if the functionality already exists, you can do so via the respective function on the `C` generic and either the `state: ApiContext<State<...>>` struct or by first getting a transaction via `C::Transaction::begin_<write|read>(state.v1_state.catalog.clone()).await?;`. If you need to add a new function to the storage backend, extend the `Catalog` trait and implement it in the respective modules within `implementations`. Remember to do appropriate AuthZ checks within the function of the respective `Service` trait.

### I changed the audit log format

Audit log records — every line with `"event_source": "audit"` — carry a `MAJOR.MINOR` version in their `audit_format` field, declared as `AUDIT_FORMAT` in `crates/lakekeeper/src/service/events/backends/audit/mod.rs`. Consumers use it to pick a parser, so it has to be accurate. Nothing else in the tree observes the emitted JSON, which is why the fixtures below exist.

**The tests tell you which kind of change you made.** Run `just test`. If a fixture test fails, the failure message says whether the change is breaking or additive, and which JSON path moved:

- **Failure says "BREAKS CONSUMERS"** — a field was removed, renamed, retyped, or its value changed. Bump the **major**: `1.4` becomes `2.0`, and regenerate the fixtures.
- **Failure says "gained a field"** — the change is purely additive and existing consumers keep working. Bump the **minor**: `1.4` becomes `1.5`, and regenerate the fixtures.

Regenerate with `just update-audit-fixtures`, then read the resulting diff: it is exactly what a consumer's pipeline will see. If it contains anything you did not intend, that is the bug.

**What the fixtures are, and what a major bump does to them.** A fixture is a record of what the *current* code emits: the test emits an event and compares. That is the whole mechanism, and it has a consequence worth being clear about — a fixture can only ever describe the format the code emits *now*. After a major bump the code emits the new shape, so an old-version fixture cannot be reproduced and therefore cannot be tested. The path is `fixtures/v1/` today, and it is a literal in the test helper; a second version directory would need that helper changed first.

So on a major bump: regenerate the fixtures in place. Renaming, merging or dropping a fixture is fine — they are test scenarios, not a numbered archive, and a scenario that no longer exists should not have a fixture. If you want the previous format kept as a reference for consumers still reading old logs, that is a **documentation** decision: copy what you need into `docs/docs/logging.md` under the old version, where it will be read. Do not leave stale fixture files behind expecting them to be checked, because nothing can check them.

One consequence for the bump checker: it compares fixtures by name, so renaming or removing them leaves it nothing to compare. It reports that it could not verify the bump rather than claiming the format did not change, and passes. The Rust fixture tests still catch the change itself — only the version bump goes unverified, so check that one by hand.

**Then, in the same change:**

1. Update the field tables in `docs/docs/logging.md`. Two tests check this from different directions: one walks the fixtures and fails on any field they emit that is undocumented, and one iterates the key enums and fails on any key the emitter *can* produce that is undocumented. Neither can check that a *description* is still accurate, so re-read the row you touched.
2. Put an `audit` mention in the pull request title, or a `## Release notes` section in the body, describing the change in consumer terms. Major bumps must appear in the release notes; a consumer that discovers a format change from a parse failure in production has been let down.

**Adding a key, an action, or a variant.** These are compile errors by design — the build stops at the place where the wire name and the documentation have to be decided. Each is an *additive* change, so it wants a minor bump unless it also renames or replaces something.

| The build fails at | You added | What to do |
|---|---|---|
| `EntityField::as_str` (`events/context.rs`) | an entity field key | Pick the wire name in the `match`, then add a row to the entity field table in `docs/docs/logging.md` |
| `EntityType::as_str` (`events/context.rs`) | an `entity_type` value | Same, and add it to the `entity_type` list in the docs |
| `ActionContextKey::as_str` (`events/context.rs`) | an action context key | Same, and add a row to the action context table |
| `impl CatalogAction for Catalog*Action` (`service/authz/mod.rs`) | a catalog action | Decide what audit context the action should carry. If none, add it to the explicit no-context list in that `match` — the list exists so that this is a decision rather than a default |

On that last row, what is and is not guaranteed. Every `action_descriptor` match in the tree is exhaustive today, so adding a variant to any `Catalog*Action` enum does fail the build. But only five of them carry `#[deny(clippy::wildcard_enum_match_arm)]`, and there are 22 `action_descriptor` impls across four files — including nine in `crates/authz-openfga/src/relations.rs` and one each in `service/authn.rs` and `service/authz/instance_admin.rs`. For the unprotected ones the guarantee rests on nobody adding a `_ =>` arm, which is exactly what the deny exists to prevent elsewhere. Extending the deny to the rest is cheap and worth doing when one of those files is next touched.
| `determining_factor_tag` / `policy_effect_tag` / `failure_reason_tag` (audit test module) | a variant of an enum that reaches the wire through `#[derive(Valuable)]` | Give it a wire tag and document what it means. These types have no hand-written `visit`, so the derive would otherwise emit a new variant with nothing to stop it |

The first four fail a plain `cargo build`, because they are production code. The last lives in the audit test module, so it fails `cargo test` and `just check` but **not** `cargo build` — CI runs both, but a local `cargo build` will not tell you about it.

**Do not add a `_ =>` arm to any of those matches.** Every one of them is exhaustive on purpose: the missing wildcard is the entire mechanism. A wildcard turns each of these build failures into silence — a new entity key would take some other key's wire name, and a new action would emit no audit context — and nothing downstream would notice, because a fixture can only exercise a variant that already exists.

The rule is enforced rather than trusted to review: those functions carry `#[deny(clippy::wildcard_enum_match_arm)]`, so a wildcard fails the build with *"wildcard match will also match any future added variants"*, and clippy even prints the explicit list to write instead. Note this one is a clippy lint, so it fails `just check` and CI but not a bare `cargo build`.

Two things worth knowing if you are tempted:

- Adding a wildcard *alongside* the full list is caught anyway, by rustc's own `unreachable_patterns`. The dangerous edit is **replacing** arms with a wildcard, which compiles clean without the deny above. That is the case the lint exists for.
- The exhaustive lists make some of these functions long. One carries `#[allow(clippy::too_many_lines)]` for that reason. Do not "fix" the length by collapsing the list.

There is deliberately no unit test for this. A test cannot observe a compile failure: if a probe variant exists, either the match covers it and the crate does not build (so no test runs), or a wildcard absorbs it and the test passes while proving nothing. The lint is the test.

The key enums exist for exactly this reason. Before them the key space was only checked against the fixtures, so a key emitted on a path no fixture exercised was invisible — see the coverage note below.

**The bump itself is checked.** The fixture tests classify a change, but only until you regenerate: after `just update-audit-fixtures` they pass whether you bumped the version correctly, bumped it the wrong way, or left it alone. So a separate check compares the committed fixtures either side of the merge base and requires the bump to match what actually changed. Run it locally with `just check-audit-format-bump`; CI runs it on every pull request.

| What changed in the fixtures | Version bump | Verdict |
|---|---|---|
| Nothing | none | OK |
| Nothing | minor or major | **Illegal** — leave `AUDIT_FORMAT` alone. A bump tells every consumer to re-check their parser, so an empty one costs them work for nothing |
| Fields added | minor | OK |
| Fields added | none | **Illegal** — bump the minor, so consumers can tell which builds carry the new fields |
| Fields added | major | **Illegal** — a major bump tells every consumer their parser is broken and to re-check it, which for an added field is false. Bump the minor |
| A field removed, renamed, or retyped | major | OK |
| A field removed, renamed, or retyped | minor | **Illegal** — a minor bump says the opposite, that old parsers keep working. Bump the major |
| A field removed, renamed, or retyped | none | **Illegal** — bump the major |

Also rejected: bumping both halves at once (a major bump resets the minor to zero, so `1.4` goes to `2.0`, not `2.1` — bumping both says two different things happened), skipping numbers, going backwards, and removing `AUDIT_FORMAT` altogether.

Two things it deliberately does **not** treat as format changes, because they are not:

- **A fixture's values changing.** Comparison is on key paths and JSON types, not values. Containers record their own type too, so an empty object, an empty array and an absent field stay distinguishable from one another. Fixtures get edited to be more realistic — a scenario is corrected, an id is made deterministic — and demanding a major bump for `true` becoming `false` in a test input would be wrong.
- **Fixtures being added or removed.** Only fixtures present in both revisions are compared. A new fixture describes a scenario that was previously untested, not a format that was previously different.

The decision table and the classification are self-tested: `python3 .github/scripts/check-audit-format-bump.py --self-test` exercises every shape-versus-bump combination and the edge cases above, and CI runs it alongside the check. Note that the end-to-end path can only be exercised against real history, so the self-test covers the logic while the run against the merge base covers the git plumbing.

**What `audit_format` does not cover.** The keys the log subscriber adds — `timestamp`, `level`, `message`, `target`, `span`, `spans`, `filename`, `line_number` — belong to `tracing-subscriber`, not to Lakekeeper, and can move on a dependency upgrade with no version bump. They are stripped before fixture comparison for that reason, and `docs/docs/logging.md` states it as a contract.

**What the tests actually cover, and what they do not.** Worth knowing before you trust a green suite:

| Part of the format | How it is checked |
|---|---|
| Entity field key names, `entity_type` values, action context key names | **Exhaustively, from the type system.** The enums are closed, so a key cannot exist without a `match` arm and a documented row |
| Which variants the derived audit enums can emit | **Exhaustively**, via the tag functions above |
| That every record declares `audit_format` | Structurally: one emission site, checked by a test |
| Record **shape** — nesting, value types, which optional fields are omitted versus `null`, and the singular/plural arity switch | **By example only.** The fixtures pin the shape of the scenarios they cover, and nothing pins the shape of a scenario they do not |

That last row is the real limit. A shape change on a path no fixture exercises will not fail anything. This is not hypothetical: comparing these fixtures against audit records from a running server found five keys no fixture emitted, two of them undocumented, and enumerating the action context keys afterwards found two more. Sampling missed all of them.

So: **if you change a code path that no fixture exercises, add a fixture.** Adding one is cheap — write a test that builds the event, run `just update-audit-fixtures`, and register the name in `FIXTURE_NAMES`. Adding a fixture also widens the fixture-walking documentation test, which is the main reason to bother.

**Changes deliberately deferred to the next major bump.** These are known warts. None is worth a major bump on its own, so batch them all into one, so consumers absorb a single break rather than several:

- `warehouse-id` on an entity versus `warehouse_id` in a grant context (`events/context.rs` versus `events/backends/audit/mod.rs`) — the same logical field with two spellings in the same log stream. The action context keys are similarly split, four kebab-case against eighteen snake_case; one convention has to be picked for all of them at once. This is now a cheap change to make: every wire name lives in a single `as_str` per key enum, so the normalisation is a few `match` arms rather than a hunt through call sites.
- The empty-array enum encoding: `{"Forbid": []}`, `{"ActionForbidden": []}`. The array is always empty — it is an artefact of how `valuable-serde` renders a Rust enum variant with no payload, not a design decision. Every code generator turns it into a wrapper class, and `jq` users need `keys[0]` instead of reading a string. Plain strings would be better.
- The `action` / `actions` and `entity` / `entities` arity switch. A record carries the singular key when one item was checked and the plural key otherwise, so every consumer needs both paths. Always emitting arrays, even of length one, would be simpler for everyone.
- `writes` and `deletes` in the `apply_grants` action context are counts encoded as JSON strings. They should be numbers.
- Optional fields are encoded inconsistently: some are omitted when absent, others are emitted as `null`. Three code paths produce two different outcomes; each is documented at its definition, marked `Optional fields: path N of 3`. One rule should apply everywhere.

## Debugging complex issues and prototyping using our examples

To debug more complex issues, work on prototypes or simply an initial manual test, you can use one of the `examples`. Unless you are working on AuthN or AuthZ, you'll most likely want to use the minimal example. All examples come with a `docker-compose-build.yaml` which will build the catalog image from source. The invocation looks like this: `docker compose -f docker-compose.yaml -f docker-compose-build.yaml up -d --build`. Aside from building the catalog, the `docker-compose-build.yaml` overlay also exposes the docker services to your host, so you can also use it as a development environment by e.g. pointing your env vars to the docker container to test against its minio instance.
If you made changes to SQL queries, you'll have to run `just sqlx-prepare` before rebuilding the catalog image. This will update the sqlx queries in `.sqlx` to enable static checking of the queries without a migrated database.

After spinning the example up, you may head to `localhost:8888` and use one of the notebooks.

## Working with SQLx

This crate uses sqlx. For development and compilation a Postgres Database is required. This is part of the [Initial setup](#initial-setup).
If your database credentials used differ, please modify the `.env` accordingly and run `source .env` again.

Run:

```sh
# Migrate db. Make sure you have sqlx-cli install with `cargo install sqlx-cli`
# Run this locally if you change the db schema via `crates/lakekeeper-storage-postgres/migrations`,
# e.g. after adding a table or dropping a column.
sqlx database create
sqlx migrate run --source crates/lakekeeper-storage-postgres/migrations

# If you changed any of the SQL statements embedded in Rust code, run this before pushing to GitHub.
just sqlx-prepare
```

This will update the sqlx queries in `.sqlx` to enable static checking of the queries without a migrated database. Remember to `git add .sqlx` before committing. If you forget, your PR will fail to build on GitHub.
Be careful, if the command failed, `.sqlx` will be empty. But do not worry, it wouldn't build on GitHub so there's no way of really breaking things.

### ⚠️ Schema Qualification Warning

**IMPORTANT**: When adding new migrations, do **NOT** schema qualify references to any database objects. Schema qualification will break deployments that place the application in a schema different than the public one.

**❌ Incorrect - Do NOT do this:**

```sql
-- This will break deployments in non-public schemas
CREATE TABLE public.my_new_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

INSERT INTO public.my_new_table (name) VALUES ('example');

ALTER TABLE public.existing_table ADD COLUMN new_column INTEGER;
```

**✅ Correct - Do this instead:**

```sql
-- This will work in any schema
CREATE TABLE my_new_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

INSERT INTO my_new_table (name) VALUES ('example');

ALTER TABLE existing_table ADD COLUMN new_column INTEGER;
```

The migration system will automatically apply the migration in the correct schema context, so explicit schema qualification is unnecessary and will cause issues in deployments where Lakekeeper is deployed to a custom schema.

Operators pick that schema with [`LAKEKEEPER__PG_SCHEMA`](configuration.md#using-a-non-public-postgres-schema). Do not export it in a shell you build or test from: `cargo sqlx prepare` ignores it and uses `DATABASE_URL`, and the postgres crate reads `LAKEKEEPER_TEST__` in its own tests but `LAKEKEEPER__` when compiled as a dependency, so it would apply unevenly.

### Inspecting the db

The db schema is the result of all migrations applied in order. To inspect it you can:

```shell
# Assumes you set up the db as described above

# Get a shell in the db's container
docker exec -it postgres-16 /bin/bash

# Then you can connect to the db
psql "postgresql://postgres:postgres@localhost:5432/postgres"
# And inspect it, for instance by describing views or tables
\d+ active_tabulars

# Or you can dump the entire schema
pg_dump --schema-only "postgresql://postgres:postgres@localhost:5432/postgres" > /home/lakekeeper_schema.sql
# Copy it out of the container and then inspect it or pass it as context to LLMs
docker cp postgres-16:/home/lakekeeper_schema.sql .
```

### Extension tables (`ext_*` prefix)

Lakekeeper reserves the `ext_*` table-name prefix for downstream extensions
that need to store their own state in the catalog database. The convention is
an operational contract between upstream and any extension:

| Rule | What it means |
|---|---|
| Reserved prefix | Upstream core migrations **never** create tables matching `ext_*`. Extensions own that namespace. The integration test `test_core_does_not_create_ext_objects` enforces this. |
| FK direction | Extension tables FK *into* upstream tables. Upstream never FKs into `ext_*`. |
| CASCADE required | Every FK from an `ext_*` table to an upstream table should be `ON DELETE CASCADE` or `ON DELETE SET NULL`. Enforce in the extension crate's own CI — upstream cannot inspect downstream migration sets. |
| Scope of allowed objects | `ext_*` may name tables and objects owned by those tables (indexes, sequences). Triggers, functions, indexes, or views attached to upstream-owned objects are not permitted under the prefix — they would survive extension removal and could brick OSS. |
| Tracker tables | Each registered extension migration source uses its own SQLx tracker, named `ext_<name>_sqlx_migrations`. Core's `_sqlx_migrations` is untouched. |

#### Registering extension migrations

Extensions call upstream's `migrate(pool, extensions)` with their own
migration source. Every registered source runs **inside the same outer
transaction** as core migrations — either every migration commits or the
entire upgrade rolls back. Partial state is impossible.

```rust
use lakekeeper_storage_postgres::migrations::{ExtensionMigrations, migrate};

// `name` must be 1–40 chars: first [a-z_], remaining [a-z0-9_]; rejected at
// the start of `migrate()` otherwise. Derives `ext_my_extension_sqlx_migrations`.
let extensions = vec![ExtensionMigrations::builder()
    .name("my_extension")
    .migrator(sqlx::migrate!("./migrations")) // embedded at compile time
    .build()];
let server_id = migrate(&pool, extensions).await?;
```

Optional fields not shown: `.data_hooks(map)` for Rust-side hooks tied to
specific migration versions, and `.sha_patches(set)` for in-place edits to
already-shipped migrations.

The `data_hooks` field on `ExtensionMigrations` is a
`HashMap<i64, Box<dyn MigrationHook>>` keyed by the migration's version id.
Each entry's `MigrationHook` runs immediately after the matching extension
migration is applied, inside the same transaction — use it for Rust-side
data backfills tied to a specific SQL migration. Pass `HashMap::new()`
when no hooks are needed (the common case in the snippet above).

Callers that don't register extensions use the back-compat shim
`migrate_core_only(pool)`. Core upstream tooling and tests already do.

#### Recovery: removing an extension's state

Dropping the extension binary and removing its tables restores the database
to a working OSS-only state. The SQL below scans every relation kind that
the `ext_*` prefix may name and drops it:

```sql
-- Run inside the catalog database. Drops every ext_* table and tracker
-- (CASCADE handles dependent indexes, sequences, and constraints).
DO $$
DECLARE r record;
BEGIN
    -- Tables (covers extension state + per-source `_sqlx_migrations` trackers).
    FOR r IN SELECT c.relname
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = current_schema()
               AND c.relkind IN ('r', 'p')
               AND c.relname LIKE 'ext\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP TABLE %I CASCADE', r.relname);
    END LOOP;

    -- Defensive sweeps for object kinds the convention forbids extensions
    -- from creating on upstream-owned tables, but that may exist if a
    -- non-conforming extension was deployed.
    FOR r IN SELECT t.tgname, c.relname AS tbl
             FROM pg_trigger t
             JOIN pg_class c ON c.oid = t.tgrelid
             WHERE NOT t.tgisinternal
               AND t.tgname LIKE 'ext\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP TRIGGER %I ON %I', r.tgname, r.tbl);
    END LOOP;

    FOR r IN SELECT typname FROM pg_type t
             JOIN pg_namespace n ON n.oid = t.typnamespace
             WHERE n.nspname = current_schema()
               AND typname LIKE 'ext\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP TYPE %I CASCADE', r.typname);
    END LOOP;

    FOR r IN SELECT p.proname FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = current_schema()
               AND p.proname LIKE 'ext\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP FUNCTION %I CASCADE', r.proname);
    END LOOP;
END $$;
```

After running this, the OSS binary boots cleanly against the remaining
catalog state.

## KV2 / Vault

This catalog supports KV2 as a backend for secrets. Tests for KV2 are disabled by default. To enable them, you need to run the following commands:

```shell
docker run -d -p 8200:8200 --cap-add=IPC_LOCK -e 'VAULT_DEV_ROOT_TOKEN_ID=myroot' -e 'VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200' hashicorp/vault

# append some more env vars to the .env file, it should already have PG related entries defined above.

# the values below configure KV2
echo 'export ICEBERG_REST__KV2__URL="http://localhost:8200"' >> .env
echo 'export ICEBERG_REST__KV2__USER="test"' >> .env
echo 'export ICEBERG_REST__KV2__PASSWORD="test"' >> .env
echo 'export ICEBERG_REST__KV2__SECRET_MOUNT="secret"' >> .env

source .env
# setup vault
./tests/vault-setup.sh http://localhost:8200

# Select kv2 tests
cargo nextest run --all-features --all-targets \
    --ignore-default-filter -E "test(::kv2_integration_tests::)"
```

## Test cloud storage profiles

Currently, we're not aware of a good way of testing cloud storage integration against local deployments. That means, to test against AWS S3, GCS and ADLS Gen2, you need to set the following environment variables. For more information, take a look at the [Storage Guide](storage.md). A sample `.env` could look like this:

```sh
export LAKEKEEPER_TEST__AZURE_TENANT_ID=<your tenant id>
export LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM=<your azure adls filesystem name>
export LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME=<your azure storage account name>
# Auth Method 1: Client Credentials
export LAKEKEEPER_TEST__AZURE_CLIENT_ID=<your entra id app registration client id>
export LAKEKEEPER_TEST__AZURE_CLIENT_SECRET=<your entra id app registration client secret>
# Auth Method 2: Shared Key
export LAKEKEEPER_TEST__AZURE_STORAGE_SHARED_KEY=<shared key>

export LAKEKEEPER_TEST__AWS_S3_BUCKET=<your aws s3 bucket>
export LAKEKEEPER_TEST__AWS_S3_REGION=<your aws s3 region>
export LAKEKEEPER_TEST__AWS_S3_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export LAKEKEEPER_TEST__AWS_S3_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export LAKEKEEPER_TEST__AWS_S3_STS_ROLE_ARN=arn:aws:iam::123456789012:role/role-name

# the values below should work with the default minio in our docker-compose
export LAKEKEEPER_TEST__S3_BUCKET=tests
export LAKEKEEPER_TEST__S3_REGION=local
export LAKEKEEPER_TEST__S3_ACCESS_KEY=minio-root-user
export LAKEKEEPER_TEST__S3_SECRET_KEY=minio-root-password
export LAKEKEEPER_TEST__S3_ENDPOINT=http://localhost:9000

export LAKEKEEPER_TEST__GCS_CREDENTIAL='{"type": "service_account","project_id": "..", ...}'
export LAKEKEEPER_TEST__GCS_BUCKET=name-of-gcs-bucket-without-hns
export LAKEKEEPER_TEST__GCS_HNS_BUCKET=name-of-gcs-bucket-with-hns
```

You may then run tests by ignoring the nextest's default filter and selecting the desired tests:

```sh
source .example.env-from-above
cargo nextest run --all-features --ignore-default-filter -E "test(::aws_integration_tests::)"
# see .config/nextest.toml for all filters
```

## Running integration test

Our integration tests are written in Python and use pytest. They are located in the `tests` folder. The integration tests spin up Lakekeeper and all the dependencies via `docker compose`. Please check the [Integration Test Docs](https://github.com/lakekeeper/lakekeeper/tree/main/tests) for more information.

### Running Authorization unit tests

Some authorization unit tests need to be run against an OpenFGA server. They are excluded by our nextest `default-filter`. The workflow for executing them is:

```bash
# Start an OpenFGA server in a docker container
docker rm --force openfga-client && docker run -d --name openfga-client -p 36080:8080 -p 36081:8081 -p 36300:3000 openfga/openfga:v1.14 run

# Set Lakekeeper's OpenFGA endpoint
export LAKEKEEPER_TEST__OPENFGA__ENDPOINT="http://localhost:36081"

# Use a filterset to select the tests
cargo nextest run --all-features --ignore-default-filter -E "test(::openfga_integration_tests::)"
```

## Extending Authz

When adding a new endpoint, you may need to extend the authorization model. Please check the [Authorization Docs](./authorization.md) for more information. For OpenFGA, perform the following steps:

1. Add the new action to the relevant enum in `crate::service::authz`, e.g. `CatalogViewAction::CanUndrop`. Actions that must carry request context for policy-based authorizers are parameterized variants — see `CatalogProjectAction::CreateWarehouse`.
1. In the `lakekeeper-authz-openfga` crate (`crates/authz-openfga/src/relations.rs`), add or reuse a relation on the resource enum (e.g. `RoleRelation::CanUndrop`) and map the action to it in the `ReducedRelation` impl (e.g. `CatalogViewAction::CanUndrop => ViewRelation::CanUndrop`).
1. Bump the model version by **renaming the latest folder** — e.g. `git mv authz/openfga/v4.7 authz/openfga/v4.8`. Do **not** create a new folder alongside the old one. For a **backward-compatible** change (adding a type, relation, or action; no rewrite of existing tuples) the rename is all you need: existing stores re-migrate to the new model id on startup and their tuples keep authorizing the same actions. This holds **whether or not the previous version was already released** — a released store simply re-migrates to the new id. The **only** exception is a change that rewrites/migrates existing tuples: that one gets a brand-new folder while the old folder is kept for the migration chain (see `v4.0`, which introduced `lakekeeper_table` / `lakekeeper_view` and migrated tuples). Rule of thumb: backward-compatible ⇒ rename the folder; tuple migration ⇒ add a new folder.
1. Edit the relevant component(s) under `authz/openfga/<version>/components/*.fga` (e.g. add `define can_undrop: modify` to `view.fga`), then regenerate and validate:

   ```bash
   just update-openfga   # fga model transform <latest>/fga.mod > <latest>/schema.json
   just test-openfga     # runs the <latest>/store.fga.yaml assertions
   ```

   (Requires the `fga` CLI — download from the [OpenFGA repo](https://github.com/openfga/cli/releases/).)
1. In `crates/authz-openfga/src/migration.rs` bump `ACTIVE_MODEL_VERSION` to the new version. For backward-compatible changes, repoint the current `add_model_*_current` call (schema-path `include_str!` + version). For tuple-migrating changes, add another `add_model` call carrying the migration fn.
1. Record the change under the new version heading in `authz/openfga/README.md`.

## Building the docs locally

```bash
cd site
just serve
```
