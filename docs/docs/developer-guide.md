---
description: "Contribute to Lakekeeper: development setup, pull request and CI expectations, conventional commits, and the contributor licence agreement."
---

# Developer Guide

All commits to main go through a PR. CI checks have to pass before merging the PR. Keep in mind that CI checks include lints. Before merge, commits are squashed, but GitHub is taking care of this, so don't worry. PR titles should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). We encourage small and orthogonal PRs. If you want to work on a bigger feature, please open an issue and discuss it with us first.

If you want to work on something but don't know what, take a look at our issues tagged with `help wanted`. If you're still unsure, please reach out to us via the [Lakekeeper Discord](https://discord.gg/jkAGG8p93B). If you have questions while working on something, please use the GitHub issue or our Discord. We are happy to guide you!

## Foundation & CLA

We hate red tape. Currently, all committers need to sign the CLA in GitHub. To ensure the future of Lakekeeper, we want to donate the project to a foundation. We are not sure yet if this is going to be Apache, Linux, a Lakekeeper foundation or something else. Currently, we prefer to spend our time on adding cool new features to Lakekeeper, but we will revisit this topic during 2026.

## Initial Setup

To work on small and self-contained features, it is usually enough to have a Postgres database running while setting a few envs. The code block below should get you started up to running most unit tests as well as clippy.

```bash
# start postgres
docker run -d --name postgres-16 -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17
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

### I need to change the audit log format

Audit records — every line with `"event_source": "audit"` — carry a `MAJOR.MINOR` version in their `audit_format` field, declared as `AUDIT_FORMAT` in `crates/lakekeeper/src/service/events/backends/audit/mod.rs`. Consumers route on it.

**You never pick that number.** It is derived from committed state:

```text
AUDIT_FORMAT = the version in audit-format/released.json, raised once by the highest level among audit-format/unreleased/*.md
```

What a change owes is a **fragment**: one file recording how badly it affects a consumer, and describing it in that consumer's terms. `just update-audit-fixtures` reads the fragments and writes the version for you.

This is why a release ships `4.0` rather than `4.4`. The baseline is raised by the *highest* level among the unreleased fragments, not once per fragment, so a release moves the version at most once however many changes it carries, and a major change absorbs every minor change in the same cycle. The release notes still list all of them — that is what the fragments are for. The version says how badly you are affected; the list says what happened.

#### Terms

- **Audit type** — a Rust type that reaches the audit log, marked `#[audit_part]`. The attribute makes it serialisable, describes it as JSON Schema from its doc comments, and registers it with the emitter of its crate. Everything that reaches a record is one of these: a record part, an operation context, a value vocabulary or a key vocabulary.
- **Field** — a name in the record: `action_name`, `entity_type`, `warehouse_id`. Adding one changes the record's shape.
- **Value** — the string a field holds: `get_metadata` in `action_name`, `table` in `entity_type`. Every value comes from a **value vocabulary**, an enum marked `#[audit_part(field = "…")]` whose variant names are the values of that field. Adding one leaves the shape identical, which is why consumers are told to tolerate values they do not recognise.
- **Key vocabulary** — an enum marked `#[audit_part(keys_of = "…")]` whose variant names are the *keys* of an object: `EntityField` inside an entity, `ActionContextKey` inside an action, `HandlerContextKey` for `push_extra_context`. Adding one adds a field, so it is a `minor` change, not a free one. The two forms are separate types on the way out — a key yields a `WireKey` and a value a `WireStr`, and neither converts into the other — so a key cannot be emitted where a value belongs, or the other way round.
- **Shape** — which fields exist, their JSON types, their nesting, and the singular/plural arity switch.
- **Crate schema** — a committed `audit-schema.json` at the root of every crate that declares audit types: the definitions of that crate's own types, generated from its registry by its own tests.
- **Schema** — `audit-format/schema.json`, the merge of the crate schemas. Every object, every field with its description, and every closed set of values a record can carry. It is published to the documentation site as `docs/docs/audit/schema.json`, which is what customers read.
- **Fixture** — a committed golden record under `src/service/events/backends/audit/fixtures/v<MAJOR>/`, generated by the test that asserts against it. Pins the emitted bytes *by example*: only the scenarios it covers.
- **Fragment** — a committed `audit-format/unreleased/*.md` recording one change: its level, and prose for the release notes. Written in the pull request that makes the change; assembled and deleted at release.
- **Level** — `major`, `minor` or `none`: how badly the change affects a consumer. The only judgement you make.
- **Baseline** — `audit-format/released.json`, the version the most recent release on this branch shipped. Maintained by the release recipe, and `null` until a release carries one.

The schema pins every declared field and value, fixtures pin the emitted bytes of the scenarios they cover, fragments record intent, and nothing else in the tree observes the emitted JSON.

**Which level.** A vocabulary enum reaches the log as string *values*, so renaming a variant changes the payload even though no field moves. Adding one does not: `docs/docs/logging.md` tells consumers that value sets are open and that an unrecognised value must be treated as opaque, so a new action or entity type is new capability rather than a changed format. A new *field* is a changed format, because a consumer reading the record's shape sees it.

| Change                                                                               | Level   |
| ------------------------------------------------------------------------------------ | ------- |
| A field added                                                                        | `minor` |
| A new field on an entity or an action                                                | `minor` |
| A variant added to a key vocabulary, which is the same thing                         | `minor` |
| A new action, entity type, or other wire *value*                                     | `none`  |
| A field removed, renamed or retyped                                                  | `major` |
| A wire value renamed or removed                                                      | `major` |
| A fixture edited, renamed, added or dropped, with no change to what the server emits | none    |

`none` is a level you may declare and not one you must: a fragment for a new action puts it in the release notes without moving the version. The last row needs no fragment at all — nothing reached a consumer.

#### Steps

**1. Make the change.**

- **A value**: add a variant to the value vocabulary that owns the field and emit `Variant::as_wire()`. The enum already carries `#[audit_part(field = "...")]`, so the wire name derives from the variant name and the enum's case style, and the value reaches the schema with no list to update.
- **A whole new vocabulary**: put `#[audit_part(field = "...")]` on the enum for a set of values, or `#[audit_part(keys_of = "...")]` for a set of object keys. That is the entire registration. A doc comment on a variant becomes that name's description in the schema, which is the only place a consumer can read what the name means.
- **A field**: add it to the part or context struct with a doc comment, or add a variant to the key vocabulary that governs it.
- **A new kind of operation record, from any crate**: an operations enum, an outcomes enum and a context struct, each with the attribute, then `OperationRecord::new(..).context(..).emit()`.

**Every name is `lower_snake_case`.** That is the whole convention: one or more runs of `[a-z0-9]` joined by single underscores, starting with a letter. It covers a record's own fields, every key, and every value of a closed set, and it is the spelling every other Lakekeeper log line already uses. A vocabulary declares `#[audit(rename_all = "snake_case")]` and needs no per-variant rename; a test rejects a vocabulary name spelled any other way, and the check is on by default, so a vocabulary cannot go unchecked by being forgotten. The field names of a record or a part hold to the same rule without a test behind them: a field reaches the wire kebab-case only through an explicit `#[serde(rename = "...")]`, which is one visible line in a diff, so review is what catches it. One such rename is deliberate — `policy-id` inside `determined_by`, which quotes the management API's shape so a single parser reads a `/check` response and an audit record alike. A value set spelled somewhere else adds `external_values` — `#[audit_part(field = "update_kinds", external_values)]` — and is then skipped; name the vocabulary it follows in the doc comment. Three do: `entity_type`, `resource_type` and `update_kinds`.

Never build a wire name from a bare string. `WireStr::new` and `WireKey::new` exist for the attribute's expansion to call and nothing else. A test fails on any other caller inside `crates/lakekeeper`; in a crate it does not scan, review is what catches it. A name that reaches the wire outside a vocabulary is in no schema, so renaming it later breaks every consumer while the format check reports nothing.

Every vocabulary gets `as_wire()` and `as_str()` from the attribute, both from the same match. Do not write an `as_str` of your own: a `strum`, `serde` or hand-written derivation is a second source of truth, and it agrees with the registered name only until someone renames a variant.

**A context value drawn from a fixed set is a vocabulary; one derived from the request is data.** The `context` map of an action is open on its values, and has to be: most of them are a warehouse id, a namespace name, something the caller sent. Those need nothing. But a few keys hold a choice from a fixed set — `root_level` is `included` or `excluded`, `privilege_scope` is `every` or `only`, `update_kinds` is drawn from a closed list of commit kinds — and a consumer writes rules against those exactly as they do against `action_name`. Put `#[audit_part(field = "<the key>")]` on the enum behind such a value, so its values reach the schema and a rename fails the format check.

This is the easiest thing in the audit log to get wrong, because at the emission site a closed value and a free one look identical: both are a string pushed into the same map. Nothing in the code distinguishes them, so the decision has to be made by whoever adds the key. Ask whether a consumer could reasonably switch on the value. If yes, it is a vocabulary.

If the enum lives in a crate that cannot depend on `lakekeeper` — `iceberg-ext` is the case that exists — the attribute cannot be used, because its expansion names `::lakekeeper`. Register it by hand instead, as `TableUpdateKind` is registered in `events/backends/audit/mod.rs`, reading the same `VariantNames` the attribute would have read.

**Before setting `skip_log` on an error, ask what else records the event.** `skip_log` suppresses the ordinary error line, and the usual reason is that an audit record says the same thing with the principal named. That holds only while the audit record reaches the log, so ask `crate::audit::enabled()` first: it is `false` under `LAKEKEEPER__AUDIT__TRACING__ENABLED=false` and under a `tracing` filter that drops the `lakekeeper::audit` target, and suppressing both lines would leave the event in no log at all. Where a second, unconditional line already exists — a fail-closed admission gate warns whatever the audit configuration — suppress unconditionally instead, so an outage does not surface as an error this server did not have. The same gate opens every shape's `emit()`, so the configuration switch reaches records built outside the event listener too, and a switched-off audit trail serializes nothing.

**Never add a `_ =>` arm** to an `action_descriptor` match. The missing wildcard is the mechanism: with one, a new action silently emits no context. They carry `#[deny(clippy::wildcard_enum_match_arm)]`, so a wildcard fails `just check` and CI, though not a bare `cargo build`. A wildcard *alongside* the full list is caught by rustc's `unreachable_patterns`; the dangerous edit is replacing arms with one. Not every `action_descriptor` impl carries the deny yet, and `CatalogAction` is public, so authorizer crates have impls this repository cannot see; add the deny when you next touch an unprotected one.

**An action declares the `context` keys it can carry, and the schema publishes the pairing.** The named fields of the variant naming the action are its keys — `Drop { force, purge }` carries `force` and `purge` — so the usual case needs nothing beyond the fields you already wrote. Two cases cannot be read off the fields. A field whose *type* picks the keys lends its name to none of them and lists them instead, with `#[audit(expands_to = "a, b")]`; `SubtreeGrantScope` is the case that exists, where one field writes six keys. A variant with no fields at all, whose context a handler assembles in its own `event_actions` body, lists them with `#[audit(carries = "a, b")]` on the variant; `ManagementAction::ApplyGrants` is the case that exists. Declaring both is rejected — the fields already say it.

**A key whose value comes from a closed set says which set.** `#[audit(values_of = "RootLevelGrants")]` on the `ActionContextKey` variant makes the schema publish a `$ref` to that vocabulary instead of a bare `string`, so a consumer routing on `root_level` reads the two values it can be. Four keys use it: `root_level`, `privilege_scope`, `resource_types`, `update_kinds`. It is a reference by schema name, so `every_declared_value_set_is_a_vocabulary` checks the name resolves to a value vocabulary, and `no_fixture_value_falls_outside_its_declared_set` checks emitted records against it — the value reaches the wire as a `String`, so nothing else would notice a spelling the set does not hold. `values_of` and `holds` are alternatives: a value is a name from a set, or a whole object.

Two tests hold a declaration to the code, and they cover different things. `no_fixture_action_carries_an_undeclared_key` compares emitted fixtures against the declaration, so it sees whatever a fixture exercises. `every_event_actions_key_is_declared` reads every `event_actions` body in the tree, so it sees an action nobody pinned. Neither reaches a key pushed through `context_pairs`, whose keys come from a call this cannot follow — that is what `expands_to` is for, and a fixture built from the real code is what checks it.

**If you are adding audit types from another crate in this repository**, put the attribute on them, re-export Lakekeeper's emitter module as `crate::audit_emitter`, and add one test calling `lakekeeper::audit::schema::assert_crate_schema_committed`. `crates/authz-openfga` is the worked example. A crate outside this repository declares its own emitter and commits its own schema and fragments; the mechanism is shared and the vocabulary is not.

**2. Write a fragment.** Copy `audit-format/TEMPLATE.md` to `audit-format/unreleased/<descriptive-name>.md`, set `level` from the table above, and describe the change in the terms an operator parsing the log thinks in — which field moved, and what a parser has to do about it. The body is copied verbatim into the release notes, so write it for them rather than for a reviewer. One file per pull request; a name nobody else will pick keeps concurrent changes from colliding.

**First read what is already in `audit-format/unreleased/`.** A fragment describes the state at release, not your commit. If a fragment there already covers the field you are touching, fold your change into it and reword it to describe where the field ends up — `git mv` it if the name no longer fits. Two fragments describing one field leave a reader to replay a history they never saw. The same applies when your change undoes an unreleased one: withdraw that fragment rather than adding a second.

**3. Regenerate: `just update-audit-fixtures`, then `just update-audit-schema`.** The first computes `AUDIT_FORMAT` from the baseline and your fragment, renames the fixture directory if the major moved, and regenerates the fixtures. The second regenerates each crate schema from the types, merges them into `audit-format/schema.json`, and publishes that to `docs/docs/audit/schema.json`. Read both diffs. They are exactly what a consumer's pipeline will see, so anything in them you did not intend is the bug.

**4. Update `docs/docs/logging.md`** if the meaning changed. The schema regenerates itself and is published as `docs/docs/audit/schema.json`, so prose is for what a field means and when it appears, not for listing fields. On a major bump the example records need updating; they are the only documentation carrying a version value.

**5. Check it: `just check-audit-format`.** CI runs it on every pull request.

Keep exactly one fixture directory. A fixture records what the current code emits, so a retired format can neither be reproduced nor kept passing; it lives in the release notes and in git history instead.

Two things follow from the version being derived rather than bumped. Withdrawing a fragment **lowers** the required version again — if the change it described is reverted before release, delete the fragment and rerun the recipe, and a major that never shipped disappears along with its fixture directory rename. And a pull request targeting a `rel-*` branch may not change the format at all: a patch release shipping a different `audit_format` than main would give one number two meanings, so CI rejects a fragment or a moved constant there. Rework the change or hold it for the next minor.

#### What the checks cover

| Change                                                    | What fails                                          |
| --------------------------------------------------------- | ---------------------------------------------------- |
| A field or value on any audit type                        | the crate schema test, then the schema merge test   |
| A closed set of values pushed into a `context` map as a bare string | nothing — put the attribute on its enum             |
| A type that reaches the log without the attribute         | the registry test that the attribute is the only way |
| A crate that declares audit types and commits no schema   | the schema merge test                               |
| A field with no doc comment                               | the schema test                                     |
| A value that is not `lower_snake_case`                    | the house style test                                |
| A record or part field that is not `lower_snake_case`     | nothing — review catches it                         |
| A wire value built from a bare string                     | the constructor test                                |
| A record emitted outside the shapes module                | the emission site test                              |
| A field added, moved or retyped where a fixture covers it | the fixture comparison                              |
| A captured record whose objects do not match the schema   | the part validation in the fixture and corpus tests |
| A rule that holds for every record                        | the contract rules                                  |
| A handler that stops emitting                             | the corpus test                                     |
| A change with no fragment, or an understated one          | `just check-audit-format`                           |
| `AUDIT_FORMAT` not equal to what the fragments imply      | `just check-audit-format`                           |

The fragments are the claim; the schema and the fixtures are the evidence. Both pass again once you regenerate, whether you wrote a fragment or not. `check-audit-format` closes that gap by comparing the committed fixtures and the committed schema either side of the merge base and requiring a fragment that does not understate what it finds. It then checks `AUDIT_FORMAT` for **equality** with the value the baseline and the fragments imply — not for having moved, which is what makes a withdrawn fragment correct the version instead of leaving it over-claimed. Its decision table is self-tested with `--self-test`.

Checked by declaration, for every registered type: **fields, their JSON types, whether they are required, and every value of every vocabulary**. The schema sees a field on a path no fixture exercises, because it is generated from the types rather than from a sample. Enum values are keyed by the type that owns them, which is what makes a rename visible: six action enums emit `get_metadata`, and a comparison over the flattened set would not notice one of them renaming it.

Checked by example only: **which optional fields are omitted versus `null`, and the singular/plural arity switch**. The fixtures pin the scenarios they cover and nothing else, so if you change a code path no fixture exercises, add a fixture: write a test that builds the event, run `just update-audit-fixtures`, and register the name in `FIXTURE_NAMES`.

A fixture is compared by value, so everything in the record has to be deterministic. Where the emitter generates an id, pin it through a test-only seam rather than normalising the record afterwards — `RequestMetadataTestBuilder::request_id` and `AdmissionRejection::with_error_id` are the two that exist, and a new one belongs next to them. Normalising instead would pin the normalised form and not what the code emits, which is the only thing a fixture is for.

Comparison of fixtures is on field paths and JSON types rather than values, and containers record their own type, so `{}`, `[]` and an absent field stay distinguishable. A changed fixture *value* is reported but not classified, because nothing can tell a renamed wire value from a more realistic test input, and fixtures present in only one revision are not compared. Neither demands a fragment: both fire on changes that did nothing to the format.

**Extend the corpus test while you are here.** `crates/lakekeeper-integration-tests/tests/audit_corpus.rs` drives real requests through the service layer and checks rules that hold for *any* record, plus validation of every part against the committed schema. It catches the one thing the schema cannot: a handler that stops emitting at all, which every schema test passes happily. Drive one more call through `CatalogServer` or `ApiServer`, raise `EXPECTED_RECORDS` by however many records it emits, and the existing rules apply to whatever it produced. That count is exact because it is also what catches capture silently going to zero. Run it with `just test-audit-corpus`, which needs the local Postgres from the [Initial setup](#initial-setup). Keep every test in that file on a current-thread runtime: capture is thread-local, so `flavor = "multi_thread"` would capture nothing.

Outside `audit_format` entirely: the fields the subscriber adds — `timestamp`, `level`, `message`, `target`, `span`, `spans`, `filename`, `line_number` — belong to `tracing-subscriber` and can move on a dependency upgrade. They are stripped before comparison, and `docs/docs/logging.md` states that as a contract.

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

| Rule                     | What it means                                                                                                                                                                                                                                            |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Reserved prefix          | Upstream core migrations **never** create tables matching `ext_*`. Extensions own that namespace. The integration test `test_core_does_not_create_ext_objects` enforces this.                                                                            |
| FK direction             | Extension tables FK *into* upstream tables. Upstream never FKs into `ext_*`.                                                                                                                                                                             |
| CASCADE required         | Every FK from an `ext_*` table to an upstream table should be `ON DELETE CASCADE` or `ON DELETE SET NULL`. Enforce in the extension crate's own CI — upstream cannot inspect downstream migration sets.                                                  |
| Scope of allowed objects | `ext_*` may name tables and objects owned by those tables (indexes, sequences). Triggers, functions, indexes, or views attached to upstream-owned objects are not permitted under the prefix — they would survive extension removal and could brick OSS. |
| Tracker tables           | Each registered extension migration source uses its own SQLx tracker, named `ext_<name>_sqlx_migrations`. Core's `_sqlx_migrations` is untouched.                                                                                                        |

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

To check a new S3-compatible store, point the `LAKEKEEPER_TEST__S3_*` variables at it and run the `s3_compat` profile, which selects exactly the tests that use those variables and nothing else:

```sh
cargo nextest run --profile s3_compat --all-features --all-targets --workspace
```

This is what the SeaweedFS workflow runs.

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
1. Edit the relevant component(s) under `authz/openfga/<version>/components/*.fga` (e.g. add `define can_undrop: modify_effective` to `lakekeeper_view.fga`). An action reads the `_effective` twin of a privilege; the bare relation holds only what was granted on the object itself, then regenerate and validate:

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
