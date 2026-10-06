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

Audit records (every line with `"event_source": "audit"`) carry a `MAJOR.MINOR` version in their `audit_format` field. It is declared as `AUDIT_FORMAT` in `crates/lakekeeper/src/service/events/backends/audit/mod.rs`. Consumers route on it.

**Never pick that number.** `.github/scripts/check-audit-format.py` derives it from committed state:

```text
AUDIT_FORMAT = the version the last release tag declares, raised once by the highest level among the fragments in audit-format/unreleased/ that tag does not carry
```

A change owes a **fragment**: one file that states how much the change affects a consumer and describes it in the consumer's terms. `just update-audit-fixtures` reads the fragments and writes the version.

The version moves at most once per release, by the highest fragment level. A major change absorbs every minor change of the same cycle, so a release ships `4.0`, not `4.4`. The release notes still list every fragment: the version tells a consumer how much they are affected, the list tells them what changed.

#### Terms

- **Audit type**: a Rust type that reaches the audit log, marked `#[audit_part]`. The attribute makes it serialisable, builds its JSON Schema from its doc comments, and registers it with the emitter of its crate. Everything in a record is an audit type: a record part, an operation context, a value vocabulary or a key vocabulary.
- **Field**: a name in the record, such as `action_name`, `entity_type` or `warehouse_id`. Adding one changes the record's shape.
- **Value**: the string a field holds, such as `get_metadata` in `action_name`. Every value comes from a **value vocabulary**: an enum marked `#[audit_part(field = "…")]` whose variant names are the values of that field. Adding a value leaves the shape unchanged; consumers are told to tolerate values they do not know. A set that must not grow without a major version adds `closed`, as `decision`, `privilege_scope`, `root_level` and `effect` do.
- **Key vocabulary**: an enum marked `#[audit_part(keys_of = "…")]` whose variant names are the *keys* of an object: `EntityField` in an entity, `ActionContextKey` in an action, `HandlerContextKey` in the record's `context`. The schema publishes each key as a property of its object, described by the variant's doc comment, which the attribute requires. Adding a key adds a field, so it is `minor`. A key is emitted as a `WireKey` and a value as a `Wire<Vocabulary>`; neither converts into the other, so a key cannot land where a value belongs. A `Wire<T>` names its vocabulary in its type: a field typed `Wire<ActorType>` takes no other set's value, and its schema is a `$ref` to that set.
- **Shape**: which fields exist, their JSON types, their nesting, and the switch between singular and plural fields.
- **Schema**: `docs/docs/audit/schema.json`, the file customers download and validate against. It lists every object, every field with its description, and every value set. `crates/lakekeeper-integration-tests/tests/audit_schema.rs` generates it with `lakekeeper::audit::schema::assert_published_schema`, from the registry of a test binary that links every crate declaring audit types. The test fails if such a crate is not linked.
- **Fixture**: a committed golden record in `crates/lakekeeper/src/service/events/backends/audit/fixtures/`, written by the test that asserts against it. It pins the emitted bytes of the scenarios it covers, and nothing else.
- **Fragment**: a committed `audit-format/unreleased/*.md` file for one change: its level, and text for the release notes. Written in the pull request that makes the change; copied into the release notes and deleted at release.
- **Level**: `major`, `minor` or `none`: how much the change affects a consumer. The only judgement you make.
- **Baseline**: the version declared by the last release tag reachable from your commit. A release tag matches `vX.Y.Z`; prereleases do not count. There is no baseline until a release carries an audit format. A fragment that tag already carries, with the same text, shipped with it: it raises nothing and waits to be cleared.

The schema pins every declared field and value. Fixtures pin the emitted bytes of the scenarios they cover. Fragments record intent. The corpus test validates real emitted records against the schema but pins none of their bytes.

**Which level.** A value vocabulary reaches the log as strings, so renaming a variant changes the payload even though no field moves. Adding a variant does not: `docs/docs/logging.md` tells consumers that value sets are open and that an unknown value is opaque. A new field changes the shape, and a consumer reading the shape sees it.

| Change                                                                                       | Level       |
| -------------------------------------------------------------------------------------------- | ----------- |
| A field added, including a new key of an entity, action or context (a key vocabulary variant) | `minor`     |
| A new action, entity type or other value of an open set                                      | `none`      |
| A value added to a `closed` set                                                              | `major`     |
| A field removed, renamed or retyped                                                          | `major`     |
| A value renamed or removed                                                                   | `major`     |
| A fixture edited, renamed, added or dropped, with no change to what the server emits         | no fragment |

A `none` fragment is optional. Write one to put a new action in the release notes without moving the version.

#### Steps

**1. Make the change.**

- **A value**: add a variant to the value vocabulary of the field and emit `Variant::as_wire()`. The enum already carries `#[audit_part(field = "...")]`, so the variant reaches the schema with no list to update.
- **A new vocabulary**: put `#[audit_part(field = "...")]` on the enum for a set of values, or `#[audit_part(keys_of = "...")]` for a set of object keys. Nothing else registers it. A variant's doc comment becomes that name's description in the schema; write what the name means to a consumer.
- **A field**: add it to the part or context struct with a doc comment, or add a variant to the key vocabulary of its object.
- **A field on a record shape**: add it to the struct in `shapes.rs` with a doc comment. `#[audit_part(shape = "...")]` writes the shape's `emit()` from its fields, so the field reaches the record and the schema under its own name. The attribute rejects `rename`, `skip` and `flatten` on a shape's fields, and a shape outside the `lakekeeper` crate.
- **A new kind of operation record, from any crate**: an operations enum, an outcomes enum and a context struct, each with the attribute, then `OperationRecord::new(..).context(..).emit()`.

**Every name is `lower_snake_case`**: runs of `[a-z0-9]` joined by single underscores, starting with a letter. This covers a record's own fields, every key, and every value this log owns. Every other Lakekeeper log line uses the same spelling. A vocabulary's names are its variant names in `snake_case`, or the name `#[audit(rename = "...")]` gives a variant. The macro reads no `strum` or `serde` attribute, so an enum that is also an API type keeps its API spelling to itself. The macro rejects any other spelling at compile time.

The field names of records and parts follow the same rule, but no test checks them. A field reaches the wire in another spelling only through an explicit `#[serde(rename = "...")]`, so check for it in review. One rename is deliberate: `policy-id` inside `determined_by` keeps the management API's spelling, so one parser reads a `/check` response and an audit record alike.

A value set spelled by someone else adds `external_values`, as in `#[audit_part(field = "entity_type", external_values)]`, states its spelling with `#[audit(rename_all = "kebab-case")]`, and is exempt from the case check. Name the vocabulary it follows in the doc comment. Three sets do this: `entity_type`, `resource_type` and `update_kinds`.

**Never build a wire name from a bare string.** `Wire::new` and `WireKey::new` are for the attribute's expansion only. A test fails on any other caller inside `crates/lakekeeper`; in other crates, review must catch it. A name that reaches the wire outside a vocabulary is in no schema, so renaming it later breaks consumers and the format check reports nothing.

The attribute generates `as_wire()` and `as_str()` from one match. Never write your own `as_str` (by hand, `strum` or `serde`): it is a second source for the name and drifts from the registered one when a variant is renamed.

**A context value drawn from a fixed set is a vocabulary; a value derived from the request is data.** Most context values are data: a warehouse id, a namespace name, something the caller sent. A key holding a `String` needs nothing more. Some keys hold a choice from a fixed set: `root_level` is `included` or `excluded`, `privilege_scope` is `every` or `only`, `update_kinds` comes from a list of commit kinds. Consumers write rules against these as they do against `action_name`. Put `#[audit_part(field = "<the key>")]` on the enum behind such a value and let the key hold `Wire<ThatEnum>`, so its values reach the schema and a rename fails the format check.

The compiler cannot make this decision: a `String` key accepts a closed value as easily as a free one. Ask whether a consumer could reasonably switch on the value. If yes, it is a vocabulary.

If the enum lives in a crate that cannot depend on `lakekeeper`, such as `iceberg-ext`, the attribute does not work: its expansion names `::lakekeeper`. Register the enum by hand, as `TableUpdateKind` is registered in `events/backends/audit/mod.rs`, from the same `VariantNames` the attribute would read.

**Before setting `skip_log` on an error, ask what else records the event.** `skip_log` suppresses the ordinary error line, usually because an audit record says the same thing with the principal named. That record exists only while the audit trail reaches the log, so ask `crate::audit::enabled()` first. It is `false` under `LAKEKEEPER__AUDIT__TRACING__ENABLED=false` and under a `tracing` filter that drops the `lakekeeper::audit` target; suppressing the error line then leaves the event in no log at all. Where a second, unconditional line already exists, such as the warning a fail-closed admission gate always writes, suppress unconditionally, so an outage does not show up as an error this server did not have. The same switch gates every shape's `emit()` and `OperationRecord::context`, so records built outside the event listener follow it too, and a disabled audit trail serialises nothing.

**Never add a `_ =>` arm to an `action_descriptor` match.** With one, a new action silently emits no context. The matches carry `#[deny(clippy::wildcard_enum_match_arm)]`, so a wildcard fails `just check` and CI, but not a plain `cargo build`. A wildcard next to the full list is caught by rustc's `unreachable_patterns`; the risky edit is replacing arms with one. Every impl in this repository that matches on its action carries the deny. `EventAction` is public, so an impl in another crate needs it too.

**Only an `EventAction` reaches a record.** A handler's action implements `EventAction`, and only a vocabulary enum can, so every action name in a record is declared. An authorizer's own action types (`Authorizer::TableAction` and its siblings, such as the OpenFGA relations) name what that authorizer checks. They implement `Display` for the forbidden errors, never `EventAction`. An authorizer endpoint that records its own action declares a small vocabulary for it and converts it into its own type for the check, as `PermissionAction` in `crates/authz-openfga` does.

**An action declares the `context` keys it can carry, and the schema publishes the pairing.** By default the named fields of the action's variant are its keys: `Drop { force, purge }` carries `force` and `purge`. Two cases need more:

- A field whose *type* picks the keys lists them with `#[audit(expands_to = "a, b")]`. Example: a `SubtreeGrantScope` field writes six keys.
- A variant with no fields, whose context the handler assembles in `event_actions`, lists them with `#[audit(carries = "a, b")]` on the variant. Example: `ManagementAction::ApplyGrants`.

Declaring both on one variant is rejected. An unnamed field of an action always needs `expands_to`, with `expands_to = ""` for no keys.

**A context key holds its value, and the held type is what the schema publishes.** Context keys sit in two places: inside each entry of `actions`, beside `action_name`, and in the record's own `context` object. A key of either is a variant holding one value: `ActionContextKey::Force(bool)`, `HandlerContextKey::SelfRead(bool)`, `ActionContextKey::RootLevel(Wire<RootLevelGrants>)`. The held type must implement `Serialize` and `JsonSchema`: serde writes the value, and the schema publishes what schemars writes for the type. A value from a closed set is a `Wire<Vocabulary>`, whose schema is a `$ref` to the set; a part is a `$ref` to its definition. Write an action's keys with `.context(ActionContextKey::X(v))` and the record's own with `push_extra_context(HandlerContextKey::X(v))`, which accepts only keys of the `context` object. A wrong type, a value outside its set, or a flag written as `"true"` does not compile. An entity field holds nothing: its value is always a string.

Two tests hold the declarations to the code. `every_carried_key_is_a_declared_key` checks from the registry that every key an action says it carries is declared on the `action` object. `no_fixture_action_carries_an_undeclared_key` checks emitted fixtures against the declarations, so it sees only what fixtures exercise. For that reason the fixtures for `apply_grants` and `revoke_subtree_grants`, whose context a handler assembles, are built from the handlers' own `event_actions()`.

**Audit types in another crate of this repository**: put the attribute on them, import Lakekeeper's emitter with `use lakekeeper::audit_emitter;` at the crate root, and add `use <crate> as _;` to `crates/lakekeeper-integration-tests/tests/audit_schema.rs` so the schema test links the crate. The schema test fails until you do. `crates/authz-openfga` is the worked example.

**A product outside this repository** uses the same machinery with its own vocabulary and version. It declares its emitter once with `lakekeeper::declare_audit_emitter!` in the crate that owns its `AUDIT_FORMAT`; its other crates import that crate's `audit_emitter` module. One test in a binary that links all its crates calls `lakekeeper::audit::schema::assert_published_schema` and writes the product's schema, which is published next to Lakekeeper's. It runs this repository's format checker over its own fragments; `audit-format/config.json` points the checker at its paths (`audit_dir`, `schema`, `version_const`, `version_search_path`, `release_notes`, and optionally `release_tag_pattern`, `fragments` and `release_table`). Lakekeeper+ is the worked example.

**2. Write a fragment.** First read what is already in `audit-format/unreleased/`. A fragment describes the state at release, not your commit. If a fragment already covers the field you touch, fold your change into it and reword it to describe where the field ends up; `git mv` it if its name does not fit. If your change undoes one a fragment describes, delete that fragment. Two fragments about one field make the reader replay changes that never shipped.

Fold into a fragment, or delete one, only if no release has shipped it. The folder is cleared after each release, once its notes are written, so for a while it also holds fragments the last release shipped. `just check-audit-format` lists them as "shipped with vX.Y.Z and are still here", and the notes of that release quote them. A shipped fragment is closed: leave it as it is, and describe your change, even one that undoes it, in a new fragment. The same holds when a merge from `main` reports a fragment you edited as deleted there: it shipped and was cleared. Take the deletion, and put only your change in a new fragment.

Otherwise copy `audit-format/TEMPLATE.md` to `audit-format/unreleased/<descriptive-name>.md` and set `level` from the table above. Describe the change for an operator parsing the log: which field moved and what their parser has to do. The body is copied verbatim into the release notes. Use one file per pull request, with a name no other unreleased fragment has, so concurrent changes do not collide. A fragment is its name and its text, so the name of a cleared fragment is free again.

**3. Regenerate: `just update-audit-fixtures`, then `just update-audit-schema`.** Both compute `AUDIT_FORMAT` from the baseline and the fragments. The first regenerates the fixtures, the second `docs/docs/audit/schema.json`. Review both diffs: they are what a consumer's pipeline will see, so anything you did not intend is a bug.

**4. Update `docs/docs/logging.md`** if a meaning changed. The schema lists the fields; the prose says what a field means and when it appears. A test validates every complete audit record example on that page against the schema, so a shape change fails until the examples are updated.

**5. Commit, then run `just check-audit-format`.** It reads `HEAD`, not the working tree. CI runs it on every pull request, and again on every push to `main` and `rel-*`, so a pull request that passed against an older base is checked as merged.

A fixture records what the current code emits, so a retired format cannot be reproduced. It lives in the release notes and in git history.

Because the version is derived, deleting a fragment **lowers** the required version again. If the change it described is reverted before release, delete the fragment and rerun the recipe; a major that never shipped disappears.

A pull request targeting a `rel-*` branch must not change the format: a patch release with a different `audit_format` than main would give one number two meanings. CI rejects a fragment or a moved constant there. Rework the change or hold it for the next minor.

#### What the checks cover

| Change                                                                     | What fails                                              |
| -------------------------------------------------------------------------- | ------------------------------------------------------- |
| A field or value on any audit type                                         | the published schema test                               |
| A closed set of values pushed into a `context` map as a bare string        | nothing; put the attribute on its enum                  |
| A type that reaches the log without the attribute                          | `audit_part_is_only_implemented_through_the_attribute`  |
| A crate that declares audit types but is not linked into the schema test   | the published schema test                               |
| A field or key with no doc comment                                         | the attribute, at compile time                          |
| A value that is not `lower_snake_case`                                     | the attribute, at compile time                          |
| A record or part field that is not `lower_snake_case`                      | nothing; review catches it                              |
| A wire value built from a bare string                                      | `only_the_attribute_turns_a_bare_string_into_a_wire_value` |
| A record emitted outside the shapes module                                 | `only_the_shapes_emit_audit_records`                    |
| A field added, moved or retyped where a fixture covers it                  | the fixture comparison                                  |
| A captured record that does not match the schema                           | whole-record validation in the fixture and corpus tests |
| A handler that stops emitting                                              | the corpus test                                         |
| A change with no fragment, or a fragment with too low a level              | `just check-audit-format`                               |
| `AUDIT_FORMAT` not equal to what the fragments imply                       | `just check-audit-format`                               |

Regenerating makes the schema and fixture tests pass whether or not you wrote a fragment. `check-audit-format` catches that: it compares the committed fixtures and schema with the last release tag. Everything that changed since the release must be covered by the unreleased fragments together, and what this pull request changed must be covered by a fragment it adds, raises or rewords. So a revert of unreleased work owes nothing, and deleting an old fragment cannot leave a released change undescribed. Then it checks that `AUDIT_FORMAT` **equals** the version the baseline and the fragments imply, so a deleted fragment lowers the version. Until a release carries an audit format, what this pull request changed is compared with its merge base, and must be covered by a fragment it adds, raises or rewords. `--self-test` runs the checker's own tests.

Checked by declaration, for every registered type: **fields, their JSON types, whether they are required, and every value of every vocabulary**. The schema is generated from the types, so it covers fields no fixture exercises. Values are keyed by the type that owns them, so a rename in one enum shows even when other action enums emit the same name, such as `get_metadata`. The comparison fails closed:

- `minor`: a property added.
- `major`: a property removed, a required flag changed, a value removed, a closed set gaining a value, a tagged union gaining a kind, and any difference without a rule.
- `none`: a value added to an open set, a description changed.

Checked by example only: **whether an optional field is omitted or `null`, and the switch between singular and plural fields**. Fixtures pin only the scenarios they cover. If you change a code path no fixture exercises, add a fixture: write a test that builds the event, run `just update-audit-fixtures`, and add the name to `FIXTURE_NAMES`.

**Build a fixture from input a real request could carry.** A fixture is a published example, so an impossible one teaches consumers a wrong shape. Nothing else catches it: `action_name` is open in the schema, so validation accepts an action paired with an entity kind that cannot carry it. Handlers cannot make this mistake, because they pair action and entity through the type system (`APIEventContext::for_table` takes only a `CatalogTableAction`); a hand-assembled fixture can. Pair an action only with an entity whose vocabulary declares it (`read_data` is a table's and a generic table's, `update_properties` a namespace's). Prefer building the descriptor with the action's own `action_descriptor()`, or the whole record from the handler's `event_actions()`. A fixture with several actions and several entities emits a decision per pair, so every pair must be possible.

A fixture is compared by value, so the record must be deterministic. Pin generated ids through a test-only seam: `RequestMetadataTestBuilder::request_id`, `RequestMetadata::with_request_id` and `AdmissionRejection::with_error_id`. Add a new seam next to them. Never rewrite the record after capture: the fixture would then pin the rewritten form, not what the code emits. `time` is the one exception, because the clock has no seam: `audit::validate::pin_time` checks that it is RFC 3339 in UTC and replaces it with a fixed value. Every fixture writer calls it.

`check-audit-format` compares fixtures on field paths and JSON types, not values. Containers record their own type, so `{}`, `[]` and an absent field stay distinct. Values are not compared, because a renamed value cannot be told apart from a more realistic test input, and a renamed value of a registered set already shows in the schema. A fixture whose name is gone is compared with the fixture that took its place: among the new names, the one closest to it. So a plain rename owes nothing, and a field dropped in a rename still counts as removed. Rename fixtures in a pull request of their own: a rename next to an unrelated new fixture can be paired with the wrong one. A fixture deleted with no new name is compared with every fixture left; what none of them records counts as removed: `major`.

**Extend the corpus test.** `crates/lakekeeper-integration-tests/tests/audit_corpus.rs` drives real requests through the service layer and validates every captured record against the committed schema. It catches what the schema cannot: a handler that stops emitting. Drive one more call through `CatalogServer` or `ApiServer` and raise `EXPECTED_RECORDS` by the number of records it emits. The count is exact so that capture silently dropping to zero fails. Run it with `just test-audit-corpus`; it needs the local Postgres from the [Initial setup](#initial-setup). Keep every test in that file on a current-thread runtime: capture is thread-local, so `flavor = "multi_thread"` captures nothing.

Outside `audit_format`: the fields the subscriber adds (`timestamp`, `level`, `message`, `target`, `span`, `spans`, `filename`, `line_number`) belong to `tracing-subscriber` and can change on a dependency upgrade. They are stripped before comparison, and `docs/docs/logging.md` states this as a contract.

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
