# Contributing to Lakekeeper

Thanks for your interest in contributing! This is a quick-start guide.
For full details, see the [Developer Guide](docs/docs/developer-guide.md).

## Before you start

- Small, self-contained fixes: just open a PR.
- Bigger features: open an issue and discuss with us first.
- Not sure what to work on? Check issues tagged `help wanted`.
- To claim an open issue with no assignees, comment only `/assign` on the issue.
- Questions? Use the GitHub issue or our [Discord](https://discord.gg/jkAGG8p93B).

## Contributor License Agreement

All committers need to sign the CLA in GitHub before a PR can be merged.

## Pull requests

- All commits to `main` go through a PR.
- CI must pass, including lints.
- PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
- Commits are squashed at merge (handled by GitHub automatically).
- Keep PRs small and orthogonal.

## Development setup

```
# start postgres
docker run -d --name postgres-16 -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17

# set envs
echo 'export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres' > .env
echo 'export ICEBERG_REST__PG_ENCRYPTION_KEY="abc"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_READ="postgresql://postgres:postgres@localhost/postgres"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgresql://postgres:postgres@localhost/postgres"' >> .env
source .env

# migrate db (requires sqlx-cli: cargo install sqlx-cli)
sqlx database create
sqlx migrate run --source crates/lakekeeper-storage-postgres/migrations

# run tests (requires cargo-nextest: cargo install cargo-nextest)
cargo nextest run --all-features

# run clippy
just check-clippy

# format code (requires cargo-sort: cargo install cargo-sort)
just fix-format
```

If you change SQL queries, run `just sqlx-prepare` and commit the updated
`.sqlx` directory before pushing, or the build will fail on CI.

## Where to put tests

Unit tests live close to the code they test. Shared test helpers live in
`crates/lakekeeper/src/tests/mod.rs`. Integration tests are Python/pytest,
under `tests/` (see [Integration Test Docs](tests/README.md)).

## Extending AuthZ

Adding a new endpoint may require extending the authorization model.
See the [Authorization Docs](docs/docs/authorization.md) and the
[Developer Guide](docs/docs/developer-guide.md#extending-authz) for the
OpenFGA versioning steps.

## Changing the audit log format

Any change to audit record fields or wire values requires a fragment
under `audit-format/unreleased/` and passing `just check-audit-format`.
See [Developer Guide](docs/docs/developer-guide.md#i-need-to-change-the-audit-log-format)
for the full process.

## Knowledge graph (optional)

If you use a coding agent, the [graphify](https://github.com/safishamsi/graphify) knowledge graph of the code in `graphify-out/graph.json` gives it a map of the repository. The graph is committed and shared, following graphify's [shared team graph](https://docs.graphify.com/cookbooks/shared-team-graph) recipe; the other files in `graphify-out/` are gitignored and rebuilt locally. `.graphifyignore` lists files the graph skips.

- `just graph` rebuilds the graph (needs [`uv`](https://docs.astral.sh/uv/)). It runs locally, costs no API tokens, and takes a few seconds. Rerun it after code changes and commit the updated `graph.json` with them.
- `just graph-setup` registers graphify's union merge driver in your clone, once. `.gitattributes` routes `graph.json` through it, so two branches that both changed the graph merge without a conflict.
- To let your coding agent query the graph, install graphify and its skill or hooks for your agent; see the [graphify quickstart](https://docs.graphify.com/open-source/quickstart). Some integrations, such as `graphify claude install`, write to tracked files like `CLAUDE.md` and `.claude/settings.json`; keep those changes out of your commits.

## Building docs locally

```
cd site
just serve
```

## License

By contributing, you agree your contributions will be licensed under the
[Apache License 2.0](LICENSE).
