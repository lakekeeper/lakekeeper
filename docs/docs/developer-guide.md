# Developer Guide

All commits to main go through a PR. CI checks have to pass before merging the PR. Keep in mind that CI checks include lints. Before merge, commits are squashed, but GitHub is taking care of this, so don't worry. PR titles should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). We encourage small and orthogonal PRs. If you want to work on a bigger feature, please open an issue and discuss it with us first. 

If you want to work on something but don't know what, take a look at our issues tagged with `help wanted`. If you're still unsure, please reach out to us via the [Lakekeeper Discord](https://discord.gg/jkAGG8p93B). If you have questions while working on something, please use the GitHub issue or our Discord. We are happy to guide you!

## Foundation & CLA
We hate red tape. Currently, all committers need to sign the CLA in GitHub. To ensure the future of Lakekeeper, we want to donate the project to a foundation. We are not sure yet if this is going to be Apache, Linux, a Lakekeeper foundation or something else. Currently, we prefer to spend our time on adding cool new features to Lakekeeper, but we will revisit this topic during 2026.

## Initial Setup

To work on small and self-contained features, it is usually enough to have a Postgres database running while setting a few envs. The code block below should get you started up to running most unit tests as well as clippy.

```bash
# start postgres
docker run -d --name postgres-16 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
# set envs
echo 'export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres' > .env
echo 'export ICEBERG_REST__PG_ENCRYPTION_KEY="abc"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_READ="postgresql://postgres:postgres@localhost/postgres"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgresql://postgres:postgres@localhost/postgres"' >> .env
source .env

# Migrate db
cd crates/lakekeeper
sqlx database create && sqlx migrate run
cd ../..

# Run tests (make sure you have cargo nextest installed, `cargo install cargo-nextest`)
cargo nextest run --all-features

# run clippy
just check-clippy
# formatting the code. You may have to install nightly rust toolchain
just fix-format
```
Keep in mind that some tests are gated by `TEST_*` env vars. You can find a list of them in the [Testing section](#test-cloud-storage-profiles) below or by searching for `needs_env_var` within files ending with `.rs`.
There are a few cargo commands we run on CI. You may install [just](https://crates.io/crates/just) to run them conveniently.
If you made any changes to SQL queries, please follow [Working with SQLx](#working-with-sqlx) before submitting your PR.

## Code structure

### What is where?

We have three crates, `lakekeeper`, `lakekeeper-bin` and `iceberg-ext`. The bulk of the code is in `lakekeeper`. The `lakekeeper-bin` crate contains the main entry point for the catalog. The `iceberg-ext` crate contains extensions to `iceberg-rust`. 

#### lakekeeper

The `lakekeeper` crate contains the core of the catalog. It is structured into several modules:

1. `api` - contains the implementation of the REST API handlers as well as the `axum` router instantiation.
2. `catalog` - contains the core business logic of the REST catalog
3. `service` - contains various function blocks that make up the whole service, e.g., authn, authz and implementations of specific cloud storage backends.
4. `tests` - contains integration tests and some common test helpers, see below for more information.
5. `implementations` - contains the concrete implementation of the catalog backend, currently there's only a Postgres implementation and an alternative for Postgres as secret-store, `kv2`.

#### lakekeeper-bin

The main function branches out into multiple commands, amongst others, there's a health-check, migrations, but also serve which is likely the most relevant to you. In case you are forking us to implement your own AuthZ backend, you'll want to change the `serve` command to use your own implementation, just follow the call-chain.

### Where to put tests?

We try to keep unit-tests close to the code they are testing. E.g., all tests for the database module of tables are located in `crates/lakekeeper/src/implementations/postgres/tabular/table/mod.rs`. While working on more complex features we noticed a lot of repetition within tests and started to put commonly used functions into `crates/lakekeeper/src/tests/mod.rs`. Within the `tests` module, there are also some higher-level tests that cannot be easily mapped to a single module or require a non-trivial setup. Depending on what you are working on, you may want to put your tests there.

### I need to add an endpoint

You'll start at `api` and add the endpoint function to either `management` or `iceberg` depending on whether the endpoint belongs to official iceberg REST specification. The likely next step is to extend the respective `Service` trait so that there's a function to be called from the REST handler. Within the trait function, depending on your feature, you may need to store or fetch something from the storage backend. Depending on if the functionality already exists, you can do so via the respective function on the `C` generic and either the `state: ApiContext<State<...>>` struct or by first getting a transaction via `C::Transaction::begin_<write|read>(state.v1_state.catalog.clone()).await?;`. If you need to add a new function to the storage backend, extend the `Catalog` trait and implement it in the respective modules within `implementations`. Remember to do appropriate AuthZ checks within the function of the respective `Service` trait.

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
# Run this locally if you change the db schema via `crates/lakekeeper/migrations`,
# e.g. after adding a table or dropping a column.
cd crates/lakekeeper
sqlx database create && sqlx migrate run
cd ../..

# If you changed any of the SQL statements embedded in Rust code, run this before pushing to GitHub.
just sqlx-prepare

```
This will update the sqlx queries in `.sqlx` to enable static checking of the queries without a migrated database. Remember to `git add .sqlx` before committing. If you forget, your PR will fail to build on GitHub.
Be careful, if the command failed, `.sqlx` will be empty. But do not worry, it wouldn't build on GitHub so there's no way of really breaking things.

## KV2 / Vault

This catalog supports KV2 as a backend for secrets. Tests for KV2 are disabled by default. To enable them, you need to run the following commands:

```shell
docker run -d -p 8200:8200 --cap-add=IPC_LOCK -e 'VAULT_DEV_ROOT_TOKEN_ID=myroot' -e 'VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200' hashicorp/vault

# append some more env vars to the .env file, it should already have PG related entries defined above.

# this will enable the KV2 tests
echo 'export TEST_KV2=1' >> .env
# the values below configure KV2
echo 'export ICEBERG_REST__KV2__URL="http://localhost:8200"' >> .env
echo 'export ICEBERG_REST__KV2__USER="test"' >> .env
echo 'export ICEBERG_REST__KV2__PASSWORD="test"' >> .env
echo 'export ICEBERG_REST__KV2__SECRET_MOUNT="secret"' >> .env

source .env
# setup vault
./tests/vault-setup.sh http://localhost:8200

cargo test --all-features --all-targets
```

## Test cloud storage profiles

Currently, we're not aware of a good way of testing cloud storage integration against local deployments. That means, to test against AWS S3, GCS and ADLS Gen2, you need to set the following environment variables. For more information, take a look at the [Storage Guide](storage.md). A sample `.env` could look like this:

```sh
# TEST_AZURE=<some-value> controls a proc macro which either includes or excludes the azure tests
# if you compiled without TEST_AZURE, you'll have to change a file or do a cargo clean before rerunning tests. The same applies for the TEST_AWS and TEST_MINIO env vars.
export TEST_AZURE=1
export AZURE_TENANT_ID=<your tenant id>
export AZURE_CLIENT_ID=<your entra id app registration client id>
export AZURE_CLIENT_SECRET=<your entra id app registration client secret>
export AZURE_STORAGE_ACCOUNT_NAME=<your azure storage account name>
export AZURE_STORAGE_FILESYSTEM=<your azure adls filesystem name>

export TEST_AWS=1
export AWS_S3_BUCKET=<your aws s3 bucket>
export AWS_S3_REGION=<your aws s3 region>
# replace with actual values
export AWS_S3_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_S3_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_S3_STS_ROLE_ARN=arn:aws:iam::123456789012:role/role-name

# the values below should work with the default minio in our docker-compose
export TEST_MINIO=1
export LAKEKEEPER_TEST__S3_BUCKET=tests
export LAKEKEEPER_TEST__S3_REGION=local
export LAKEKEEPER_TEST__S3_ACCESS_KEY=minio-root-user
export LAKEKEEPER_TEST__S3_SECRET_KEY=minio-root-password
export LAKEKEEPER_TEST__S3_ENDPOINT=http://localhost:9000
```

You may then run a test via:

```sh
source .example.env-from-above
cargo test service::storage::s3::test::aws::test_can_validate
```

## Running integration test

Our integration tests are written in Python and use pytest. They are located in the `tests` folder. The integration tests spin up Lakekeeper and all the dependencies via `docker compose`. Please check the [Integration Test Docs](https://github.com/lakekeeper/lakekeeper/tree/main/tests) for more information.

### Running Authorization unit tests

Some authorization unit tests need to be run against an OpenFGA server. They are enabled only if TEST_OPENFGA is set. The workflow for executing them is:

```bash
# Start an OpenFGA server in a docker container
docker rm --force openfga-client && docker run -d --name openfga-client -p 36080:8080 -p 36081:8081 -p 36300:3000 openfga/openfga:v1.8 run

# Set Lakekeeper's OpenFGA endpoint
export LAKEKEEPER_TEST__OPENFGA__ENDPOINT="http://localhost:36081"

# Enable and run the tests
export TEST_OPENFGA=1
cargo nextest run --all-features --lib openfga
```

## Extending Authz

When adding a new endpoint, you may need to extend the authorization model. Please check the [Authorization Docs](./authorization.md) for more information. For openfga, you'll have to perform the following steps:

1. extend the respective enum in `crate::service::authz` by adding the new action, e.g. `crate::service::authz::CatalogViewAction::CanUndrop`
1. add the relation to `crate::service::authz::implementations::openfga::relations`, e.g. add `ViewRelation::CanUndrop`
1. add the mapping from the `implementations` type to the `service` type in `openfga::relations`, e.g. `CatalogViewAction::CanUndrop => ViewRelation::CanUndrop`
1. create a new authz schema version by renaming the version for backward compatible changes, e.g. `authz/openfga/v2.1/` to `authz/openfga/v2.2/`. For non-backward compatible changes create a new major version folder.
1. apply your changes, e.g. add `define can_undrop: modify` to the `view` type in `authz/openfga/v2.2/schema.fga`
1. regenerate `schema.json` via `./fga model transform --file authz/openfga/v2.2/schema.fga > authz/openfga/v2.2/schema.json` (download the `fga` binary from the [OpenFGA repo](https://github.com/openfga/cli/releases/))
1. Head to `crate::service::authz::implementations::openfga::migration.rs`, modify `ACTIVE_MODEL_VERSION` to the newer version. For backwards compatible changes, change the `add_model` section. For changes that require migrations, add an additional `add_model` section that includes the migration fn.

```rust
pub(super) static ACTIVE_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(3, 0)); // <- Change this for every change in the model


fn get_model_manager(
    client: &BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> openfga_client::migration::TupleModelManager<BasicAuthLayer> {
    openfga_client::migration::TupleModelManager::new(
        client.clone(),
        &store_name.unwrap_or(AUTH_CONFIG.store_name.clone()),
        &AUTH_CONFIG.authorization_model_prefix,
    )
    .add_model(
        serde_json::from_str(include_str!(
            // Change this for backward compatible changes.
            // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
            "../../../../../../../authz/openfga/v3.0/schema.json"
        ))
        // Change also the model version in this string:
        .expect("Model v3.0 is a valid AuthorizationModel in JSON format."),
        AuthorizationModelVersion::new(3, 0),
        // For major version upgrades, this is where tuple migrations go.
        None::<MigrationFn<_>>,
        None::<MigrationFn<_>>,
    )
}
```


