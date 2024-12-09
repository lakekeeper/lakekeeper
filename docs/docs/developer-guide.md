# Developer Guide

All commits to main should go through a PR. CI checks should pass before merging the PR.
Before merge commits are squashed. PR titles should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

## Foundation & CLA
We hate red tape. Currently all committers need to sign the CLA in github. To ensure the future of Lakekeeper, we want to donate the project to a foundation. We are not sure yet if this is going to be Apache, Linux, a Lakekeeper foundation or something else. Currently we prefer to spent our time on adding cool new features to Lakekeeper, but we will revisit this topic during 2026.

## Quickstart

```bash
# start postgres
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
# set envs
echo 'export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres' > .env
echo 'export ICEBERG_REST__PG_ENCRYPTION_KEY="abc"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_READ="postgresql://postgres:postgres@localhost/postgres"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgresql://postgres:postgres@localhost/postgres"' >> .env
source .env

# migrate db
cd crates/iceberg-catalog
sqlx database create && sqlx migrate run
cd ../..

# run tests
cargo test --all-features --all-targets

# run clippy
cargo clippy --all-features --all-targets
```

This quickstart does not run tests against cloud-storage providers or KV2. For that, please refer to the sections below.

## Developing with docker compose

The following shell snippet will start a full development environment including the catalog plus its dependencies and a jupyter server with spark. The iceberg-catalog and its migrations will be built from source. This can be useful for development and testing.

```sh
$ cd examples
$ docker-compose -f docker-compose.yaml -f docker-compose-latest.yaml up -d --build
```

You may then head to `localhost:8888` and try out one of the notebooks.

## Working with SQLx

This crate uses sqlx. For development and compilation a Postgres Database is required. You can use Docker to launch
one.:

```sh
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
```
The `crates/iceberg-catalog` folder contains a `.env.sample` File.
Copy this file to `.env` and add your database credentials if they differ.

Run:

```sh
sqlx database create
sqlx migrate run
```

## KV2 / Vault

This catalog supports KV2 as backend for secrets. Tests for KV2 are disabled by default. To enable them, you need to run the following commands:

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

Currently, we're not aware of a good way of testing cloud storage integration against local deployments. That means, in order to test against AWS S3, GCS and ADLS Gen2, you need to set the following environment variables. For more information take a look at the [Storage Guide](storage.md). A sample `.env` could look like this:

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

Please check the [Integration Test Docs](https://github.com/lakekeeper/lakekeeper/tree/main/tests).


## Extending Authz

When adding a new endpoint, you may need to extend the authorization model. Please check the [Authorization Docs](./authorization.md) for more information. For openfga, you'll have to perform the following steps:

1. extend the respective enum in `crate::service::authz` by adding the new action, e.g. `crate::service::authz::CatalogViewAction::CanUndrop`
2. add the relation to `crate::service::authz::implementations::openfga::relations`, e.g. add `ViewRelation::CanUndrop`
3. add the mapping from the `implementations` type to the `service` type in `openfga::relations`, e.g. `CatalogViewAction::CanUndrop => ViewRelation::CanUndrop`
4. create a new authz schema version by copying the latest existing one, e.g. `authz/openfga/v1/` to `authz/openfga/v2/`
5. apply your changes, e.g. add `define can_undrop: modify` to the `view` type in `authz/openfga/v2/schema.fga`
6. create a diff between the old and new schema via `diff -u authz/openfga/v1/schema.fga authz/openfga/v2/schema.fga > authz/openfga/v2/changed.diff` to help your reviewers
7. regenerate `schema.json` via `./fga model transform --file authz/openfga/v2/schema.fga > authz/openfga/v2/schema.json` (download the `fga` binary from the [OpenFGA repo](https://github.com/openfga/cli/releases/))
8. Head to `crate::service::authz::implementations::openfga::models.rs`, extend `CollaborationModels` with a field for your version, e.g., `v2` and then add your new model version on top of the file, like:
```rust
const V2_MODEL: &str = include_str!("../../../../../../../authz/openfga/v2/schema.json");

static MODEL: LazyLock<CollaborationModels> = LazyLock::new(|| CollaborationModels {
    v1: serde_json::from_str(V1_MODEL).expect("Failed to parse OpenFGA model V1 as JSON"),
    // this is your added model below
    v2: serde_json::from_str(V2_MODEL).expect("Failed to parse OpenFGA model V2 as JSON"),
});
```
9. set your model as the active model like: `const ACTIVE_MODEL: ModelVersion = ModelVersion::V2;`
10. implement the migration in `crate::service::authz::implementations::openfga::migrations::migrate` like:
```rust             
match model_version {
    ModelVersion::V1 => {
    // no migration to be done, we start at v1
    }
    ModelVersion::V2 => v2::migrate(client, &store).await,
}
```

