Tests that require external components

## Integration Tests

Integration tests have external dependencies, they are typically run with docker-compose. Running the tests requires the
docker image for the server to be specified via env-vars.

Run the following commands from the crate's root folder.
`run.sh` is a host-side wrapper: it automatically selects the correct Spark
Docker image based on the Iceberg version suffix (Spark 4 for ≥ 1.10, Spark 3
otherwise) and adds any required docker-compose overlay files. No manual
`LAKEKEEPER_TEST__SPARK_IMAGE` export is needed.

```sh
docker build -t localhost/lakekeeper-local:latest -f docker/full-debug.Dockerfile .
export LAKEKEEPER_TEST__SERVER_IMAGE=localhost/lakekeeper-local:latest
cd tests

# All default-version tests
bash run_default.sh

# S3 (STS) — uses Spark 4 image automatically
bash run.sh spark_minio_sts-1.10.1
# S3 (Remote Signing)
bash run.sh spark_minio_remote_signing-1.10.1
# S3a (alternative protocol)
bash run.sh spark_minio_s3a-1.10.1
# ADLS
bash run.sh spark_adls-1.10.1
# WASBS (alternative protocol)
bash run.sh spark_wasbs-1.10.1
# OneLake (Microsoft Fabric)
bash run.sh spark_onelake-1.10.1
# Pyiceberg
bash run.sh pyiceberg
# Pyiceberg with legacy MD5 checksums for S3
bash run.sh pyiceberg-legacy_md5
# With OpenFGA Authorization
bash run.sh spark_openfga-1.10.1
# Starrocks
bash run.sh starrocks
# Trino
bash run.sh trino
# Trino with Open Policy Agent
bash run.sh trino_opa
# S3 STS with separate STS endpoint (nginx proxy)
bash run.sh spark_minio_sts_separate_endpoint-1.10.1
# S3 System Identity
bash run.sh spark_aws_system_identity_sts-1.10.1
# S3 SSE-KMS (needs LAKEKEEPER_TEST__AWS_KMS_S3_BUCKET + LAKEKEEPER_TEST__AWS_S3_KMS_ARN)
bash run.sh spark_aws_kms-1.10.1
```

To override the Spark image (e.g. for local testing against a custom build):

```sh
LAKEKEEPER_TEST__SPARK_IMAGE=my-custom/spark:latest bash run.sh spark_minio_sts-1.10.1
```

## Environment variables

Most suites only need `LAKEKEEPER_TEST__SERVER_IMAGE` plus the variables for
the storage backend(s) under test. Below is the full per-backend list; vars
prefixed `LAKEKEEPER_TEST__` are forwarded into the test containers by
`docker-compose.yaml`.

### S3 / MinIO

`run.sh` provisions a local MinIO container; defaults baked into
`docker-compose.yaml` cover the local case. Override only if you point at a
non-default MinIO or another S3-compatible backend.

### AWS S3 (real cloud)

| Var | Purpose |
|---|---|
| `LAKEKEEPER_TEST__AWS_S3_BUCKET` | bucket name |
| `LAKEKEEPER_TEST__AWS_S3_REGION` | region (e.g. `us-east-1`) |
| `LAKEKEEPER_TEST__AWS_S3_STS_ROLE_ARN` | role to assume for STS-mode tests |
| `LAKEKEEPER_TEST__AWS_S3_ACCESS_KEY_ID` / `SECRET_ACCESS_KEY` | access keys (or use system identity overlay) |

### Generic ADLS / Azure Storage

| Var | Purpose |
|---|---|
| `LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME` | storage account |
| `LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM` | container/filesystem name |
| `LAKEKEEPER_TEST__AZURE_CLIENT_ID` / `CLIENT_SECRET` / `TENANT_ID` | Entra app reg with rights on the account |

### OneLake (Microsoft Fabric)

The Python Spark suite (`spark_onelake`) reuses the `AZURE_CLIENT_*` /
`AZURE_TENANT_ID` vars above for the Entra app reg (a OneLake warehouse
authenticates the same way as a generic ADLS account). The Rust integration
tests use parallel `ONELAKE_CLIENT_*` / `ONELAKE_TENANT_ID` vars — in practice
you set both to the same values.

| Var | Required by | Purpose |
|---|---|---|
| `LAKEKEEPER_TEST__ONELAKE_WORKSPACE_ID` | Rust + Spark | Fabric workspace UUID |
| `LAKEKEEPER_TEST__ONELAKE_LAKEHOUSE_ID` | Rust + Spark | lakehouse UUID inside the workspace |
| `LAKEKEEPER_TEST__ONELAKE_CLIENT_ID` | Rust | Entra app client ID (Rust tests only) |
| `LAKEKEEPER_TEST__ONELAKE_CLIENT_SECRET` | Rust | client secret (Rust tests only) |
| `LAKEKEEPER_TEST__ONELAKE_TENANT_ID` | Rust | tenant ID (Rust tests only) |
| `LAKEKEEPER_TEST__AZURE_CLIENT_ID` | Spark | client ID — reused from the Azure block |
| `LAKEKEEPER_TEST__AZURE_CLIENT_SECRET` | Spark | client secret — reused |
| `LAKEKEEPER_TEST__AZURE_TENANT_ID` | Spark | tenant ID — reused |
| `LAKEKEEPER_TEST__ONELAKE_REGION` | Rust regional, Spark `regional` mode | Azure region slug (e.g. `centralus`) |
| `LAKEKEEPER_TEST__ONELAKE_ENDPOINT_MODE` | Spark only | comma-separated subset of `default,regional,workspace-private-link`. Default: `default` |

The Rust live tests are marked `#[ignore]` — opt in with
`cargo test -- --ignored` or `cargo nextest run --run-ignored=all`. Examples:

```sh
# All OneLake Rust tests against the global + regional endpoints
LAKEKEEPER_TEST__ONELAKE_WORKSPACE_ID=... \
LAKEKEEPER_TEST__ONELAKE_LAKEHOUSE_ID=... \
LAKEKEEPER_TEST__ONELAKE_CLIENT_ID=... \
LAKEKEEPER_TEST__ONELAKE_CLIENT_SECRET=... \
LAKEKEEPER_TEST__ONELAKE_TENANT_ID=... \
LAKEKEEPER_TEST__ONELAKE_REGION=centralus \
cargo test -p lakekeeper --lib onelake_integration_tests -- --ignored

# Spark OneLake — default + regional only
LAKEKEEPER_TEST__ONELAKE_ENDPOINT_MODE=default,regional ...other_vars... \
bash run.sh spark_onelake-1.10.1
```

`workspace-private-link` requires the caller to have a Fabric workspace-level
private-link endpoint provisioned and reachable from the test container. The
host pattern is `<wsid-no-dashes>.z<xy>.dfs.fabric.microsoft.com`. If DNS
isn't configured, the test will fail with a connection error — that's
expected; private-link infra setup is out of scope for the test harness.
Tenant-level private link is transparent at this layer (it just changes how
the global onelake FQDN resolves), so it requires no dedicated mode — use
`default`.

### GCS

| Var | Purpose |
|---|---|
| `LAKEKEEPER_TEST__GCS_BUCKET` | bucket name |
| `LAKEKEEPER_TEST__GCS_CREDENTIAL` | service-account JSON (full text) |
