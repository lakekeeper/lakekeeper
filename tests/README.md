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
# S3 System Identity
bash run.sh spark_aws_system_identity_sts-1.10.1
```

To override the Spark image (e.g. for local testing against a custom build):

```sh
LAKEKEEPER_TEST__SPARK_IMAGE=my-custom/spark:latest bash run.sh spark_minio_sts-1.10.1
```
