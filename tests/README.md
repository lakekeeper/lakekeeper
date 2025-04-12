Tests that require external components

## Integration Tests

Integration tests have external dependencies, they are typically run with docker-compose. Running the tests requires the
docker image for the server to be specified via env-vars.

Run the following commands from the crate's root folder:

```sh
docker build -t localhost/iceberg-catalog-local:latest -f docker/full.Dockerfile .
export LAKEKEEPER_TEST__SPARK_IMAGE=apache/spark:3.5.1-java17-python3
export LAKEKEEPER_TEST__SERVER_IMAGE=localhost/iceberg-catalog-local:latest
cd tests
# Regular tests
docker compose run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run_all.sh"
# S3a (alternative protocol)
docker compose run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh spark_minio_s3a-1.7.1"
# ADLS 
docker compose run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh spark_adls-1.8.0"
# WASBS (alternative protocol)
docker compose run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh spark_wasbs-1.8.1"
# Pyiceberg
docker compose run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh pyiceberg"
# With Authorization
docker compose -f docker-compose.yaml -f docker-compose-openfga-overlay.yaml run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh spark_openfga-1.7.1"
# Starrocks only
docker compose -f docker-compose.yaml -f docker-compose-starrocks-overlay.yaml run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh starrocks"
# Trino only
docker compose -f docker-compose.yaml run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh trino"
# Trino with Open Policy Agent
docker compose -f docker-compose.yaml -f docker-compose-openfga-overlay.yaml -f docker-compose-trino-opa-overlay.yaml run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh trino_opa"
# S3 System Identity
docker compose -f docker-compose.yaml -f docker-compose-s3-system-identity-overlay.yaml run spark /opt/entrypoint.sh bash -c "cd /opt/tests && bash run.sh spark_aws_sts"
```
