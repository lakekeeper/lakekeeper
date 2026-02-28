#!/bin/bash
# run_default.sh — dual-mode convenience runner using Spark 4 / Iceberg 1.10.1.
#
# HOST mode (default): launches the Spark 4 container and runs all tox envs.
#
# CONTAINER mode (LAKEKEEPER_IN_CONTAINER=1): runs tox directly.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

DEFAULT_ICEBERG_VERSION="1.10.1"

# ── Container-side execution ──────────────────────────────────────────────────
if [ "${LAKEKEEPER_IN_CONTAINER:-0}" = "1" ]; then
    setup_python
    export LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION="${LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION:-$DEFAULT_ICEBERG_VERSION}"
    echo "Running all tests (iceberg ${LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION})..."
    cd "${SCRIPT_DIR}/python"
    exec tox -qe pyiceberg,spark_minio_remote_signing,spark_minio_sts,spark_adls,spark_gcs,trino,spark_minio_s3a
fi

# ── Host-side execution ───────────────────────────────────────────────────────
export LAKEKEEPER_TEST__SPARK_IMAGE="${LAKEKEEPER_TEST__SPARK_IMAGE:-$LAKEKEEPER_SPARK4_IMAGE}"
echo "Using Spark image: $LAKEKEEPER_TEST__SPARK_IMAGE" >&2
echo "Iceberg version  : ${LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION:-$DEFAULT_ICEBERG_VERSION}" >&2

exec docker compose -f "${SCRIPT_DIR}/docker-compose.yaml" run --quiet-pull spark \
/opt/entrypoint.sh bash -c \
'cd /opt/tests && LAKEKEEPER_IN_CONTAINER=1 bash run_default.sh'
