#!/bin/bash
# run_all.sh — dual-mode convenience runner for all default-version tests.
#
# HOST mode (default): launches the Spark container with the Spark 3 image
#   (all envs here use the default Iceberg version) and runs all tox envs.
#
# CONTAINER mode (LAKEKEEPER_IN_CONTAINER=1): runs tox directly.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ── Container-side execution ──────────────────────────────────────────────────
if [ "${LAKEKEEPER_IN_CONTAINER:-0}" = "1" ]; then
    setup_python
    echo "Running all tests..."
    cd "${SCRIPT_DIR}/python"
    exec tox -qe pyiceberg,spark_minio_remote_signing,spark_minio_sts,spark_adls,spark_gcs,trino,spark_minio_s3a
fi

# ── Host-side execution ───────────────────────────────────────────────────────
# run_all.sh uses the default Iceberg version, which requires Spark 3.
export LAKEKEEPER_TEST__SPARK_IMAGE="${LAKEKEEPER_TEST__SPARK_IMAGE:-$LAKEKEEPER_SPARK4_IMAGE}"
echo "Using Spark image: $LAKEKEEPER_TEST__SPARK_IMAGE" >&2

exec docker compose -f "${SCRIPT_DIR}/docker-compose.yaml" run --quiet-pull spark \
/opt/entrypoint.sh bash -c \
"cd /opt/tests && LAKEKEEPER_IN_CONTAINER=1 bash run_all.sh"
