#!/bin/bash
# run.sh — dual-mode test runner.
#
# HOST mode (default): selects the right Spark Docker image from the Iceberg
#   version suffix, picks the correct docker-compose overlay files, and
#   re-invokes this script inside the container.
#
# CONTAINER mode (LAKEKEEPER_IN_CONTAINER=1): runs tox directly.
#
# Usage (host):
#   cd tests && bash run.sh <tox-env>[-<iceberg-version>]
#   e.g.  bash run.sh spark_minio_sts-1.10.1
#         bash run.sh pyiceberg
#         bash run.sh trino_opa
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

TOX_NAME="${1%%-*}"
SPARK_VERSION="${1#*-}"
# No version suffix → SPARK_VERSION equals TOX_NAME; normalise to empty.
[ "$SPARK_VERSION" = "$TOX_NAME" ] && SPARK_VERSION=""

# ── Container-side execution ──────────────────────────────────────────────────
if [ "${LAKEKEEPER_IN_CONTAINER:-0}" = "1" ]; then
    setup_python
    if [ -n "$SPARK_VERSION" ]; then
        export LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION="$SPARK_VERSION"
    fi
    echo "Running tests: $TOX_NAME  iceberg version: ${LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION:-default}"
    cd "${SCRIPT_DIR}/python"
    exec tox -qe "${TOX_NAME}"
fi

# ── Host-side execution ───────────────────────────────────────────────────────
# Auto-select the Spark image unless the caller already exported one.
REQUIRED_IMAGE="$(spark_image_for_iceberg_version "$SPARK_VERSION")"
export LAKEKEEPER_TEST__SPARK_IMAGE="${LAKEKEEPER_TEST__SPARK_IMAGE:-$REQUIRED_IMAGE}"
echo "Using Spark image : $LAKEKEEPER_TEST__SPARK_IMAGE" >&2
echo "Iceberg version   : ${SPARK_VERSION:-default}" >&2

# Build the docker compose -f arguments, mirroring overlay rules from CI.
# The order of the elif chain matters: 'openfga' must be checked before 'opa'.
COMPOSE_ARGS=("-f" "${SCRIPT_DIR}/docker-compose.yaml")
if [[ "$1" == *"openfga"* ]]; then
    COMPOSE_ARGS+=("-f" "${SCRIPT_DIR}/docker-compose-openfga-overlay.yaml")
    elif [[ "$1" == *"kv2"* ]]; then
    COMPOSE_ARGS+=("-f" "${SCRIPT_DIR}/docker-compose-vault-overlay.yaml")
    elif [[ "$1" == *"opa"* ]]; then
    COMPOSE_ARGS+=("-f" "${SCRIPT_DIR}/docker-compose-openfga-overlay.yaml")
    COMPOSE_ARGS+=("-f" "${SCRIPT_DIR}/docker-compose-trino-opa-overlay.yaml")
    elif [[ "$1" == *"starrocks"* ]]; then
    COMPOSE_ARGS+=("-f" "${SCRIPT_DIR}/docker-compose-starrocks-overlay.yaml")
    elif [[ "$1" == *"aws_system_identity"* ]]; then
    COMPOSE_ARGS+=("-f" "${SCRIPT_DIR}/docker-compose-s3-system-identity-overlay.yaml")
    elif [[ "$1" == *"legacy_md5"* ]]; then
    COMPOSE_ARGS+=("-f" "${SCRIPT_DIR}/docker-compose-legacy-md5-overlay.yaml")
fi

exec docker compose "${COMPOSE_ARGS[@]}" run --quiet-pull spark \
/opt/entrypoint.sh bash -c \
"cd /opt/tests && LAKEKEEPER_IN_CONTAINER=1 bash run.sh $1"
