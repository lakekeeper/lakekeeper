#!/bin/bash
# down.sh — tear down all docker-compose stacks used by the test suite.
#
# Includes every overlay so containers are removed regardless of which test
# combination was last run.  Named volumes are removed too (--volumes), giving
# a clean slate for the next run.
#
# Usage:
#   cd tests && bash down.sh
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# docker-compose.yaml references LAKEKEEPER_TEST__SPARK_IMAGE for the spark
# service; it must be set to a valid image or compose will refuse to parse the
# project even for `down`.
export LAKEKEEPER_TEST__SPARK_IMAGE="${LAKEKEEPER_TEST__SPARK_IMAGE:-$LAKEKEEPER_SPARK4_IMAGE}"

exec docker compose \
-f "${SCRIPT_DIR}/docker-compose.yaml" \
-f "${SCRIPT_DIR}/docker-compose-openfga-overlay.yaml" \
-f "${SCRIPT_DIR}/docker-compose-vault-overlay.yaml" \
-f "${SCRIPT_DIR}/docker-compose-trino-opa-overlay.yaml" \
-f "${SCRIPT_DIR}/docker-compose-starrocks-overlay.yaml" \
-f "${SCRIPT_DIR}/docker-compose-s3-system-identity-overlay.yaml" \
-f "${SCRIPT_DIR}/docker-compose-legacy-md5-overlay.yaml" \
down --volumes --remove-orphans
