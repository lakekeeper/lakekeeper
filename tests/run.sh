#!/bin/bash
# This script is meant to run in  apache/spark:3.5.1-java17-python3 like docker images
set -eo pipefail

source common.sh

setup_python

TOX_NAME="${1%%-*}"
SPARK_VERSION="${1#*-}"

# unset if empty else export
if [ "$SPARK_VERSION" != "$TOX_NAME" ]; then
  export LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION="$SPARK_VERSION"
fi


# Running tests
echo "Running tests test: $TOX_NAME iceberg version: $LAKEKEEPER_TEST__SPARK_ICEBERG_VERSION..."
cd python
tox -qe "${TOX_NAME}"
