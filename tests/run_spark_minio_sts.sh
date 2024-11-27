#!/bin/bash
# This script is meant to run in  apache/spark:3.5.1-java17-python3 like docker images
set -e

source common.sh

setup_python

# Running tests
echo "Running tests ..."
cd python
tox -qe spark_minio_sts
