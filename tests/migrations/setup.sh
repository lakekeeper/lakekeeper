#!/bin/bash
# This script is meant to run in  apache/spark:3.5.1-java17-python3 like docker images
set -e

export HOME=/opt/spark/work-dir
export PATH=$PATH:/opt/spark/bin:/opt/spark/work-dir/.local/bin

echo "Modifying the PYTHONPATH ..."
# Initialize PYTHONPATH if not already set
: "${PYTHONPATH:=}"

# Add pyspark to the PYTHONPATH
# Iterate over all zips in $SPARK_HOME/python/lib and add them to the PYTHONPATH
for i in /opt/spark/python/lib/*.zip; do
    PYTHONPATH="$PYTHONPATH:$i"
done
export PYTHONPATH
