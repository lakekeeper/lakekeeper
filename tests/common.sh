#!/bin/bash
set -e

# ── Spark Docker image constants ─────────────────────────────────────────────
# Spark 3.5 is used for Iceberg < 1.10; Spark 4 is required for Iceberg >= 1.10
# (VARIANT type, Iceberg format v3 reads, etc.).
LAKEKEEPER_SPARK3_IMAGE="apache/spark:3.5.6-java17-python3"
LAKEKEEPER_SPARK4_IMAGE="apache/spark:4.0.2-scala2.13-java21-python3-ubuntu"

# Returns the appropriate Spark docker image tag for the given Iceberg version
# string (e.g. "1.10.1" → Spark 4 image, "1.9.2" → Spark 3 image).
# Non-numeric / absent version strings default to the Spark 3 image.
function spark_image_for_iceberg_version() {
    local version="${1:-}"
    if [ -z "$version" ]; then
        echo "$LAKEKEEPER_SPARK3_IMAGE"
        return
    fi
    local major minor
    major=$(echo "$version" | cut -d. -f1)
    minor=$(echo "$version" | cut -d. -f2)
    # Guard against non-numeric tokens (e.g. "legacy_md5")
    if ! [[ "$major" =~ ^[0-9]+$ ]] || ! [[ "$minor" =~ ^[0-9]+$ ]]; then
        echo "$LAKEKEEPER_SPARK3_IMAGE"
        return
    fi
    if [ "$major" -gt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -ge 10 ]; }; then
        echo "$LAKEKEEPER_SPARK4_IMAGE"
    else
        echo "$LAKEKEEPER_SPARK3_IMAGE"
    fi
}

# ── Container helpers ─────────────────────────────────────────────────────────
# setup_python must only be called from inside the Spark container.
function setup_python() {
    # These paths only exist inside the container image.
    export HOME=/opt/spark/work-dir
    export PATH=$PATH:/opt/spark/bin:/opt/spark/work-dir/.local/bin
    
    echo "Installing tox ..."
    pip3 install -q tox-uv
    
    echo "Modifying the PYTHONPATH ..."
    # Initialize PYTHONPATH if not already set
    : "${PYTHONPATH:=}"
    
    # Add pyspark to the PYTHONPATH
    # Iterate over all zips in $SPARK_HOME/python/lib and add them to the PYTHONPATH
    for i in /opt/spark/python/lib/*.zip; do
        PYTHONPATH="$PYTHONPATH:$i"
    done
    export PYTHONPATH
}