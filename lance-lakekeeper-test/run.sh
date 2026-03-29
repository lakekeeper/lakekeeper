#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo ">>> Starting Docker infrastructure..."
docker compose up -d

echo ">>> Waiting for Lakekeeper to start..."
timeout=120
elapsed=0
while [ $elapsed -lt $timeout ]; do
    status=$(docker compose ps lakekeeper --format '{{.State}}' 2>/dev/null || echo "unknown")
    if echo "$status" | grep -qi "running"; then
        echo "  Lakekeeper container is running."
        break
    fi
    sleep 3
    elapsed=$((elapsed + 3))
    echo "  Waiting... (${elapsed}s)"
done

if [ $elapsed -ge $timeout ]; then
    echo "  ERROR: Timed out waiting for Lakekeeper"
    docker compose logs
    docker compose down -v
    exit 1
fi

echo ">>> Installing Python dependencies..."
pip install -q -r requirements.txt

echo ">>> Running integration tests..."
test_exit=0
pytest test_lance_lakekeeper.py -v || test_exit=$?

echo ">>> Tearing down Docker infrastructure..."
docker compose down -v

exit $test_exit
