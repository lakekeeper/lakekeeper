#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Preserves pytest's exit code while guaranteeing teardown on every exit path
# (pip-install failure, signal, set -e from any pre-pytest step). Without
# this trap, an early failure would leak containers and the host's port
# bindings.
test_exit=0
cleanup() {
    rc=$?
    echo ">>> Tearing down Docker infrastructure..."
    docker compose logs || true
    docker compose down -v || true
    # If pytest set test_exit, propagate that; otherwise use the script's
    # current exit code (e.g. from set -e firing before pytest ran).
    if [ "$test_exit" -ne 0 ]; then
        exit "$test_exit"
    fi
    exit "$rc"
}
trap cleanup EXIT

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
    exit 1
fi

echo ">>> Installing Python dependencies..."
pip install -q -r requirements.txt

echo ">>> Running integration tests..."
pytest test_lance.py -v || test_exit=$?
