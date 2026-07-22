#!/usr/bin/env bash
# Tear the stack down and reset to a clean, pre-run state (next ./up.sh starts fresh).
#   ./down.sh
set -euo pipefail
cd "$(dirname "$0")"

export PATH="/opt/homebrew/bin:/usr/local/bin:/opt/podman/bin:$PATH"
if command -v docker >/dev/null 2>&1; then DOCKER=docker
elif command -v podman >/dev/null 2>&1; then DOCKER=podman
else echo "ERROR: need 'docker' or 'podman' on PATH." >&2; exit 1; fi

echo "Stopping stack ($DOCKER)..."
"$DOCKER" compose down -v

echo "Removing generated run artifacts..."
rm -rf .up .env __pycache__

echo "Reset to a clean state. Start again with ./up.sh"
