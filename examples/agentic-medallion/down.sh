#!/usr/bin/env bash
# Tear the stack down and reset the example to a clean, pre-run state, so the next
# ./up.sh starts fresh (peter re-bootstraps, warehouse + data rebuilt).
#
#   ./down.sh            # stop + wipe catalog/storage + generated files;
#                        #   KEEPS the downloaded Ollama models (no ~6 GB re-pull)
#   ./down.sh --purge    # also delete the Ollama models volume (full clean)
#
# Note: db / openfga / seaweedfs keep their data in ephemeral container layers, so a
# plain `compose down` already resets the catalog. Only the Ollama models live in a
# named volume, which --purge removes.
set -euo pipefail
cd "$(dirname "$0")"

export PATH="/opt/homebrew/bin:/usr/local/bin:/opt/podman/bin:$PATH"
if command -v docker >/dev/null 2>&1; then DOCKER=docker
elif command -v podman >/dev/null 2>&1; then DOCKER=podman
else echo "ERROR: need 'docker' or 'podman' on PATH." >&2; exit 1; fi

DOWN_V=""
if [ "${1:-}" = "--purge" ] || [ "${1:-}" = "-v" ]; then
  DOWN_V="-v"
  echo "Purging Ollama models too (next run re-pulls them)."
fi

echo "Stopping stack ($DOCKER)..."
"$DOCKER" compose --profile ml down $DOWN_V

echo "Removing generated run artifacts..."
rm -rf .up .env __pycache__ notebooks/.ipynb_checkpoints
rm -rf data/images
rm -f data/manifest.json
mkdir -p data

echo
echo "Reset to a clean state. Start again with ./up.sh"
[ -n "$DOWN_V" ] && echo "(Ollama models were removed.)" || echo "(Ollama models kept — no re-download needed.)"
