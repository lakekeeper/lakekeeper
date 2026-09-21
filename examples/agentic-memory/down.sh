#!/usr/bin/env bash
# Tear the stack down and return to a clean pre-run state.
#
#   ./down.sh            # reset, keep the downloaded models
#   ./down.sh --purge    # drop the models too (~2.5 GB)
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE=(docker compose)
command -v docker >/dev/null 2>&1 || COMPOSE=(podman compose)

if [[ "${1:-}" == "--purge" ]]; then
  "${COMPOSE[@]}" down -v --remove-orphans
  echo "stack and model volume removed"
else
  # Keep the named ollama-models volume so the next run does not re-download.
  "${COMPOSE[@]}" down --remove-orphans
  "${COMPOSE[@]}" rm -fsv db openfga-db silo >/dev/null 2>&1 || true
  echo "stack down; models kept (use --purge to drop them)"
fi

rm -rf notebooks/.ipynb_checkpoints .ipynb_checkpoints
