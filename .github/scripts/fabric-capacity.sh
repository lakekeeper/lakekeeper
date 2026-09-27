#!/usr/bin/env bash
# Resume or suspend the Fabric capacity behind the OneLake tests.
#
# Usage: fabric-capacity.sh resume|suspend
# Env:   CAP                   ARM resource id of the capacity
#        CAPACITY_API_VERSION  Microsoft.Fabric API version
#
# `resume` returns once the capacity reports Active, since OneLake rejects
# requests before that. `suspend` only acts on an Active capacity, so calling
# it more than once is harmless. Both expect an `az` session that may read,
# resume and suspend the capacity.
set -euo pipefail

: "${CAP:?CAP must be set to the capacity resource id}"
: "${CAPACITY_API_VERSION:?CAPACITY_API_VERSION must be set}"

capacity_state() {
  az resource show --api-version "${CAPACITY_API_VERSION}" --ids "${CAP}" \
    --query properties.state -o tsv
}

capacity_action() {
  az resource invoke-action --action "$1" --api-version "${CAPACITY_API_VERSION}" \
    --ids "${CAP}" >/dev/null
}

case "${1:-}" in
  resume)
    state=""
    for _ in $(seq 1 60); do
      state=$(capacity_state)
      case "${state}" in
        Active)
          echo "capacity is Active"
          exit 0
          ;;
        Paused)
          # Refused while another request is in flight; the next poll sees
          # the capacity's state either way.
          capacity_action resume || echo "resume request refused, re-checking"
          ;;
        *)
          echo "capacity state: ${state}"
          ;;
      esac
      sleep 10
    done
    echo "capacity did not reach Active within 10 minutes (last state: ${state})" >&2
    exit 1
    ;;
  suspend)
    state=$(capacity_state)
    echo "capacity state: ${state}"
    if [ "${state}" = "Active" ]; then
      capacity_action suspend
      echo "suspend submitted"
    fi
    ;;
  *)
    echo "usage: $0 resume|suspend" >&2
    exit 2
    ;;
esac
