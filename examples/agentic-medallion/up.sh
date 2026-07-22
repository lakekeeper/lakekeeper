#!/usr/bin/env bash
# Bring up the agentic-medallion stack with the two host-facing dependencies wired
# correctly:
#
#  1. Device-code login — peter approves a Keycloak URL in his HOST browser, so the
#     verification URL must point at the host (KEYCLOAK_BROWSER_URL). The kernel
#     still reaches Keycloak in-network for the token exchange.
#  2. S3 data plane — the warehouse S3 endpoint is signed into every request and
#     vended to clients, so it must be ONE URL that resolves from BOTH the in-network
#     kernel AND a host browser (so the LK UI can navigate dataset files). Neither
#     seaweedfs:8333 (container-only) nor localhost:8333 (host-only) works for both;
#     the host LAN IP does. Compose can't know it, so this script detects + injects it.
#
# Usage:
#   ./up.sh                 # Keycloak on localhost; S3 on the auto-detected LAN IP
#   ./up.sh 10.0.0.5        # remote Docker host: browser reaches Keycloak here
#   HOST_IP=10.0.0.5 ./up.sh  # force the S3 LAN IP
set -euo pipefail
cd "$(dirname "$0")"

# Resolve a container CLI. A `docker` *shell alias* (e.g. `alias docker=podman`)
# is NOT visible inside a script, so detect the real binary; prefer docker, fall
# back to podman. Also make sure the usual install dirs are on PATH.
export PATH="/opt/homebrew/bin:/usr/local/bin:/opt/podman/bin:$PATH"
if command -v docker >/dev/null 2>&1; then DOCKER=docker
elif command -v podman >/dev/null 2>&1; then DOCKER=podman
else echo "ERROR: need 'docker' or 'podman' on PATH (a shell alias won't work in a script)." >&2; exit 1; fi
echo "Using container CLI: $DOCKER"

# --- 1) Keycloak browser host (device-code verification URL) -----------------
kc_host="${1:-${PUBLIC_HOST:-}}"
if [[ -z "$kc_host" ]]; then
  case "${DOCKER_HOST:-}" in
    tcp://*) kc_host="$(echo "${DOCKER_HOST#tcp://}" | sed 's#[:/].*##')" ;;
    ssh://*) kc_host="$(echo "${DOCKER_HOST#ssh://}" | sed 's#.*@##; s#[:/].*##')" ;;
    *)       kc_host="localhost" ;;
  esac
fi
export KEYCLOAK_BROWSER_URL="http://${kc_host}:30080"

# --- 2) Host LAN IP for the S3 endpoint (reachable from container AND browser) --
if [[ -z "${HOST_IP:-}" ]]; then
  if command -v ipconfig >/dev/null 2>&1; then           # macOS
    HOST_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true)"
  fi
  if [[ -z "${HOST_IP:-}" ]] && command -v hostname >/dev/null 2>&1; then  # Linux
    HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
fi
if [[ -z "${HOST_IP:-}" ]]; then
  echo "ERROR: could not detect a host LAN IP. Set it explicitly: HOST_IP=<ip> ./up.sh" >&2
  exit 1
fi
export S3_ENDPOINT="http://${HOST_IP}:8333"

# The S3 endpoint is baked into the warehouse at create time. If your host IP
# changed since the last run, an existing warehouse still points at the old,
# now-unreachable address (warehouse read/write 408s) — recreate the catalog.
if [ -f .env ]; then
  OLD=$(grep '^S3_ENDPOINT=' .env 2>/dev/null | cut -d= -f2-)
  if [ -n "$OLD" ] && [ "$OLD" != "$S3_ENDPOINT" ]; then
    echo "!! S3 endpoint changed since last run: $OLD -> $S3_ENDPOINT"
    echo "!! An existing warehouse still points at the OLD address and will 408."
    echo "!! Run ./down.sh first, then ./up.sh, to recreate the catalog with the new IP."
    echo
  fi
fi

# Persist for later plain `docker compose` invocations.
{ printf 'KEYCLOAK_BROWSER_URL=%s\n' "$KEYCLOAK_BROWSER_URL"
  printf 'S3_ENDPOINT=%s\n' "$S3_ENDPOINT"; } > .env

echo "Keycloak (browser): $KEYCLOAK_BROWSER_URL"
echo "S3 endpoint:        $S3_ENDPOINT"
echo

# --- 3) Bring up the full stack (catalog + JupyterLab + Ollama + bucket-cors) --
"$DOCKER" compose --profile ml up -d --build

# --- 4) Confirm CORS is live from the host (fallback to host aws-cli) ----------
echo -n "Checking bucket CORS"
cors_ok=""
for _ in $(seq 1 30); do
  if curl -s -X OPTIONS "${S3_ENDPOINT}/medallion" \
        -H 'Origin: http://example.com' -H 'Access-Control-Request-Method: PUT' -i 2>/dev/null \
        | grep -qi 'access-control-allow-origin'; then cors_ok=1; echo " — ok"; break; fi
  echo -n "."; sleep 2
done
if [ -z "$cors_ok" ] && command -v aws >/dev/null 2>&1; then
  echo " — applying from host"
  AWS_ACCESS_KEY_ID=seaweedfs-root-user AWS_SECRET_ACCESS_KEY=seaweedfs-root-password \
  AWS_DEFAULT_REGION=local-01 aws --endpoint-url "${S3_ENDPOINT}" s3api put-bucket-cors \
    --bucket medallion --cors-configuration \
    '{"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET","PUT","POST","DELETE","HEAD"],"AllowedHeaders":["*"],"ExposeHeaders":["ETag"]}]}' || true
elif [ -z "$cors_ok" ]; then
  echo " — WARNING: CORS not confirmed and no host aws-cli to apply it"
fi

cat <<EOF

Stack is up.

  1. Pull the local models once:
       docker compose exec ollama ollama pull moondream
       docker compose exec ollama ollama pull gemma2:2b

  2. Open JupyterLab:  http://localhost:8888/lab/tree/notebooks   and run notebooks/ in order (00 -> 01 -> 02).
     In 00-setup, approve the device-login URL (at ${KEYCLOAK_BROWSER_URL}) as  peter / iceberg.

  3. Lakekeeper console: http://localhost:8181  (browse metadata/grants; the warehouse
     S3 endpoint is ${S3_ENDPOINT}, so data-file access works from the host browser too).
EOF
