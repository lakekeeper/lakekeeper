#!/usr/bin/env bash
# Bring up the whole stack and pull the two local models.
#
#   ./up.sh                 # localhost (Docker Desktop / podman machine)
#   ./up.sh 10.0.0.5        # remote docker host: pass the address your browser uses
#
# Ports can be overridden when something already holds the defaults — a natively
# running Lakekeeper takes 8181 and 9000:
#   LK_PORT=8185 S3_PORT=9010 S3_CONSOLE_PORT=9011 JUPYTER_PORT=8890 \
#     KEYCLOAK_PORT=30085 ./up.sh
set -euo pipefail
cd "$(dirname "$0")"

HOST="${1:-localhost}"
LK_PORT="${LK_PORT:-8181}"
S3_PORT="${S3_PORT:-9000}"
S3_CONSOLE_PORT="${S3_CONSOLE_PORT:-9001}"
JUPYTER_PORT="${JUPYTER_PORT:-8888}"
KEYCLOAK_PORT="${KEYCLOAK_PORT:-30080}"
OLLAMA_PORT="${OLLAMA_PORT:-11434}"
CHAT_MODEL="${CHAT_MODEL:-qwen2.5:3b}"
EMBED_MODEL="${EMBED_MODEL:-nomic-embed-text}"

# peter approves the device-code login in a HOST browser, so Keycloak needs an
# address that resolves there; the kernel still reaches it in-network.
export KEYCLOAK_BROWSER_URL="http://${HOST}:${KEYCLOAK_PORT}"

# The S3 endpoint is signed into every request AND vended to clients, so it has to be
# ONE URL that resolves from both the in-network kernel and a host browser (the
# Lakekeeper console lists data files from the browser). `silo:9000` is container-only
# and `localhost:9000` is host-only; the host LAN IP is the address that works for both.
# Compose cannot know it, so detect it here.
if [[ -z "${HOST_IP:-}" ]]; then
  if command -v ipconfig >/dev/null 2>&1; then                      # macOS
    HOST_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true)"
  fi
  if [[ -z "${HOST_IP:-}" ]] && command -v hostname >/dev/null 2>&1; then   # Linux
    HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
fi
if [[ -z "${HOST_IP:-}" ]]; then
  echo "ERROR: could not detect a host LAN IP. Set it explicitly: HOST_IP=<ip> ./up.sh" >&2
  exit 1
fi
export S3_ENDPOINT="http://${HOST_IP}:${S3_PORT}"

# The endpoint is baked into the warehouse when it is created. If your IP moved since
# the last run, an existing warehouse still points at the old address and every read
# 408s — so say so rather than letting the notebooks fail mysteriously.
if [[ -f .env ]]; then
  OLD="$(grep '^S3_ENDPOINT=' .env 2>/dev/null | cut -d= -f2- || true)"
  if [[ -n "$OLD" && "$OLD" != "$S3_ENDPOINT" ]]; then
    echo "!! S3 endpoint changed since last run: $OLD -> $S3_ENDPOINT"
    echo "!! An existing warehouse still points at the OLD address and will 408."
    echo "!! Reset with: ./down.sh && ./up.sh  (then re-run the notebooks)."
    echo
  fi
fi
{ printf 'KEYCLOAK_BROWSER_URL=%s\n' "$KEYCLOAK_BROWSER_URL"
  printf 'S3_ENDPOINT=%s\n' "$S3_ENDPOINT"; } > .env

export LK_PORT S3_PORT S3_CONSOLE_PORT JUPYTER_PORT KEYCLOAK_PORT OLLAMA_PORT
export CHAT_MODEL EMBED_MODEL

COMPOSE=(docker compose)
command -v docker >/dev/null 2>&1 || COMPOSE=(podman compose)

echo "→ building the workbench and starting the stack"
"${COMPOSE[@]}" up -d --build

# CORS: the console fetches objects straight from storage, so the bucket has to allow
# the browser's origin. Silo is started with MINIO_API_CORS_ALLOW_ORIGIN=*; confirm it
# is actually live, and fall back to setting it explicitly if a host aws-cli exists.
echo -n "→ checking bucket CORS"
cors_ok=""
for _ in $(seq 1 30); do
  if curl -s -X OPTIONS "${S3_ENDPOINT}/agentmem" \
        -H 'Origin: http://example.com' -H 'Access-Control-Request-Method: GET' -i 2>/dev/null \
        | grep -qi 'access-control-allow-origin'; then cors_ok=1; echo " — ok"; break; fi
  echo -n "."; sleep 2
done
if [[ -z "$cors_ok" ]] && command -v aws >/dev/null 2>&1; then
  echo " — applying from host"
  AWS_ACCESS_KEY_ID=silo-root-user AWS_SECRET_ACCESS_KEY=silo-root-password \
  AWS_DEFAULT_REGION=local-01 aws --endpoint-url "${S3_ENDPOINT}" s3api put-bucket-cors \
    --bucket agentmem --cors-configuration \
    '{"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET","PUT","POST","DELETE","HEAD"],"AllowedHeaders":["*"],"ExposeHeaders":["ETag"]}]}' || true
elif [[ -z "$cors_ok" ]]; then
  echo " — WARNING: CORS not confirmed; the console will not list data files"
fi

echo "→ waiting for Lakekeeper"
for _ in $(seq 1 60); do
  curl -sf "http://localhost:${LK_PORT}/health" >/dev/null 2>&1 && break
  sleep 2
done

echo "→ pulling local models (first run downloads ~2.5 GB)"
"${COMPOSE[@]}" exec -T ollama ollama pull "${EMBED_MODEL}"
"${COMPOSE[@]}" exec -T ollama ollama pull "${CHAT_MODEL}"

cat <<EOF

  Ready.

    JupyterLab   http://localhost:${JUPYTER_PORT}/lab/tree/notebooks
    Lakekeeper   http://localhost:${LK_PORT}   (S3 endpoint ${S3_ENDPOINT}, so the
                 console can list data files from your browser)
    Keycloak     ${KEYCLOAK_BROWSER_URL}   (peter / iceberg)
    Silo console http://localhost:${S3_CONSOLE_PORT}

  Work through notebooks/ in order. 00-setup has one interactive step: peter
  approves a device-code login in your browser.
EOF
