#!/usr/bin/env bash
# Bring up the minimal stack so the IN-BROWSER query engine (DuckDB-WASM / LoQE in
# the Lakekeeper console) can read & write the local SeaweedFS warehouse — not just
# the in-container engines (Trino/Spark/Jupyter).
#
# Why a wrapper instead of plain `docker compose up`?
#   A warehouse's S3 endpoint is baked into the SigV4 signature AND vended to
#   clients, so it must be ONE URL that resolves the same from every client. Two
#   clients see the network differently ("split-horizon"):
#     - the WRITER  (lakekeeper, in a container) reaches SeaweedFS at seaweedfs:8333
#     - the BROWSER (LoQE, on the host)          cannot resolve `seaweedfs`
#   Neither seaweedfs:8333 nor localhost:8333 works for both. The host LAN IP does:
#   a container reaches it via the host's published port, and so does the browser.
#   Compose can't know the host LAN IP, so this script detects it and injects it
#   into the warehouse endpoint before the stack starts.
#
# Usage:
#   ./up.sh                 # auto-detect the host LAN IP
#   HOST_IP=192.168.1.50 ./up.sh
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

# 1) Detect the host LAN IP (reachable from both containers and the browser).
if [ -z "${HOST_IP:-}" ]; then
  if command -v ipconfig >/dev/null 2>&1; then           # macOS
    HOST_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true)"
  fi
  if [ -z "${HOST_IP:-}" ] && command -v hostname >/dev/null 2>&1; then  # Linux
    HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
fi
if [ -z "${HOST_IP:-}" ]; then
  echo "ERROR: could not detect a host LAN IP. Set it explicitly: HOST_IP=<ip> ./up.sh" >&2
  exit 1
fi
ENDPOINT="http://${HOST_IP}:8333"
echo "Host LAN IP: ${HOST_IP}   (S3 endpoint: ${ENDPOINT})"

# 2) Inject the LAN IP into the warehouse endpoint via a generated copy. The
#    committed create-default-warehouse.json is left untouched (seaweedfs:8333),
#    so plain `docker compose up` still works for the in-container engines.
mkdir -p .up
sed "s#http://seaweedfs:8333#${ENDPOINT}#g" \
  create-default-warehouse.json > .up/create-default-warehouse.json
export WAREHOUSE_FILE="${PWD}/.up/create-default-warehouse.json"

# 3) Bring up the stack (bucket-cors applies wildcard CORS to the bucket).
"$DOCKER" compose up -d

# 4) Wait for Lakekeeper to be healthy.
echo -n "Waiting for Lakekeeper"
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8181/health >/dev/null 2>&1; then echo " — up"; break; fi
  echo -n "."; sleep 2
done

# 5) Confirm CORS is live from the host's perspective; apply from the host as a
#    fallback if the in-network init service hasn't (needs host aws-cli).
echo -n "Checking bucket CORS"
cors_ok=""
for _ in $(seq 1 30); do
  if curl -s -X OPTIONS "${ENDPOINT}/examples" \
        -H 'Origin: http://example.com' -H 'Access-Control-Request-Method: PUT' -i 2>/dev/null \
        | grep -qi 'access-control-allow-origin'; then cors_ok=1; echo " — ok"; break; fi
  echo -n "."; sleep 2
done
if [ -z "$cors_ok" ] && command -v aws >/dev/null 2>&1; then
  echo " — applying from host"
  AWS_ACCESS_KEY_ID=seaweedfs-root-user AWS_SECRET_ACCESS_KEY=seaweedfs-root-password \
  AWS_DEFAULT_REGION=local-01 aws --endpoint-url "${ENDPOINT}" s3api put-bucket-cors \
    --bucket examples --cors-configuration \
    '{"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET","PUT","POST","DELETE","HEAD"],"AllowedHeaders":["*"],"ExposeHeaders":["ETag"]}]}' || true
elif [ -z "$cors_ok" ]; then
  echo " — WARNING: CORS not confirmed and no host aws-cli to apply it"
fi

cat <<EOF

Stack is up. Warehouse S3 endpoint: ${ENDPOINT}

  Lakekeeper console : http://localhost:8181   <- open here, run LoQE in the browser
  Jupyter            : http://localhost:8888
  Swagger UI         : http://localhost:8181/swagger-ui/#/

In the console, attach the "demo" warehouse and run an in-browser (LoQE) query, e.g.
  CREATE TABLE demo.public.t AS SELECT 1 AS x;
  SELECT * FROM demo.public.t;

Note: run ./up.sh (not \`docker compose up\`) for the browser flow. If you previously
ran plain \`docker compose up\`, reset first:  docker compose down -v && ./up.sh
EOF
