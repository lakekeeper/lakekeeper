#!/usr/bin/env bash
# Re-point the warehouse's S3 endpoint at the current host LAN IP — WITHOUT tearing
# anything down. Use this after your machine's IP changes (Wi-Fi switch, hotspot,
# sleep/wake) and the notebooks start failing with 408 / "Connection refused" /
# ShortTermCredentialError against a now-dead IP.
#
# It updates only the endpoint (same bucket + key-prefix, so no data loss), using
# the PIPELINE service account (which has warehouse `modify` = `can_update_storage`)
# — no browser login. Afterwards just re-run the failing notebook cell.
#
#   ./refresh-ip.sh            # auto-detect current LAN IP
#   HOST_IP=192.168.1.9 ./refresh-ip.sh
set -euo pipefail
cd "$(dirname "$0")"
export PATH="/opt/homebrew/bin:/usr/local/bin:/opt/podman/bin:$PATH"

# 1) Current host LAN IP (same detection as up.sh).
if [ -z "${HOST_IP:-}" ]; then
  if command -v ipconfig >/dev/null 2>&1; then
    HOST_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true)"
  fi
  if [ -z "${HOST_IP:-}" ] && command -v hostname >/dev/null 2>&1; then
    HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
fi
[ -z "${HOST_IP:-}" ] && { echo "ERROR: could not detect a host LAN IP. Set HOST_IP=<ip> ./refresh-ip.sh" >&2; exit 1; }
ENDPOINT="http://${HOST_IP}:8333"

LK="http://localhost:8181"
KC="http://localhost:30080/realms/iceberg/protocol/openid-connect/token"

# 2) PIPELINE token (client-credentials; it holds warehouse `modify`). Published
#    ports are used, so this runs from the host with no container CLI.
TOKEN=$(curl -s "$KC" -d grant_type=client_credentials -d client_id=bootstrap \
  -d client_secret=bootstrap-secret-0000000000000000 -d scope=lakekeeper \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
[ -z "$TOKEN" ] && { echo "ERROR: could not get a token from Keycloak at $KC" >&2; exit 1; }

# 3) Warehouse id (the catalog URL prefix).
WH=$(curl -s "$LK/catalog/v1/config?warehouse=medallion" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("overrides",{}).get("prefix") or d.get("defaults",{}).get("prefix") or "")')
[ -z "$WH" ] && { echo "ERROR: no 'medallion' warehouse found (run ./up.sh + notebook 00 first)." >&2; exit 1; }

# 4) Update only the endpoint — same bucket/key-prefix, so no data moves.
BODY=$(cat <<JSON
{
  "storage-profile": {
    "type": "s3", "bucket": "medallion", "key-prefix": "warehouse",
    "endpoint": "${ENDPOINT}", "sts-endpoint": "${ENDPOINT}",
    "sts-role-arn": "arn:aws:iam::000000000000:role/LakekeeperVendedRole",
    "region": "local-01", "path-style-access": true,
    "flavor": "s3-compat", "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3", "credential-type": "access-key",
    "access-key-id": "seaweedfs-root-user", "secret-access-key": "seaweedfs-root-password"
  }
}
JSON
)
CODE=$(curl -s -o /tmp/refresh-ip.out -w '%{http_code}' -X POST "$LK/management/v1/warehouse/$WH/storage" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "$BODY")

if [ "$CODE" = "200" ]; then
  # Keep .env consistent for up.sh's staleness check (preserve KEYCLOAK_BROWSER_URL).
  KCB=$(grep '^KEYCLOAK_BROWSER_URL=' .env 2>/dev/null || echo 'KEYCLOAK_BROWSER_URL=http://localhost:30080')
  { echo "$KCB"; echo "S3_ENDPOINT=${ENDPOINT}"; } > .env
  echo "OK — warehouse endpoint re-pointed to ${ENDPOINT}."
  echo "Re-run the failing notebook cell; no restart needed."
else
  echo "FAILED (HTTP $CODE):" >&2; cat /tmp/refresh-ip.out >&2; echo >&2; exit 1
fi
