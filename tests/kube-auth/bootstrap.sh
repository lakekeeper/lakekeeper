#!/bin/sh

set -eu

# Verifies Kubernetes service-account authentication end-to-end against a real
# TokenReview: bootstraps as the calling SA, then checks that the resolved user
# ID matches the configured subject source ($1: "uid" (default) or "username").

MODE="${1:-uid}"
TOKEN="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"
NAMESPACE="$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)"
AUTH="Authorization: Bearer $TOKEN"
BASE="my-lakekeeper:8181/management/v1"

# First authenticated call bootstraps the server; the caller becomes the first user.
curl -f -H "Content-Type: application/json" -H "$AUTH" "$BASE/bootstrap" -d '{"accept-terms-of-use": true}'

body="$(curl -f -s -H "$AUTH" "$BASE/whoami")"
echo "whoami: $body"
# The whoami response is compact JSON with a single `"id":"..."` (the user ID);
# no other field serializes that key ahead of it. jq isn't in curlimages/curl.
id="$(echo "$body" | grep -o '"id":"[^"]*"' | head -n1 | sed 's/"id":"//; s/"$//')"
echo "resolved user id: $id (mode=$MODE)"

case "$MODE" in
  username)
    expected="kubernetes~system:serviceaccount:$NAMESPACE:default"
    [ "$id" = "$expected" ] || { echo "FAIL: expected '$expected'"; exit 1; }
    ;;
  uid)
    echo "$id" | grep -Eq '^kubernetes~[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' \
      || { echo "FAIL: expected 'kubernetes~<uuid>'"; exit 1; }
    ;;
  *)
    echo "FAIL: unknown mode '$MODE'"; exit 1
    ;;
esac

echo "OK: subject source '$MODE' verified"
