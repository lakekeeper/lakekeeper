#!/bin/sh

set -eu

curl -f -H "Content-Type: application/json" -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" my-lakekeeper:8181/management/v1/bootstrap -d '{"accept-terms-of-use": true}'
