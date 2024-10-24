#!/bin/sh
set -eux

ROOT_DIR=/app

# Replace env vars in JavaScript files
echo "Replacing env constants in JS"

sed -i "s|VITE_APP_ICEBERG_CATALOG_URL_PLACEHOLDER|${VITE_APP_ICEBERG_CATALOG_URL}|g" ${ROOT_DIR}/assets/*.js


echo "Starting Nginx"
nginx -g 'daemon off;'
