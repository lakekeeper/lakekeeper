#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

wait_for() {
    local secs=$1 msg=$2; shift 2
    SECONDS=0
    until "$@" 2>/dev/null; do
        [ $SECONDS -ge "$secs" ] && echo "$msg" && exit 1
        sleep 2
    done
}

detect_engine() {
    for cmd in docker podman nerdctl; do
        if command -v "$cmd" >/dev/null 2>&1; then
            echo "$cmd"
            return 0
        fi
    done

    echo "No container engine found (docker/podman/nerdctl)" >&2
    return 1
}

tiering_job_running() {
    "$CONTAINER_CMD" compose exec -T jobmanager ./bin/flink list -r 2>/dev/null \
        | grep -q 'Tiering Service'
}

if [ -z "${CONTAINER_CMD:-}" ]; then
    CONTAINER_CMD=$(detect_engine) || exit 1
fi
echo "Using $CONTAINER_CMD as container engine"

echo "--- Waiting for services ---"
wait_for 120 "jobmanager did not start" \
    "$CONTAINER_CMD" compose exec -T jobmanager true
wait_for 120 "lakekeeper did not become healthy" \
    "$CONTAINER_CMD" compose exec -T jobmanager \
    curl -sf http://lakekeeper:8181/health

echo "--- Creating table ---"
$CONTAINER_CMD compose exec -T jobmanager ./bin/sql-client.sh embedded <<'SQL'
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT,
    customer_id INT NOT NULL,
    total_price DECIMAL(15, 2),
    order_date DATE,
    status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '10s'
);
SQL

echo "--- Starting tiering job ---"
$CONTAINER_CMD compose exec -d jobmanager ./bin/flink run \
    /opt/flink/lib/fluss-flink-tiering-0.9.0-incubating.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type rest \
    --datalake.iceberg.uri http://lakekeeper:8181/catalog \
    --datalake.iceberg.warehouse fluss-warehouse
wait_for 30 "tiering job did not start" tiering_job_running

echo "--- Starting continuous ingestion ---"
$CONTAINER_CMD compose exec -T jobmanager ./bin/sql-client.sh embedded <<'SQL'
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;
USE demo;

CREATE TEMPORARY TABLE source_orders (
    order_id BIGINT,
    customer_id INT NOT NULL,
    total_price DECIMAL(15, 2),
    order_date DATE,
    status STRING
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '5',
    'fields.order_id.expression' = '#{number.numberBetween ''1'',''1000000''}',
    'fields.customer_id.expression' = '#{number.numberBetween ''100'',''200''}',
    'fields.total_price.expression' = '#{number.randomDouble ''2'',''5'',''500''}',
    'fields.order_date.expression' = '#{date.past ''30'' ''DAYS''}',
    'fields.status.expression' = '#{regexify ''(completed|pending|shipped){1}''}'
);

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

INSERT INTO orders SELECT * FROM source_orders;
SQL

DUCKDB_QUERY="
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID 'rustfs-root-user', SECRET 'rustfs-root-password',
    ENDPOINT 'localtest.me:9000', USE_SSL false, URL_STYLE 'path');
CREATE SECRET (TYPE ICEBERG, ENDPOINT 'http://lakekeeper:8181/catalog', TOKEN 'dummy');
ATTACH 'fluss-warehouse' AS lk (TYPE ICEBERG);
SELECT * FROM lk.demo.orders LIMIT 5;
"

echo "--- Waiting for data to be tiered to Iceberg ---"
SECONDS=0
while [ $SECONDS -lt 120 ]; do
    output=$($CONTAINER_CMD compose run --rm -T duckdb duckdb -c "$DUCKDB_QUERY" 2>&1)
    if echo "$output" | grep -q "0 rows"; then
        sleep 5
        continue
    fi
    echo "--- DuckDB query result ---"
    echo "$output"
    echo ""
    echo "Data is continuously flowing. Query anytime with:"
    echo "  $CONTAINER_CMD compose run --rm duckdb duckdb"
    echo ""
    echo "Or open the Lakekeeper UI at http://localhost:8181"
    exit 0
done

echo "Tiering did not produce data within 120s"
exit 1
