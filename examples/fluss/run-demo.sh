#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "--- Waiting for services ---"
until docker compose exec -T jobmanager true 2>/dev/null; do sleep 1; done
docker compose exec -T jobmanager bash -c 'until curl -sf http://lakekeeper:8181/health > /dev/null 2>&1; do sleep 1; done'

echo "--- Creating table ---"
docker compose exec -T jobmanager ./bin/sql-client.sh embedded <<'SQL'
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
docker compose exec -d jobmanager ./bin/flink run \
    /opt/flink/lib/fluss-flink-tiering-0.9.0-incubating.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type rest \
    --datalake.iceberg.uri http://lakekeeper:8181/catalog \
    --datalake.iceberg.warehouse fluss-warehouse
sleep 5

echo "--- Starting continuous ingestion ---"
docker compose exec -T jobmanager ./bin/sql-client.sh embedded <<'SQL'
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

echo "--- Waiting for data to be tiered to Iceberg ---"
sleep 30

echo "--- DuckDB query result ---"
docker compose run --rm -T duckdb duckdb -c "
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID 'rustfs-root-user', SECRET 'rustfs-root-password',
    ENDPOINT 'localtest.me:9000', USE_SSL false, URL_STYLE 'path');
CREATE SECRET (TYPE ICEBERG, ENDPOINT 'http://lakekeeper:8181/catalog', TOKEN 'dummy');
ATTACH 'fluss-warehouse' AS lk (TYPE ICEBERG);
SELECT * FROM lk.demo.orders LIMIT 5;
"

echo ""
echo "Data is continuously flowing. Query anytime with:"
echo "  docker compose run --rm duckdb duckdb"
echo ""
echo "Or open the Lakekeeper UI at http://localhost:8181"
