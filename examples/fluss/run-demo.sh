#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "--- Waiting for services ---"
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

echo "--- Inserting data ---"
docker compose exec -T jobmanager ./bin/sql-client.sh embedded <<'SQL'
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;
USE demo;
INSERT INTO orders VALUES
    (1, 100, 29.99,  DATE '2026-03-01', 'completed'),
    (2, 101, 49.50,  DATE '2026-03-02', 'pending'),
    (3, 102, 15.00,  DATE '2026-03-03', 'completed'),
    (4, 100, 89.99,  DATE '2026-03-04', 'shipped'),
    (5, 103, 120.00, DATE '2026-03-05', 'completed');
SQL

echo "--- Waiting for tiering ---"
DUCKDB_QUERY="
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID 'rustfs-root-user', SECRET 'rustfs-root-password',
    ENDPOINT 'rustfs:9000', USE_SSL false, URL_STYLE 'path');
CREATE SECRET (TYPE ICEBERG, ENDPOINT 'http://lakekeeper:8181/catalog', TOKEN 'dummy');
ATTACH 'fluss-warehouse' AS lk (TYPE ICEBERG);
SELECT order_id, customer_id, total_price, order_date, status FROM lk.demo.orders ORDER BY order_id;
"

for i in $(seq 1 12); do
    sleep 10
    echo "  attempt $i/12..."
    output=$(docker run --rm --network fluss_iceberg_net duckdb/duckdb duckdb -c "$DUCKDB_QUERY" 2>&1)
    if echo "$output" | grep -q 'completed'; then
        echo ""
        echo "--- DuckDB query result ---"
        echo "$output"
        exit 0
    fi
done

echo "Tiering did not complete in time."
exit 1
