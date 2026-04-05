# Fluss + Lakekeeper Example

[Apache Fluss](https://fluss.apache.org/) streaming data tiered into Iceberg tables managed by Lakekeeper.

No authentication or authorization is configured.

## Services

| Service          | URL                   |
|------------------|-----------------------|
| Lakekeeper       | http://localhost:8181  |
| Flink Web UI     | http://localhost:8083  |
| RustFS Console   | http://localhost:9001  |
| RustFS API       | http://localhost:9000  |

RustFS credentials: `rustfs-root-user` / `rustfs-root-password`

## Quick Start

```bash
cd examples/fluss
docker compose up -d
./run-demo.sh
```

This creates a Fluss table with datalake tiering enabled, inserts sample data,
waits for it to be tiered to Iceberg, and queries it via DuckDB through
Lakekeeper's REST catalog.

On first start, JARs are downloaded from Maven Central. This may take a minute.

## Manual Usage

Connect to Flink SQL CLI:

```bash
docker compose exec jobmanager ./bin/sql-client.sh
```

Create a datalake-enabled table:

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

CREATE TABLE orders (
    order_id BIGINT,
    customer_id INT NOT NULL,
    total_price DECIMAL(15, 2),
    order_date DATE,
    status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true'
);
```

Start the tiering service:

```bash
docker compose exec jobmanager ./bin/flink run \
    /opt/flink/lib/fluss-flink-tiering-0.9.0-incubating.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type rest \
    --datalake.iceberg.uri http://lakekeeper:8181/catalog \
    --datalake.iceberg.warehouse fluss-warehouse
```

Tiered data is queryable by any Iceberg-compatible engine through Lakekeeper's REST catalog.
