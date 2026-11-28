# DataFusion

Run a SQL query on an Iceberg table from Lakekeeper with [Apache DataFusion](https://datafusion.apache.org/python/). PyIceberg loads the table metadata location and short-lived storage credentials from the REST catalog. The native Iceberg provider uses them to let DataFusion read the table from S3.

Use Docker Compose 2.19.0 or later. The Compose overlay uses `!reset` to clear the port mappings inherited from the minimal example. Run the following command from the repository root. The query container installs its Python wheels from PyPI.

```bash
cd examples/datafusion
docker compose -p lakekeeper-datafusion -f ../minimal/docker-compose.yaml -f docker-compose.yaml run --rm datafusion
```

The command starts Lakekeeper, PostgreSQL and Silo on a private Docker network. The catalog in this demo runs without authentication. Compose keeps its ports inside the network.

The client creates the `datafusion_demo.trips` table, adds three rows and filters them with SQL. Re-running the command uses the same demo table. The result contains two rows.

```text
+----+-------------+
| id | destination |
+----+-------------+
| 2  | Berlin      |
| 3  | Lisbon      |
+----+-------------+
```

The following command removes the demo containers and their data volumes.

```bash
docker compose -p lakekeeper-datafusion -f ../minimal/docker-compose.yaml -f docker-compose.yaml down --volumes
```
