# Apache Spark

This page shows how to read and write a **Lance** [generic table](generic-tables.md) from **PySpark**. The Lakekeeper client resolves the table location and vends short-lived storage credentials; Spark (and `pylance`) then read/write the data directly.

!!! note "For Iceberg tables, use the REST catalog instead"
    If you want plain Spark SQL over Iceberg tables (`spark.sql("SELECT ... FROM lakekeeper.ns.table")`), configure Lakekeeper as an Iceberg REST catalog — see [Query Engines → Spark](engines.md#spark). You do not need the client library or the credential wiring below for that.

## How it works

The [`lakekeeper-client-spark`](https://github.com/lakekeeper/lakekeeper-clients/tree/main/java) JAR adds a small `LakekeeperSpark` helper on top of the Java client. From PySpark you reach it through `spark._jvm` (py4j) — no separate Python package is required. The helper:

1. Loads the generic table with `vended=true` to get short-lived STS credentials.
2. Translates the Iceberg-style `s3.*` credentials into Hadoop `fs.s3a.*` config and applies them to the active `SparkSession`.
3. Reads/writes the table's storage location with `spark.read().format(<format>)` / `df.write().format(<format>)`, where the format comes from the Lakekeeper table metadata.

Because Lance also has a fast native Python writer (`pylance`), the example below **writes** with `pylance` and **reads back** through the Spark shim — but you can do both from Spark.

## Prerequisites

```sh
pip install pyspark pylance pyarrow numpy
```

Download two JARs and point env vars at them:

- `lakekeeper-client-spark-3.5_2.13-<version>.jar` — from GitHub Packages
- The [Lance Spark connector](https://github.com/lancedb/lance/releases) JAR

| Variable | Description |
|---|---|
| `LAKEKEEPER` | Base URL (default `http://localhost:8181`) |
| `WAREHOUSE_ID` | Warehouse **UUID** (used as the URL path prefix — not the name) |
| `TOKEN` **or** `OAUTH_TOKEN_URL` / `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET` / `OAUTH_SCOPE` | Auth |
| `LAKEKEEPER_JAR` | Path to `lakekeeper-client-spark-*.jar` |
| `LANCE_SPARK_JAR` | Path to the Lance Spark connector JAR |

The warehouse and namespace must already exist; the client creates the table.

## Start Spark with the JARs

```python
import os
from pyspark.sql import SparkSession, DataFrame

jars = ",".join(j for j in [os.environ["LAKEKEEPER_JAR"], os.environ.get("LANCE_SPARK_JAR", "")] if j)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("lakekeeper-lance-demo")
    .config("spark.jars", jars)
    .getOrCreate()
)
```

## Build the Lakekeeper client via py4j

`spark._jvm` gives direct access to any class on the Spark JVM classpath:

```python
jvm = spark._jvm

# Auth — static token or OAuth2 client_credentials
if token := os.environ.get("TOKEN"):
    auth = jvm.io.lakekeeper.client.auth.StaticToken(token)
else:
    auth = jvm.io.lakekeeper.client.auth.ClientCredentials(
        os.environ["OAUTH_TOKEN_URL"],
        os.environ["OAUTH_CLIENT_ID"],
        os.environ["OAUTH_CLIENT_SECRET"],
        os.environ.get("OAUTH_SCOPE"),   # scope (nullable)
        60,                              # refreshMarginSeconds
        30,                              # timeoutSeconds
    )

client = (
    jvm.io.lakekeeper.client.LakekeeperClient.builder()
    .baseUrl(os.environ.get("LAKEKEEPER", "http://localhost:8181"))
    .warehouse(os.environ["WAREHOUSE_ID"])
    .auth(auth)
    .build()
)
```

## Create the Lance table

```python
GenericTableFormat = jvm.io.lakekeeper.client.GenericTableFormat

try:
    client.genericTables().create(
        "examples", "spark_lance_embeddings",
        GenericTableFormat.normalize(GenericTableFormat.LANCE),
        None,                               # base-location — let the server assign
        None,                               # doc
        {"embedding-dim": "128"},           # properties (Java Map via py4j)
    )
except Exception as e:                      # tolerate re-runs
    if "ConflictException" in type(e).__name__ or "409" in str(e):
        pass
    else:
        raise
```

## Write with pylance, read back with Spark

`LakekeeperSpark.read()` calls Lakekeeper, wires the vended credentials into `fs.s3a.*`, and returns a JVM `Dataset<Row>` — wrap it as a Python `DataFrame`:

```python
import numpy as np, pyarrow as pa, lance

# 1. Fresh vended credentials for the pylance write
ICEBERG_TO_LANCE = {
    "s3.access-key-id": "aws_access_key_id", "s3.secret-access-key": "aws_secret_access_key",
    "s3.session-token": "aws_session_token", "s3.region": "aws_region",
    "client.region": "aws_region", "s3.endpoint": "aws_endpoint",
}

def lance_opts(resp):
    props = {}
    for cred in resp.getStorageCredentials():
        props.update(cred.getConfig())
    if resp.getConfig():
        props.update(resp.getConfig())
    opts = {ICEBERG_TO_LANCE[k]: v for k, v in props.items() if k in ICEBERG_TO_LANCE}
    if opts.get("aws_endpoint", "").startswith("http://"):
        opts["allow_http"] = "true"        # MinIO/SeaweedFS over http
    return opts

resp = client.genericTables().load("examples", "spark_lance_embeddings", True)
rng = np.random.default_rng(42)
data = pa.table({
    "id": pa.array(range(1000), type=pa.int64()),
    "embedding": pa.FixedSizeListArray.from_arrays(
        pa.array(rng.standard_normal(1000 * 128).astype(np.float32)), 128),
})
lance.write_dataset(data, resp.getLocation(), storage_options=lance_opts(resp), mode="overwrite")

# 2. Read it back through the Spark shim
LakekeeperSpark = jvm.io.lakekeeper.spark.LakekeeperSpark
java_df = LakekeeperSpark.read(spark._jsparkSession, client, "examples", "spark_lance_embeddings")
df = DataFrame(java_df, spark)
print("rows:", df.count())
df.select("id").show(5)
```

## Write back from Spark

`LakekeeperSpark.write()` fetches fresh vended credentials, then saves the DataFrame to the table location. Pass write options (including `mode`) as a `java.util.HashMap`:

```python
opts = jvm.java.util.HashMap()
opts.put("mode", "overwrite")

subset = df.filter("id < 100")
LakekeeperSpark.write(spark._jsparkSession, client, "examples", "spark_lance_embeddings",
                      subset._jdf, opts)
```

When you're done, release the client and Spark:

```python
client.close()
spark.stop()
```

## Related

- [Query Engines → Spark](engines.md#spark) — Iceberg tables via the REST catalog (no client library needed)
- [Generic Tables](generic-tables.md) — the catalog concept
- [Python Client](generic-tables-pylakekeeper.md) — the same vending flow in pure Python (no Spark)
- [Apache Flink](generic-tables-flink.md) — streaming into generic tables from Java
- Source & notebook: [`lakekeeper-clients` on GitHub](https://github.com/lakekeeper/lakekeeper-clients/tree/main/python/examples)
