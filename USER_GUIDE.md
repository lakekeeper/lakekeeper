# Using the Catalog

If you just want a quick glimpse, use the self-contained Quickstart describe in the main [README](#README). Otherwise, continue here.

# Deployment

There are multiple ways to deploy the catalog. It ships as a single standalone binary that can be deployed anywhere you like. For high availability, we recommend to [Deploy on Kubernetes](#deployment-on-kubernetes).

## Deployment on Kubernetes

We recommend deploying the catalog on Kubernetes using our [Helm Chart](https://github.com/hansetag/tip-catalog-charts/tree/main/charts/tip-catalog). Please check the Helm Chart's documentation for possible values.

## Deployment Standalone

For single node deployments, you can also download the Binary from [Github Releases](https://github.com/lakekeeper/lakekeeper/releases).

A basic configuration via environment variables would look something like this:

```sh
export ICEBERG_REST__BASE_URI=http://localhost:8080
# For single-tenant deployments, otherwise skip:
export ICEBERG_REST__DEFAULT_PROJECT_ID="00000000-0000-0000-0000-000000000000"
export ICEBERG_REST__PG_DATABASE_URL_READ="postgres://postgres_user:postgres_urlencoded_password@hostname:5432/catalog_database"
export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgres://postgres_user:postgres_urlencoded_password@hostname:5432/catalog_database"
export ICEBERG_REST__PG_ENCRYPTION_KEY="MySecretEncryptionKeyThatIBetterNotLoose"
```

Now we need to migrate the Database:
`iceberg-catalog migrate`

Finally, we can run the server:
`iceberg-catalog serve`

# Creating a Warehouse

Now that the catalog is up-and-running, two endpoints are available:

1. `<ICEBERG_REST__BASE_URI>/catalog` is the Iceberg REST API
2. `<ICEBERG_REST__BASE_URI>/management` contains the management API

Now that the server is running, we need to create a new warehouse. Lets assume we have an AWS S3-bucket, create a file called `create-warehouse-request.json`:

```json
{
  "warehouse-name": "test",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "demo-catalog-iceberg",
    "key-prefix": "test_warehouse",
    "assume-role-arn": null,
    "endpoint": null,
    "region": "eu-central-1",
    "path-style-access": null
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "<my-access-key>",
    "aws-secret-access-key": "<my-secret-access-key>"
  }
}
```

We now create a new Warehouse by POSTing the request to the management API:

```sh
curl -X POST http://localhost:8080/management/v1/warehouse -H "Content-Type: application/json" -d @create-warehouse-request.json
```

If you want to use a different storage backend, see the [STORAGE.MD](STORAGE.MD) for example configurations.

That's it - we can now use the catalog:

```python
import pandas as pd
import pyspark

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
ICEBERG_VERSION = "1.6.1"

# if you use adls as storage backend, you need iceberg-azure instead of iceberg-aws-bundle
configuration = {
    "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "demo",
    "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.demo.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
    "spark.sql.catalog.demo.uri": "http://localhost:8080/catalog/",
    "spark.sql.catalog.demo.token": "dummy",
    "spark.sql.catalog.demo.warehouse": "00000000-0000-0000-0000-000000000000/test",
}
spark_conf = pyspark.SparkConf()
for k, v in configuration.items():
    spark_conf = spark_conf.set(k, v)

spark = pyspark.sql.SparkSession.builder.config(conf=spark_conf).getOrCreate()

spark.sql("USE demo")

spark.sql("CREATE NAMESPACE IF NOT EXISTS my_namespace")
print(f"\n\nCurrently the following namespace exist:")
print(spark.sql("SHOW NAMESPACES").toPandas())
print("\n\n")

sdf = spark.createDataFrame(
    pd.DataFrame(
        [[1, 1.2, "foo"], [2, 2.2, "bar"]], columns=["my_ints", "my_floats", "strings"]
    )
)

spark.sql("DROP TABLE IF EXISTS demo.my_namespace.my_table")
spark.sql(
    "CREATE TABLE demo.my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
)
sdf.writeTo("demo.my_namespace.my_table").append()
spark.table("demo.my_namespace.my_table").show()
```

For more examples also check the [/examples/notebooks](examples/notebooks) as well as our [integration tests](tests/python/tests/).

