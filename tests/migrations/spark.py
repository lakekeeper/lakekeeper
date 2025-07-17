import argparse
import json
import sys
import time

import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, LongType, StructType, StructField, StringType

# Leave at least one table undropped.
TABLES_TO_MAINTAIN = ["my_table_0", "my_table_1"]
TABLES_TO_DROP = ["my_table_2"]
TABLE_POST_MIGRATION = "my_table_3"

TABLE_SCHEMA = StructType([
        StructField("id", LongType(), True),
        StructField("strings", StringType(), True),
        StructField("floats", FloatType(), True),
])

def spark_session(catalog_url):
    """
    Creates and returns a spark session.
    """
    WAREHOUSE = "demo"

    SPARK_VERSION = pyspark.__version__
    SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
    ICEBERG_VERSION = "1.6.1"

    config = {
        f"spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.lakekeeper.type": "rest",
        f"spark.sql.catalog.lakekeeper.uri": catalog_url,
        f"spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
        f"spark.sql.catalog.lakekeeper.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "lakekeeper",
        "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    }
    spark_config = SparkConf().setMaster('local').setAppName("Iceberg-REST")
    for k, v in config.items():
        spark_config = spark_config.set(k, v)
    spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

    spark.sql("USE lakekeeper")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS my_namespace")
    spark.sql("SHOW NAMESPACES").show()

    return spark

def read(spark):
    """
    Reads data from tables that are expected to exist.
    """
    print("Reading data")
    for table in TABLES_TO_MAINTAIN:
        spark.sql(f"SELECT * FROM my_namespace.{table}").show()

def get_short_soft_delete_expiration():
    with open("./create-warehouse/soft-delete-1sec.json") as f:
        data = json.load(f)
        return data["delete-profile"]["expiration-seconds"]

def write_pre_migration(spark):
    """
    Creates tables and drops some of them for the provided spark session.
    """
    # Lakekeeper migration issues can be related to (soft) deleted tables.
    # So create and drop some tables to simulate that situation.
    print("Creating tables")
    for table in TABLES_TO_MAINTAIN + TABLES_TO_DROP:
        df = spark.createDataFrame([], TABLE_SCHEMA)
        df.writeTo(f"my_namespace.{table}").createOrReplace()

        # Insert some rows.
        schema = spark.table(f"my_namespace.{table}").schema
        data = [
            [1, 'a-string', 1.1],
            [2, 'b-string', 2.2]
        ]
        df = spark.createDataFrame(data, schema)
        df.writeTo(f"my_namespace.{table}").append()

        spark.sql(f"SELECT * FROM my_namespace.{table}").show()

    # Use all `DROP` variants to delete some of the tables.
    print("Deleting some of the tables")
    for table in TABLES_TO_DROP:
        spark.sql(f"DROP TABLE my_namespace.{table}")

    # Sleep to let (short) soft-delete timeout expire.
    # Actually only necessary only if the warehouse is configured with short soft delete expiration.
    # However waiting a few seconds here doesn't hurt.
    time.sleep(get_short_soft_delete_expiration())

def write_post_migration(spark):
    """
    Writes to existing tables and creates a new one.
    """
    # existing tables
    for table in TABLES_TO_MAINTAIN:
        schema = spark.table(f"my_namespace.{table}").schema
        data = [[3, 'c-string', 3.3]]
        df = spark.createDataFrame(data, schema)
        df.writeTo(f"my_namespace.{table}").append()

        spark.sql(f"SELECT * FROM my_namespace.{table}").show()

    # new table
    df = spark.createDataFrame([], TABLE_SCHEMA)
    df.writeTo(f"my_namespace.{TABLE_POST_MIGRATION}").createOrReplace()
    schema = spark.table(f"my_namespace.{TABLE_POST_MIGRATION}").schema
    data = [[4, 'd-string', 4.4]]
    df = spark.createDataFrame(data, schema)
    df.writeTo(f"my_namespace.{TABLE_POST_MIGRATION}").append()
    spark.sql(f"SELECT * FROM my_namespace.{TABLE_POST_MIGRATION}").show()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "task",
        choices = ["read", "write_pre_migration", "write_post_migration"]
    )
    parser.add_argument("catalog_url")
    args = parser.parse_args()

    spark = spark_session(args.catalog_url)
    if args.task == "read":
        read(spark)
    elif args.task == "write_pre_migration":
        write_pre_migration(spark)
    elif args.task == "write_post_migration":
        write_post_migration(spark)
    return 0

if __name__ == "__main__":
    sys.exit(main())
