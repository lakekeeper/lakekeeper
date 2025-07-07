import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, LongType, StructType, StructField, StringType

# This CATALOG_URL works for the "docker compose" testing and development environment
# Change 'lakekeeper' if you are not running on "docker compose" (f. ex. 'localhost' if Lakekeeper is running locally).
CATALOG_URL = "http://lakekeeper_initial:8181/catalog"
WAREHOUSE = "demo"

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
ICEBERG_VERSION = "1.6.1"

config = {
    f"spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.lakekeeper.type": "rest",
    f"spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
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

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS my_namespace")
spark.sql("SHOW NAMESPACES").show()

schema = StructType([
  StructField("id", LongType(), True),
  StructField("strings", StringType(), True),
  StructField("floats", FloatType(), True),
])

df = spark.createDataFrame([], schema)
df.writeTo("my_namespace.my_table").createOrReplace()

schema = spark.table("my_namespace.my_table").schema
data = [
    [1, 'a-string', 2.2],
    [2, 'b-string', 3.3]
]
df = spark.createDataFrame(data, schema)
df.writeTo("my_namespace.my_table").append()

spark.sql(f"SELECT * FROM my_namespace.my_table").show()
