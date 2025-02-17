import json

import conftest
import pandas as pd
import pytest
import requests
import pyiceberg.io as io
import time
import fsspec
from conftest import settings


@pytest.fixture(scope="session")
def s3_warehouse_location(warehouse: conftest.Warehouse):
    return f"s3://{settings.s3_bucket}"


def test_create_table_s3a(spark, warehouse: conftest.Warehouse, s3_warehouse_location):
    spark.sql("CREATE NAMESPACE test_create_table_s3a")

    s3a_path = f"{s3_warehouse_location}/test_create_table_s3a".replace(
        "s3://", "s3a://"
    )
    s3a_path = f"{s3a_path}-{int(time.time())}"

    # Create regular table
    spark.sql(
        f"""CREATE TABLE test_create_table_s3a.my_table 
        (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg
        LOCATION '{s3a_path}'
        """
    )
    loaded_table = warehouse.pyiceberg_catalog.load_table(
        ("test_create_table_s3a", "my_table")
    )
    location = loaded_table.location()
    assert location.startswith("s3a://")

    # Insert data
    spark.sql(
        f"""INSERT INTO test_create_table_s3a.my_table 
        VALUES (1, 1.1, 'a'), (2, 2.2, 'b')
        """
    )

    # Read data
    df = spark.sql("SELECT * FROM test_create_table_s3a.my_table").toPandas()
    pd.testing.assert_frame_equal(
        df,
        pd.DataFrame(
            {"my_ints": [1, 2], "my_floats": [1.1, 2.2], "strings": ["a", "b"]}
        ),
        check_dtype=False,
    )


def test_register_table(
    spark, namespace, warehouse: conftest.Warehouse, s3_warehouse_location
):
    s3a_path = f"{s3_warehouse_location}/test_register_table_s3a".replace(
        "s3://", "s3a://"
    )
    s3a_path = f"{s3a_path}-{int(time.time())}"

    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT) USING iceberg LOCATION '{s3a_path}'"
    )
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1)")
    table = warehouse.pyiceberg_catalog.load_table((*namespace.name, "my_table"))
    assert spark.sql(f"SHOW TABLES IN {namespace.spark_name}").toPandas().shape[0] == 1

    # Remove table from catalog
    delete_uri = (
        warehouse.server.catalog_url.strip("/")
        + "/"
        + "/".join(
            [
                "v1",
                str(warehouse.warehouse_id),
                "namespaces",
                namespace.url_name,
                "tables",
                f"my_table?purgeRequested=false",
            ]
        )
    )
    requests.delete(
        delete_uri, headers={"Authorization": f"Bearer {warehouse.access_token}"}
    ).raise_for_status()
    time.sleep(4)

    # Can't query table anymore
    assert spark.sql(f"SHOW TABLES IN {namespace.spark_name}").toPandas().shape[0] == 0

    assert table.metadata_location.startswith("s3a://")

    spark.sql(
        f"""
    CALL {warehouse.normalized_catalog_name}.system.register_table (
        table => '{namespace.spark_name}.my_registered_table',
        metadata_file => '{table.metadata_location}'
    )"""
    )

    pdf = spark.sql(
        f"SELECT * FROM {namespace.spark_name}.my_registered_table"
    ).toPandas()
    assert pdf["my_ints"].tolist() == [1]
