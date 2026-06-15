"""Spark integration test for S3 server-side encryption with KMS.

Verifies that when a warehouse is configured with ``aws-kms-key-arn``, Lakekeeper
advertises ``s3.sse.type=kms`` / ``s3.sse.key=<arn>`` to clients so that
*client-side* writes (Spark via vended credentials) are encrypted with the
configured KMS key — independent of any bucket-default-encryption configuration.

Runs in its own tox env (``spark_aws_kms``) so it can own the Spark session
without colliding with the shared session-scoped ``spark`` fixture. Skips unless
the dedicated KMS bucket + key + STS role are configured (same env vars as the
Rust KMS integration tests).
"""

import uuid
from typing import Optional

import boto3
import conftest
import pytest

from conftest import settings

# All objects under the test prefix must be encrypted with this type.
EXPECTED_SSE = "aws:kms"


def _require_kms_settings():
    if not settings.aws_kms_s3_bucket:
        pytest.skip("LAKEKEEPER_TEST__AWS_KMS_S3_BUCKET is not set")
    if not settings.aws_s3_kms_arn:
        pytest.skip("LAKEKEEPER_TEST__AWS_S3_KMS_ARN is not set")
    if not settings.aws_s3_region:
        pytest.skip("LAKEKEEPER_TEST__AWS_S3_REGION is not set")
    if not settings.aws_s3_sts_role_arn:
        pytest.skip("LAKEKEEPER_TEST__AWS_S3_STS_ROLE_ARN is not set")
    # Without system identity, both the warehouse credential and the boto3
    # inspection client use these access keys; skip cleanly if they are absent
    # rather than failing later with an opaque authentication error.
    if not settings.aws_s3_use_system_identity and (
        not settings.aws_s3_access_key or not settings.aws_s3_secret_access_key
    ):
        pytest.skip(
            "LAKEKEEPER_TEST__AWS_S3_ACCESS_KEY / "
            "LAKEKEEPER_TEST__AWS_S3_SECRET_ACCESS_KEY is not set "
            "(required when not using system identity)"
        )


@pytest.fixture(scope="module")
def kms_warehouse(server: conftest.Server, project) -> conftest.Warehouse:
    _require_kms_settings()

    test_id = uuid.uuid4().hex
    # `assume-role-arn` (not just `sts-role-arn`): the base IAM user has no KMS
    # permissions, so Lakekeeper's own writes (warehouse-validation, metadata) must
    # assume the KMS-capable role too. Vending falls back to this role for clients.
    storage_profile = {
        "type": "s3",
        "bucket": settings.aws_kms_s3_bucket,
        "region": settings.aws_s3_region,
        "path-style-access": False,
        "key-prefix": f"kms-spark-test/{test_id}",
        "flavor": "aws",
        "sts-enabled": True,
        "assume-role-arn": settings.aws_s3_sts_role_arn,
        "aws-kms-key-arn": settings.aws_s3_kms_arn,
        "layout": {
            "type": "full-hierarchy",
            "namespace": "{name}-{uuid}",
            "table": "{name}-{uuid}",
        },
    }
    if settings.aws_s3_use_system_identity:
        storage_credential = {
            "type": "s3",
            "credential-type": "aws-system-identity",
        }
    else:
        storage_credential = {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": settings.aws_s3_access_key,
            "aws-secret-access-key": settings.aws_s3_secret_access_key,
        }

    warehouse_name = f"warehouse-kms-{uuid.uuid4()}"
    warehouse_id = server.create_warehouse(
        warehouse_name,
        project_id=project,
        storage_config={
            "storage-profile": storage_profile,
            "storage-credential": storage_credential,
        },
    )
    return conftest.Warehouse(
        access_token=server.access_token,
        server=server,
        project_id=project,
        warehouse_id=warehouse_id,
        warehouse_name=warehouse_name,
    )


@pytest.fixture(scope="module")
def kms_spark(kms_warehouse: conftest.Warehouse):
    """A dedicated Spark session bound to the KMS warehouse (vended credentials)."""
    try:
        import findspark

        findspark.init()
    except ImportError:
        pytest.skip("findspark not installed")

    import pyspark
    import pyspark.sql

    pyspark_version = ".".join(pyspark.__version__.split(".")[:2])
    scala_version = "2.13" if int(pyspark_version.split(".")[0]) >= 4 else "2.12"

    spark_jars_packages = (
        f"org.apache.iceberg:iceberg-spark-runtime-{pyspark_version}_{scala_version}:{settings.spark_iceberg_version},"
        f"org.apache.iceberg:iceberg-aws-bundle:{settings.spark_iceberg_version}"
    )

    catalog_name = kms_warehouse.normalized_catalog_name
    configuration = {
        "spark.jars.packages": spark_jars_packages,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": catalog_name,
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        f"spark.sql.catalog.{catalog_name}.uri": kms_warehouse.server.catalog_url,
        f"spark.sql.catalog.{catalog_name}.credential": f"{settings.openid_client_id}:{settings.openid_client_secret}",
        f"spark.sql.catalog.{catalog_name}.warehouse": f"{kms_warehouse.project_id}/{kms_warehouse.warehouse_name}",
        f"spark.sql.catalog.{catalog_name}.scope": settings.openid_scope,
        f"spark.sql.catalog.{catalog_name}.oauth2-server-uri": f"{settings.token_endpoint}",
        # Force the client to perform its own writes so the SSE-KMS config we vend
        # actually takes effect (rather than the catalog writing on its behalf).
        f"spark.sql.catalog.{catalog_name}.header.X-Iceberg-Access-Delegation": "vended-credentials",
    }

    spark_conf = pyspark.SparkConf().setMaster("local[*]")
    for k, v in configuration.items():
        spark_conf = spark_conf.set(k, v)

    spark = pyspark.sql.SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sql(f"USE {catalog_name}")
    yield spark
    spark.stop()


def _s3_client():
    """S3 client for inspecting written objects.

    The base IAM user has no S3/KMS permissions on the KMS bucket — only the role
    does (it is what Lakekeeper assumes for its own writes). So assume that same role
    before inspecting, mirroring how the catalog accesses the bucket.
    """
    base = {"region_name": settings.aws_s3_region}
    if not settings.aws_s3_use_system_identity:
        base["aws_access_key_id"] = str(settings.aws_s3_access_key)
        base["aws_secret_access_key"] = str(settings.aws_s3_secret_access_key)

    sts = boto3.client("sts", **base)
    creds = sts.assume_role(
        RoleArn=settings.aws_s3_sts_role_arn,
        RoleSessionName="lakekeeper-kms-test-inspect",
    )["Credentials"]
    return boto3.client(
        "s3",
        region_name=settings.aws_s3_region,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )


def _expected_key_id() -> Optional[str]:
    """Key id portion of a key ARN, or None for alias ARNs (can't match exactly)."""
    arn = settings.aws_s3_kms_arn
    if arn and ":key/" in arn:
        return arn.split(":key/", 1)[1]
    return None


def test_spark_writes_are_sse_kms_encrypted(
    kms_spark, kms_warehouse: conftest.Warehouse
):
    ns = "kms_ns"
    table = f"{ns}.t"
    kms_spark.sql(f"CREATE NAMESPACE {ns}")
    kms_spark.sql(f"CREATE TABLE {table} (id INT, val STRING) USING iceberg")
    kms_spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")

    # Data round-trips through the encrypted bucket.
    pdf = kms_spark.sql(f"SELECT * FROM {table} ORDER BY id").toPandas()
    assert pdf["id"].tolist() == [1, 2]
    assert pdf["val"].tolist() == ["a", "b"]

    # Resolve the physical table location via the catalog (REST only, no S3 access).
    iceberg_table = kms_warehouse.pyiceberg_catalog.load_table((ns, "t"))
    location = iceberg_table.location()  # s3://bucket/key-prefix/.../t-<uuid>
    assert location.startswith("s3://")
    bucket, _, prefix = location[len("s3://") :].partition("/")

    s3 = _s3_client()
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    keys = [o["Key"] for o in objects]
    assert keys, f"no objects written under {location}"

    # Spark wrote at least one data file; that file must be SSE-KMS encrypted.
    data_files = [k for k in keys if k.endswith(".parquet")]
    assert data_files, f"no parquet data files under {location}: {keys}"

    expected_key_id = _expected_key_id()
    for key in keys:
        head = s3.head_object(Bucket=bucket, Key=key)
        assert (
            head.get("ServerSideEncryption") == EXPECTED_SSE
        ), f"{key} not SSE-KMS encrypted: {head.get('ServerSideEncryption')}"
        if expected_key_id is not None:
            assert expected_key_id in head.get(
                "SSEKMSKeyId", ""
            ), f"{key} encrypted with unexpected key {head.get('SSEKMSKeyId')}"
