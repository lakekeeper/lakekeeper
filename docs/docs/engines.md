# Query Engines

In this page we document how query engines can be configured to connect to Lakekeeper. Please also check the documentation of your query engine to obtain additional information. All Query engines that support the Apache Iceberg REST Catalog (IRC) also support Lakekeeper.

If Lakekeeper Authorization is enabled, Lakekeeper enforces permissions based on the `sub` field in the received tokens. For query engines used by a single user, the user should use its own credentials to log-in to Lakekeeper.

For query engines shared by multiple users, Lakekeeper supports two architectures that allow a shared query engine to enforce permissions for individual users:

1. OAuth2 enabled query engines should use standard OAuth2 Token-Exchange to exchange the user's token of the query engine for a Lakekeeper token (RFC8693). The Catalog then receives a token that has the `sub` field set to the user using the query engine, instead of the technical user that is used to configure the catalog in the query engine itself.
2. Query engines flexible enough to connect to external permission management systems such as Open Policy Agent (OPA), can directly enforce the same permissions on Data that Lakekeeper uses. Please find more information and a complete docker compose example with trino in the [Open Policy Agent Guide](opa.md).

Shared query engines must use the same Identity Provider as Lakekeeper in both scenarios unless user-ids are mapped, for example in OPA.

We are tracking open issues and missing features in query engines in a [Tracking Issue on Github](https://github.com/lakekeeper/lakekeeper/issues/399).

## Generic Iceberg REST Clients

All Apache Iceberg REST clients are compatible with Lakekeeper, as Lakekeeper fully implements the standard Iceberg REST Catalog API specification. This page only contains some exemplary tools and configurations to help you get started. For tools not listed here, please refer to their documentation for specific configuration details and best practices when connecting to an Iceberg REST Catalog. Always check with your tool provider for the most up-to-date information regarding supported features and configuration options.

When using Lakekeeper with authentication enabled, remember that you can follow the approaches described at the beginning of this page: either use credentials specific to individual users or leverage OAuth2 token exchange for shared query engines. The authentication parameters typically include credential pairs, OAuth2 server URIs, and scopes as shown in the examples above.

## <img src="/assets/trino.svg" width="30"> Trino

The following docker compose examples are available for trino:

- [`Minimal`](https://github.com/lakekeeper/lakekeeper/tree/main/examples/minimal): No authentication
- [`Access-Control-Simple`](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control-simple): Lakekeeper secured with OAuth2, single technical User for trino
- [`Access-Control-Advanced`](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control-advanced): Single trino instance secured by OAuth2 shared by multiple users. Lakekeeper Permissions for each individual user enforced by trino via the Open Policy Agent bridge.

If [Soft-Deletion](./concepts.md#soft-deletion) is enabled in Lakekeeper, make sure to set `"iceberg.unique-table-location" = 'true'`, to ensure that tables can be recreated in new locations while their dropped counterparts are waiting for expiration.

As Lakekeeper supports nesting of namespaces, we recommend to set `"iceberg.rest-catalog.nested-namespace-enabled" = 'true'`.

Basic setup in trino:

=== "S3-Compatible"

    Trino supports vended-credentials from Iceberg REST Catalogs for S3, so that no S3 credentials are required when creating the Catalog.

    ```sql
    CREATE CATALOG lakekeeper USING iceberg
    WITH (
        "iceberg.catalog.type" = 'rest',
        "iceberg.rest-catalog.uri" = '<Lakekeeper Catalog URI, i.e. http://localhost:8181/catalog>',
        "iceberg.rest-catalog.warehouse" = '<Name of the Warehouse in Lakekeeper>',
        "iceberg.rest-catalog.nested-namespace-enabled" = 'true',
        "iceberg.rest-catalog.vended-credentials-enabled" = 'true',
        "iceberg.unique-table-location" = 'true',
        "s3.region" = '<AWS Region to use. For S3-compatible storage use a non-existent AWS region, such as local>',
        "fs.native-s3.enabled" = 'true'
        -- Required for some S3-compatible storages:
        "s3.path-style-access" = 'true',
        "s3.endpoint" = '<Custom S3 endpoint>',
        -- Required Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.rest-catalog.security" = 'OAUTH2',
        "iceberg.rest-catalog.oauth2.credential" = '<Client-ID>:<Client-Secret>',
        "iceberg.rest-catalog.oauth2.server-uri" = '<Token Endpoint of your IdP, i.e. http://keycloak:8080/realms/iceberg/protocol/openid-connect/token>',
        -- Optional Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.rest-catalog.oauth2.scope" = '<Scopes to request from the IdP, i.e. lakekeeper>'
    )
    ```

=== "Azure"

    Trino does not support vended-credentials for Azure, so that Storage Account credentials must be specified in Trino. If you are interested in vended-credentials for Azure, please up-vote the [Trino Issue](https://github.com/trinodb/trino/issues/23238).

    Please find additional configuration Options in the [Trino docs](https://trino.io/docs/current/object-storage/file-system-azure.html#object-storage-file-system-azure--page-root).

    ```sql
    CREATE CATALOG lakekeeper USING iceberg
    WITH (
        "iceberg.catalog.type" = 'rest',
        "iceberg.rest-catalog.uri" = '<Lakekeeper Catalog URI, i.e. http://localhost:8181/catalog>',
        "iceberg.rest-catalog.warehouse" = '<Name of the Warehouse in Lakekeeper>',
        "iceberg.rest-catalog.nested-namespace-enabled" = 'true',
        "iceberg.unique-table-location" = 'true',
        "fs.native-azure.enabled" = 'true',
        "azure.auth-type" = 'OAUTH',
        "azure.oauth.client-id" = '<Client-ID for an Application with Storage Account access>',
        "azure.oauth.secret" = '<Client-Secret>',
        "azure.oauth.tenant-id" = '<Tenant-ID>',
        "azure.oauth.endpoint" = 'https://login.microsoftonline.com/<Tenant-ID>/v2.0',
        -- Required Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.rest-catalog.security" = 'OAUTH2',
        "iceberg.rest-catalog.oauth2.credential" = '<Client-ID>:<Client-Secret>', -- Client-ID used to access Lakekeeper. Typically different to `azure.oauth.client-id`.
        "iceberg.rest-catalog.oauth2.server-uri" = '<Token Endpoint of your IdP, i.e. http://keycloak:8080/realms/iceberg/protocol/openid-connect/token>',
        -- Optional Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.rest-catalog.oauth2.scope" = '<Scopes to request from the IdP, i.e. lakekeeper>'
    )
    ```

=== "GCS"

    Trino does not support vended-credentials for GCS, so that GCS credentials must be specified in Trino. If you are interested in vended-credentials for GCS, please up-vote the [Trino Issue](https://github.com/trinodb/trino/issues/24518).

    Please find additional configuration Options in the [Trino docs](https://trino.io/docs/current/object-storage/file-system-gcs.html).


    ```sql
    CREATE CATALOG lakekeeper USING iceberg
    WITH (
        "iceberg.catalog.type" = 'rest',
        "iceberg.rest-catalog.uri" = '<Lakekeeper Catalog URI, i.e. http://localhost:8181/catalog>',
        "iceberg.rest-catalog.warehouse" = '<Name of the Warehouse in Lakekeeper>',
        "iceberg.rest-catalog.nested-namespace-enabled" = 'true',
        "iceberg.unique-table-location" = 'true',
        "fs.native-gcs.enabled" = 'true',
        "gcs.project-id" = '<Identifier for the project on Google Cloud Storage>',
        "gcs.json-key" = '<Your Google Cloud service account key in JSON format>',
        -- Required Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.rest-catalog.security" = 'OAUTH2',
        "iceberg.rest-catalog.oauth2.credential" = '<Client-ID>:<Client-Secret>', -- Client-ID used to access Lakekeeper. Typically different to `azure.oauth.client-id`.
        "iceberg.rest-catalog.oauth2.server-uri" = '<Token Endpoint of your IdP, i.e. http://keycloak:8080/realms/iceberg/protocol/openid-connect/token>',
        -- Optional Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.rest-catalog.oauth2.scope" = '<Scopes to request from the IdP, i.e. lakekeeper>'
    )
    ```

## <img src="/assets/spark.svg" width="40" background-color="red"> Spark

The following docker compose examples are available for spark:

- [`Minimal`](https://github.com/lakekeeper/lakekeeper/tree/main/examples/minimal): No authentication
- [`Access-Control-Simple`](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control-simple): Lakekeeper secured with OAuth2, single technical User for spark

Basic setup in spark:

=== "S3-Compatible / Azure / GCS"

    Spark supports credential vending for all storage types, so that no credentials need to be specified in spark when creating the catalog.

    ```python
    import pyspark
    import pyspark.sql

    pyspark_version = pyspark.__version__
    pyspark_version = ".".join(pyspark_version.split(".")[:2]) # Strip patch version
    iceberg_version = "1.8.1"

    # Disable the jars which are not needed
    spark_jars_packages = (
        f"org.apache.iceberg:iceberg-spark-runtime-{pyspark_version}_2.12:{iceberg_version},"
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version},"
        f"org.apache.iceberg:iceberg-azure-bundle:{iceberg_version},"
        f"org.apache.iceberg:iceberg-gcp-bundle:{iceberg_version}"
    )

    catalog_name = "lakekeeper"
    configuration = {
        "spark.jars.packages": spark_jars_packages,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": catalog_name,
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        f"spark.sql.catalog.{catalog_name}.uri": "<Lakekeeper Catalog URI, i.e. http://localhost:8181/catalog>",
        # Required Parameters if OAuth2 authentication is enabled for Lakekeeper:
        f"spark.sql.catalog.{catalog_name}.credential": "<Client-ID>:<Client-Secret>", # Client-ID used to access Lakekeeper
        f"spark.sql.catalog.{catalog_name}.oauth2-server-uri": "<Token Endpoint of your IdP, i.e. http://keycloak:8080/realms/iceberg/protocol/openid-connect/token>",
        f"spark.sql.catalog.{catalog_name}.warehouse": "<Name of the Warehouse in Lakekeeper>",
        # Optional Parameters if OAuth2 authentication is enabled for Lakekeeper:
        f"spark.sql.catalog.{catalog_name}.scope": "<Scopes to request from the IdP, i.e. lakekeeper>",
        # Optional Parameter to configure which kind of vended-credential to use for S3:
        f"spark.sql.catalog.{catalog_name}.header.X-Iceberg-Access-Delegation": "vended-credentials" # Alternatively "remote-signing"
    }

    spark_conf = pyspark.SparkConf().setMaster("local[*]")

    for k, v in configuration.items():
        spark_conf = spark_conf.set(k, v)
    
    spark = pyspark.sql.SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sql(f"USE {catalog_name}")
    ```

## <img src="/assets/python.svg" width="30"> PyIceberg

```python
import pyiceberg.catalog
import pyiceberg.catalog.rest
import pyiceberg.typedef

catalog = pyiceberg.catalog.rest.RestCatalog(
    name="my_catalog_name",
    uri="<Lakekeeper Catalog URI, i.e. http://localhost:8181/catalog>",
    warehouse="<Name of the Warehouse in Lakekeeper>",
    #  Required Parameters if OAuth2 authentication is enabled for Lakekeeper:
    credential="<Client-ID>:<Client-Secret>",
    **{
        "oauth2-server-uri": "http://localhost:30080/realms/<keycloak realm name>/protocol/openid-connect/token"
    },
    # Optional Parameters if OAuth2 authentication is enabled for Lakekeeper:
    scope="<Scopes to request from the IdP, i.e. lakekeeper>",
)

print(catalog.list_namespaces())
```

## <img src="/assets/athena.svg" width="30"> AWS Athena (Spark)

Amazon Athena is a serverless query service that allows you to use SQL or PySpark to query data in Lakekeeper without provisioning infrastructure. The following steps demonstrate how to connect Athena PySpark with Lakekeeper.

**1. Create an Apache Spark workgroup in the AWS Athena console:**

* Go to the Athena console > Administration > Workgroups
* Create a workgroup with Apache Spark as the analytics engine

**2. Create a new PySpark notebook:**

* Give your notebook a name
* Select your Spark workgroup
* Configure JSON properties with Lakekeeper catalog settings

    ```json
    {
        "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakekeeper.type": "rest",
        "spark.sql.catalog.lakekeeper.uri": "<Lakekeeper Catalog URI>",
        "spark.sql.catalog.lakekeeper.warehouse": "<Name of the Warehouse in Lakekeeper>",
        "spark.sql.defaultCatalog": "lakekeeper",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakekeeper.credential": "<Client-ID>:<Client-Secret>", 
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": "<Token Endpoint of your IdP>"
    }
    ```

**3. Verify the connection in your notebook:**

```python
# Verify connectivity to your Lakekeeper catalog
spark.sql("select count(*) from lakekeeper.<namespace>.<table>").show()
```

Amazon Athena has Iceberg pre-installed, so no additional package installations are required.


## <img src="/assets/starrocks.svg" width="30"> Starrocks

Starrocks is improving the Iceberg REST support quickly. This guide is written for Starrocks 3.3, which does not support vended-credentials for AWS S3 with custom endpoints.

The following docker compose examples are available for starrocks:

- [`Minimal`](https://github.com/lakekeeper/lakekeeper/tree/main/examples/minimal): No authentication
- [`Access-Control`](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control): Lakekeeper secured with OAuth2, single technical user for starrocks


=== "S3-Compatible"

    ```sql
    CREATE EXTERNAL CATALOG rest_catalog
    PROPERTIES
    (
        "type" = "iceberg",
        "iceberg.catalog.type" = "rest",
        "iceberg.catalog.uri" = "<Lakekeeper Catalog URI, i.e. http://localhost:8181/catalog>",
        "iceberg.catalog.warehouse" = "<Name of the Warehouse in Lakekeeper>",
        -- Required Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.catalog.oauth2-server-uri" = "<Token Endpoint of your IdP, i.e. http://keycloak:8080/realms/iceberg/protocol/openid-connect/token>",
        "iceberg.catalog.credential" = "<Client-ID>:<Client-Secret>",
        -- Optional Parameters if OAuth2 authentication is enabled for Lakekeeper:
        "iceberg.catalog.scope" = "<Scopes to request from the IdP, i.e. lakekeeper>",
        -- S3 specific configuration, probably not required anymore in version 3.4.1 and newer.
        "aws.s3.region" = "<AWS Region to use. For S3-compatible storage use a non-existent AWS region, such as local>",
        "aws.s3.access_key" = "<S3 Access Key>",
        "aws.s3.secret_key" = "<S3 Secret Access Key>",
        -- Required for some S3-compatible storages:
        "aws.s3.endpoint" = "<Custom S3 endpoint>",
        "aws.s3.enable_path_style_access" = "true"
    )
    ```

## <img src="/assets/olake.svg" width="30"> OLake

OLake is an open-source, quick and scalable tool for replicating Databases to Apache Iceberg or Data Lakehouses written in Go. Visit the [Olake Iceberg Documentation](https://olake.io/docs/writers/iceberg/catalog/rest#rest-catalog) for the full documentation, and additional information on Olake.

=== "S3-Compatible"

    ```json
    {
    "type": "ICEBERG",
        "writer": {
            "catalog_type": "rest",
            "normalization": false,
            "rest_catalog_url": "http://localhost:8181/catalog",
            "iceberg_s3_path": "warehouse",
            "iceberg_db": "ICEBERG_DATABASE_NAME"
        }
    }
    ```

