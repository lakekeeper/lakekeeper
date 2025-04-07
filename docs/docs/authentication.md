# Authentication
Authentication is crucial for securing access to Lakekeeper. By enabling authentication, you ensure that only authorized users can access and interact with your data. Lakekeeper supports authentication via any OpenID (or OAuth 2) capable identity provider as well as authentication for Kubernetes service accounts, allowing you to integrate with your existing identity providers.

Authentication and Authorization are distinct processes in Lakekeeper. Authentication verifies the identity of users, ensuring that only authorized individuals can access the system. This is performed via an Identity Provider (IdP) such as OpenID or Kubernetes. Authorization, on the other hand, determines what authenticated users are allowed to do within the system. Lakekeeper is extendable and can connect to different authorization systems. By default, Lakekeeper uses OpenFGA to manage and evaluate permissions, providing a robust and flexible authorization model. For more details, see the [Authorization guide](./authorization.md).

Lakekeeper does not issue API-Keys or Client-Credentials itself. Instead, it relies on external IdPs for authentication, ensuring a secure and centralized management of user identities. This approach minimizes the risk of credential leakage and simplifies the integration with existing security infrastructures.

## OpenID Provider
Lakekeeper can be configured to integrate with all common identity providers. For best performance, tokens are validated locally against the server keys (`jwks_uri`). This requires all incoming tokens to be JWT tokens. If you require support for opaque tokens, please upvote the corresponding [Github Issue](https://github.com/lakekeeper/lakekeeper/issues/620).

If `LAKEKEEPER__OPENID_PROVIDER_URI` is specified, Lakekeeper will  verify access tokens against this provider. The provider must provide the `.well-known/openid-configuration` endpoint and the openid-configuration needs to have `jwks_uri` and `issuer` defined. Optionally, if `LAKEKEEPER__OPENID_AUDIENCE` is specified, Lakekeeper validates the `aud` field of the provided token to match the specified value. We recommend to specify the audience in all deployments, so that tokens leaked for other applications in the same IdP cannot be used to access data in Lakekeeper.

Users are automatically added to Lakekeeper after successful Authentication (user provides a valid token with the correct issuer and audience). If a User does not yet exist in Lakekeeper's Database, the provided JWT token is parsed. The following fields are parsed:

- `name`: `name` or `given_name`/ `first_name` and `family_name`/ `last_name` or `app_displayname` or `preferred_username`
- `subject`: `sub` unless `subject_claim` is set, then it will be the value of the claim.
- `claims`: all claims
- `email`: `email` or `upn` if it contains an `@` or `preferred_username` if it contains an `@`

If the `name` cannot be determined because none of the claims are available, the principal is registered under the name `Nameless App with ID <user-id>`.
Lakekeeper determines the ID of users in the following order:

1. If `LAKEKEEPER__OPENID_SUBJECT_CLAIM` is set, this field is used and must be present.
1. If `oid` is present, it is used. The main motivation to prefer the `oid` over the `sub` is that the `sub` field is not unique across applications, while the `oid` is. (See for example [Entra-ID](https://learn.microsoft.com/en-us/entra/identity-platform/id-token-claims-reference)). Lakekeeper needs to the same IDs as query engines in order to share Permissions.
1. If the `sub` field is present, use it, otherwise fail.

IDs from the OIDC provider in Lakekeeper have the form `oidc~<ID from the provider>`.

### Authenticating Machine Users
All common iceberg clients and IdPs support the OAuth2 `Client-Credential` flow. The `Client-Credential` flow requires a `Client-ID` and `Client-Secret` that is provided in a secure way to the client. In the following sections we demonstrate for selected IdPs how applications can be setup for machine users to connect.

### Authenticating Humans
Human Authentication flows are interactive by nature and are typically performed directly by the IdP. This enables the use of all security options that the IdP supports, including 2FA, hardware keys, single-sign-on and more. The recommended flows for authentication are Authorization Code Flow [RFC6749#section-4.1](https://datatracker.ietf.org/doc/html/rfc6749#section-4.1) with PKCE and Device Code Flow [RFC8628](https://datatracker.ietf.org/doc/html/rfc8628).

At the time of writing all common iceberg clients (spark, trino, starrocks, pyiceberg, ...) do not support any authorization flow that is suitable for human users natively. The iceberg community is working on introducing those flows and we started an [initiative](https://docs.google.com/document/d/1buW9PCNoHPeP7Br5_vZRTU-_3TExwLx6bs075gi94xc/edit?tab=t.0) to standardize and document them as part of the iceberg docs.

Until iceberg clients are natively ready for human flows, authentication flows have to be performed outside of iceberg clients. To make this process as easy as possible, the Lakekeeper UI offers the option to get a new token for a human user:

![](../../assets/generate-token.png)

The lifetime of this token is specified in the corresponding application in your IdP. We recommend to set the lifetime to no longer than one day.

### Keycloak
We are creating two Client: The first client with a "public" profile for the Lakekeeper API & UI and the second client for a machine client (e.g. Spark). Repeat step 2 for each machine client that is needed.

#### Client 1: Lakekeeper

1. Create a new "Client":
    - **Client Type**: choose "OpenID Connect"
    - **Client ID**: choose any, for this example we choose  `lakekeeper`
    - **Name**: choose any, for this example we choose  `Lakekeeper Catalog`
    - **Client authentication**: Leave "Off". We need a public client.
    - **Authentication Flows**: Enable "Standard flow", OAuth 2.0 Device Authorization Grant".
    - **Valid redirect URIs**: For testing a wildcard "*" can be set. Otherwise the URL where the Lakekeeper UI is reachable for the user suffixed by `/callback`. E.g.: `http://localhost:8181/ui/callback`.
1. When the client is created, click on the "Advanced" tab of this client, scroll down to "Advanced settings" and set "Access Token Lifespan" to "Expires in" - 12 Hours.
1. Create a new "Client scope" in the left side menu:
    - **Name**: choose any, for this example we choose  `lakekeeper` 
    - **Description**: `Client of Lakekeeper`
    - **Type**: Optional
1. When the scope is created, we need to add a new mapper. This is recommended because Lakekeeper can validate the `audience` (target service) of the token for increased security. In order to add the `lakekeeper` audience to the token every time the `lakekeeper` scope is requested, we create a new mapper. Select the "Mappers" tab of the previously created `lakekeeper` scope. Select "Configure a new mapper" -> "Audience". ![](../../assets/keycloak-audience-mapper.png)
    - **Name**: choose any, for this example we choose  `Add lakekeeper Audience` 
    - **Included Client Audience**: Select the id of the previously created App 1. In our example this is `lakekeeper`.
    - Make sure `Add to access token` and `Add to token introspection` is enabled.
1. Finally, we need to grant the `spark` client permission to use the `lakekeeper` scope which adds the correct audience to the issued token. Select the "Client scopes" tab of the `lakekeeper` client and select "Add client scope". Select the previously created scope, in our example this is `lakekeeper`. We recommend adding the scope as "Default".

We are now ready to deploy Lakekeeper and login via the UI. Set the following environment variables / configurations:
```sh
LAKEKEEPER__BASE_URI=http://localhost:8181 (URI where lakekeeper is reachable)
LAKEKEEPER__OPENID_PROVIDER_URI=http://localhost:30080/realms/iceberg (URI of the keycloak realm)
LAKEKEEPER__OPENID_AUDIENCE=lakekeeper (ID of Client 1)
LAKEKEEPER__UI__OPENID_CLIENT_ID="lakekeeper" (ID of Client 1)
# LAKEKEEPER__UI__OPENID_SCOPE="lakekeeper" (Name of the created scope, not required if scope was added as default)
```

#### Client 2: Machine User

Repeat this process for each query engine / machine user that is required:

1. Create a new "Client":
    - **Client Type**: choose "OpenID Connect"
    - **Client ID**: choose any, for this example we choose  `spark`.
    - **Name**: choose any, for this example we choose  `Spark Client accessing Lakekeeper`
    - **Client authentication**: Turn "On". Leave "Authorization" turned "Off".
    - **Authentication Flows**: Enable "Service accounts roles".
1. When the client is created, click on "Credentials", choose "Client Authenticator" as "Client Id and Secret". Copy the `Client Secret` for later use.
1. Finally, we need to grant the `spark` client permission to use the `lakekeeper` scope which adds the correct audience to the issued token. Select the "Client scopes" tab of the `spark` client and select "Add client scope". Select the previously created scope, in our example this is `lakekeeper`. We recommend adding the scope as "Optional". By adding an optional scope the client can be re-used for other services, i.e. if Spark needs to access another catalog in the future.

That's it! We can now use the second App Registration to sign into Lakekeeper using Spark or other query engines. A Spark configuration would look like:

=== "PyIceberg"

    ```python
    import pyiceberg.catalog
    import pyiceberg.catalog.rest
    import pyiceberg.typedef

    catalog = pyiceberg.catalog.rest.RestCatalog(
        name="my_catalog_name",
        uri="http://localhost:8181/catalog",
        warehouse="<warehouse name>",
        credential="<Client-ID of Client 2>:<Client-Secret of Client 2>",
        scope="lakekeeper", # Name of the created scope
        **{
            "oauth2-server-uri": "http://localhost:30080/realms/<keycloak realm name>/protocol/openid-connect/token"
        },
    )

    print(catalog.list_namespaces())
    ```

=== "PySpark"

    ```python
    import pyspark

    conf = {
        "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-azure-bundle:1.7.0",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakekeeper.type": "rest",
        "spark.sql.catalog.lakekeeper.uri": "http://localhost:8181/catalog",
        "spark.sql.catalog.lakekeeper.credential": "<Client-ID of Client 2>:<Client-Secret of Client 2>",
        "spark.sql.catalog.lakekeeper.warehouse": "<warehouse name>",
        "spark.sql.catalog.lakekeeper.scope": "lakekeeper", # Name of the created scope
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": "http://localhost:30080/realms/<keycloak realm name>/protocol/openid-connect/token",
    }
    config = pyspark.SparkConf().setMaster("local")

    for k, v in conf.items():
        config = config.set(k, v)

    spark = pyspark.sql.SparkSession.builder.config(conf=config).getOrCreate()

    try:
        spark.sql("USE `lakekeeper`")
    except Exception as e:
        print(e.stackTrace)
        raise e
    spark.sql("CREATE NAMESPACE IF NOT EXISTS `test`")
    spark.sql("CREATE OR REPLACE TABLE `test`.`test_tbl` AS SELECT 1 a")
    ```

If Authorization is enabled, the client will throw an error as no permissions have been granted yet. During this initial connect to the `/config` endpoint of Lakekeeper, the user is automatically provisioned so that it should show up when searching for users in the "Grant" dialog and user search endpoints.

### Entra-ID (Azure)
We are creating three App-Registrations: The first for Lakekeeper itself, the second for the Lakekeeper UI the third for a machine client (e.g. Spark) to access Lakekeeper. Repeat step 3 for each machine client that is needed. While App-Registrations can also be shared, the recommended setup we propose here offers more flexibility and better security.

#### App 1: Lakekeeper UI Application

1. Create a new "App Registration"
    - **Name**: choose any, for this example we choose `Lakekeeper-UI`
    - **Redirect URI**: Add the URL where the Lakekeeper UI is reachable for the user suffixed by `/callback`. E.g.: `http://localhost:8181/ui/callback`. If asked, select type "Single Page Application (SPA)".
1. In the "Overview" page of the "App Registration" note down the `Application (client) ID`. Also note the `Directory (tenant) ID`.
1. Finally we recommend to set a policy for tokens to expire in 12 hours instead of the default ~1 hour. Please follow the [Microsoft Tutorial](https://learn.microsoft.com/en-us/entra/identity-platform/configure-token-lifetimes#create-a-policy-and-assign-it-to-an-app) to assign a corresponding policy to the Application. (If you find a good way to do this via the UI, please let us know so that we can update this documentation page!)

#### App 2: Lakekeeper Application

1. Create a new "App Registration"
    - **Name**: choose any, for this example we choose `Lakekeeper`
    - **Redirect URI**: Leave empty.
1. When the App Registration is created, select "Manage" -> "Expose an API" and on the top select "Add" beside `Application ID URI`. ![](../../assets/idp-azure-application-id-uri.png) Note down the `Application ID URI` (should be `api://<Client ID>`).
1. Still in the "Expose an API" menus, select "Add a Scope". Fill the fields as follows:
    - **Scope name**: lakekeeper
    - **Who can consent?** Admins and users
    - **Admin consent display name**: Lakekeeper API
    - **Admin consent description**: Access Lakekeeper API
    - **State**: Enabled
1. After the `lakekeeper` scope is created, click "Add a client application" under the "Authorized client applications" headline. Select the previously created scope and paste as `Client ID` the previously noted ID from App 1.
1. In the "Overview" page of the "App Registration" note down the `Application (client) ID`.

We are now ready to deploy Lakekeeper and login via the UI. Set the following environment variables / configurations:
```bash
LAKEKEEPER__BASE_URI=http://localhost:8181 (URI where lakekeeper is reachable)
// Note the v2.0 at the End of the provider URI!
LAKEKEEPER__OPENID_PROVIDER_URI=https://login.microsoftonline.com/<Tenant ID>/v2.0
LAKEKEEPER__OPENID_AUDIENCE="api://<Client ID from App 2 (lakekeeper)>"
LAKEKEEPER__UI__OPENID_CLIENT_ID="<Client ID from App 1 (lakekeeper-ui)>"
LAKEKEEPER__UI__OPENID_SCOPE="openid profile api://<Client ID from App 2>/lakekeeper"
LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS="https://sts.windows.net/<Tenant ID>/"
// The additional issuer URL is required as https://login.microsoftonline.com/<Tenant ID>/v2.0/.well-known/openid-configuration
// shows https://login.microsoftonline.com as the issuer but actually
// issues tokens for https://sts.windows.net/. This is a well-known
// problem in Entra ID.
```

Before continuing with App 2, we recommend to create a Warehouse using any of the supported storages. Please check the [Storage Documentation](./storage.md) for more information. Without a Warehouse, we won't be able to test App 3.

#### App 3: Machine User
Repeat this process for each query engine / machine user that is required:

1. Create a new "App Registration"
    - **Name**: choose any, for this example we choose `Spark`
    - **Redirect URI**: Leave empty - we are going to use the Client Credential Flow
2. When the App Registration is created, select "Manage" -> "Certificates & secrets" and create a "New client secret". Note down the secrets "Value".

That's it! We can now use the second App Registration to sign into Lakekeeper using Spark or other query engines. A Spark configuration would look like:

=== "PyIceberg"

    ```python
    import pyiceberg.catalog
    import pyiceberg.catalog.rest
    import pyiceberg.typedef

    catalog = pyiceberg.catalog.rest.RestCatalog(
        name="my_catalog_name",
        uri="http://localhost:8181/catalog",
        warehouse="<warehouse name>",
        credential="<Client-ID of App 3 (spark)>:<Client-Secret of App 3 (spark)>",
        scope="email openid api://<Client-ID of App 2 (lakekeeper)>/.default",
        **{
            "oauth2-server-uri": "https://login.microsoftonline.com/<Tenant ID>/oauth2/v2.0/token"
        },
    )

    print(catalog.list_namespaces())
    ```

=== "PySpark"

    ```python
    import pyspark

    conf = {
        "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-azure-bundle:1.7.0",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.azure-docs": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.azure-docs.type": "rest",
        "spark.sql.catalog.azure-docs.uri": "http://localhost:8181/catalog",
        "spark.sql.catalog.azure-docs.credential": "<Client-ID of App 3 (spark)>:<Client-Secret of App 3 (spark)>",
        "spark.sql.catalog.azure-docs.warehouse": "<warehouse name>",
        "spark.sql.catalog.azure-docs.scope": "email openid api://<Client-ID of App 2 (lakekeeper)>/.default",
        "spark.sql.catalog.azure-docs.oauth2-server-uri": "https://login.microsoftonline.com/<Tenant ID>/oauth2/v2.0/token",
    }
    config = pyspark.SparkConf().setMaster("local")

    for k, v in conf.items():
        config = config.set(k, v)

    spark = pyspark.sql.SparkSession.builder.config(conf=config).getOrCreate()

    try:
        spark.sql("USE `azure-docs`")
    except Exception as e:
        print(e.stackTrace)
        raise e
    spark.sql("CREATE NAMESPACE IF NOT EXISTS `test`")
    spark.sql("CREATE OR REPLACE TABLE `test`.`test_tbl` AS SELECT 1 a")
    ```

If Authorization is enabled, the client will throw an error as no permissions have been granted yet. During this initial connect to the `/config` endpoint of Lakekeeper, the user is automatically provisioned so that it should show up when searching for users in the "Grant" dialog and user search endpoints. While we try to extract the name of the application from its token, this might not be possible in all setups. As a fallback we use the `Client ID` as the name of the user. Once permissions have been granted, the user is able to perform actions.


## Kubernetes
If `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is set to true, Lakekeeper validates incoming tokens against the default kubernetes context of the system. Lakekeeper uses the [`TokenReview`](https://kubernetes.io/docs/reference/kubernetes-api/authentication-resources/token-review-v1/) to determine the validity of a token. By default the `TokenReview` resource is protected. When deploying Lakekeeper on Kubernetes, make sure to grant the `system:auth-delegator` Cluster Role to the service account used by Lakekeeper:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: allow-token-review
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:auth-delegator
subjects:
- kind: ServiceAccount
  name: <lakekeeper-serviceaccount>
  namespace: <lakekeeper-namespace>
```
The [Lakekeeper Helm Chart](https://github.com/lakekeeper/lakekeeper-charts/tree/main/charts/lakekeeper) creates the required binding by default.
