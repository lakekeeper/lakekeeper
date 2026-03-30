# Configuration

Lakekeeper is configured via environment variables. Settings listed in this page are shared between all projects and warehouses. Previous to Lakekeeper Version `0.5.0` please prefix all environment variables with `ICEBERG_REST__` instead of `LAKEKEEPER__`.

For most deployments, we recommend to set at least the following variables: `LAKEKEEPER__PG_DATABASE_URL_READ`, `LAKEKEEPER__PG_DATABASE_URL_WRITE`, `LAKEKEEPER__PG_ENCRYPTION_KEY`.

## Routing and Base-URL

Some Lakekeeper endpoints return links pointing at Lakekeeper itself. By default, these links are generated using the `x-forwarded-host`, `x-forwarded-proto`, `x-forwarded-port` and `x-forwarded-prefix` headers, if these are not present, the `host` header is used. If this is not working for you, you may set the `LAKEKEEPER_BASE_URI` environment variable to the base-URL where Lakekeeper is externally reachable. This may be necessary if Lakekeeper runs behind a reverse proxy or load balancer, and you cannot set the headers accordingly. In general, we recommend relying on the headers. To respect the `host` header but not the `x-forwarded-` headers, set `LAKEKEEPER__USE_X_FORWARDED_HEADERS` to `false`.

### General

| Variable                                           | Example                                | Description |
|----------------------------------------------------|----------------------------------------|-----|
| <nobr>`LAKEKEEPER__BASE_URI`</nobr>                | <nobr>`https://example.com:8181`<nobr> | Optional base-URL where the catalog is externally reachable. Default: `None`. See [Routing and Base-URL](#routing-and-base-url). |
| <nobr>`LAKEKEEPER__ENABLE_DEFAULT_PROJECT`<nobr>   | `true`                                 | If `true`, the NIL Project ID ("00000000-0000-0000-0000-000000000000") is used as a default if the user does not specify a project when connecting. This option is enabled by default, which we recommend for all single-project (single-tenant) setups. Default: `true`. |
| `LAKEKEEPER__RESERVED_NAMESPACES`                  | `system,examples,information_schema`   | Reserved Namespaces that cannot be created via the REST interface |
| `LAKEKEEPER__METRICS_PORT`                         | `9000`                                 | Port where the Prometheus metrics endpoint is reachable. Default: `9000` |
| `LAKEKEEPER__LISTEN_PORT`                          | `8181`                                 | Port Lakekeeper listens on. Default: `8181` |
| `LAKEKEEPER__BIND_IP`                              | `0.0.0.0`, `::1`, `::`                 | IP Address Lakekeeper binds to. Default: `0.0.0.0` (listen to all incoming IPv4 packages) |
| `LAKEKEEPER__SECRET_BACKEND`                       | `postgres`                             | The secret backend to use. If `kv2` (Hashicorp KV Version 2) is chosen, you need to provide [additional parameters](#vault-kv-version-2) Default: `postgres`, one-of: [`postgres`, `kv2`] |
| `LAKEKEEPER__SERVE_SWAGGER_UI`                     | `true`                                 | If `true`, Lakekeeper serves a swagger UI for management & catalog openAPI specs under `/swagger-ui` |
| `LAKEKEEPER__ALLOW_ORIGIN`                         | `*`                                    | A comma separated list of allowed origins for CORS. |
| <nobr>`LAKEKEEPER__USE_X_FORWARDED_HEADERS`</nobr> | <nobr>`false`<nobr>                    | If true, Lakekeeper respects the `x-forwarded-host`, `x-forwarded-proto`, `x-forwarded-port` and `x-forwarded-prefix` headers in incoming requests. This is mostly relevant for the `/config` endpoint. Default: `true` (Headers are respected.) |

### Pagination

Lakekeeper has default values for `default` and `max` page sizes of paginated queries. These are safeguards against malicious requests and the problems related to large page sizes described below.

The REST catalog [spec](https://github.com/apache/iceberg/blob/404c8057275c9cfe204f2c7cc61114c128fbf759/open-api/rest-catalog-open-api.yaml#L2030-L2032) requires servers to return *all* results if `pageToken` is not set in the request. To obtain that behavior, set `LAKEKEEPER__PAGINATION_SIZE_MAX` to 4294967295, which corresponds to `u32::MAX`. Larger page sizes would lead to practical problems. Things to keep in mind:

- Retrieving huge numbers of rows is expensive, which might be exploited by malicious requests.
- Requests may time out or responses may exceed size limits for huge numbers of results.

| Variable                                          | Example            | Description |
|---------------------------------------------------|--------------------|-----|
| <nobr>`LAKEKEEPER__PAGINATION_SIZE_DEFAULT`<nobr> | <nobr>`1024`<nobr> | The default page size used for paginated queries. This value is used if the request's `pageToken` is set but empty. Default: `100` |
| <nobr>`LAKEKEEPER__PAGINATION_SIZE_MAX`<nobr>     | <nobr>`2048`<nobr> | The max page size used for paginated queries. This value is used if the request's `pageToken` is not set. Default: `1000` |

### Storage

| Variable                                                    | Example            | Description |
|-------------------------------------------------------------|--------------------|-----|
| <nobr>`LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS`<nobr>     | <nobr>`true`<nobr> | Lakekeeper supports using AWS system identities (i.e. through `AWS_*` environment variables or EC2 instance profiles) as storage credentials for warehouses. This feature is disabled by default to prevent accidental access to restricted storage locations. To enable AWS system identities, set `LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS` to `true`. Default: `false` (AWS system credentials disabled) |
| `LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS`           | <nobr>`true`<nobr> | By default, when using AWS system credentials, users must specify an `assume-role-arn` for Lakekeeper to assume when accessing S3. Setting this option to `true` allows Lakekeeper to use system credentials directly without role assumption, meaning the system identity must have direct access to warehouse locations. Default: `false` (direct system credential access disabled) |
| `LAKEKEEPER__S3_REQUIRE_EXTERNAL_ID_FOR_SYSTEM_CREDENTIALS` | <nobr>`true`<nobr> | Controls whether an `external-id` is required when assuming a role with AWS system credentials. External IDs provide additional security when cross-account role assumption is used. Default: true (external ID required) |
| <nobr>`LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS`<nobr>   | <nobr>`true`<nobr> | Lakekeeper supports using Azure system identities (i.e. through `AZURE_*` environment variables or VM managed identities) as storage credentials for warehouses. This feature is disabled by default to prevent accidental access to restricted storage locations. To enable Azure system identities, set `LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS` to `true`. Default: `false` (Azure system credentials disabled) |
| `LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS`                 | <nobr>`true`<nobr> | Lakekeeper supports using GCP system identities (i.e. through `GOOGLE_APPLICATION_CREDENTIALS` environment variables or the Compute Engine Metadata Server) as storage credentials for warehouses. This feature is disabled by default to prevent accidental access to restricted storage locations. To enable GCP system identities, set `LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS` to `true`. Default: `false` (GCP system credentials disabled) |

### Persistence Store

Currently Lakekeeper supports only Postgres as a persistence store. You may either provide connection strings using `PG_DATABASE_URL_*` or use the `PG_*` environment variables. Connection strings take precedence. Postgres needs to be Version 15 or higher.

Lakekeeper supports configuring separate database URLs for read and write operations, allowing you to utilize read replicas for better scalability. By directing read queries to dedicated replicas via `LAKEKEEPER__PG_DATABASE_URL_READ`, you can significantly reduce load on your database primary (specified by `LAKEKEEPER__PG_DATABASE_URL_WRITE`), improving overall system performance as your deployment scales. This separation is particularly beneficial for read-heavy workloads. When using read replicas, be aware that replication lag may occur between the primary and replica databases depending on your Database setup. This means that immediately after a write operation, the changes might not be instantly visible when querying a read-only Lakekeeper endpoint (which uses the read replica). Consider this potential lag when designing applications that require immediate read-after-write consistency. For deployments where read-after-write consistency is critical, you can simply omit the `LAKEKEEPER__PG_DATABASE_URL_READ` setting, which will cause all operations to use the primary database connection.

| Variable                                               | Example                                               | Description |
|--------------------------------------------------------|-------------------------------------------------------|-----|
| `LAKEKEEPER__PG_DATABASE_URL_READ`                     | `postgres://postgres:password@localhost:5432/iceberg` | Postgres Database connection string used for reading. Defaults to `LAKEKEEPER__PG_DATABASE_URL_WRITE`. |
| `LAKEKEEPER__PG_DATABASE_URL_WRITE`                    | `postgres://postgres:password@localhost:5432/iceberg` | Postgres Database connection string used for writing. If `LAKEKEEPER__PG_DATABASE_URL_READ` is not specified, this connection is also used for reading. |
| `LAKEKEEPER__PG_ENCRYPTION_KEY`                        | `This is unsafe, please set a proper key`             | If `LAKEKEEPER__SECRET_BACKEND=postgres`, this key is used to encrypt secrets. It is required to change this for production deployments. |
| `LAKEKEEPER__PG_READ_POOL_CONNECTIONS`                 | `10`                                                  | Number of connections in the read pool |
| `LAKEKEEPER__PG_WRITE_POOL_CONNECTIONS`                | `5`                                                   | Number of connections in the write pool |
| `LAKEKEEPER__PG_HOST_R`                                | `localhost`                                           | Hostname for read operations. Defaults to `LAKEKEEPER__PG_HOST_W`. |
| `LAKEKEEPER__PG_HOST_W`                                | `localhost`                                           | Hostname for write operations |
| `LAKEKEEPER__PG_PORT`                                  | `5432`                                                | Port number |
| `LAKEKEEPER__PG_USER`                                  | `postgres`                                            | Username for authentication |
| `LAKEKEEPER__PG_PASSWORD`                              | `password`                                            | Password for authentication |
| `LAKEKEEPER__PG_DATABASE`                              | `iceberg`                                             | Database name |
| `LAKEKEEPER__PG_SSL_MODE`                              | `require`                                             | SSL mode (disable, allow, prefer, require) |
| `LAKEKEEPER__PG_SSL_ROOT_CERT`                         | `/path/to/root/cert`                                  | Path to SSL root certificate |
| <nobr>`LAKEKEEPER__PG_ENABLE_STATEMENT_LOGGING`</nobr> | `true`                                                | Enable SQL statement logging |
| `LAKEKEEPER__PG_TEST_BEFORE_ACQUIRE`                   | `true`                                                | Test connections before acquiring from the pool |
| `LAKEKEEPER__PG_CONNECTION_MAX_LIFETIME`               | `1800`                                                | Maximum lifetime of connections in seconds |
| `LAKEKEEPER__PG_ACQUIRE_TIMEOUT`                       | `10`                                                  | Timeout to acquire a new postgres connection in seconds. Default: `5` |

### Vault KV Version 2

Configuration parameters if a Vault KV version 2 (i.e. Hashicorp Vault) compatible storage is used as a backend. Currently, we only support the `userpass` authentication method. Configuration may be passed as single values like `LAKEKEEPER__KV2__URL=http://vault.local` or as a compound value:
`LAKEKEEPER__KV2='{url="http://localhost:1234", user="test", password="test", secret_mount="secret"}'`

| Variable                                     | Example               | Description |
|----------------------------------------------|-----------------------|-------|
| `LAKEKEEPER__KV2__URL`                       | `https://vault.local` | URL of the KV2 backend |
| `LAKEKEEPER__KV2__USER`                      | `admin`               | Username to authenticate against the KV2 backend |
| `LAKEKEEPER__KV2__PASSWORD`                  | `password`            | Password to authenticate against the KV2 backend |
| <nobr>`LAKEKEEPER__KV2__SECRET_MOUNT`</nobr> | `kv/data/iceberg`     | Path to the secret mount in the KV2 backend |


### Task Queues

Lakekeeper uses task queues internally to remove soft-deleted tabulars and purge tabular files. The following global configuration options are available:

| Variable                                                                          | Example    | Description |
|-----------------------------------------------------------------------------------|------------|-----|
| <nobr>`LAKEKEEPER__TASK_POLL_INTERVAL`</nobr>                                     | 3600ms/30s | Interval between polling for new tasks. Default: 10s. Supported units: ms (milliseconds) and s (seconds), leaving the unit out is deprecated, it'll default to seconds but is due to be removed in a future release. |
| `LAKEKEEPER__TASK_TABULAR_EXPIRATION_WORKERS`                                     | 2          | Number of workers spawned to expire soft-deleted tables and views. |
| `LAKEKEEPER__TASK_TABULAR_PURGE_WORKERS`                                          | 2          | Number of workers spawned to purge table files after dropping a table with the purge option. |
| <nobr>`LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS`</nobr><span class="lkp"></span> | 2          | Number of workers spawned that work on expire Snapshots tasks. See [Expire Snapshots Docs](./table-maintenance.md#expire-snapshots) for more information. |

### NATS

Lakekeeper can publish change events to NATS. The following configuration options are available:

| Variable                                   | Example                 | Description |
|--------------------------------------------|-------------------------|-------|
| `LAKEKEEPER__NATS_ADDRESS`                 | `nats://localhost:4222` | The URL of the NATS server to connect to |
| `LAKEKEEPER__NATS_TOPIC`                   | `iceberg`               | The subject to publish events to |
| `LAKEKEEPER__NATS_USER`                    | `test-user`             | User to authenticate against NATS, needs `LAKEKEEPER__NATS_PASSWORD` |
| `LAKEKEEPER__NATS_PASSWORD`                | `test-password`         | Password to authenticate against nats, needs `LAKEKEEPER__NATS_USER` |
| <nobr>`LAKEKEEPER__NATS_CREDS_FILE`</nobr> | `/path/to/file.creds`   | Path to a file containing NATS credentials |
| `LAKEKEEPER__NATS_TOKEN`                   | `xyz`                   | NATS token to use for authentication |

### Kafka

Lakekeeper uses [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) to enable publishing events to Kafka.

The following features of rust-rdkafka are enabled:

- tokio
- ztstd
- gssapi-vendored
- curl-static
- ssl-vendored
- libz-static

This means that all features of [librdkafka](https://github.com/confluentinc/librdkafka) are usable. All necessary dependencies are statically linked and cannot be disabled. If you want to use dynamic linking or disable a feature, you'll have to fork Lakekeeper and change the features accordingly. Please refer to the documentation of rust-rdkafka for details on how to enable dynamic linking or disable certain features.

To publish events to Kafka, set the following environment variables:

| Variable                                     | Example                                                                   | Description |
|----------------------------------------------|---------------------------------------------------------------------------|-----|
| `LAKEKEEPER__KAFKA_TOPIC`                    | `lakekeeper`                                                              | The topic to which events are published |
| `LAKEKEEPER__KAFKA_CONFIG`                   | `{"bootstrap.servers"="host1:port,host2:port","security.protocol"="SSL"}` | [librdkafka Configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) as "Dictionary". Note that you cannot use "JSON-Style-Syntax". Also see notes below |
| <nobr>`LAKEKEEPER__KAFKA_CONFIG_FILE`</nobr> | `/path/to/config_file`                                                    | [librdkafka Configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) to be loaded from a file. Also see notes below |

##### Notes

`LAKEKEEPER__KAFKA_CONFIG` and `LAKEKEEPER__KAFKA_CONFIG_FILE` are mutually exclusive and the values are not merged, if both variables are set. In case that both are set, `LAKEKEEPER__KAFKA_CONFIG` is used.

A `LAKEKEEPER__KAFKA_CONFIG_FILE` could look like this:

```
{
  "bootstrap.servers"="host1:port,host2:port",
  "security.protocol"="SASL_SSL",
  "sasl.mechanisms"="PLAIN",
}
```

Checking configuration parameters is deferred to `rdkafka`



### Logging Cloudevents

Cloudevents can also be logged, if you do not have Nats up and running. This feature can be enabled by setting
Cloudevents can also be logged, if you do not have Nats or Kafka up and running. This feature can be enabled by setting

`LAKEKEEPER__LOG_CLOUDEVENTS=true`

### Authentication

To prohibit unwanted access to data, we recommend to enable Authentication.

Authentication is enabled if:

* `LAKEKEEPER__OPENID_PROVIDER_URI` is set OR
* `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is set to true

In Lakekeeper multiple Authentication mechanisms can be enabled together, for example OpenID + Kubernetes. Lakekeeper builds an internal Authenticator chain of up to three identity providers. Incoming tokens need to be JWT tokens - Opaque tokens are not yet supported. Incoming tokens are introspected, and each Authentication provider checks if the given token can be handled by this provider. If it can be handled, the token is authenticated against this provider, otherwise the next Authenticator in the chain is checked.

The following Authenticators are available. Enabled Authenticators are checked in order:

1. **OpenID / OAuth2**<br>
   **Enabled if:** `LAKEKEEPER__OPENID_PROVIDER_URI` is set<br>
    **Validates Token with:** Locally with JWKS Keys fetched from the well-known configuration.<br>
   **Accepts JWT if** (both must be true):<br>
    - Issuer matches the issuer provided in the `.well-known/openid-configuration` of the `LAKEKEEPER__OPENID_PROVIDER_URI` OR issuer matches any of the `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS`.<br>
    - If `LAKEKEEPER__OPENID_AUDIENCE` is specified, any of the configured audiences must be present in the token<br>
1. **Kubernetes**<br>
   **Enabled if:** `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is true<br>
   **Validates Token with:** Kubernetes `TokenReview` API
   **Accepts JWT if:**<br>
    - Token audience matches any of the audiences provided in `LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE`<br>
    - If `LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE` is not set, all tokens proceed to validation! We highly recommend to configure audiences, for most deployments `https://kubernetes.default.svc` works.<br>
1. **Kubernetes Legacy Tokens**<br>
   **Enabled if:** `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is true and `LAKEKEEPER__KUBERNETES_AUTHENTICATION_ACCEPT_LEGACY_SERVICEACCOUNT` is true<br>
   **Validates Token with:** Kubernetes `TokenReview` API<br>
   **Accepts JWT if:**<br>
    - Tokens issuer is `kubernetes/serviceaccount` or `https://kubernetes.default.svc.cluster.local`

If `LAKEKEEPER__OPENID_PROVIDER_URI` is specified, Lakekeeper will  verify access tokens against this provider. The provider must provide the `.well-known/openid-configuration` endpoint and the openid-configuration needs to have `jwks_uri` and `issuer` defined.

Typical values for `LAKEKEEPER__OPENID_PROVIDER_URI` are:

* Keycloak: `https://keycloak.local/realms/{your-realm}`
* Entra-ID: `https://login.microsoftonline.com/{your-tenant-id-here}/v2.0/`

Please check the [Authentication Guide](./authentication.md) for more details.

| Variable                                                                  | Example                                      | Description |
|---------------------------------------------------------------------------|----------------------------------------------|-----|
| <nobr>`LAKEKEEPER__OPENID_PROVIDER_URI`</nobr>                            | `https://keycloak.local/realms/{your-realm}` | OpenID Provider URL. Lakekeeper expects to find `<LAKEKEEPER__OPENID_PROVIDER_URI>/.well-known/openid-configuration` and load JWKS tokens from there. Do not include the `/.well-known/openid-configuration` in the provided URL. |
| `LAKEKEEPER__OPENID_AUDIENCE`                                             | `the-client-id-of-my-app`                    | If set, the `aud` of the provided token must match the value provided. Multiple allowed audiences can be provided as a comma separated list. |
| `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS`                                   | `https://sts.windows.net/<Tenant>/`          | A comma separated list of additional issuers to trust. The issuer defined in the `issuer` field of the `.well-known/openid-configuration` is always trusted. `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS` has no effect if `LAKEKEEPER__OPENID_PROVIDER_URI` is not set. |
| `LAKEKEEPER__OPENID_SCOPE`                                                | `lakekeeper`                                 | Specify a scope that must be present in provided tokens received from the openid provider. |
| `LAKEKEEPER__OPENID_SUBJECT_CLAIM`                                        | `sub` or `oid,sub`                           | Specify the claim(s) in the user's JWT used to identify a User. Accepts a single claim name or a comma-separated list of claim names; the first claim present in the token is used. By default Lakekeeper tries `oid` first, then falls back to `sub`. We strongly recommend setting this configuration explicitly in production deployments. Entra-ID users want to use `oid`; users from all other IdPs most likely want to use `sub`. |
| `LAKEKEEPER__OPENID_ROLES_CLAIM`                                          | `resource_access.lakekeeper.roles`           | Specify the claim to use in provided JWT tokens to extract roles. The field should contain an array of strings or a single string. Supports nested claims using dot notation, e.g., "resource_access.account.roles". Currently only has an effect when using the Cedar Authorizer. Requires a project ID to be set via the `x-project-id` header or `LAKEKEEPER__DEFAULT_PROJECT_ID`. |
| `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION`                            | true                                         | If true, kubernetes service accounts can authenticate to Lakekeeper. This option is compatible with `LAKEKEEPER__OPENID_PROVIDER_URI` - multiple IdPs (OIDC and Kubernetes) can be enabled simultaneously. |
| `LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE`                          | `https://kubernetes.default.svc`             | Audiences that are expected in Kubernetes tokens. Only has an effect if `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is true. |
| `LAKEKEEPER_TEST__KUBERNETES_AUTHENTICATION_ACCEPT_LEGACY_SERVICEACCOUNT` | `false`                                      | Add an authenticator that handles tokens with no audiences and the issuer set to `kubernetes/serviceaccount`. Only has an effect if `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is true. |


### Authorization
Authorization is only effective if [Authentication](#authentication) is enabled. Authorization must not be enabled after Lakekeeper has been bootstrapped! Please create a new Lakekeeper instance, bootstrap it with authorization enabled, and migrate your tables.

| Variable                                 | Example    | Description          |
|------------------------------------------|------------|----------------------|
| <nobr>`LAKEKEEPER__AUTHZ_BACKEND`</nobr> | `allowall` | The authorization backend to use. If `openfga` or `cedar` is chosen, additional parameters are required (see below). The `allowall` backend disables authorization - authenticated users can access all endpoints. Default: `allowall`, one-of: [`openfga`, `allowall`, `cedar`] |

##### OpenFGA
| Variable                                                 | Example                                                                    | Description |
|----------------------------------------------------------|----------------------------------------------------------------------------|-----|
| <nobr>`LAKEKEEPER__OPENFGA__ENDPOINT`</nobr>             | `http://localhost:35081`                                                   | OpenFGA Endpoint (gRPC). |
| `LAKEKEEPER__OPENFGA__STORE_NAME`                        | `lakekeeper`                                                               | The OpenFGA Store to use. Default: `lakekeeper` |
| `LAKEKEEPER__OPENFGA__API_KEY`                           | `my-api-key`                                                               | The API Key used for [Pre-shared key authentication](https://openfga.dev/docs/getting-started/setup-openfga/configure-openfga#pre-shared-key-authentication) to OpenFGA. If `LAKEKEEPER__OPENFGA__CLIENT_ID` is set, the API Key is ignored. If neither API Key nor Client ID is specified, no authentication is used. |
| <nobr>`LAKEKEEPER__OPENFGA__CLIENT_ID`</nobr>            | `12345`                                                                    | The Client ID to use for Authenticating if OpenFGA is secured via [OIDC](https://openfga.dev/docs/getting-started/setup-openfga/configure-openfga#oidc). |
| `LAKEKEEPER__OPENFGA__CLIENT_SECRET`                     | `abcd`                                                                     | Client Secret for the Client ID. |
| `LAKEKEEPER__OPENFGA__TOKEN_ENDPOINT`                    | `https://keycloak.example.com/realms/master/protocol/openid-connect/token` | Token Endpoint to use when exchanging client credentials for an access token for OpenFGA. Required if Client ID is set |
| `LAKEKEEPER__OPENFGA__SCOPE`                             | `openfga`                                                                  | Additional scopes to request in the Client Credential flow. |
| `LAKEKEEPER__OPENFGA__AUTHORIZATION_MODEL_PREFIX`        | `collaboration`                                                            | Explicitly set the Authorization model prefix. Defaults to `collaboration` if not set. We recommend to use this setting only in combination with `LAKEKEEPER__OPENFGA__AUTHORIZATION_MODEL_PREFIX`. |
| `LAKEKEEPER__OPENFGA__AUTHORIZATION_MODEL_VERSION`       | `3.1`                                                                      | Version of the model to use. If specified, the specified model version must already exist. This can be used to roll-back to previously applied model versions or to connect to externally managed models. Migration is disabled if the model version is set. Version should have the format <major>.<minor>. |
| <nobr>`LAKEKEEPER__OPENFGA__MAX_BATCH_CHECK_SIZE`</nobr> | `50`                                                                       | p The maximum number of checks than can be handled by a batch check request. This is a [configuration option](https://openfga.dev/docs/getting-started/setup-openfga/configuration#OPENFGA_MAX_CHECKS_PER_BATCH_CHECK) of the `OpenFGA` server with default value 50. |


##### Cedar <span class="lkp"></span>

Please check the [Authorization User Guide](./authorization.md#authorization-with-cedar) for more information on Cedar.

| Variable                                               | Example                                                 | Description |
|--------------------------------------------------------|---------------------------------------------------------|-----|
| `LAKEKEEPER__CEDAR__POLICY_SOURCES__LOCAL_FILES`       | `[/path/to/policies1.cedar,/path/to/policies2.cedar]`   | List of local file paths containing Cedar policies in Cedar format (not JSON). |
| `LAKEKEEPER__CEDAR__ENTITY_JSON_SOURCES__LOCAL_FILES`  | `[/path/to/entities1.json,/path/to/entities2.json]`     | List of local JSON file paths containing additional Cedar entities (typically roles). |
| `LAKEKEEPER__CEDAR__POLICY_SOURCES__K8S_CM`            | `[my-cm-1, my-cm-2]`                                    | List of Kubernetes ConfigMap names in the same namespace as Lakekeeper. Every key ending with `.cedar` is treated as a policy source in Cedar format (not JSON). |
| `LAKEKEEPER__CEDAR__ENTITY_JSON_SOURCES__K8S_CM`       | `[my-cm-1, my-cm-2]`                                    | List of Kubernetes ConfigMap names in the same namespace as Lakekeeper. Every key ending with `.cedarentities.json` is treated as an entity source. |
| `LAKEKEEPER__CEDAR__REFRESH_INTERVAL_SECS`             | `5`                                                     | Refresh interval in seconds for reloading policies and entities from Kubernetes ConfigMaps and local files. Default: `5` seconds. See [Cedar Authorization](./authorization.md#authorization-with-cedar) for more information. |
| <nobr>`LAKEKEEPER__CEDAR__REFRESH_DISABLED`</nobr>     | `false`                                                 | When set to `true`, disables periodic reloading of policies and entities entirely. Useful in environments where Cedar configuration is known to be static and the polling overhead is undesirable. Default: `false`. |
| `LAKEKEEPER__CEDAR__EXTERNALLY_MANAGED_USER_AND_ROLES` | `false`                                                 | When set to `true`, Lakekeeper expects all roles and users to be managed externally via entities.json and does not extract `Lakekeeper::Role` or `Lakekeeper::User` entities from the user's token. When set to `false` (default), Lakekeeper automatically provides `Lakekeeper::Role` and `Lakekeeper::User` entities to Cedar based on information extracted from the user's token. When set to `false`, ensure `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured to specify which claim in the token contains role information. |
| <nobr>`LAKEKEEPER__CEDAR__SCHEMA_FILE`</nobr>          | `/path/to/custom/schema.cedarschema`                    | Path to a custom Cedar schema file that replaces the embedded default schema entirely. Use this only when you need complete control over the schema definition. Your custom schema must maintain compatibility with all Lakekeeper-provided entities (Server, Project, Warehouse, Namespace, Table, View, and optionally User & Role). For most use cases, prefer `LAKEKEEPER__CEDAR__SCHEMA_FRAGMENT_FILE` to extend the built-in schema. |
| `LAKEKEEPER__CEDAR__SCHEMA_FRAGMENT_FILE`              | `/path/to/schema-fragment.cedarschema`                  | Path to a Cedar schema fragment file that extends the embedded default schema. This is the recommended approach for adding custom entity types or grouped actions while preserving compatibility with Lakekeeper's built-in schema. The fragment is merged with the default schema at startup. |
| `LAKEKEEPER__CEDAR__PROPERTY_PARSE_PREFIXES`           | `["access_", "access-"]`                                | List of property key prefixes that trigger entity-reference parsing for ABAC. Table, Namespace, and View properties whose key starts with one of these prefixes are parsed as JSON arrays of `role:` / `role-full:` / `user:` references. Parsed values are exposed in Cedar as `roles: Set<Role>` and `users: Set<User>` on each `ResourcePropertyValue`. Set to `[]` to disable parsing entirely. Default: `["access_", "access-"]`. See [Property-Based Access Control](./authorization.md#property-based-access-control). |
| `LAKEKEEPER__CEDAR__GLOBAL_ROLE_IDS_ENABLED`           | `false`                                                 | When `true`, the `global_role_ids: Set<String>` attribute on every `Lakekeeper::User` entity is populated with the `source_id` of every provider-resolved role (token claims, LDAP, etc.). This enables simpler policies such as `principal.global_role_ids.contains("admins")` without needing to specify a `provider_id`. Only meaningful when all configured role providers use globally unique `source_id` values (i.e. no two providers assign the same `source_id` to different roles). When `false` (default), `global_role_ids` is always an empty set. |
| `LAKEKEEPER__CEDAR__USER_DERIVATIONS__<NAME>__SOURCE`  | `source_id`                                             | Source field for a user identity derivation rule. Supported values: `source_id` (the user's subject in the IdP) or `provider_id` (e.g. `oidc`, `kubernetes`). `<NAME>` is a human-readable key (e.g. `EMAIL_PARTS`) used in error messages. See [User Identity Derivations](./authorization.md#user-identity-derivations). |
| `LAKEKEEPER__CEDAR__USER_DERIVATIONS__<NAME>__PATTERN` | <nobr>`^(?<username>[^@]+)`<br>`@(?<domain>.+)$`</nobr> | Regex pattern with named capture groups for a user identity derivation rule. Each named group that matches a non-empty substring becomes a string tag on the `UserDerivedAttributes` entity, accessible in policies via `principal.derived_attributes.hasTag("…")` / `principal.derived_attributes.getTag("…")`. Invalid patterns cause a startup error. See [User Identity Derivations](./authorization.md#user-identity-derivations). |
| `LAKEKEEPER__CEDAR__USER_DERIVATIONS__<NAME>__TRANSFORM` | `lowercase` | Optional transformation applied to all captured values before they become Cedar tags. Supported values: `none` (default — keep as-is), `lowercase`, `uppercase`. Because Cedar string comparison is case-sensitive, use `lowercase` to normalize captured values so policies can compare against a known-case literal (e.g. `getTag("domain") == "example.com"`). If different capture groups need different transforms, use separate derivation entries with distinct regexes. See [User Identity Derivations](./authorization.md#user-identity-derivations). |

**Debug configurations for Cedar**

| Variable                                              | Example | Description |
|-------------------------------------------------------|---------|------------|
| <nobr>`LAKEKEEPER__CEDAR__DEBUG__LOG_ENTITIES`</nobr> | `false` | If `true`, logs all internal entities (excluding externally managed entities) for each authorization request at debug level. This is useful for debugging authorization issues but can be verbose and impacts performance. Logging only occurs when both this flag is `true` AND debug logging is enabled (`RUST_LOG=debug`). Default: `false`. |


### UI

When using the built-in UI which is hosted as part of the Lakekeeper binary, most values are pre-set with the corresponding values of Lakekeeper itself. Customization is typically required if Authentication is enabled. Please check the [Authentication guide](./authentication.md) for more information.

| Variable                                           | Example                                      | Description |
|----------------------------------------------------|----------------------------------------------|-----|
| <nobr>`LAKEKEEPER__UI__OPENID_PROVIDER_URI`</nobr> | `https://keycloak.local/realms/{your-realm}` | OpenID provider URI used for login in the UI. Defaults to `LAKEKEEPER__OPENID_PROVIDER_URI`. Set this only if the IdP is reachable under a different URI from the users browser and lakekeeper. |
| `LAKEKEEPER__UI__OPENID_CLIENT_ID`                 | `lakekeeper-ui`                              | Client ID to use for the Authorization Code Flow of the UI. Required if Authentication is enabled. Defaults to `lakekeeper` |
| `LAKEKEEPER__UI__OPENID_REDIRECT_PATH`             | `/callback`                                  | Path where the UI receives the callback including the tokens from the users browser. Defaults to: `/callback` |
| <nobr>`LAKEKEEPER__UI__OPENID_SCOPE`</nobr>        | `openid email`                               | Scopes to request from the IdP. Defaults to `openid profile email`. |
| <nobr>`LAKEKEEPER__UI__OPENID_RESOURCE`</nobr>     | `lakekeeper-api`                             | Resources to request from the IdP. If not specified, the `resource` field is omitted (default). |
| `LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_PATH` | `/logout`                                    | Path the UI calls when users are logged out from the IdP. Defaults to `/logout` |
| `LAKEKEEPER__UI__LAKEKEEPER_URL`                   | `https://example.com/lakekeeper`             | URI where the users browser can reach Lakekeeper. Defaults to the value of `LAKEKEEPER__BASE_URI`. |
| `LAKEKEEPER__UI__OPENID_TOKEN_TYPE`                | `access_token`                               | The token type to use for authenticating to Lakekeeper. The default value `access_token` works for most IdPs. Some IdPs, such as the Google Identity Platform, recommend the use of the OIDC ID Token instead. To use the ID token instead of the access token for Authentication, specify a value of `id_token`. Possible values are `access_token` and `id_token`. |

### Caching
Lakekeeper uses in-memory caches to speed up certain operations.

**Short-Term Credentials (STC) Cache**

When Lakekeeper vends short-term credentials for cloud storage access (S3 STS, Azure SAS tokens, or GCP access tokens), these credentials can be cached to reduce load on cloud identity services and improve response times.

| Variable                                        | Example | Description      |
|-------------------------------------------------|---------|------------------|
| <nobr>`LAKEKEEPER__CACHE__STC__ENABLED`</nobr>  | `true`  | Enable or disable the short-term credentials cache. Default: `true` |
| <nobr>`LAKEKEEPER__CACHE__STC__CAPACITY`</nobr> | `10000` | Maximum number of credential entries to cache. Default: `10000` |

*Expiry Mechanism*: Cached credentials automatically expire based on the validity period of the underlying cloud credentials. Lakekeeper caches credentials for half their lifetime (e.g., if GCP STS returns credentials valid for 1 hour, they're cached for 30 minutes) with a maximum cache duration of 1 hour. This ensures credentials remain fresh while reducing unnecessary identity service calls.

*Metrics*: The STC cache exposes Prometheus metrics for monitoring:

- `lakekeeper_cache_size{cache_type="stc"}`: Current number of entries in the cache
- `lakekeeper_cache_hits_total{cache_type="stc"}`: Total number of cache hits
- `lakekeeper_cache_misses_total{cache_type="stc"}`: Total number of cache misses

**Warehouse Cache**

Caches warehouse metadata to reduce database queries for warehouse lookups.

| Configuration Key                                             | Type    | Default | Description |
|---------------------------------------------------------------|---------|---------|-----|
| <nobr>`LAKEKEEPER__CACHE__WAREHOUSE__ENABLED`<nobr>           | boolean | `true`  | Enable/disable warehouse caching. Default: `true` |
| <nobr>`LAKEKEEPER__CACHE__WAREHOUSE__CAPACITY`<nobr>          | integer | `1000`  | Maximum number of warehouses to cache. Default: `1000` |
| <nobr>`LAKEKEEPER__CACHE__WAREHOUSE__TIME_TO_LIVE_SECS`<nobr> | integer | `60`    | Time-to-live for cache entries in seconds. Default: `60` |

If the cache is enabled, changes to Storage Profile may take up to the configured TTL (default: 60 seconds) to be reflected in all Lakekeeper workers. If a single worker is used, the Cache is always up to date. Warehouse metadata is guaranteed to be fresh for load table & view operations also for multi-worker deployments.

*Metrics*: The Warehouse cache exposes Prometheus metrics for monitoring:

- `lakekeeper_cache_size{cache_type="warehouse"}`: Current number of entries in the cache
- `lakekeeper_cache_hits_total{cache_type="warehouse"}`: Total number of cache hits
- `lakekeeper_cache_misses_total{cache_type="warehouse"}`: Total number of cache misses

**Namespace Cache**

Caches namespace metadata and hierarchies to reduce database queries for namespace lookups. Namespace lookups are also required for table & view operations.

| Configuration Key                                             | Type    | Default | Description |
|---------------------------------------------------------------|---------|---------|-----|
| <nobr>`LAKEKEEPER__CACHE__NAMESPACE__ENABLED`<nobr>           | boolean | `true`  | Enable/disable namespace caching. Default: `true` |
| <nobr>`LAKEKEEPER__CACHE__NAMESPACE__CAPACITY`<nobr>          | integer | `1000`  | Maximum number of namespaces to cache. Default: `1000` |
| <nobr>`LAKEKEEPER__CACHE__NAMESPACE__TIME_TO_LIVE_SECS`<nobr> | integer | `60`    | Time-to-live for cache entries in seconds. Default: `60` |

If the cache is enabled, changes to namespace properties may take up to the configured TTL (default: 60 seconds) to be reflected in all Lakekeeper workers. If a single worker is used, the Cache is always up to date. The namespace cache stores both individual namespaces and their parent hierarchies for efficient lookups.

*Metrics*: The Namespace cache exposes Prometheus metrics for monitoring:

- `lakekeeper_cache_size{cache_type="namespace"}`: Current number of entries in the cache
- `lakekeeper_cache_hits_total{cache_type="namespace"}`: Total number of cache hits
- `lakekeeper_cache_misses_total{cache_type="namespace"}`: Total number of cache misses

**Secrets Cache**

Caches storage secrets to reduce load on the secret store. Since Lakekeeper never updates secrets, long TTLs can significantly increase resilience against secret store outages, especially when the secret store is external to the main database backend.

| Configuration Key                                           | Type    | Default | Description |
|-------------------------------------------------------------|---------|---------|-----|
| <nobr>`LAKEKEEPER__CACHE__SECRETS__ENABLED`<nobr>           | boolean | `true`  | Enable/disable secrets caching. Default: `true` |
| <nobr>`LAKEKEEPER__CACHE__SECRETS__CAPACITY`<nobr>          | integer | `500`   | Maximum number of secrets to cache. Default: `500` |
| <nobr>`LAKEKEEPER__CACHE__SECRETS__TIME_TO_LIVE_SECS`<nobr> | integer | `600`   | Time-to-live for cache entries in seconds. Default: `600` (10 minutes) |

*Metrics*: The Secrets cache exposes Prometheus metrics for monitoring:

- `lakekeeper_cache_size{cache_type="secrets"}`: Current number of entries in the cache
- `lakekeeper_cache_hits_total{cache_type="secrets"}`: Total number of cache hits
- `lakekeeper_cache_misses_total{cache_type="secrets"}`: Total number of cache misses

**Role Cache**

Caches role metadata to reduce database queries for role lookups. The role cache uses a two-tier caching mechanism: a primary cache indexed by role ID and a secondary index by project ID and role identifier, enabling efficient lookups from both identifiers. Note that this cache only stores role definitions and does not include any information about role assignments to users or principals.

| Configuration Key                                        | Type    | Default | Description |
|----------------------------------------------------------|---------|---------|-----|
| <nobr>`LAKEKEEPER__CACHE__ROLE__ENABLED`<nobr>           | boolean | `true`  | Enable/disable role caching. Default: `true` |
| <nobr>`LAKEKEEPER__CACHE__ROLE__CAPACITY`<nobr>          | integer | `10000` | Maximum number of roles to cache. Default: `10000` |
| <nobr>`LAKEKEEPER__CACHE__ROLE__TIME_TO_LIVE_SECS`<nobr> | integer | `120`   | Time-to-live for cache entries in seconds. Default: `120` (2 minutes) |

If the cache is enabled, changes to role metadata may take up to the configured TTL (default: 120 seconds) to be reflected in all Lakekeeper workers. If a single worker is used, the cache is always up to date. The cache is automatically invalidated when roles are updated or deleted.

*Metrics*: The Role cache exposes Prometheus metrics for monitoring:

- `lakekeeper_cache_size{cache_type="role"}`: Current number of entries in the cache
- `lakekeeper_cache_hits_total{cache_type="role"}`: Total number of cache hits
- `lakekeeper_cache_misses_total{cache_type="role"}`: Total number of cache misses

**User Assignments Cache**

Caches the set of roles assigned to each user (`UserId → role assignments`). This is the hot-path cache checked on every authorization request and is also the in-memory layer used by the LDAP role provider's two-layer caching scheme. The TTL must not exceed `LAKEKEEPER__CACHE__ROLE__TIME_TO_LIVE_SECS` to bound the window in which a deleted role can still appear in assignment results.

| Configuration Key                                                    | Type    | Default | Description |
|----------------------------------------------------------------------|---------|---------|-----|
| <nobr>`LAKEKEEPER__CACHE__USER_ASSIGNMENTS__ENABLED`<nobr>           | boolean | `true`  | Enable/disable user-assignments caching. Default: `true` |
| <nobr>`LAKEKEEPER__CACHE__USER_ASSIGNMENTS__CAPACITY`<nobr>          | integer | `50000` | Maximum number of users whose assignments are held in memory. Default: `50000` |
| <nobr>`LAKEKEEPER__CACHE__USER_ASSIGNMENTS__TIME_TO_LIVE_SECS`<nobr> | integer | `120`   | Time-to-live for cache entries in seconds. Must not exceed `LAKEKEEPER__CACHE__ROLE__TIME_TO_LIVE_SECS`. Default: `120` (2 minutes) |

*Metrics*: The User Assignments cache exposes Prometheus metrics for monitoring:

- `lakekeeper_cache_size{cache_type="user_assignments"}`: Current number of entries in the cache
- `lakekeeper_cache_hits_total{cache_type="user_assignments"}`: Total number of cache hits
- `lakekeeper_cache_misses_total{cache_type="user_assignments"}`: Total number of cache misses

### Endpoint Statistics

Lakekeeper collects statistics about the usage of its endpoints. Every Lakekeeper instance accumulates endpoint calls for a certain duration in memory before writing them into the database. The following configuration options are available:

| Variable                                               | Example | Description |
|--------------------------------------------------------|---------|-----------|
| <nobr>`LAKEKEEPER__ENDPOINT_STAT_FLUSH_INTERVAL`<nobr> | 30s     | Interval in seconds to write endpoint statistics into the database. Default: 30s, valid units are (s\|ms) |

### SSL Dependencies

You may be running Lakekeeper in your own environment which uses self-signed certificates for e.g. Minio. Lakekeeper is built with reqwest's `rustls-tls-native-roots` feature activated, this means `SSL_CERT_FILE` and `SSL_CERT_DIR` environment variables are respected. If both are not set, the system's default CA store is used. If you want to use a custom CA store, set `SSL_CERT_FILE` to the path of the CA file or `SSL_CERT_DIR` to the path of the CA directory. The certificate used by the server cannot be a CA. It needs to be an end entity certificate, else you may run into `CaUsedAsEndEntity` errors.

### Request Limits

Lakekeeper allows you to configure limits on incoming requests to protect against resource exhaustion and denial-of-service attacks.

| Variable                                         | Example   | Description   |
|--------------------------------------------------|-----------|---------------|
| <nobr>`LAKEKEEPER__MAX_REQUEST_BODY_SIZE`</nobr> | `2097152` | Maximum request body size in bytes. Default: `2097152` (2 MB) |
| <nobr>`LAKEKEEPER__MAX_REQUEST_TIME`</nobr>      | `30s`     | Maximum time allowed for a request to complete. Accepts format `{number}{ms\|s}`. Default: `30s` |

### Idempotency

Lakekeeper supports the [Iceberg REST Catalog Idempotency](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) specification. When enabled, clients can send an `Idempotency-Key` header on mutation requests to guarantee at-most-once execution. The server advertises support via the `idempotency-key-lifetime` field in the `GET /v1/config` response.

| Variable | Example | Description |
|---|---|---|
| <nobr>`LAKEKEEPER__IDEMPOTENCY__ENABLED`</nobr> | `true` | Enable idempotency key support. When enabled, `idempotency-key-lifetime` is advertised in `getConfig`. Default: `true` |
| <nobr>`LAKEKEEPER__IDEMPOTENCY__LIFETIME`</nobr> | `PT30M` | How long idempotency records are kept, in ISO-8601 duration format. This value is advertised to clients. Default: `PT30M` (30 minutes) |
| <nobr>`LAKEKEEPER__IDEMPOTENCY__GRACE_PERIOD`</nobr> | `PT5M` | Grace period added on top of lifetime for clock skew and transit delays, in ISO-8601 duration format. Default: `PT5M` (5 minutes) |
| <nobr>`LAKEKEEPER__IDEMPOTENCY__CLEANUP_TIMEOUT`</nobr> | `PT30S` | Maximum time a background cleanup task may run before being considered dead. If exceeded, the next attempt takes over. Default: `PT30S` (30 seconds) |

### Audit Logging

Lakekeeper can generate detailed audit logs for all authorization events. Audit logs are written to the standard logging output and can be filtered by the `event_source = "audit"` field. For more information, see the [Audit Logging Guide](./audit-logging.md).

| Variable                                           | Example | Description   |
|----------------------------------------------------|---------|---------------|
| <nobr>`LAKEKEEPER__AUDIT__TRACING__ENABLED`</nobr> | `true`  | Enable audit logging for authorization events. When enabled, all authorization checks (both successful and failed) are logged at the `INFO` level with `event_source = "audit"`. Audit logs include the actor, action, resource, and outcome. Default: `false` |

### Role Provider

Authorizers such as `Cedar` support pluggable role providers that resolve a user's group memberships from an external directory (e.g. LDAP / Active Directory). Multiple providers can be configured in parallel, each with a unique identifier. `OpenFGA` does not use role providers — roles are stored directly in OpenFGA.

##### Chain settings

| Variable                                                             | Default | Description |
|----------------------------------------------------------------------|---------|-----|
| <nobr>`LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_UNHANDLED_USERS`</nobr>  | `true`  | When `true`, an audit event is emitted whenever a user is not matched by any configured role provider. Useful for detecting misconfigured domain filters. Set to `false` to suppress these events for deployments where some users are intentionally not covered by any provider. |
| <nobr>`LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_ROLE_ASSIGNMENTS`</nobr> | `false` | When `true`, an audit event listing every resolved role name is emitted after each successful role resolution. Very noisy — intended for debugging role-provider configuration only. See [Logging — Operational Audit Events](./logging.md) for the event schema. |

##### Token role provider

When `LAKEKEEPER__OPENID_ROLES_CLAIM` is set, Lakekeeper extracts roles directly from the authenticated user's JWT. A built-in token role provider is added to the chain **automatically** — no additional configuration is required.

The token role provider only applies to OIDC-authenticated users (those whose identity was established via the configured OpenID Connect provider). It is a no-op for users authenticated through other mechanisms (e.g. Kubernetes service accounts).

The provider uses the reserved identifier `oidc`. If you declare a role provider with this identifier in your configuration, the automatic provider is suppressed and your custom provider takes its place.

##### LDAP role provider

Each LDAP provider is configured under a unique `<ID>` of your choosing. All variables below use the prefix `LAKEKEEPER__ROLE_PROVIDER__<ID>__`.

**Required fields:**

| Variable                       | Example                                | Description |
|--------------------------------|----------------------------------------|-----|
| <nobr>`…__TYPE`</nobr>         | `ldap`                                 | Provider type. Must be `ldap`. |
| <nobr>`…__URL`</nobr>          | `ldaps://ldap.example.com:636`         | LDAP server URL. Use `ldap://` for plain-text or STARTTLS, `ldaps://` for TLS. |
| <nobr>`…__DOMAINS`</nobr>      | `["example.com","*.corp.example.com"]` | JSON array of domain patterns. Only users whose login name ends with one of these domains are resolved via this provider. Supports `*` (any number of characters) and `?` (exactly one character). |
| <nobr>`…__USER_BASE_DN`</nobr> | `ou=people,dc=example,dc=com`          | Base DN for the LDAP user search. |

**Authentication:**

| Variable                        | Default     | Description                  |
|---------------------------------|-------------|------------------------------|
| <nobr>`…__BIND_DN`</nobr>       | (anonymous) | Distinguished name of the service account used to bind. Omit for anonymous bind. |
| <nobr>`…__BIND_PASSWORD`</nobr> |             | Password for the service account. Required when `…__BIND_DN` is set; can also be supplied via `…__BIND_PASSWORD_FILE`. |

**User search:**

| Variable                             | Default         | Description         |
|--------------------------------------|-----------------|---------------------|
| <nobr>`…__USER_SEARCH_FILTER`</nobr> | `(uid=${USER})` | LDAP filter used to locate a user entry. The literal `${USER}` is replaced with the subject portion of the user's login name (the part before `@`). |
| <nobr>`…__USER_SEARCH_SCOPE`</nobr>  | `sub`           | Search scope: `sub` (entire subtree), `one` (one level below base), or `base`. |

**Group / role mapping:**

| Variable                                   | Default    | Description        |
|--------------------------------------------|------------|--------------------|
| <nobr>`…__USER_MEMBER_OF_ATTRIBUTE`</nobr> | `memberOf` | Multi-valued attribute on the user entry that lists the groups the user belongs to. The default (`memberOf`) is correct for Active Directory and OpenLDAP with the `memberof` overlay. |
| <nobr>`…__GROUP_NAME_SOURCE`</nobr>        | `dn_cn`    | How to derive the role name from a group entry. `dn_cn` extracts the `CN=` component from the group's distinguished name (recommended for AD/ADFS). |
| <nobr>`…__GROUP_CASE`</nobr>               | `keep`     | Case transformation applied to the resolved group name before it is stored as a role. One of `keep`, `upper`, or `lower`. |

**Connection and TLS:**

| Variable                               | Default | Description               |
|----------------------------------------|---------|---------------------------|
| <nobr>`…__STARTTLS`</nobr>             | `false` | Upgrade a plain TCP connection with STARTTLS before binding. Only applies to `ldap://` URLs. |
| <nobr>`…__ALLOW_INSECURE`</nobr>       | `false` | Skip TLS certificate verification. **Do not use in production.** |
| <nobr>`…__CONNECT_TIMEOUT_SECS`</nobr> | `30`    | Seconds to wait when establishing the initial connection. |
| <nobr>`…__READ_TIMEOUT_SECS`</nobr>    | `60`    | Seconds to wait for an LDAP response. |

**Caching & performance:**

Each LDAP provider uses a two-layer cache to avoid a network round-trip to the LDAP server on every request:

1. **In-memory layer** — role assignments are held in a per-node moka cache (see [User Assignments Cache](#caching) above). Reads that hit this layer incur no I/O at all.
2. **Database layer** — on an in-memory miss, role assignments are read from (and re-populate) the database. The database record includes a `synced_at` timestamp that is compared against `SYNC_INTERVAL_SECS` to decide whether the data is still fresh.

If the database record is older than `SYNC_INTERVAL_SECS`, Lakekeeper contacts LDAP, writes the fresh assignments back to both the database and the in-memory cache, and returns the result. If LDAP is temporarily unreachable, the stale database record is served instead and an audit warning is emitted — the request is never failed solely due to an LDAP outage.

| Variable                             | Default | Description                 |
|--------------------------------------|---------|-----------------------------|
| <nobr>`…__SYNC_INTERVAL_SECS`</nobr> | `300`   | Maximum age (in seconds) of a cached role-assignment record before Lakekeeper re-fetches from LDAP. Increase to reduce LDAP traffic; decrease when group membership changes must propagate more quickly. Also controls the TTL of the corresponding database record. |

**Startup and resilience:**

| Variable                                       | Default | Description       |
|------------------------------------------------|---------|-------------------|
| <nobr>`…__REQUIRE_CONNECTED_ON_STARTUP`</nobr> | `false` | When `true`, Lakekeeper refuses to start if this provider cannot connect. Useful for catching misconfiguration early. When `false`, the provider starts in a disconnected state and reconnects automatically on first use. |
| <nobr>`…__RECONNECT_COOLDOWN_SECS`</nobr>      | `30`    | Minimum seconds between reconnection attempts after a failure. |

**IDP filtering (optional):**

| Variable                  | Default      | Description                       |
|---------------------------|--------------|-----------------------------------|
| <nobr>`…__IDP_IDS`</nobr> | *(all IDPs)* | JSON array of identity provider IDs. When set, only users from these IDPs are resolved via this provider. Omit to allow all IDPs. |

**Example — minimal LDAP provider (env vars):**
```bash
LAKEKEEPER__ROLE_PROVIDER__MY_LDAP__TYPE=ldap
LAKEKEEPER__ROLE_PROVIDER__MY_LDAP__URL=ldaps://ldap.corp.example.com:636
LAKEKEEPER__ROLE_PROVIDER__MY_LDAP__DOMAINS=["corp.example.com"]
LAKEKEEPER__ROLE_PROVIDER__MY_LDAP__USER_BASE_DN=ou=people,dc=corp,dc=example,dc=com
LAKEKEEPER__ROLE_PROVIDER__MY_LDAP__BIND_DN=cn=svc-lakekeeper,ou=service-accounts,dc=corp,dc=example,dc=com
LAKEKEEPER__ROLE_PROVIDER__MY_LDAP__BIND_PASSWORD_FILE=/run/secrets/ldap-password
```

##### File-based configuration

All providers can alternatively be configured through a single TOML file. This is convenient when secrets management or config management tools produce a single artefact (e.g. Vault agent, Kubernetes projected volumes, Ansible templates).

Point `LAKEKEEPER__ROLE_PROVIDER_FILE` at a standard TOML file. Each provider is a section `[role_provider.<id>]` where `<id>` is the provider ID you choose. Multiple providers can be defined in the same file.

**Example — two LDAP providers in one file:**

`/etc/lakekeeper/role-providers.toml`:
```toml
[role_provider.corporate]
type = "ldap"
url = "ldaps://ldap.corp.example.com:636"
domains = ["corp.example.com"]
user_base_dn = "ou=people,dc=corp,dc=example,dc=com"
bind_dn = "cn=svc-lakekeeper,ou=service-accounts,dc=corp,dc=example,dc=com"
bind_password = "s3cr3t"

[role_provider.subsidiary]
type = "ldap"
url = "ldaps://ldap.subsidiary.example.com:636"
domains = ["subsidiary.example.com"]
user_base_dn = "ou=users,dc=subsidiary,dc=example,dc=com"
bind_dn = "cn=svc-lakekeeper,ou=service-accounts,dc=subsidiary,dc=example,dc=com"
bind_password = "s3cr3t"
```

Then set the single environment variable:
```bash
LAKEKEEPER__ROLE_PROVIDER_FILE=/etc/lakekeeper/role-providers.toml
```

> **Combining file and environment variables:** The file and env-var approaches can be combined. The file is loaded first and env vars are merged on top — env vars override individual fields for the same provider while unset fields are preserved from the file. This makes it easy to store non-sensitive configuration in the file and inject secrets via env vars:
>
> ```toml
> # /etc/lakekeeper/role-providers.toml (checked in, no secrets)
> [role_provider.corporate]
> type = "ldap"
> url = "ldaps://ldap.corp.example.com:636"
> domains = ["corp.example.com"]
> user_base_dn = "ou=people,dc=corp,dc=example,dc=com"
> bind_dn = "cn=svc-lakekeeper,ou=service-accounts,dc=corp,dc=example,dc=com"
> ```
>
> ```bash
> # Injected at runtime (e.g. from a secrets manager)
> LAKEKEEPER__ROLE_PROVIDER_FILE=/etc/lakekeeper/role-providers.toml
> LAKEKEEPER__ROLE_PROVIDER__CORPORATE__BIND_PASSWORD=s3cr3t
> ```

### Debug

Lakekeeper provides debugging options to help troubleshoot issues during development. These options should **not** be enabled in production environments as they can expose sensitive data and impact performance.

| Variable                                                   | Example | Description |
|------------------------------------------------------------|---------|-------|
| <nobr>`LAKEKEEPER__DEBUG__LOG_REQUEST_BODIES`</nobr>       | `true`  | If set to `true`, Lakekeeper will log all incoming and outgoing request bodies at debug level. This is useful for debugging API interactions but should **never** be enabled in production as it can expose sensitive data (credentials, tokens, etc.) and significantly impact performance. Default: `false` |
| <nobr>`LAKEKEEPER__DEBUG__MIGRATE_BEFORE_SERVE`</nobr>     | `true`  | If set to `true`, Lakekeeper waits for the DB (30s) and runs migrations when `serve` is called. Default: `false` |
| <nobr>`LAKEKEEPER__DEBUG__AUTO_SERVE`</nobr>               | `true`  | If set to `true`, Lakekeeper will automatically start the server when no subcommand is provided (i.e., when running the binary without arguments). This is useful for development environments to quickly start the server without explicitly specifying the `serve` command. Default: `false` |
| <nobr>`LAKEKEEPER__DEBUG__EXTENDED_LOGS`</nobr>            | `false` | Controls whether file names and line numbers are included in JSON log output. When set to `false`, these fields are omitted for cleaner logs. When set to `true`, each log entry includes `filename` and `line_number` fields for easier debugging. Default: `false` |
| <nobr>`LAKEKEEPER__DEBUG__LOG_AUTHORIZATION_HEADER`</nobr> | `false` | If set to `true`, the `Authorization` header is included in request trace spans for the `/catalog/v1/config` and `/management/v1/info` endpoints. This exposes sensitive credentials (tokens, passwords) and should **never** be enabled in production. Default: `false` |

**Warning**: Debug options can expose sensitive information in logs and should only be used in secure development environments.

### Test Configurations
| Variable                                          | Example | Description    |
|---------------------------------------------------|---------|----------------|
| <nobr>`LAKEKEEPER__SKIP_STORAGE_VALIDATION`<nobr> | true    | If set to true, Lakekeeper does not validate the provided storage configuration & credentials when creating or updating Warehouses. This is not suitable for production. Default: false |


### License Configuration
<nobr><span class="lkp"></span></nobr>, the enterprise distribution of Lakekeeper, requires a License to operate. The license can be provided via either of the following environment variables. If both are set, `LAKEKEEPER__LICENSE__KEY` takes precedence.

| Variable                                     | Example                | Description |
|----------------------------------------------|------------------------|------|
| <nobr>`LAKEKEEPER__LICENSE__KEY`</nobr>      | `<license-key>`        | License key as a string. Takes precedence over `LAKEKEEPER__LICENSE__KEY_PATH` if both are set. |
| <nobr>`LAKEKEEPER__LICENSE__KEY_PATH`</nobr> | `/path/to/license.lic` | Path to a file containing the license key. |
