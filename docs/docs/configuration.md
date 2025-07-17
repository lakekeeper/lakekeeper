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
| `LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS`                 | <nobr>`true`<nobr> | Lakekeeper supports using AWS system identities (i.e. through `AWS_*` environment variables or EC2 instance profiles) as storage credentials for warehouses. This feature is disabled by default to prevent accidental access to restricted storage locations. To enable AWS system identities, set `LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS` to `true`. Default: `false` (AWS system credentials disabled) |
| `LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS`           | <nobr>`true`<nobr> | By default, when using AWS system credentials, users must specify an `assume-role-arn` for Lakekeeper to assume when accessing S3. Setting this option to `true` allows Lakekeeper to use system credentials directly without role assumption, meaning the system identity must have direct access to warehouse locations. Default: `false` (direct system credential access disabled) |
| `LAKEKEEPER__S3_REQUIRE_EXTERNAL_ID_FOR_SYSTEM_CREDENTIALS` | <nobr>`true`<nobr> | Controls whether an `external-id` is required when assuming a role with AWS system credentials. External IDs provide additional security when cross-account role assumption is used. Default: true (external ID required) |
| `LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS`               | <nobr>`true`<nobr> | Lakekeeper supports using Azure system identities (i.e. through `AZURE_*` environment variables or VM managed identities) as storage credentials for warehouses. This feature is disabled by default to prevent accidental access to restricted storage locations. To enable Azure system identities, set `LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS` to `true`. Default: `false` (Azure system credentials disabled) |
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

| Variable                         | Example    | Description                  |
|----------------------------------|------------|------------------------------|
| `LAKEKEEPER__TASK_POLL_INTERVAL` | 3600ms/30s | Interval between polling for new tasks. Default: 10s. Supported units: ms (milliseconds) and s (seconds), leaving the unit out is deprecated, it'll default to seconds but is due to be removed in a future release. |

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
| <nobr>`LAKEKEEPER__OPENID_PROVIDER_URI`</nobr>                            | `https://keycloak.local/realms/{your-realm}` | OpenID Provider URL. |
| `LAKEKEEPER__OPENID_AUDIENCE`                                             | `the-client-id-of-my-app`                    | If set, the `aud` of the provided token must match the value provided. Multiple allowed audiences can be provided as a comma separated list. |
| `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS`                                   | `https://sts.windows.net/<Tenant>/`          | A comma separated list of additional issuers to trust. The issuer defined in the `issuer` field of the `.well-known/openid-configuration` is always trusted. `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS` has no effect if `LAKEKEEPER__OPENID_PROVIDER_URI` is not set. |
| `LAKEKEEPER__OPENID_SCOPE`                                                | `lakekeeper`                                 | Specify a scope that must be present in provided tokens received from the openid provider. |
| `LAKEKEEPER__OPENID_SUBJECT_CLAIM`                                        | `sub` or `oid`                               | Specify the field in the user's claims that is used to identify a User. By default Lakekeeper uses the `oid` field if present, otherwise the `sub` field is used. We strongly recommend setting this configuration explicitly in production deployments. Entra-ID users want to use the `oid` claim, users from all other IdPs most likely want to use the `sub` claim. |
| `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION`                            | true                                         | If true, kubernetes service accounts can authenticate to Lakekeeper. This option is compatible with `LAKEKEEPER__OPENID_PROVIDER_URI` - multiple IdPs (OIDC and Kubernetes) can be enabled simultaneously. |
| `LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE`                          | `https://kubernetes.default.svc`             | Audiences that are expected in Kubernetes tokens. Only has an effect if `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is true. |
| `LAKEKEEPER_TEST__KUBERNETES_AUTHENTICATION_ACCEPT_LEGACY_SERVICEACCOUNT` | `false`                                      | Add an authenticator that handles tokens with no audiences and the issuer set to `kubernetes/serviceaccount`. Only has an effect if `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is true. |


### Authorization
Authorization is only effective if [Authentication](#authentication) is enabled. Authorization must not be enabled after Lakekeeper has been bootstrapped! Please create a new Lakekeeper instance, bootstrap it with authorization enabled, and migrate your tables.

| Variable                                                 | Example                                                                    | Description |
|----------------------------------------------------------|----------------------------------------------------------------------------|-----|
| `LAKEKEEPER__AUTHZ_BACKEND`                              | `allowall`                                                                 | The authorization backend to use. If `openfga` is chosen, you need to provide [additional parameters](#authorization). The `allowall` backend disables authorization - authenticated users can access all endpoints. Default: `allowall`, one-of: [`openfga`, `allowall`] |
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

### Endpoint Statistics

Lakekeeper collects statistics about the usage of its endpoints. Every Lakekeeper instance accumulates endpoint calls for a certain duration in memory before writing them into the database. The following configuration options are available:

| Variable                                               | Example | Description |
|--------------------------------------------------------|---------|-----------|
| <nobr>`LAKEKEEPER__ENDPOINT_STAT_FLUSH_INTERVAL`<nobr> | 30s     | Interval in seconds to write endpoint statistics into the database. Default: 30s, valid units are (s\|ms) |

### SSL Dependencies

You may be running Lakekeeper in your own environment which uses self-signed certificates for e.g. Minio. Lakekeeper is built with reqwest's `rustls-tls-native-roots` feature activated, this means `SSL_CERT_FILE` and `SSL_CERT_DIR` environment variables are respected. If both are not set, the system's default CA store is used. If you want to use a custom CA store, set `SSL_CERT_FILE` to the path of the CA file or `SSL_CERT_DIR` to the path of the CA directory. The certificate used by the server cannot be a CA. It needs to be an end entity certificate, else you may run into `CaUsedAsEndEntity` errors.


### Test Configurations
| Variable                                          | Example | Description    |
|---------------------------------------------------|---------|----------------|
| <nobr>`LAKEKEEPER__SKIP_STORAGE_VALIDATION`<nobr> | true    | If set to true, Lakekeeper does not validate the provided storage configuration & credentials when creating or updating Warehouses. This is not suitable for production. Default: false |
