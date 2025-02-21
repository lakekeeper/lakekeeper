# Configuration

Lakekeeper is configured via environment variables. Settings listed in this page are shared between all projects and warehouses. Previous to Lakekeeper Version `0.5.0` please prefix all environment variables with `ICEBERG_REST__` instead of `LAKEKEEPER__`.

For most deployments, we recommend to set at least the following variables: `LAKEKEEPER__PG_DATABASE_URL_READ`, `LAKEKEEPER__PG_DATABASE_URL_WRITE`, `LAKEKEEPER__PG_ENCRYPTION_KEY`.

## Routing and Base-URL

Some Lakekeeper endspoints return links pointing at Lakekeeper itself. By default, these links are generated using the `x-forwarded-for`, `x-forwarded-proto` and `x-forwarded-port` headers, if these are not present, the `host` header is used. If these heuristics are not working for you, you may set the `LAKEKEEPER_BASE_URI` environment variable to the base-URL where Lakekeeper is externally reachable. This may be necessary if Lakekeeper runs behind a reverse proxy or load balancer, and you cannot set the headers accordingly. In general, we recommend relying on the headers.

### General

| Variable                                         | Example                                | Description                                                                                                                                                                                                                                                               |
|--------------------------------------------------|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <nobr>`LAKEKEEPER__BASE_URI`</nobr>              | <nobr>`https://example.com:8181`<nobr> | Optional base-URL where the catalog is externally reachable. Default: `None`. See [Routing and Base-URL](#routing-and-base-url).                                                                                                                                          |
| <nobr>`LAKEKEEPER__ENABLE_DEFAULT_PROJECT`<nobr> | `true`                                 | If `true`, the NIL Project ID ("00000000-0000-0000-0000-000000000000") is used as a default if the user does not specify a project when connecting. This option is enabled by default, which we recommend for all single-project (single-tenant) setups. Default: `true`. |
| `LAKEKEEPER__RESERVED_NAMESPACES`                | `system,examples,information_schema`   | Reserved Namespaces that cannot be created via the REST interface                                                                                                                                                                                                         |
| `LAKEKEEPER__METRICS_PORT`                       | `9000`                                 | Port where the Prometheus metrics endpoint is reachable. Default: `9000`                                                                                                                                                                                                  |
| `LAKEKEEPER__LISTEN_PORT`                        | `8181`                                 | Port the Lakekeeper listens on. Default: `8181`                                                                                                                                                                                                                           |
| `LAKEKEEPER__SECRET_BACKEND`                     | `postgres`                             | The secret backend to use. If `kv2` (Hashicorp KV Version 2) is chosen, you need to provide [additional parameters](#vault-kv-version-2) Default: `postgres`, one-of: [`postgres`, `kv2`]                                                                                 |
| `LAKEKEEPER__ALLOW_ORIGIN`                       | `*`                                    | A comma separated list of allowed origins for CORS.                                                                                                                                                                                                                       |


### Persistence Store

Currently Lakekeeper supports only Postgres as a persistence store. You may either provide connection strings using `PG_DATABASE_URL_READ` or use the `PG_*` environment variables. Connection strings take precedence:

| Variable                                               | Example                                               | Description |
|--------------------------------------------------------|-------------------------------------------------------|-----|
| `LAKEKEEPER__PG_DATABASE_URL_READ`                     | `postgres://postgres:password@localhost:5432/iceberg` | Postgres Database connection string used for reading. Defaults to `LAKEKEEPER__PG_DATABASE_URL_WRITE`. |
| `LAKEKEEPER__PG_DATABASE_URL_WRITE`                    | `postgres://postgres:password@localhost:5432/iceberg` | Postgres Database connection string used for writing. |
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

### Vault KV Version 2

Configuration parameters if a Vault KV version 2 (i.e. Hashicorp Vault) compatible storage is used as a backend. Currently, we only support the `userpass` authentication method. Configuration may be passed as single values like `LAKEKEEPER__KV2__URL=http://vault.local` or as a compound value:
`LAKEKEEPER__KV2='{url="http://localhost:1234", user="test", password="test", secret_mount="secret"}'`

| Variable                                     | Example               | Description |
|----------------------------------------------|-----------------------|-------|
| `LAKEKEEPER__KV2__URL`                       | `https://vault.local` | URL of the KV2 backend |
| `LAKEKEEPER__KV2__USER`                      | `admin`               | Username to authenticate against the KV2 backend |
| `LAKEKEEPER__KV2__PASSWORD`                  | `password`            | Password to authenticate against the KV2 backend |
| <nobr>`LAKEKEEPER__KV2__SECRET_MOUNT`</nobr> | `kv/data/iceberg`     | Path to the secret mount in the KV2 backend |


### Task queues

Lakekeeper uses task queues internally to remove soft-deleted tabulars and purge tabular files. The following global configuration options are available:

| Variable                                  | Example                   | Description                                                                                                                                                                                                          |
|-------------------------------------------|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `LAKEKEEPER__QUEUE_CONFIG__MAX_RETRIES`   | 5                         | Number of retries before a task is considered failed  Default: 5                                                                                                                                                     |
| `LAKEKEEPER__QUEUE_CONFIG__MAX_AGE`       | 3600                      | Amount of seconds before a task is considered stale and could be picked up by another worker. Default: 3600                                                                                                          |
| `LAKEKEEPER__QUEUE_CONFIG__POLL_INTERVAL` | 3600ms/30s/30(deprecated) | Interval between polling for new tasks. Default: 10s. Supported units: ms (milliseconds) and s (seconds), leaving the unit out is deprecated, it'll default to seconds but is due to be removed in a future release. |

### Nats

Lakekeeper can publish change events to Nats (Kafka is coming soon). The following configuration options are available:

| Variable                                   | Example                 | Description |
|--------------------------------------------|-------------------------|-------|
| `LAKEKEEPER__NATS_ADDRESS`                 | `nats://localhost:4222` | The URL of the NATS server to connect to |
| `LAKEKEEPER__NATS_TOPIC`                   | `iceberg`               | The subject to publish events to |
| `LAKEKEEPER__NATS_USER`                    | `test-user`             | User to authenticate against nats, needs `LAKEKEEPER__NATS_PASSWORD` |
| `LAKEKEEPER__NATS_PASSWORD`                | `test-password`         | Password to authenticate against nats, needs `LAKEKEEPER__NATS_USER` |
| <nobr>`LAKEKEEPER__NATS_CREDS_FILE`</nobr> | `/path/to/file.creds`   | Path to a file containing nats credentials |
| `LAKEKEEPER__NATS_TOKEN`                   | `xyz`                   | Nats token to use for authentication |
### Logging Cloudevents

Cloudevents can also be logged, if you do not have Nats up and running. This feature can be enabled by setting

`LAKEKEEPER__LOG_CLOUDEVENTS=true`

### Authentication

To prohibit unwanted access to data, we recommend to enable Authentication.

Authentication is enabled if:

* `LAKEKEEPER__OPENID_PROVIDER_URI` is set OR
* `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is set to true

External OpenID and Kubernetes Authentication can also be enabled together. If `LAKEKEEPER__OPENID_PROVIDER_URI` is specified, Lakekeeper will  verify access tokens against this provider. The provider must provide the `.well-known/openid-configuration` endpoint and the openid-configuration needs to have `jwks_uri` and `issuer` defined. 

Typical values for `LAKEKEEPER__OPENID_PROVIDER_URI` are:

* Keycloak: `https://keycloak.local/realms/{your-realm}`
* Entra-ID: `https://login.microsoftonline.com/{your-tenant-id-here}/v2.0/`

Please check the [Authentication Guide](./authentication.md) for more details.

| Variable                                       | Example                                      | Description |
|------------------------------------------------|----------------------------------------------|-----|
| <nobr>`LAKEKEEPER__OPENID_PROVIDER_URI`</nobr> | `https://keycloak.local/realms/{your-realm}` | OpenID Provider URL. |
| `LAKEKEEPER__OPENID_AUDIENCE`                  | `the-client-id-of-my-app`                    | If set, the `aud` of the provided token must match the value provided. Multiple allowed audiences can be provided as a comma separated list. |
| `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS`        | `https://sts.windows.net/<Tenant>/`          | A comma separated list of additional issuers to trust. The issuer defined in the `issuer` field of the `.well-known/openid-configuration` is always trusted. `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS` has no effect if `LAKEKEEPER__OPENID_PROVIDER_URI` is not set. |
| `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` | true                                         | If true, kubernetes service accounts can authenticate to Lakekeeper. This option is compatible with `LAKEKEEPER__OPENID_PROVIDER_URI` - multiple IdPs (OIDC and Kubernetes) can be enabled simultaneously. |
| `LAKEKEEPER__OPENID_SCOPE`                     | `lakekeeper`                                 | Specify a scope that must be present in provided tokens received from the openid provider. |
| `LAKEKEEPER__OPENID_SUBJECT_CLAIM`             | `sub` or `oid`                               | Specify the field in the user's claims that is used to identify a User. By default Lakekeeper uses the `oid` field if present, otherwise the `sub` field is used. We strongly recommend setting this configuration explicitly in production deployments. Entra-ID users want to use the `oid` claim, users from all other IdPs most likely want to use the `sub` claim. |

### Authorization
Authorization is only effective if [Authentication](#authentication) is enabled. Authorization must not be enabled after Lakekeeper has been bootstrapped! Please create a new Lakekeeper instance, bootstrap it with authorization enabled, and migrate your tables.

| Variable                                      | Example                                                                    | Description |
|-----------------------------------------------|----------------------------------------------------------------------------|-----|
| `LAKEKEEPER__AUTHZ_BACKEND`                   | `allowall`                                                                 | The authorization backend to use. If `openfga` is chosen, you need to provide [additional parameters](#authorization). The `allowall` backend disables authorization - authenticated users can access all endpoints. Default: `allowall`, one-of: [`openfga`, `allowall`] |
| <nobr>`LAKEKEEPER__OPENFGA__ENDPOINT`</nobr>  | `http://localhost:35081`                                                   | OpenFGA Endpoint (gRPC). |
| `LAKEKEEPER__OPENFGA__STORE_NAME`             | `lakekeeper`                                                               | The OpenFGA Store to use. Default: `lakekeeper` |
| `LAKEKEEPER__OPENFGA__API_KEY`                | `my-api-key`                                                               | The API Key used for [Pre-shared key authentication](https://openfga.dev/docs/getting-started/setup-openfga/configure-openfga#pre-shared-key-authentication) to OpenFGA. If `LAKEKEEPER__OPENFGA__CLIENT_ID` is set, the API Key is ignored. If neither API Key nor Client ID is specified, no authentication is used. |
| <nobr>`LAKEKEEPER__OPENFGA__CLIENT_ID`</nobr> | `12345`                                                                    | The Client ID to use for Authenticating if OpenFGA is secured via [OIDC](https://openfga.dev/docs/getting-started/setup-openfga/configure-openfga#oidc). |
| `LAKEKEEPER__OPENFGA__CLIENT_SECRET`          | `abcd`                                                                     | Client Secret for the Client ID. |
| `LAKEKEEPER__OPENFGA__TOKEN_ENDPOINT`         | `https://keycloak.example.com/realms/master/protocol/openid-connect/token` | Token Endpoint to use when exchanging client credentials for an access token for OpenFGA. Required if Client ID is set |


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


### SSL Dependencies

You may be running Lakekeeper in your own environment which uses self-signed certificates for e.g. Minio. Lakekeeper is built with reqwest's `rustls-tls-native-roots` feature activated, this means `SSL_CERT_FILE` and `SSL_CERT_DIR` environment variables are respected. If both are not set, the system's default CA store is used. If you want to use a custom CA store, set `SSL_CERT_FILE` to the path of the CA file or `SSL_CERT_DIR` to the path of the CA directory. The certificate used by the server cannot be a CA. It needs to be an end entity certificate, else you may run into `CaUsedAsEndEntity` errors.
