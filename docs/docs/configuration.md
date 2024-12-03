# Configuration

Lakekeeper is configured via environment variables. Settings listed in this page are shared between all projects and warehouses. Previous to Lakekeeper Version `0.5.0` please prefix all environment variables with `ICEBERG_REST__` instead of `LAKEKEEPER__`.

For most deployments, we recommend to set at least the following variables: `LAKEKEEPER__BASE_URI`, `LAKEKEEPER__PG_DATABASE_URL_READ`, `LAKEKEEPER__PG_DATABASE_URL_WRITE`, `LAKEKEEPER__PG_ENCRYPTION_KEY`.

### General

| Variable                                         | Example                                | Description |
|--------------------------------------------------|----------------------------------------|-----|
| <nobr>`LAKEKEEPER__BASE_URI`</nobr>              | <nobr>`https://example.com:8080`<nobr> | Base URL where the catalog is externally reachable. Default: `https://localhost:8080` |
| <nobr>`LAKEKEEPER__ENABLE_DEFAULT_PROJECT`<nobr> | `true`                                 | If `true`, the NIL Project ID ("00000000-0000-0000-0000-000000000000") is used as a default if the user does not specify a project when connecting. This option is enabled by default, which we recommend for all single-project (single-tenant) setups. Default: `true`. |
| `LAKEKEEPER__RESERVED_NAMESPACES`                | `system,examples,information_schema`   | Reserved Namespaces that cannot be created via the REST interface |
| `LAKEKEEPER__METRICS_PORT`                       | `9000`                                 | Port where the Prometheus metrics endpoint is reachable. Default: `9000` |
| `LAKEKEEPER__LISTEN_PORT`                        | `8080`                                 | Port the Lakekeeper listens on. Default: `8080` |
| `LAKEKEEPER__SECRET_BACKEND`                     | `postgres`                             | The secret backend to use. If `kv2` (Hashicorp KV Version 2) is chosen, you need to provide [additional parameters](#vault-kv-version-2) Default: `postgres`, one-of: [`postgres`, `kv2`] |

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

| Variable                                  | Example | Description            |
|-------------------------------------------|---------|------------------------|
| `LAKEKEEPER__QUEUE_CONFIG__MAX_RETRIES`   | 5       | Number of retries before a task is considered failed  Default: 5 |
| `LAKEKEEPER__QUEUE_CONFIG__MAX_AGE`       | 3600    | Amount of seconds before a task is considered stale and could be picked up by another worker. Default: 3600 |
| `LAKEKEEPER__QUEUE_CONFIG__POLL_INTERVAL` | 10      | Amount of seconds between polling for new tasks. Default: 10 |

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
| `LAKEKEEPER__OPENID_AUDIENCE`                  | `the-client-id-of-my-app`                    | If set, the `aud` of the provided token must match the value provided. |
| `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` | true                                         | If true, kubernetes service accounts can authenticate to Lakekeeper. This option is compatible with `LAKEKEEPER__OPENID_PROVIDER_URI` - multiple IdPs (OIDC and Kubernetes) can be enabled simultaneously. |

### SSL Dependencies

You may be running Lakekeeper in your own environment which uses self-signed certificates for e.g. minio. Lakekeeper is built with reqwest's `rustls-tls-native-roots` feature activated, this means `SSL_CERT_FILE` and `SSL_CERT_DIR` environment variables are respected. If both are not set, the system's default CA store is used. If you want to use a custom CA store, set `SSL_CERT_FILE` to the path of the CA file or `SSL_CERT_DIR` to the path of the CA directory. The certificate used by the server cannot be a CA. It needs to be an end entity certificate, else you may run into `CaUsedAsEndEntity` errors.
