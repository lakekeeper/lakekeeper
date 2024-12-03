# Lakekeeper Catalog for Apache Iceberg

[![Website](https://img.shields.io/badge/https-lakekeeper.io-blue?color=3d4db3&logo=firefox&style=for-the-badge&logoColor=white)](https://lakekeeper.io/)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/jkAGG8p93B)
[![Docker on quay](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://quay.io/repository/lakekeeper/catalog?tab=tags&filter_tag_name=like%3Av)
[![Helm Chart](https://img.shields.io/badge/Helm-0F1689?style=for-the-badge&logo=Helm&labelColor=0F1689)](https://github.com/lakekeeper/lakekeeper-charts/tree/main/charts/lakekeeper)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/lakekeeper&color=3f6ec6&labelColor=&style=for-the-badge&logoColor=white)](https://artifacthub.io/packages/helm/lakekeeper/lakekeeper)


[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Unittests](https://github.com/lakekeeper/lakekeeper/actions/workflows/unittests.yml/badge.svg)](https://github.com/lakekeeper/lakekeeper/actions/workflows/unittests.yml)
[![Spark Integration](https://github.com/lakekeeper/lakekeeper/actions/workflows/spark-integration.yml/badge.svg)](https://github.com/lakekeeper/lakekeeper/actions/workflows/spark-integration.yml)
[![Pyiceberg Integration](https://github.com/lakekeeper/lakekeeper/actions/workflows/pyiceberg-integration.yml/badge.svg)](https://github.com/lakekeeper/lakekeeper/actions/workflows/pyiceberg-integration.yml)
[![Trino Integration](https://github.com/lakekeeper/lakekeeper/actions/workflows/trino-integration.yml/badge.svg)](https://github.com/lakekeeper/lakekeeper/actions/workflows/trino-integration.yml)
[![Starrocks Integration](https://github.com/lakekeeper/lakekeeper/actions/workflows/starrocks-integration.yml/badge.svg)](https://github.com/lakekeeper/lakekeeper/actions/workflows/starrocks-integration.yml)


Please visit [docs.lakekeeper.io](docs.lakekeeper.io) for more information and the documentation.

This is Lakekeeper: A **secure**, **fast** and **easy to use**  implementation of the [Apache Iceberg](https://iceberg.apache.org/) REST Catalog specification based on [apache/iceberg-rust](https://github.com/apache/iceberg-rust). If you have questions, feature requests or just want a chat, we are hanging around in [Discord](https://discord.gg/jkAGG8p93B)!

<p align="center">
<img src="https://github.com/lakekeeper/lakekeeper/raw/main/assets/Lakekeeper-Overview.png" width="500">
</p>

# Quickstart

A Docker Container is available on [quay.io](https://quay.io/repository/lakekeeper/lakekeeper?tab=info).
We have prepared a minimal docker-compose file to demonstrate the usage of `spark` with our catalog:

```sh
git clone https://github.com/lakekeeper/lakekeeper.git
cd lakekeeper/examples/minimal
docker compose up
```

Then open your browser and head to `localhost:8888` to load the example Jupyter notebooks or head to `localhost:8080` for the Lakekeeper UI.

For more information on deployment, please check the [Getting Started Guide](docs.lakekeeper.io/getting-started/).

# Scope and Features

The Iceberg Catalog REST interface has become the standard for catalogs in open Lakehouses. It natively enables multi-table commits, server-side deconflicting and much more. It is figuratively the (**TIP**) of the Iceberg.

- **Written in Rust**: Single all-in-one binary - no JVM or Python env required.
- **Storage Access Management**: Lakekeeper secures access to your data using [Vended-Credentials](ToDo) and [remote signing for S3](ToDo). All major Hyperscalers (AWS, Azure, GCP) as well as on-premise deployments with S3 are supported.
- **Openid Provider Integration**: Use your own identity provider for authentication, just set `LAKEKEEPER__OPENID_PROVIDER_URI` and you are good to go.
- **Native Kubernetes Integration**: Use our helm chart to easily deploy high available setups and natively authenticate kubernetes service accounts with Lakekeeper. Kubernetes and OpenID authentication can be used simultaneously. A [Kubernetes Operator](https://github.com/lakekeeper/lakekeeper-operator) is currently in development.
- **Change Events**: Built-in support to emit change events (CloudEvents), which enables you to react to any change that happen to your tables.
- **Change Approval**: Changes can also be prohibited by external systems. This can be used to prohibit changes to tables that would invalidate Data Contracts, Quality SLOs etc. Simply integrate with your own change approval via our `ContractVerification` trait.
- **Multi-Tenant capable**: A single deployment of Lakekeeper can serve multiple projects - all with a single entrypoint. Each project itself supports multiple Warehouses to which compute engines can connect.
- **Customizable**: Lakekeeper is meant to be extended. We expose the Database implementation (`Catalog`), `SecretsStore`, `Authorizer`, Events (`CloudEventBackend`) and `ContractVerification` as interfaces (Traits). This allows you to tap into any access management system of your company or stream change events to any system you like - simply by implementing a handful methods.
- **Well-Tested**: Integration-tested with `spark`, `pyiceberg`, `trino` and `starrocks`.
- **High Available & Horizontally Scalable**: There is no local state - the catalog can be scaled horizontally easily.
- **Fine Grained Access (FGA):** Lakekeeper's default Authorization system leverages [OpenFGA](https://openfga.dev/). If your company already has a different system in place, you can integrate with it by implementing a handful of methods in the `Authorizer` trait.

If you are missing something, we would love to hear about it in a [Github Issue](https://github.com/lakekeeper/lakekeeper/issues/new).


# Status

### Supported Operations - Iceberg-Rest

| Operation | Status  | Description                                                              |
|-----------|:-------:|--------------------------------------------------------------------------|
| Namespace | ![done] | All operations implemented                                               |
| Table     | ![done] | All operations implemented - additional integration tests in development |
| Views     | ![done] | Remove unused files and log entries                                      |
| Metrics   | ![open] | Endpoint is available but doesn't store the metrics                      |

### Storage Profile Support

| Storage              |    Status    | Comment                                                   |
|----------------------|:------------:|-----------------------------------------------------------|
| S3 - AWS             | ![semi-done] | vended-credentials & remote-signing, assume role missing  |
| S3 - Custom          |   ![done]    | vended-credentials & remote-signing, tested against minio |
| Azure ADLS Gen2      |   ![done]    |                                                           |
| Azure Blob           |   ![open]    |                                                           |
| Microsoft OneLake    |   ![open]    |                                                           |
| Google Cloud Storage |   ![done]    |                                                           |

Details on how to configure the storage profiles can be found in the [Storage Guide](STORAGE.md).

### Supported Catalog Backends

| Backend  | Status  | Comment |
|----------|:-------:|---------|
| Postgres | ![done] |         |
| MongoDB  | ![open] |         |

### Supported Secret Stores

| Backend         | Status  | Comment       |
|-----------------|:-------:|---------------|
| Postgres        | ![done] |               |
| kv2 (hcp-vault) | ![done] | userpass auth |

### Supported Event Stores

| Backend | Status  | Comment                                                                          |
|---------|:-------:|----------------------------------------------------------------------------------|
| Nats    | ![done] |                                                                                  |
| Kafka   | ![open] | Available in branch already, we are currently struggling with cross-compilation. |

### Supported Operations - Management API

| Operation            | Status  | Description                                        |
|----------------------|:-------:|----------------------------------------------------|
| Warehouse Management | ![done] | Create / Update / Delete a Warehouse               |
| AuthZ                | ![open] | Manage access to warehouses, namespaces and tables |
| More to come!        | ![open] |                                                    |

### Auth(N/Z) Handlers

| Operation       | Status  | Description                                                                                                        |
|-----------------|:-------:|--------------------------------------------------------------------------------------------------------------------|
| OIDC (AuthN)    | ![done] | Secure access to the catalog via OIDC                                                                              |
| Custom (AuthZ)  | ![done] | If you are willing to implement a single rust Trait, the `AuthZHandler` can be implement to connect to your system |
| OpenFGA (AuthZ) | ![open] | Internal Authorization management                                                                                  |

# Undrop Tables

When a table or view is dropped, it is not immediately deleted from the catalog. Instead, it is marked as dropped and a job for its cleanup is scheduled. The table, including its data if `purgeRequested=True`, is then deleted after the configured expiration delay has passed. This will allow for a recovery of tables that have been dropped by accident.

`Undrop` of a table is only possible if soft-deletes are enabled for a Warehouse.


# Configuration

The basic setup of the Catalog is configured via environment variables. As this catalog supports a multi-tenant setup, each catalog ("warehouse") also comes with its own configuration options including its Storage Configuration. The documentation of the Management-API for warehouses is hosted at the unprotected `/swagger-ui` endpoint.

Following options are global and apply to all warehouses:

### General

Previous to Lakekeeper Version `0.5.0` please prefix all environment variables with `ICEBERG_REST__` instead of `LAKEKEEPER__`.

| Variable                             | Example                    | Description                                                                                                                                                                                                                                                               |
|--------------------------------------|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `LAKEKEEPER__BASE_URI`               | `https://example.com:8080` | Base URL where the catalog is externally reachable. Default: `https://localhost:8080`                                                                                                                                                                                     |
| `LAKEKEEPER__ENABLE_DEFAULT_PROJECT` | `true`                     | If `true`, the NIL Project ID ("00000000-0000-0000-0000-000000000000") is used as a default if the user does not specify a project when connecting. This option is enabled by default, which we recommend for all single-project (single-tenant) setups. Default: `true`. |
| `LAKEKEEPER__RESERVED_NAMESPACES`    | `system,examples`          | Reserved Namespaces that cannot be created via the REST interface                                                                                                                                                                                                         |
| `LAKEKEEPER__METRICS_PORT`           | `9000`                     | Port where the metrics endpoint is reachable. Default: `9000`                                                                                                                                                                                                             |
| `LAKEKEEPER__LISTEN_PORT`            | `8080`                     | Port the server listens on. Default: `8080`                                                                                                                                                                                                                               |
| `LAKEKEEPER__SECRET_BACKEND`         | `postgres`                 | The secret backend to use. If `kv2` is chosen, you need to provide additional parameters found under []() Default: `postgres`, one-of: [`postgres`, `kv2`]                                                                                                                |

### Self-signed certificates in dependencies (e.g. minio)

You may be running Lakekeeper in your own environment which uses self-signed certificates for e.g. minio. Lakekeeper is built with reqwest's `rustls-tls-native-roots` feature activated, this means `SSL_CERT_FILE` and `SSL_CERT_DIR` are respected. If both are not set, the system's default CA store is used. If you want to use a custom CA store, set `SSL_CERT_FILE` to the path of the CA file or `SSL_CERT_DIR` to the path of the CA directory. The certificate used by the server cannot be a CA. It needs to be an end entity certificate, else you may run into `CaUsedAsEndEntity` errors.

### Task queues

Currently, the catalog uses two task queues, one to ultimately delete soft-deleted tabulars and another to purge tabulars which have been deleted with the `purgeRequested=True` query parameter. The task queues are configured as follows:

| Variable                                  | Example | Description                                                                                                 |
|-------------------------------------------|---------|-------------------------------------------------------------------------------------------------------------|
| `LAKEKEEPER__QUEUE_CONFIG__MAX_RETRIES`   | 5       | Number of retries before a task is considered failed  Default: 5                                            |
| `LAKEKEEPER__QUEUE_CONFIG__MAX_AGE`       | 3600    | Amount of seconds before a task is considered stale and could be picked up by another worker. Default: 3600 |
| `LAKEKEEPER__QUEUE_CONFIG__POLL_INTERVAL` | 10      | Amount of seconds between polling for new tasks. Default: 10                                                |

The queues are currently implemented using the `sqlx` Postgres backend. If you want to use a different backend, you need to implement the `TaskQueue` trait.

### Postgres

Configuration parameters if Postgres is used as a backend, you may either provide connection strings or use the `PG_*` environment variables, connection strings take precedence:

| Variable                                  | Example                                               | Description                                                                                                                              |
|-------------------------------------------|-------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `LAKEKEEPER__PG_DATABASE_URL_READ`        | `postgres://postgres:password@localhost:5432/iceberg` | Postgres Database connection string used for reading                                                                                     |
| `LAKEKEEPER__PG_DATABASE_URL_WRITE`       | `postgres://postgres:password@localhost:5432/iceberg` | Postgres Database connection string used for writing.                                                                                    |
| `LAKEKEEPER__PG_ENCRYPTION_KEY`           | `<This is unsafe, please set a proper key>`           | If `LAKEKEEPER__SECRET_BACKEND=postgres`, this key is used to encrypt secrets. It is required to change this for production deployments. |
| `LAKEKEEPER__PG_READ_POOL_CONNECTIONS`    | `10`                                                  | Number of connections in the read pool                                                                                                   |
| `LAKEKEEPER__PG_WRITE_POOL_CONNECTIONS`   | `5`                                                   | Number of connections in the write pool                                                                                                  |
| `LAKEKEEPER__PG_HOST_R`                   | `localhost`                                           | Hostname for read operations                                                                                                             |
| `LAKEKEEPER__PG_HOST_W`                   | `localhost`                                           | Hostname for write operations                                                                                                            |
| `LAKEKEEPER__PG_PORT`                     | `5432`                                                | Port number                                                                                                                              |
| `LAKEKEEPER__PG_USER`                     | `postgres`                                            | Username for authentication                                                                                                              |
| `LAKEKEEPER__PG_PASSWORD`                 | `password`                                            | Password for authentication                                                                                                              |
| `LAKEKEEPER__PG_DATABASE`                 | `iceberg`                                             | Database name                                                                                                                            |
| `LAKEKEEPER__PG_SSL_MODE`                 | `require`                                             | SSL mode (disable, allow, prefer, require)                                                                                               |
| `LAKEKEEPER__PG_SSL_ROOT_CERT`            | `/path/to/root/cert`                                  | Path to SSL root certificate                                                                                                             |
| `LAKEKEEPER__PG_ENABLE_STATEMENT_LOGGING` | `true`                                                | Enable SQL statement logging                                                                                                             |
| `LAKEKEEPER__PG_TEST_BEFORE_ACQUIRE`      | `true`                                                | Test connections before acquiring from the pool                                                                                          |
| `LAKEKEEPER__PG_CONNECTION_MAX_LIFETIME`  | `1800`                                                | Maximum lifetime of connections in seconds                                                                                               |

### KV2 (HCP Vault)

Configuration parameters if a KV2 compatible storage is used as a backend. Currently, we only support the `userpass` authentication method. You may provide the envs as single values like `LAKEKEEPER__KV2__URL=http://vault.local` etc. or as a compound value like:
`LAKEKEEPER__KV2='{url="http://localhost:1234", user="test", password="test", secret_mount="secret"}'`

| Variable                        | Example               | Description                                      |
|---------------------------------|-----------------------|--------------------------------------------------|
| `LAKEKEEPER__KV2__URL`          | `https://vault.local` | URL of the KV2 backend                           |
| `LAKEKEEPER__KV2__USER`         | `admin`               | Username to authenticate against the KV2 backend |
| `LAKEKEEPER__KV2__PASSWORD`     | `password`            | Password to authenticate against the KV2 backend |
| `LAKEKEEPER__KV2__SECRET_MOUNT` | `kv/data/iceberg`     | Path to the secret mount in the KV2 backend      |

### Nats

If you want the server to publish events to a NATS server, set the following environment variables:

| Variable                      | Example                 | Description                                                          |
|-------------------------------|-------------------------|----------------------------------------------------------------------|
| `LAKEKEEPER__NATS_ADDRESS`    | `nats://localhost:4222` | The URL of the NATS server to connect to                             |
| `LAKEKEEPER__NATS_TOPIC`      | `iceberg`               | The subject to publish events to                                     |
| `LAKEKEEPER__NATS_USER`       | `test-user`             | User to authenticate against nats, needs `LAKEKEEPER__NATS_PASSWORD` |
| `LAKEKEEPER__NATS_PASSWORD`   | `test-password`         | Password to authenticate against nats, needs `LAKEKEEPER__NATS_USER` |
| `LAKEKEEPER__NATS_CREDS_FILE` | `/path/to/file.creds`   | Path to a file containing nats credentials                           |
| `LAKEKEEPER__NATS_TOKEN`      | `xyz`                   | Nats token to authenticate against server                            |

### OpenID Connect

If you want to limit ac
cess to the API, set `LAKEKEEPER__OPENID_PROVIDER_URI` to the URI of your OpenID Connect Provider. The catalog will then verify access tokens against this provider. The provider must have the `.well-known/openid-configuration` endpoint under `${LAKEKEEPER__OPENID_PROVIDER_URI}/.well-known/openid-configuration` and the openid-configuration needs to have the `jwks_uri` and `issuer` defined.

If `LAKEKEEPER__OPENID_PROVIDER_URI` or `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is set, every request needs have an authorization header, e.g.

```sh
curl {your-catalog-url}/catalog/v1/transactions/commit -X POST -H "authorization: Bearer {your-token-here}" -H "content-type: application/json" -d ...
```

| Variable                                       | Example                                      | Description                                                                                                                                                                                                                                                  |
|------------------------------------------------|----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `LAKEKEEPER__OPENID_PROVIDER_URI`              | `https://keycloak.local/realms/{your-realm}` | OpenID Provider URL, with keycloak this is the url pointing to your realm, for Azure App Registration it would be something like `https://login.microsoftonline.com/{your-tenant-id-here}/v2.0/`. If this variable is not set, endpoints are **not** secured |
| `LAKEKEEPER__OPENID_AUDIENCE`                  | `the-client-id-of-my-app`                    | If set, the `aud` of the provided token must match the value provided.                                                                                                                                                                                       |
| `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` | true                                         | If true, kubernetes service accounts can authenticate using their tokens to Lakekeeper. This option is compatible with `LAKEKEEPER__OPENID_PROVIDER_URI` - multiple IdPs (OIDC and kubernetes) can be enabled simultaneously.                                |

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[semi-done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChangesGrey.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
