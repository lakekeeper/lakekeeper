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


Please visit [https://docs.lakekeeper.io](https://docs.lakekeeper.io) for Documentation!

This is Lakekeeper: An Apache-Licensed, **secure**, **fast** and **easy to use**  implementation of the [Apache Iceberg](https://iceberg.apache.org/) REST Catalog specification based on [apache/iceberg-rust](https://github.com/apache/iceberg-rust). If you have questions, feature requests or just want a chat, we are hanging around in [Discord](https://discord.gg/jkAGG8p93B)!

<p align="center">
<img src="https://github.com/lakekeeper/lakekeeper/raw/main/assets/Lakekeeper-Overview.png" width="500">
</p>

# Quickstart

A Docker Container is available on [quay.io](https://quay.io/repository/lakekeeper/lakekeeper?tab=info).
We have prepared a minimal docker-compose file to demonstrate how to use the Lakekeeper catalog with common query engines.

```sh
git clone https://github.com/lakekeeper/lakekeeper.git
cd lakekeeper/examples/minimal
docker compose up
```

Then open your browser and head to [localhost:8888](localhost:8888) to load the example Jupyter notebooks or head to [localhost:8181](localhost:8181) for the Lakekeeper UI.

For more information on deployment, please check the [Getting Started Guide](https://docs.lakekeeper.io/getting-started/).

# Scope and Features

The Iceberg Catalog REST interface has become the standard for catalogs in open Lakehouses. It natively enables multi-table commits, server-side deconflicting and much more. It is figuratively the (**TIP**) of the Iceberg.

- **Written in Rust**: Single all-in-one binary - no JVM or Python env required.
- **Storage Access Management**: Lakekeeper secures access to your data using Vended-Credentials and remote signing for S3. All major Hyperscalers (AWS, Azure, GCP) as well as on-premise deployments with S3 are supported.
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

| Operation | Status  | Description                                            |
|-----------|:-------:|--------------------------------------------------------|
| Namespace | ![done] | All operations implemented                             |
| Table     | ![done] | All operations implemented - additional integration tests in development |
| Views     | ![done] | Remove unused files and log entries                    |
| Metrics   | ![open] | Endpoint is available but doesn't store the metrics    |

### Storage Profile Support

| Storage              |    Status    | Comment                                |
|----------------------|:------------:|----------------------------------------|
| S3 - AWS             | ![semi-done] | vended-credentials & remote-signing, assume role missing |
| S3 - Custom          |   ![done]    | vended-credentials & remote-signing, tested against Minio |
| Azure ADLS Gen2      |   ![done]    |                                        |
| Azure Blob           |   ![open]    |                                        |
| Microsoft OneLake    |   ![open]    |                                        |
| Google Cloud Storage |   ![done]    |                                        |

Details on how to configure the storage profiles can be found in the [Docs](https://docs.lakekeeper.io).

### Supported Catalog Backends

| Backend  | Status  | Comment |
|----------|:-------:|---------|
| Postgres | ![done] | \>=15   |
| MongoDB  | ![open] |         |

### Supported Secret Stores

| Backend         | Status  | Comment       |
| --------------- | :-----: | ------------- |
| Postgres        | ![done] |               |
| kv2 (hcp-vault) | ![done] | userpass auth |

### Supported Event Stores

| Backend | Status  | Comment |
| ------- | :-----: | ------- |
| Nats    | ![done] |         |
| Kafka   | ![done] |         |

### Supported Operations - Management API

| Operation            | Status  | Description                                 |
|----------------------|:-------:|---------------------------------------------|
| Warehouse Management | ![done] | Create / Update / Delete a Warehouse        |
| AuthZ                | ![open] | Manage access to warehouses, namespaces and tables |
| More to come!        | ![open] |                                             |

### Auth(N/Z) Handlers

| Operation       | Status  | Description                                      |
|-----------------|:-------:|--------------------------------------------------|
| OIDC (AuthN)    | ![done] | Secure access to the catalog via OIDC            |
| Custom (AuthZ)  | ![done] | If you are willing to implement a single rust Trait, the `AuthZHandler` can be implement to connect to your system |
| OpenFGA (AuthZ) | ![open] | Internal Authorization management                |


## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[semi-done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChangesGrey.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
