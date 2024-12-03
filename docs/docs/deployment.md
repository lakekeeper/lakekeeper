# Deployment

To get started quickly with the latest version of Lakekeeper check our [Getting Started](../../getting-started.md).

## Overview 

Lakekeeper is an implementation of the Apache Iceberg REST Catalog API.  Lakekeeper depends on the following, partially optional, external dependencies:

<figure markdown="span">
  ![Lakekeeper Overview](../../assets/interfaces-v1.svg){ width="100%" }
  <figcaption>Connected systems. Green boxes are recommended for production.</figcaption>
</figure>

* **Persistence Backend / Catalog** (required): We currently support only Postgres, but plan to expand our support to more Databases in the future.
* **Warehouse Storage** (required): When a new Warehouse is created, storage credentials are required.
* **Identity Provider** (optional): Lakekeeper can Authenticate incoming requests using any OIDC capable Identity Provider (IdP). Lakekeeper can also natively authenticate kubernetes service accounts.
* **Authorization System** (optional): For permission management, Lakekeeper uses the wonderful [OpenFGA](http://openfga.dev) Project. OpenFGA is automatically deployed in our docker-compose and helm installations. Authorization can only be used if Lakekeeper is connected to an Identity Provider.
* **Secret Store** (optional): By default, Lakekeeper stores all secrets (i.e. S3 access credentials) encrypted in the Persistence Backend. To increase security, Lakekeeper can also use external systems to store secrets. Currently all Hashicorp-Vault like stores are supported.
* **Event Store** (optional): Lakekeeper can send Change Events to an Event Store. Currently [Nats](http://nats.io) is supported, we are working on support for [Apache Kafka](http://kafka.apache.org)
* **Data Contract System** (optional): Lakekeeper can interface with external data contract systems to prohibit breaking changes to your tables.

## 🐳 Docker

Deploy Lakekeeper using Docker Compose for a quick and easy setup. This method is ideal for local development and testing as well as smaller deployments that don't require high availability. Please check our [Examples](ToDo) for simple standalone deployments that come with batteries included (Identity Provider, Storage (S3), Spark, Jupyter) but are not accessible (by default) for compute outside of the docker network. For real-world deployments that are usable for external compute, please continue here.

To run Lakekeeper with Authentication and Authorization an external Identity Provider is required. Please check the [Authentication Guide](./authentication.md) for more information.

=== "With Authentication & Authorization"

    ```bash
    git clone https://github.com/lakekeeper/lakekeeper
    cd docker-compose
    export LAKEKEEPER__OPENID_PROVIDER_URI="<open-id-provider-url>"
    docker-compose up -d
    ```

=== "Without Authentication"

    ```bash
    git clone https://github.com/lakekeeper/lakekeeper
    cd docker-compose
    docker-compose up -d
    ```

The services are now starting up and running in the background. To stop all services run:
```shell
docker compose stop
```


## ☸️ Kubernetes
Deploy Lakekeeper on Kubernetes for a scalable and production-ready setup. Use the provided Helm chart for easy deployment.

```bash
helm repo add lakekeeper https://lakekeeper.github.io/lakekeeper-charts/
helm install my-lakekeeper lakekeeper/lakekeeper
```

Please check the [Helm Chart](https://github.com/lakekeeper/lakekeeper-charts/tree/main/charts/lakekeeper) and its [values.yaml](https://github.com/lakekeeper/lakekeeper-charts/blob/main/charts/lakekeeper/values.yaml) for configuration options.
