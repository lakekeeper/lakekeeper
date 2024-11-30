# Lakekeeper

Lakekeeper is a **secure**, **fast** and **easy to use** Apache Iceberg REST Catalog implementation written in Rust.
Start by reading the [quickstart tutorial](getting-started.md), then check the [documentation](docs/nightly/) for more information.

## Features

### Architecture

Learn more about the way Lakekeeper works. Lakekeeper can support anything from standalone single Project deployments for small companies up to distributed multi-tenant setups.

### Getting Started

Get started quickly with Lakekeeper by using our binary, docker-compose or our helm chart.

### User Guide

Learn how to configure and use Lakekeeper - from configuring a persistent storage backend to connecting your own IdP.

### Host anywhere

...

---

# Overview

Lakekeeper is an implementation of the Apache Iceberg REST Catalog API. Currently Lakekeeper depends on the following external dependencies:

* **Persistence Backend / Catalog** (required): We currently support only Postgres, but plan to expand our support to more Databases in the future.
* **Warehouse Storage** (required): On creation of a [Warehouse](Todo: Entity Warehouse Link), 
* **Identity Provider** (optional): Lakekeeper can Authenticate incoming requests using any OIDC capable Identity Provider (IdP).
* **Authorization System** (optional): For permission management, Lakekeeper uses the wonderful [OpenFGA](http://openfga.dev) Project. OpenFGA is automatically deployed in our docker-compose and helm installations. Authorization can only be used if Lakekeeper is connected to an Identity Provider.
* **Secret Store** (optional): By default, Lakekeeper stores all secrets (i.e. S3 access credentials) encrypted in the Persistence Backend. To increase security, Lakekeeper can also use external systems to store secrets. Currently all Hashicorp-Vault like stores are supported.
* **Event Store** (optional): Lakekeeper can send Change Events to an Event Store. Currently [Nats](http://nats.io) is supported, we are working on support for [Apache Kafka](http://kafka.apache.org)
* **Data Contract System** (optional): Lakekeeper can interface with external data contract systems to prohibit breaking changes to your tables.

Please check the [User Guide] for information on configuration.

Lakekeeper serves two APIs with different purposes:

1. The Iceberg REST API is served at endpoints prefixed with `/catalog`. External query engines connect to this API to interact with the catalog. Lakekeeper also implements the S3 remote signing API which is hosted at `/<warehouse-id>/v1/aws/s3/sign`. ToDo: Swagger
1. The Lakekeeper Management API is served at endpoints prefixed with `/management`. It is used to configure Lakekeeper and manage entities that are not part of the Iceberg REST Catalog specification, such as permissions.


