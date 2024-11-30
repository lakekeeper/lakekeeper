# Lakekeeper

Lakekeeper is a **secure**, **fast** and **easy to use** Apache Iceberg REST Catalog implementation written in Rust.
Start by reading the [quickstart tutorial], then check the [User Guide] for more information.

[quickstart tutorial]: getting-started.md
[User Guide]: user-guide/README.md

<div class="text-center">
<a href="getting-started/" class="btn btn-primary" role="button">Getting Started</a>
<a href="user-guide/" class="btn btn-primary" role="button">User Guide</a>
</div>

<div class="pt-2 pb-4 px-4 my-4 bg-body-tertiary rounded-3">
<h2 class="display-4 text-center">Features</h2>

<div class="row">
  <div class="col-sm-6">
    <div class="card mb-4">
      <div class="card-body">
        <h3 class="card-title"><a href="#architecture">Architecture</a></h3>
        <p class="card-text">
            Learn more about the way Lakekeeper works. Lakekeeper can support anything from standalone single Project deployments for small companies up to distributed multi-tenant setups.
        </p>
      </div>
    </div>
  </div>
  <div class="col-sm-6">
    <div class="card mb-4">
      <div class="card-body">
        <h3 class="card-title"><a href="getting-started">Getting Started</a></h3>
        <p class="card-text">
            Get started quickly with Lakekeeper by using our binary, docker-compose or our helm chart.
        </p>
      </div>
    </div>
  </div>
</div>

<div class="row">
  <div class="col-sm-6">
    <div class="card">
      <div class="card-body">
        <h3 class="card-title"><a href="user-guide">User Guide</a></h3>
        <p class="card-text">
            Learn how to configure and use Lakekeeper - from configuring a persistent storage backend to connecting your own IdP.
        </p>
      </div>
    </div>
  </div>
  <div class="col-sm-6">
    <div class="card">
      <div class="card-body">
        <h3 class="card-title">Host anywhere</h3>
        <p class="card-text">
            ...
        </p>
      </div>
    </div>
  </div>
</div>
</div>

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


