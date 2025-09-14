# Kafka + Trino + Lakehouse Demo

This project provides a Kubernetes-based demo environment for running:
- **Apache Kafka** with Strimzi
- **Trino** for interactive queries
- **Lakekeeper** for metadata/catalog management
- Supporting storage systems (**MinIO & Postgres**)
- A sample **producer application**
- **Kafka Connect** with Iceberg Sink Connector v1.9.2

The environment is provisioned using a **Kind (Kubernetes in Docker)** cluster.

## Prerequisites

- Docker
- [Kind](https://kind.sigs.k8s.io/)
- kubectl
- GNU Make


## Usage

All commands are run via `make`.

### Start everything (cluster + services)
```bash
make all
```
This will:
- Create the Kind cluster (if it doesn’t exist)
- Build Docker images (producer + Kafka Connect)
- Install Trino
- Install storage services (MinIO, Postgres)
- Install Kafka (Strimzi, brokers, topics, schema registry, UI, connect)
- Install Lakekeeper catalog services

### Send dummy data
```bash
make run-producer
```

### Tear everything down
```bash
make destroy
```
Deletes the namespace and the Kind cluster.

## Cleanup

- `make delete-cluster` – delete only the Kind cluster
- `make delete-namespace` – delete the `kafka` namespace
- `make destroy` – delete both namespace and cluster
