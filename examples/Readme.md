# How to run Examples

> [!IMPORTANT]  
> Our examples are not designed to be used with compute outside of the docker network (e.g. external Spark). For production deployments external object storage is required. Please check our [docs](http://docs.lakekeeper.io) for more information

All examples are self-contained and run all services required for the specific scenarios inside of docker compose. To run each scenario, you need to have `docker` installed on your machine. Most docker distributes come with built-in `docker compose` today.

Currently two szenarios are available:

## Minimal
Runs Lakekeeper without Authentication and Authorization (unprotected). The example contains Jupyter (with Spark), Trino and Starrocks as query engines, minio as storage and Lakekeeper connected to a Postgres database.

To run the example run the following commands:

```bash
cd examples/minimal
docker compose up
```
Now open in your Browser:
* Jupyter: [`http://localhost:8888`](`http://localhost:8888`)
* Lakekeeper UI: [`http://localhost:8181`](`http://localhost:8181`)


## Access Control
This example demonstrates how Authentication and Authorization works. The example contains Jupyter with Spark as query engines, OpenFGA as Authorization backend and Keycloak as IdP. As Lakekeeper validates the issuer URL of tokens, it is not possible to login to the Lakekeeper UI with the browser of your local machine, as Keycloak is visible for Lakekeeper under a different URL inside docker (`keycloak:8080` instead of `localhost:30080`).

Run the example with the following command:
```bash
cd examples/access-control
docker compose up
```

Now open in your Browser:
* Jupyter: [http://localhost:8888](http://localhost:8888)
* Keycloak UI: [http://localhost:30080](http://localhost:30080)

You can also login to Keycloak using:
* Username: admin
* Password: admin

Keycloak also contains a non-admin user:
* Username: Peter
* Password: Iceberg


## Development / Re-Build image
Running `docker compose up` starts the `latest-main` release of Lakekeeper. To build a fresh image use:

```bash
docker compose -f docker-compose.yaml -f docker-compose-build.yaml up --build
```
