# How to run Examples

> [!IMPORTANT]  
> Our examples are not designed to be used with compute outside of the docker network (e.g. external Spark). For production deployments external object storage is required. Please check our [docs](http://docs.lakekeeper.io) for more information

All examples are self-contained and run all services required for the specific scenarios inside of docker compose. To run each scenario, you need to have `docker` installed on your machine. Most docker distributes come with built-in `docker compose` today.

After starting the examples, please wait for a Minute after all images are pulled - especially keycloak takes some time to start and setup.

Currently two szenarios are available:

## Minimal
Runs Lakekeeper without Authentication and Authorization (unprotected). The example contains Jupyter (with Spark), Trino and Starrocks as query engines, Minio as storage and Lakekeeper connected to a Postgres database.

To run the example run the following commands:

```bash
cd examples/minimal
docker compose up
```
Now open in your Browser:
* Jupyter: [http://localhost:8888](http://localhost:8888)
* Lakekeeper UI: [http://localhost:8181](http://localhost:8181)
* Swagger UI: [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)


## Access Control
This example demonstrates how Authentication and Authorization works. The example contains Jupyter with Spark as query engines, OpenFGA as Authorization backend and Keycloak as IdP.

Run the example with the following command:
```bash
cd examples/access-control
docker compose up
```

Now open in your Browser:
* Jupyter: [http://localhost:8888](http://localhost:8888)
* Keycloak UI: [http://localhost:30080](http://localhost:30080)
* Swagger UI: [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/) (Note that more endpoints are available than in the Minimal example as permissions are enabled)
* Lakekeeper UI (DON'T USE IT FOR BOOTSTRAPPING, There is a Notebook for it.): [http://localhost:8181](http://localhost:8181)

Start by running the `01-Bootstrap.ipynb` Notebook. After that, you can login to the [UI](http://localhost:8181) as:
* Username: `peter`
* Password: `iceberg`

You can also login to Keycloak using:
* Username: admin
* Password: admin

The Keycloak Ream "iceberg" is pre-configured.

## Development / Re-Build image
Running `docker compose up` starts the `latest-main` release of Lakekeeper. To build a fresh image use:

```bash
docker compose -f docker-compose.yaml -f docker-compose-build.yaml up --build
```
