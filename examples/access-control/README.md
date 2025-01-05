## Access Control
This example demonstrates how Authentication and Authorization works. The example contains Jupyter with Spark as query engines, OpenFGA as Authorization backend, Keycloak as IdP and Minio as storage.

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

Start by following the instructions in the `01-Bootstrap.ipynb` Notebook in Jupyter. After that, you can login to the [UI](http://localhost:8181) as:
* Username: `peter`
* Password: `iceberg`

A second user is also available which initially has no permissions:
* Username: `anna`
* Password: `iceberg`

You can also login to Keycloak using:
* Username: admin
* Password: admin

The Keycloak Ream "iceberg" is pre-configured.
