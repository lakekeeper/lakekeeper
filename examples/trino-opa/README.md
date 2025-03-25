## Trino Open Policy Agent (OPA) Bridge
This example demonstrates how a shared multi-user trino can enforce permissions for individual users using [trino's OPA Connector](https://trino.io/docs/current/security/opa-access-control.html). 

The example contains in addition to Lakekeeper:
* trino with OAuth2 Authentication and OPA access control enabled (check the [`trino`](./trino/) folder for configuration)
* Jupyter with built-in spark
* OpenFGA as Authorization backend for Lakekeeper and Keycloak as IdP
* OPA connected to Lakekeeper. OPA itself is completely stateless
* nginx (called "trino-proxy") that sets the `X-Forwarded-Proto https` header, as trino in OAuth2 settings requires encrypted connections.

Run the example with the following command:
```bash
cd examples/trino-opa
docker compose up
```

Now open your Browser and open Jupyter at [http://localhost:8888](http://localhost:8888). First run the `01-Bootstrap.ipynb` and `02-Create-Warehouse.ipynb` Notebooks. Finally follow the instructions in `03-Multi-User-Trino.ipynb` (**DON'T JUST RUN ALL!**).

The following additional sites are available
Now open your Browser:
* Trino UI: [https://localhost/ui/](https://localhost/ui/)
* Jupyter: [http://localhost:8888](http://localhost:8888)
* Keycloak UI: [http://localhost:30080](http://localhost:30080)
* Swagger UI: [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/) (Note that more endpoints are available than in the Minimal example as permissions are enabled)
* Lakekeeper UI (**Don't use this for bootstrapping**. Use the designated Notebook instead. Bootstrapping sets the initial admin user, which needs to be our technical user for the examples to work correctly.): [http://localhost:8181](http://localhost:8181)

Start by following the instructions in the `01-Bootstrap.ipynb` Notebook in Jupyter. After that, you can login to the [UI](http://localhost:8181) as:
* Username: `peter`
* Password: `iceberg`

A second user is also available which initially has no permissions:
* Username: `anna`
* Password: `iceberg`

You can also login to Keycloak using:
* Username: admin
* Password: admin

The Keycloak Realm "iceberg" is pre-configured.
