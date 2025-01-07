# Minimal
Runs Lakekeeper without Authentication and Authorization (unprotected). The example contains Jupyter (with Spark), Trino and Starrocks as query engines, Minio as storage and Lakekeeper connected to a Postgres database.

To run the example run the following commands:

```bash
cd examples/minimal
docker compose up
```

Now open your Browser:
* Jupyter: [http://localhost:8888](http://localhost:8888)
* Lakekeeper UI: [http://localhost:8181](http://localhost:8181)
* Swagger UI: [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)
