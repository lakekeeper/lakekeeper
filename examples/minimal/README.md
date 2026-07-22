# Minimal
Runs Lakekeeper without Authentication and Authorization (unprotected). The example contains Jupyter (with Spark), Trino and Starrocks as query engines, SeaweedFS as storage and Lakekeeper connected to a Postgres database. SeaweedFS runs with its IAM/STS service enabled so Lakekeeper can vend short-lived (STS) credentials to the query engines.

To run the example run the following commands:

```bash
cd examples/minimal
docker compose up
```

Now open your Browser:
* Jupyter: [http://localhost:8888](http://localhost:8888)
* Lakekeeper UI: [http://localhost:8181](http://localhost:8181)
* Swagger UI: [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)

## In-browser queries (DuckDB-WASM / LoQE)

Plain `docker compose up` works for the in-container engines (Trino, Spark, Starrocks), but **not** for the query engine that runs *inside your browser* in the Lakekeeper console. The reason is split-horizon networking: a warehouse's S3 endpoint is signed into every request and vended to clients, so it must be one URL that resolves the same from every client — but the in-container writer reaches SeaweedFS at `seaweedfs:8333` while your browser can only reach the host. Neither `seaweedfs:8333` nor `localhost:8333` satisfies both.

Use the wrapper script, which detects your host LAN IP (reachable from both containers and the browser), injects it into the warehouse endpoint, and applies bucket CORS:

```bash
cd examples/minimal
./up.sh                     # or: HOST_IP=<your-ip> ./up.sh
```

Then open the [Lakekeeper console](http://localhost:8181), attach the `demo` warehouse, and run an in-browser query such as `CREATE TABLE demo.public.t AS SELECT 1 AS x;` followed by `SELECT * FROM demo.public.t;`.

If you previously ran plain `docker compose up`, reset the warehouse first so the new endpoint takes effect: `docker compose down -v && ./up.sh`.
