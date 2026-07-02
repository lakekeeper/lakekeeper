# Lakekeeper APIs

Lakekeeper is a rust-native Apache Iceberg REST Catalog implementation. It exposes three distinct HTTP APIs. An interactive Swagger-UI for the exact Lakekeeper version and configuration you are running is available at `/swagger-ui/#/` (by default [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)).

## The three APIs

* **Iceberg REST Catalog API** (`/catalog/v1/...`) — the standard [Apache Iceberg REST specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml). Query engines (Spark, Trino, PyIceberg, ...) speak this API to create namespaces, commit tables, load metadata, and obtain vended storage credentials. You generally do not call it by hand. Reference: [Catalog API](./api/catalog.md).
* **Management API** (`/management/v1/...`) — Lakekeeper-specific administration that has no equivalent in the Iceberg spec: bootstrapping the server, creating and configuring Warehouses (storage profiles, credentials, soft-delete), managing Projects, Users, Roles, Tasks, and — when Authorization is enabled — permissions. This is what the Lakekeeper UI and your platform automation use. Reference: [Management API](./api/management.md).
* **Data API** (`/lakekeeper/v1/...`) — Lakekeeper's data-plane API for functionality beyond the Iceberg REST spec. Today it serves [Generic Tables](./generic-tables.md) — non-Iceberg formats such as Lance and Delta — including credential vending; it is the home for further data-related functions over time. Reference: [Generic Table API](./api/generic-table.md).

## Management API endpoint groups

All Management endpoints are served under `/management/v1/`. The API is grouped as follows (see Swagger-UI for the exact routes and request/response schemas of your running version):

| Group | Purpose |
|-------|---------|
| `server` | Server info, bootstrapping, and global server configuration. |
| `project` | Create and manage Projects (the top-level tenant boundary). |
| `warehouse` | Create and manage Warehouses: storage profiles, storage credentials, soft-delete/undrop settings, activation status, and format-version policy. |
| `tasks` | List, inspect, and control background tasks (e.g. soft-delete expiration and purge). |
| `user` | Manage Users provisioned in the catalog. |
| `role` | Manage Roles, which are first-class principals that can be granted permissions and assumed. |
| `permissions-openfga` | View and manage fine-grained permissions when the OpenFGA authorizer is enabled. |

## Exploring the APIs

* **Swagger-UI** (interactive, version-accurate): `/swagger-ui/#/` — by default [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/).
* **Reference pages**: [Catalog API](./api/catalog.md), [Management API](./api/management.md), [Generic Table API](./api/generic-table.md).
* **OpenAPI documents**: served by the running server and used to generate clients.

## Try it locally

```bash
git clone https://github.com/lakekeeper/lakekeeper.git
cd lakekeeper/examples/minimal
docker compose up
```

Then open your browser at [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/).
