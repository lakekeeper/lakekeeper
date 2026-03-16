# Open Policy Agent (OPA)
[Lakekeeper's Open Policy Agent bridge](https://github.com/lakekeeper/lakekeeper/tree/main/authz/opa-bridge) enables compute engines that support fine-grained access control via Open Policy Agent (OPA) as authorization engine to respect privileges in Lakekeeper. We have also prepared a self-contained [Docker Compose Example](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control-advanced) to get started quickly.

Let's imagine we have a trusted multi-user query engine such as trino, in addition to single-user query engines like pyiceberg or daft in Jupyter Notebooks. Managing permissions in trino independently of the other tools is not an option, as we do not want to duplicate permissions across query engines. Our multi-user query engine has two options:

1. **Catalog enforces permissions**: The engine contacts the Catalog on behalf of the user. To achieve this, the engine must be able to impersonate the user for the catalog application. In OAuth2 settings, this can be accomplished through downscoping tokens or other forms of Token Exchange.
2. **Compute enforces permissions**: After contacting the catalog with a god-like "I can do everything!" user (e.g. `project_admin`), the query engine then contacts the permission system, retrieves, and enforces those permissions. Note that this requires the engine to run in a trusted environment, as whoever has root access to the engine also has access to the god-like credential.

The Lakekeeper OPA Bridge enables solution 2, by exposing all permissions in Lakekeeper via OPA. The Bridge itself is a collection of OPA files in the `authz/opa-bridge` folder of the Lakekeeper GitHub repository.

The bridge also comes with a translation layer for trino to translate trino to Lakekeeper permissions and thus serve trinos OPA queries. Currently trino is the only iceberg query engine we are aware of that is flexible enough to honor external permissions via OPA. Please [let us know](https://github.com/lakekeeper/lakekeeper/issues/new/choose) if you are aware of other engines, so that we can add support.

## Configuration
Lakekeeper's OPA bridge needs to access the permissions API of Lakekeeper. As such, we need a technical user for OPA (Client ID, Client Secret) that OPA can use to authenticate to Lakekeeper. Please check the [Authentication guide](./authentication.md) for more information on how to create technical users. We recommend to use the same user for creating the catalog in trino to ensure same access. In most scenarios, this user should have the `project_admin` role.

The plugin can be customized by either editing the `configuration.rego` file or by setting environment variables. By editing the `configuration.rego` files you can also easily connect multiple lakekeeper instances to the same trino instance. Please find all available configuration options explained in the file.

### Lakekeeper Connection
If configuration is done via environment variables, the following settings are available:

| Variable                                 | Example                                                             | Description |
|------------------------------------------|---------------------------------------------------------------------|-----|
| <nobr>`LAKEKEEPER_URL`</nobr>            | <nobr>`https://lakekeeper.example.com`<nobr>                        | URL where lakekeeper is externally reachable. Default: `https://localhost:8181` |
| <nobr>`LAKEKEEPER_TOKEN_ENDPOINT`</nobr> | `http://keycloak:8080/realms/iceberg/protocol/openid-connect/token` | Token endpoint of the IdP used to secure Lakekeeper. This endpoint is used to exchange OPAs client credentials for an access token. |
| <nobr>`LAKEKEEPER_CLIENT_ID`</nobr>      | `trino`                                                             | Client ID used by OPA to access Lakekeeper's permissions API. |
| <nobr>`LAKEKEEPER_CLIENT_SECRET`</nobr>  | `abcd`                                                              | Client Secret for the Client ID. |
| <nobr>`LAKEKEEPER_SCOPE`</nobr>          | `lakekeeper`                                                        | Scopes to request from the IdP. Defaults to `lakekeeper`. Please check the [Authentication Guide](./authentication.md) for setup. |

### Catalog Mapping
All above mentioned configuration options refer to a specific Lakekeeper instance. What is missing is a mapping of trino catalogs to Lakekeeper warehouses. By default we support 4 catalogs in trino, but more can easily be added in the `configuration.rego`.

| Variable                                       | Example                   | Description |
|------------------------------------------------|---------------------------|-----|
| <nobr>`TRINO_DEV_CATALOG_NAME`</nobr>          | <nobr>`dev`<nobr>         | Name of the development catalog in trino. Default: `dev` |
| <nobr>`LAKEKEEPER_DEV_WAREHOUSE`</nobr>        | <nobr>`development`<nobr> | Name of the development warehouse in lakekeeper that corresponds to the `TRINO_DEV_CATALOG_NAME` catalog in trino. Default: `development` |
| <nobr>`TRINO_PROD_CATALOG_NAME`</nobr>         | <nobr>`prod`<nobr>        | Name of the production catalog in trino. Default: `prod` |
| <nobr>`LAKEKEEPER_PROD_WAREHOUSE`</nobr>       | <nobr>`production`<nobr>  | Name of the production warehouse in lakekeeper that corresponds to the `TRINO_PROD_CATALOG_NAME` catalog in trino. Default: `production` |
| <nobr>`TRINO_DEMO_CATALOG_NAME`</nobr>         | <nobr>`demo`<nobr>        | Name of the demo catalog in trino. Default: `demo` |
| <nobr>`LAKEKEEPER_DEMO_WAREHOUSE`</nobr>       | <nobr>`demo`<nobr>        | Name of the demo warehouse in lakekeeper that corresponds to the `TRINO_DEMO_CATALOG_NAME` catalog in trino. Default: `demo` |
| <nobr>`TRINO_LAKEKEEPER_CATALOG_NAME`</nobr>   | <nobr>`lakekeeper`<nobr>  | Name of the lakekeeper catalog in trino. Default: `lakekeeper` |
| <nobr>`LAKEKEEPER_LAKEKEEPER_WAREHOUSE`</nobr> | <nobr>`lakekeeper`<nobr>  | Name of the lakekeeper warehouse in lakekeeper that corresponds to the `TRINO_LAKEKEEPER_CATALOG_NAME` catalog in trino. Default: `lakekeeper` |

### Unmanaged Catalogs

| Variable                                       | Example | Description |
|------------------------------------------------|---------|-----|
| <nobr>`TRINO_ALLOW_UNMANAGED_CATALOGS`</nobr> | `true`  | Allow access to catalogs not listed in the `trino_catalog` array. When trino has multiple authorizers configured, ALL authorizers must allow an action for it to succeed. If trino uses catalogs managed by other authorizers (e.g. a connected PostgreSQL catalog), set this to `true` so the OPA bridge does not block access to those catalogs. Default: `false` |

### Admin Users
Admin users can be configured in the `trino_admin_users` list in `configuration.rego`. Admin users get full access to all system schemas and tables across all catalogs (including `system.metadata`, `system.runtime`, etc.) and can view queries owned by any user (`FilterViewQueryOwnedBy`, `ViewQueryOwnedBy`). Non-admin users can only view their own queries.

```rego
trino_admin_users := [
    "admin-user-uuid-here",
]
```

Specify Trino user IDs, which are typically OIDC subject identifiers (UUIDs).

### Trino Configuration
When OPA is running and configured, set the following configurations for trino in `access-control.properties`:
```yaml
access-control.name=opa
opa.policy.uri=http://<URL where OPA is reachable>/v1/data/trino/allow
opa.log-requests=true
opa.log-responses=true
opa.policy.batched-uri=http://<URL where OPA is reachable>/v1/data/trino/batch
```

## System Schema Handling
The OPA bridge distinguishes between user-created schemas (namespaces) and system schemas. User schemas are authorized via Lakekeeper's permission system, while system schemas are handled locally by the bridge.

### Trino `system` Catalog
The following schemas in the trino `system` catalog are accessible to all authenticated users:

| Schema | Allowed Tables | Description |
|--------|----------------|-------------|
| `jdbc` | all | Required by JDBC clients for metadata discovery. |
| `information_schema` | `columns`, `schemata`, `tables`, `views` | Standard SQL metadata tables. |
| `metadata` | `analyze_properties`, `catalogs`, `column_properties`, `schema_properties`, `table_comments`, `table_properties` | Catalog metadata. Tables like `*_authorization` are excluded for non-admins. |
| `runtime` | `queries` | Query monitoring. Non-admins can only see their own queries. |

Admin users have unrestricted access to all tables in all system schemas.

### Lakekeeper Catalog System Schemas
Within Lakekeeper-managed catalogs, the following schemas are treated as system schemas and require only catalog-level (`get_config`) access instead of namespace-level permissions:

| Schema | Allowed Tables | Description |
|--------|----------------|-------------|
| `information_schema` | `columns`, `schemata`, `tables`, `views` | Standard SQL metadata. |
| `schema_discovery` | `discovery`, `shallow_discovery` | Schema discovery for UI tools. |
| `system` | `iceberg_tables` | Iceberg table metadata. |

User-created schemas are authorized through Lakekeeper's permission system as usual.

## Context Forwarding
The OPA bridge forwards resource names to Lakekeeper's batch-check API for create actions. This enables Lakekeeper's authorizer (e.g. Cedar) to make authorization decisions based on the name of the resource being created:

| Trino Operation | Lakekeeper Action | Name Forwarded |
|-----------------|-------------------|----------------|
| `CreateSchema` (top-level) | `create_namespace` (warehouse) | Schema name |
| `CreateSchema` (nested) | `create_namespace` (parent namespace) | Child schema name |
| `CreateTable` | `create_table` (namespace) | Table name |
| `CreateView` / `CreateMaterializedView` | `create_view` (namespace) | View name |

Properties specified during creation (e.g. `WITH (format='PARQUET')`) are also forwarded.

A full self-contained example is [available on GitHub](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control-advanced).
