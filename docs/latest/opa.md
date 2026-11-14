---
title: "Open Policy Agent (OPA)"
description: "Use the Lakekeeper OPA bridge to enforce catalog permissions in Trino and other engines that delegate access control to Open Policy Agent."
source: "https://docs.lakekeeper.io/docs/latest/opa/"
---

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
| `LAKEKEEPER_URL`            | `https://lakekeeper.example.com`                        | URL where lakekeeper is externally reachable. Default: `https://localhost:8181` |
| `LAKEKEEPER_TOKEN_ENDPOINT` | `http://keycloak:8080/realms/iceberg/protocol/openid-connect/token` | Token endpoint of the IdP used to secure Lakekeeper. This endpoint is used to exchange OPAs client credentials for an access token. |
| `LAKEKEEPER_CLIENT_ID`      | `trino`                                                             | Client ID used by OPA to access Lakekeeper's permissions API. |
| `LAKEKEEPER_CLIENT_SECRET`  | `abcd`                                                              | Client Secret for the Client ID. |
| `LAKEKEEPER_SCOPE`          | `lakekeeper`                                                        | Scopes to request from the IdP. Defaults to `lakekeeper`. Please check the [Authentication Guide](./authentication.md) for setup. |
| `LAKEKEEPER_MAX_BATCH_CHECK_SIZE` | `1000`                                                       | Maximum number of checks per `batch-check` HTTP request. Larger values mean fewer HTTP calls but more load on the authorization backend. Default: `1000` |
| `LAKEKEEPER_LIST_EVERYTHING_GRANTS_TABULAR_METADATA` | `false` | Set to `false` to check each table and view, even for users who may list everything. See [Shortcut for broad access](#shortcut-for-broad-access). Default: `true` |

### Catalog Mapping

All above mentioned configuration options refer to a specific Lakekeeper instance. What is missing is a mapping of trino catalogs to Lakekeeper warehouses. By default we support 4 catalogs in trino, but more can easily be added in the `configuration.rego`.

| Variable                                       | Example                   | Description |
|------------------------------------------------|---------------------------|-----|
| `TRINO_DEV_CATALOG_NAME`          | `dev`         | Name of the development catalog in trino. Default: `dev` |
| `LAKEKEEPER_DEV_WAREHOUSE`        | `development` | Name of the development warehouse in lakekeeper that corresponds to the `TRINO_DEV_CATALOG_NAME` catalog in trino. Default: `development` |
| `TRINO_PROD_CATALOG_NAME`         | `prod`        | Name of the production catalog in trino. Default: `prod` |
| `LAKEKEEPER_PROD_WAREHOUSE`       | `production`  | Name of the production warehouse in lakekeeper that corresponds to the `TRINO_PROD_CATALOG_NAME` catalog in trino. Default: `production` |
| `TRINO_DEMO_CATALOG_NAME`         | `demo`        | Name of the demo catalog in trino. Default: `demo` |
| `LAKEKEEPER_DEMO_WAREHOUSE`       | `demo`        | Name of the demo warehouse in lakekeeper that corresponds to the `TRINO_DEMO_CATALOG_NAME` catalog in trino. Default: `demo` |
| `TRINO_LAKEKEEPER_CATALOG_NAME`   | `lakekeeper`  | Name of the lakekeeper catalog in trino. Default: `lakekeeper` |
| `LAKEKEEPER_LAKEKEEPER_WAREHOUSE` | `lakekeeper`  | Name of the lakekeeper warehouse in lakekeeper that corresponds to the `TRINO_LAKEKEEPER_CATALOG_NAME` catalog in trino. Default: `lakekeeper` |

### Unmanaged Catalogs

| Variable                                       | Example | Description |
|------------------------------------------------|---------|-----|
| `TRINO_ALLOW_UNMANAGED_CATALOGS` | `true`  | Blanket-allow access to all catalogs not listed in the `trino_catalog` array. When trino has multiple authorizers configured, ALL authorizers must allow an action for it to succeed. If trino uses catalogs managed by other authorizers (e.g. a connected PostgreSQL catalog), set this to `true` so the OPA bridge does not block access to those catalogs. It does not allow reading tables and views of these catalogs, other than Trino's `system` catalog, inside a DEFINER view, see [Views inside DEFINER views](#views-inside-definer-views). Default: `false`. For fine-grained control over unmanaged catalogs, use the `allow_unmanaged` extension point instead (see below). |

### Admin Users

Admin users get full access to Trino system schemas and tables across all catalogs (including `system.metadata`, `system.runtime`, etc.) and can view queries owned by any user (`FilterViewQueryOwnedBy`, `ViewQueryOwnedBy`). Non-admin users can only view their own queries. Note that this only affects Trino-level authorization — access to data in Lakekeeper-managed catalogs is still governed by Lakekeeper's own authorization.

| Variable                                  | Example                          | Description |
|-------------------------------------------|----------------------------------|-----|
| `TRINO_ADMIN_USERS`         | `user-id-1,user-id-2` | Comma-separated list of Trino user IDs (typically OIDC subject identifiers) that receive admin access. Default: empty (no admins). |

Admin users can also be configured directly in the `trino_admin_users` list in `configuration.rego`.

### Trusted Engines and View Security

Configure Trino as a [trusted engine](./configuration.md#trusted-engines) in Lakekeeper. This step is required for every deployment with authentication in which Trino creates or changes `SECURITY DEFINER` views.

Trino runs a view as its owner by default (`SECURITY DEFINER` is the default for `CREATE VIEW`). Trino stores the owner in the view property `trino.run-as-owner`. When a user queries the view, Trino asks the OPA bridge about the tables behind the view as the owner, not as the user. So the owner's permissions decide which data the view can read. For this reason, Lakekeeper lets only a trusted engine write `trino.run-as-owner` or give a view that has it a new version. [Property Protection](./view-security.md#property-protection) describes the rules, how `CREATE OR REPLACE VIEW ... SECURITY INVOKER` behaves, and which statements fail without a trusted engine.

Match the trusted engine to the subject of the service account with which the Trino catalog authenticates at Lakekeeper. See [Setting Up Trusted Engines](./view-security.md#setting-up-trusted-engines).

```bash
LAKEKEEPER__TRUSTED_ENGINES__TRINO__TYPE=trino
LAKEKEEPER__TRUSTED_ENGINES__TRINO__OWNER_PROPERTY=trino.run-as-owner
LAKEKEEPER__TRUSTED_ENGINES__TRINO__IDENTITIES__OIDC__SUBJECTS=["<trino-service-account-subject>"]
```

Do **not** add the OPA bridge client to the trusted engine. The OPA bridge only asks Lakekeeper permission questions. It never loads views and never writes view properties, so it does not need to be trusted. If it were trusted, its credentials could write the owner of any view.

We recommend giving the OPA bridge its **own** Keycloak client, separate from the Trino catalog client. The OPA bridge only needs permission-check access on Lakekeeper, which is a narrower privilege scope than what the Trino catalog client needs — keeping them separate limits blast radius if either credential leaks.

When you set up the trusted engine, [check the existing views](./view-security.md#check-existing-views) that have `trino.run-as-owner`.

### Views inside DEFINER views

Trino runs an INVOKER view inside a DEFINER view as the DEFINER view's owner. Then whoever can change the INVOKER view could read data with the owner's permissions. So inside a DEFINER view of another user, the bridge allows a view only if it has `trino.run-as-owner` and the owner may `select` it. For a table, the owner needs `read_data`. The bridge decides this for each Trino user, also if Trino authenticates to Lakekeeper with a service account.

A query over a DEFINER view that reads an INVOKER view therefore fails for every user except the owner, with `View owner does not have sufficient privileges`. [Chained Views](./view-security.md#chained-views) lists how to keep such a query working.

The bridge cannot tell views from tables in catalogs that are not listed in `trino_catalog`. `TRINO_ALLOW_UNMANAGED_CATALOGS` therefore does not allow reading tables and views of these catalogs inside a DEFINER view. This does not apply to Trino's `system` catalog, which cannot hold views. For a catalog that cannot hold Trino views, such as a PostgreSQL or Exasol catalog, add an [`allow_unmanaged`](#allow_unmanaged-unmanaged-catalog-extensions) rule:

```rego
package trino

allow_unmanaged if {
    input.action.operation == "CreateViewWithSelectFromColumns"
    input.action.resource.table.catalogName == "my_external_catalog"
}
```

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
| `metadata` | `analyze_properties`, `catalogs`, `column_properties`, `materialized_views`, `schema_properties`, `table_comments`, `table_properties` | Catalog metadata. Tables like `*_authorization` are excluded for non-admins. |
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

## Refused Schema and Table Names

Trino and Lakekeeper change some schema and table names before they use them. Trino removes white space at the start and at the end of each part of a schema name, and it skips empty parts. Lakekeeper reads a `+` in a request path as a space. The bridge checks the name that Trino acts on, so it refuses every operation on the following names, and filter operations such as `SHOW TABLES` hide them:

- A schema name with an empty part, for example `a..b`, `.sales` or `sales.`.
- A schema name with a part that begins or ends with a space, a tab, a line break, another Unicode space character or a control character, for example `" sales"` or `"sales. eu"`.
- A schema name that contains the control character U+001F.
- A schema, table or view name that contains `+`.
- A table or view name that contains `$` or `@`. Reading a metadata table such as `orders$files` is the only exception, see [Metadata Tables](#metadata-tables).

Spaces inside a name are allowed, for example `daily sales`.

Namespaces, tables and views with such names can exist in Lakekeeper, for example if another client created them. To use one of them in Trino, rename it with the Lakekeeper API or UI. Lakekeeper refuses namespace names with an empty part. It refuses `+` and control characters in the names of new tables, views and generic tables and in the new name of a rename. For a new or moved namespace, it refuses `+` in any part of the name and control characters in its last part. Lakekeeper accepts white space at the start or end of a namespace part and `$` or `@` in table and view names, so for these the bridge is the only check.

### Metadata Tables

Trino reads a name such as `orders$files` as the `$files` metadata table of the table `orders`. Reading a metadata table needs a permission on its base table, here `orders`:

- `$history`, `$refs`, `$properties` and `$metadata_log_entries` need `get_metadata`.
- `$files`, `$entries`, `$all_entries`, `$manifests`, `$all_manifests`, `$partitions` and `$snapshots` need `read_data`, because they show values of the data, such as column bounds and partition values. In `$snapshots`, such values are in the snapshot summaries, together with other values that writers add.

Users who can only see a table can list tables and columns, and run `SHOW COLUMNS`, `DESCRIBE` and `SHOW CREATE TABLE`. Tools such as Superset may read `$partitions` for such users and log the denied query. What these tools show does not change.

The bridge denies `$materialized_view_storage`. Trino cannot read it from Lakekeeper, because Trino's Iceberg REST catalog has no materialized views.

Trino 484 and later always read a name such as `orders$files` as the metadata table. Older Trino versions read a view with this name if Trino does not find the base table, or if the Trino version does not know the type, for example `$entries` before Trino 469. So on these versions the bridge also requires that no view with this name exists, or that the user has `select` on it. This costs one more request to Lakekeeper for each read of a metadata table. The bridge takes the Trino version from Trino's request. If it cannot read the version, it handles Trino as an older version.

The bridge refuses all other table and view names with `$` or `@`. If a later Trino version adds a type of metadata table, names with this type are then denied, and not checked on the wrong object.

## Extension Points

The OPA bridge provides two extension points for adding custom authorization rules without modifying the built-in policies. Both default to `false` and can be extended by creating `.rego` files in the `policies/trino/` directory.

### `allow_managed` — Managed Catalog Extensions

Use `allow_managed` for additional rules on catalogs listed in `trino_catalog`. These rules run alongside Lakekeeper's permission checks (OR logic). For example, granting UDF management permissions on a Lakekeeper-managed catalog.

To use it, create `policies/trino/allow_managed.rego` in the `trino` package and define `allow_managed` rules for the Trino operations you need. These rules bypass Lakekeeper's permission system, so review them carefully. The bridge's helpers that check an existing schema, table or view, for example `require_table_access_simple`, refuse the names listed in [Refused Schema and Table Names](#refused-schema-and-table-names). Check the name of a new table or view with `is_canonical_tabular_name`.

### `allow_unmanaged` — Unmanaged Catalog Extensions

Use `allow_unmanaged` for catalogs **not** listed in `trino_catalog` (e.g., external database catalogs like PostgreSQL or Exasol). These rules are evaluated on a fast path that never triggers Lakekeeper HTTP calls, including in batch operations.

For a simple blanket allow, set `TRINO_ALLOW_UNMANAGED_CATALOGS=true` (see above). For fine-grained control, create `policies/trino/allow_unmanaged.rego` in the `trino` package. For example, to grant read-only access to a specific external catalog for certain users:

```rego
package trino

allow_unmanaged if {
    input.action.operation in ["AccessCatalog", "FilterCatalogs", "ShowSchemas"]
    input.action.resource.catalog.name == "my_external_catalog"
}

allow_unmanaged if {
    input.action.operation == "FilterSchemas"
    input.action.resource.schema.catalogName == "my_external_catalog"
}

allow_unmanaged if {
    input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
    input.action.resource.table.catalogName == "my_external_catalog"
}
```

## Batch Optimization

For Trino filter operations (`FilterTables`, `FilterColumns`, `SelectFromColumns`, `FilterSchemas`), the OPA bridge optimizes authorization checks on Lakekeeper-managed catalogs by batching resource checks into Lakekeeper `batch-check` HTTP requests, instead of making one HTTP call per resource.

For table/column/select operations, each resource generates two checks (table + view) since Trino does not distinguish between tables and views in filter requests. A resource is allowed if either check passes. Schema filter operations generate one namespace check per resource.

When the number of checks exceeds the configured batch size, the bridge automatically splits them into multiple `batch-check` requests (chunking) and concatenates the results. The batch size can be configured per Lakekeeper instance via the `LAKEKEEPER_MAX_BATCH_CHECK_SIZE` environment variable or the `max_batch_check_size` field in `configuration.rego` (default: 1000, matching Lakekeeper's server limit). When using OpenFGA as the authorization backend, this value should be tuned to match the OpenFGA and Lakekeeper batch check settings to avoid overloading the authorization backend.

The bridge uses the results of a batch only if every `batch-check` request of it succeeds and returns one result for each check. Otherwise it allows none of the resources that these requests check, so a filter operation does not show them. If a `list_everything` check of the [shortcut for broad access](#shortcut-for-broad-access) fails, the bridge continues as if the check did not allow broad access.

System schemas (`information_schema`, `schema_discovery`, `system`) and metadata tables (e.g., `foo$snapshots`) within managed catalogs are excluded from this batch optimization: they are not included in the `batch-check` request and are instead evaluated per-resource by the OPA policies. These per-resource evaluations may still trigger Lakekeeper calls (for example, catalog-level `get_config` checks), but they do not participate in the batched table/schema authorization request.

This optimization is transparent — it produces the same results as per-resource evaluation but with significantly fewer HTTP round-trips to Lakekeeper for non-system resources.

### Shortcut for broad access

If a user may list everything in a warehouse or namespace (`list_everything`), Trino shows that user all schemas, tables and views inside it, including their columns. The bridge checks `list_everything` on the warehouse. If the warehouse check does not allow it, the bridge checks each namespace that the request contains. It does not check each table and view. This keeps queries such as `information_schema.columns` fast.

If `list_everything` should not show the columns and names of every table and view, set `LAKEKEEPER_LIST_EVERYTHING_GRANTS_TABULAR_METADATA` to `false` (or `list_everything_grants_tabular_metadata` in `configuration.rego`). The bridge then checks `get_metadata` on each table and view. It sends these checks to Lakekeeper in batches, but listing the columns of many tables still needs more calls to Lakekeeper than the shortcut. Schemas always use the shortcut.

## Context Forwarding

The OPA bridge forwards resource names, paths and other context to Lakekeeper's batch-check API for create actions, replaces, moves and drops. This enables Lakekeeper's authorizer (e.g. Cedar) to make authorization decisions based on the name of the resource being created, on the properties of a create or replace statement, on where a table or view is moved from and to, or on the flags of a drop:

| Trino Operation | Lakekeeper Action | Context Forwarded |
|-----------------|-------------------|----------------|
| `CreateSchema` (top-level) | `create_namespace` (warehouse) | Schema name |
| `CreateSchema` (nested) | `create_namespace` (parent namespace) | Child schema name |
| `CreateTable` | `create_table` (namespace) | Table name |
| `CreateView` / `CreateMaterializedView` | `create_view` (namespace) | View name |
| `RenameTable` / `RenameView` into another schema | `move` (table or view) and `accept_moved_tabular` (destination namespace) | `move` carries the destination namespace path, `accept_moved_tabular` the source namespace path |
| `CreateTable`, if a table with the name exists | `commit` (that table) | Table name and target ref `main` |
| `CreateView`, if a view with the name exists | `commit` (that view), and `change_security_mode` if the view runs as another user | View name |
| `DropTable` | `drop` (table) | `purge: true` |
| `DropView` | `drop` (view) | `purge: true` |
| `DropSchema` | `delete` (namespace) | `recursive: true` and `purge: true` |

Properties specified during creation (e.g. `WITH (format='PARQUET')`) are also forwarded. They keep the names Trino uses, for example `format`, also in the `commit` check of `CreateTable`. See [Replacing Tables and Views](#replacing-tables-and-views) for this check.

A rename into another catalog is refused.

Lakekeeper drops the tables and views that Trino drops with purge, so the `drop` checks of `DropTable` and `DropView` say `purge: true`. Trino's `DropSchema` request does not say whether the statement has `CASCADE`. With `CASCADE`, Trino drops and purges every table and view in the schema without checking each of them. The `delete` check of `DropSchema` therefore always says `recursive: true` and `purge: true`, also for an empty schema. Policies that look at these fields, for example in Cedar, apply to every `DROP SCHEMA` through Trino. OpenFGA does not use them.

The table procedures `expire_snapshots` and `remove_orphan_files` also delete files. The bridge checks them as a `commit` of any kind on the table, which with OpenFGA needs `write`.

## Replacing Tables and Views

Trino asks for the same permission to create a table as to replace it with `CREATE OR REPLACE TABLE`. The same is true for views and `CREATE OR REPLACE VIEW`. So when a user creates a table or view, the bridge also checks if one with this name already exists in the schema:

- For `CREATE TABLE` and `CREATE TABLE AS`, the bridge looks for a table with this name. If there is none, the user needs only `create_table` on the namespace. If there is one, the user also needs `commit` on that table, because replacing gives the table a new schema, new partitioning, new properties and new data.
- For `CREATE VIEW`, the bridge looks for a view with this name. If there is none, the user needs only `create_view` on the namespace. If there is one, the user also needs `commit` on that view, because replacing gives the view a new definition. If that view runs as another user, because its `trino.run-as-owner` names someone else, the user also needs `change_security_mode` on it: Trino does not tell the bridge whether the new view runs as its owner or as the caller. Lakekeeper itself checks a commit that makes a view run as its owner, see [Property Protection](./view-security.md#property-protection). So users who run the same view models, for example dbt projects run by several users, need `modify` on these views.

A table or view that was dropped and can still be restored does not count.

The bridge cannot see which changes Trino makes when it replaces a table or view, so it checks a `commit` of any kind. With OpenFGA, replacing a table or a view needs `write`. Ownership is also enough. The grant can be on the object itself or on anything that contains it, such as its namespace or warehouse. See [Authorization with OpenFGA](./authorization-openfga.md). When Trino connects to Lakekeeper with its own credentials, tables and views that users create through Trino belong to Trino's service account. Roles that replace tables or views through Trino therefore need `write` on the namespace, for example to run dbt table models with `on_table_exists='replace'` or dbt view models. For the same reason, users who create tables or views through Trino need `describe` on the namespace next to `create` to see what they created.

Trino checks the permission to create a view before it checks if the view exists. If a view with the name exists and the user does not have `commit` on it, `CREATE VIEW` fails with an access error, not with an error that the view already exists. `CREATE VIEW IF NOT EXISTS` with the name of an existing view also needs `commit` on that view.

When Trino creates a table with a new name, Lakekeeper writes one audit record for a failed `introspect_permissions` with the reason `resource_not_found`. For a view with a new name, it writes two such records. On Trino before 484, each read of a [metadata table](#metadata-tables) also writes one such record if no view has its name. On Trino 482 and 483, a read of `$partitions` writes two such records, because Trino also reads `$files` for it. These records are expected. When you alert on denied authorizations, leave out `introspect_permissions` records with `resource_not_found` from the bridge's identity.

If the bridge cannot find out if the name is free, for example because Lakekeeper does not answer, it denies the statement.

The bridge requires `iceberg.rest-catalog.case-insensitive-name-matching` to be off in the Trino catalog. Lakekeeper already matches names regardless of upper and lower case. With the option on, Trino compares names with Java's rules for upper and lower case, which treat a few letters, such as the dotless `ı`, differently from Lakekeeper. Trino can then act on another table or view than the one the bridge checked.

## Adding Files with `add_files`

The bridge allows Trino's `add_files` table procedure for users who may commit any change to the table (`commit`; with OpenFGA, `write`). The procedure reads the files at the location given in the statement. It uses the storage access that Trino has for the table:

- If Lakekeeper vends credentials to Trino, these credentials only reach the table's own location. `add_files` can then only add files from inside the table's location.
- If Trino uses its own static storage credentials, `add_files` can read every location that these credentials can read. Use `add_files` in this setup only if everyone who may commit to a table (with OpenFGA, everyone who has `write`, `modify` or ownership on the table or on a namespace, warehouse or project that contains it) may read all data that Trino can read. Otherwise remove `ADD_FILES` from the allowed procedures in `policies/trino/allow_table.rego`.

## Deployment

Use the bridge policies of the same release as the Lakekeeper server. To upgrade, upgrade the server first, then the policies. The policies ask the server for actions, such as `move`, that older servers do not know. Older servers deny these requests: with new policies on an older server, a DEFINER view of another user fails if it reads another view, and every `CREATE VIEW` fails. Policies of earlier releases ask only `commit` to replace a view, so upgrade the policies before you grant `write` on views.

A full self-contained example is [available on GitHub](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control-advanced).
