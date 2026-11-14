---
title: "View Security"
description: "Lakekeeper's DEFINER and INVOKER view security models, and how the referenced-by view chain decides whose permissions are checked."
source: "https://docs.lakekeeper.io/docs/latest/view-security/"
---

# View Security

Lakekeeper supports **DEFINER** and **INVOKER** security models for views, enabling catalogs to make context-aware authorization decisions when query engines load tables through view chains.

## Background

When a query engine executes a query that references a view, the engine sends a `loadTable` (or `loadView`) request to the catalog with a `referenced-by` query parameter. This parameter contains the chain of views through which the table is being accessed. Lakekeeper uses this chain to decide **which user's permissions** to check at each step.

Trino up to 484, and Spark with Iceberg up to 1.12.0, do not send this parameter. If such an engine sends each user's own token, Lakekeeper checks every table and view as that user, so a DEFINER view of another user works only if the user may also read what it references.

This feature is based on the [Apache Iceberg referenced-by specification](https://github.com/apache/iceberg/pull/13810).

## INVOKER vs DEFINER

### INVOKER (default)

With the INVOKER security model, a view runs with the **calling user's** permissions: what the view references is checked as the calling user. This is the default behavior — if a view does not have an owner property set, it is treated as INVOKER.

**Example:** User Alice queries a view that references a table.

```text
Alice --> View (INVOKER) --> Table
                |                |
          Check: Alice     Check: Alice
```

Alice must have permission to access both the view and the underlying table.

### DEFINER

With the DEFINER security model, the **view owner's** permissions are used for the tables and views that the DEFINER view references. This allows view owners to grant access to underlying data without granting direct table access.

**Example:** User Alice queries a DEFINER view owned by Bob that references a table.

```text
Alice --> View (DEFINER, owner=Bob) --> Table
                |                          |
          Check: Alice               Check: Bob
```

Alice needs permission to access the view, but the **table** is checked against **Bob's** permissions. Alice does not need direct access to the table.

### Chained Views

Views can reference other views. Each view in the chain is checked as the user who runs the view that references it. A DEFINER view runs as its owner. An INVOKER view runs as the calling user, also when a DEFINER view references it. So the owner's permissions end at an INVOKER view.

**Example:** A chain with mixed security models.

```text
Alice --> View1 (DEFINER, owner=Bob) --> View2 (INVOKER) --> View3 (DEFINER, owner=Carol) --> Table
              |                              |                            |                     |
        Check: Alice                   Check: Bob                   Check: Alice          Check: Carol
```

- **View1** is checked as Alice, the calling user.
- **View2** is checked as Bob, because Bob's View1 references it.
- **View3** is checked as Alice, because View2 is INVOKER and runs as Alice.
- The **Table** is checked as Carol, because View3 is DEFINER and runs as Carol.

Where Lakekeeper sees the calling user, or with the OPA bridge, this rule keeps whoever can change an INVOKER view from reading data with the owner's permissions. PostgreSQL uses the same rule.

The calling user is the identity of the request that Lakekeeper receives. If Trino sends a token for each user, this is the Trino user. If Trino authenticates with its own service account, Lakekeeper checks that account below an INVOKER view, and the [OPA bridge](./opa.md#views-inside-definer-views) decides for each Trino user. If Trino uses its own access control instead of the bridge, we recommend that views which DEFINER views read are DEFINER views as well.

Trino runs an INVOKER view inside a DEFINER view as the outer owner, so a query that Trino allows can fail. To keep such a query working, do one of these:

- Create the inner view as `SECURITY DEFINER`, so that its owner is responsible for what it reads.
- Reference the base tables directly in the outer view.
- If Trino sends a token for each user and you do not use the OPA bridge, grant these users read access to what the inner view reads.

## How It Works

When a trusted engine sends a `loadTable` or `loadView` request with the `referenced-by` query parameter, Lakekeeper:

1. Resolves all views and tables in the chain.
2. Determines the security model (DEFINER or INVOKER) for each view by checking the configured owner property (e.g. `trino.run-as-owner`).
3. Walks the chain from entry point to target. After a DEFINER view, checks use its owner. After an INVOKER view, checks use the calling user again.
4. Checks permissions for the correct user at each step.
5. Returns the result only if all checks pass.

Without a trusted engine, the `referenced-by` parameter is ignored and only the calling user's permissions on the target resource are checked (standard behavior).

### Chain Depth

The number of views a client may declare in one `referenced-by` chain is capped by [`LAKEKEEPER__REFERENCED_BY__MAX_NESTING_DEPTH`](./configuration.md#referenced-by-chains) (default `10`). Every entry widens the authorization work for that single request, so a deeper chain is rejected with `ReferencedByDepthExceeded` (HTTP 400) — the request is malformed, not unauthorized. The cap is applied to the raw list the client sent, so it holds whether or not a trusted engine matched. Every entry is validated like the target of the request — same maximum namespace depth, same restrictions on identifier parts.

## Configuration

### Prerequisites

- [Authentication](./authentication.md) must be enabled — Lakekeeper needs token information to identify engines and resolve owners.
- An [authorization backend](./authorization.md) must be configured — DEFINER views are only useful when permissions are actually enforced.

### Setting Up Trusted Engines

Configure one or more trusted engines so that Lakekeeper knows which query engines to trust. See [Trusted Engines](./configuration.md#trusted-engines) for the full configuration reference.

**Minimal example for Trino:**

```bash
LAKEKEEPER__TRUSTED_ENGINES__TRINO__TYPE=trino
LAKEKEEPER__TRUSTED_ENGINES__TRINO__OWNER_PROPERTY=trino.run-as-owner
LAKEKEEPER__TRUSTED_ENGINES__TRINO__IDENTITIES__OIDC__SUBJECTS=["<trino-service-account-subject>"]
```

Matching is **scoped to the IdP** the token was issued by. Each engine's `IDENTITIES` block is keyed per IdP (e.g. `IDENTITIES__OIDC__...`), and a token is only tested against the block matching its own IdP. Within that block, a request is matched when **either** its audience appears in `AUDIENCES` **or** its subject appears in `SUBJECTS`.

Lakekeeper trusts the identity of the token, not the software that sends it. Anyone who can get a token that matches the engine's `AUDIENCES` or `SUBJECTS` is treated as the engine and can set the owner of any view they can create. Match the engine by the subject of its own service account. List an audience in `AUDIENCES` only if your identity provider guarantees that no user can obtain a token that carries it, for example an audience that only the engine can get by [token exchange](https://datatracker.ietf.org/doc/html/rfc8693). The audience that users receive when they log in to the engine itself does not qualify, because every user of the engine holds such a token.

**What happens when a request is not matched as a trusted engine:**

- `loadTable` / `loadView` requests that include a `referenced-by` parameter are **silently ignored** with respect to that parameter — the load still succeeds, but the DEFINER chain is not resolved and permissions are evaluated against the caller only. This is logged at debug level; no error is returned. The exceptions are malformed chains, rejected with `400` before engine trust is considered: a chain deeper than the configured maximum (`ReferencedByDepthExceeded`), or one that names the object being loaded (`ReferencedByContainsTarget`).
- A view create or commit that writes a protected owner property is rejected with `403 ProtectedPropertyModification`. A commit that changes the current version of a view that has a protected owner property is rejected with `403 ProtectedViewVersionChange`. See [Property Protection](#property-protection). An ignored `referenced-by` on a load does **not** trigger these errors.

!!! note "When using the OPA bridge"

    The [OPA bridge](./opa.md) authenticates to Lakekeeper with its own client credentials to evaluate permission checks. We recommend it runs under a **dedicated** Keycloak client with narrower privileges than the Trino catalog client — not a shared one.

    Do **not** match the OPA bridge client as a trusted engine. The OPA bridge only sends permission checks. It does not load tables or views and does not write view properties. Match the client that Trino uses for the catalog instead: Trino creates the views and writes the owner property.

### Creating DEFINER Views

Only matched trusted engines can set the owner property on views. If authentication is enabled and Trino is not configured as a trusted engine, `CREATE VIEW` in Trino fails with `403 ProtectedPropertyModification` unless the view is created with `SECURITY INVOKER`. In Trino, DEFINER views are created with:

```sql
CREATE VIEW my_schema.my_view
SECURITY DEFINER
AS SELECT * FROM my_schema.my_table;
```

Trino automatically sets the `trino.run-as-owner` property on the view with the creating user as the owner. `SECURITY DEFINER` is Trino's default, so a `CREATE VIEW` without a `SECURITY` clause also sets the property.

!!! warning "Enabling trusted engines with existing views"

    When you enable trusted engines in an existing deployment, any views that **already** have the owner property set (e.g. `trino.run-as-owner`) will **immediately** be treated as DEFINER views. Lakekeeper will start checking permissions against the owner specified in that property.

    **Before enabling**, [check your existing views](#check-existing-views). Ensure that:

    1. The owner values in existing view properties are valid users in your identity provider, and each owner is the user who should own the view.
    2. Those owners have appropriate permissions on the underlying tables.
    3. You have tested the authorization chain in a non-production environment first.

    If an owner referenced in a view property does not exist or lacks permissions, queries through that view will fail once trusted engines are enabled.

### Property Protection

The protection applies only if [authentication](./authentication.md) is enabled. Without authentication, Lakekeeper cannot tell clients apart and cannot trust any of them as an engine.

Lakekeeper protects views that an engine runs as their owner. It protects `trino.run-as-owner` and the owner property of every configured trusted engine:

- **Owner property:** only a request from a matched engine whose owner property is exactly that key can set, change or remove it. Other requests that name the property receive `403 ProtectedPropertyModification`, even if they send the value the view already has. A commit that leaves the property out can change the other properties of the view.
- **View version:** on a view that has a protected owner property, only a request from a matched engine that owns this property can change the current view version, for example add a version with new SQL or go back to an older version. Other requests receive `403 ProtectedViewVersionChange`.
- **New query:** if the matched engine changes the SQL, default catalog or default namespace of such a view, and the same commit leaves the owner property out, Lakekeeper removes the owner property in that commit. The view becomes an INVOKER view, so the new query never runs as the previous owner. `CREATE OR REPLACE VIEW ... SECURITY DEFINER` in Trino sends the owner property with the user who runs the statement as the new owner, and the view stays a DEFINER view.
- **Same query:** a change that keeps the SQL, default catalog and default namespace keeps the owner, for example `COMMENT ON COLUMN` in Trino. Lakekeeper cannot tell such a change from `CREATE OR REPLACE VIEW ... SECURITY INVOKER` with the same query, so that statement also leaves the view a DEFINER view. To make it an INVOKER view, change the query in the same statement, or drop the view and create it again; dropping loses the grants on the view.
- **Security mode:** whom a view runs as has its own permission, `change_security_mode` on the view, so a user who may change a view's query cannot also change whom it runs as. A commit that adds or removes the owner property switches the view between DEFINER and INVOKER. It needs `change_security_mode` for the identity that sends the commit and, when it adds the property, also for the user that the property names. Otherwise the commit fails with `403 SecurityModeChangeForbidden`. With OpenFGA, `modify` includes `change_security_mode` and `write` does not. With Cedar, the action is `ChangeViewSecurityMode`: `manage` includes it and `write` does not. A commit that keeps the security mode does not need this permission, even if it changes the owner. In Trino, `SECURITY DEFINER` is the default: `CREATE OR REPLACE VIEW` without a `SECURITY` clause makes an INVOKER view a DEFINER view.

If Trino connects with its own service account and uses its own access control, not the OPA bridge, Lakekeeper knows the Trino user only as the owner that Trino writes into the view. Lakekeeper checks this user as `<identity provider of Trino's token>~<Trino user>`, for example `oidc~alice`, and the error message names this id. To make an existing INVOKER view a DEFINER view in this setup, use one of these ways:

1. Grant `modify` on the view to this id, for example `oidc~alice`. The grant works even if this user never signed in to Lakekeeper.
2. Keep the view INVOKER: write `SECURITY INVOKER` in the `CREATE OR REPLACE VIEW` statement.
3. Drop the view and create it again. A new view can be a DEFINER view without this permission. The grants on the old view are lost.

When Lakekeeper removes the owner property, the commit includes a `remove-properties` update for it. The stored view, the response, the view-committed event and the published CloudEvent show the removal. The authorization record in the audit log lists the properties as the engine sent them.

Trino runs a DEFINER view with the permissions of the user named in `trino.run-as-owner`, even if Lakekeeper itself has no trusted engine. With the [OPA bridge](./opa.md), Trino asks for the permissions of that user. So these rules apply with or without configured trusted engines. Refused requests are recorded in the [audit log](./logging.md) as denied authorizations.

If no trusted engine is configured, no request can write `trino.run-as-owner`, and views that already have it cannot get a new view version from any client:

- Statements that send the owner fail with `403 ProtectedPropertyModification`: in Trino, `CREATE VIEW` and `CREATE OR REPLACE VIEW` with `SECURITY DEFINER`, and `ALTER VIEW ... REFRESH` of a DEFINER view.
- Statements that change the version and leave the owner out fail with `403 ProtectedViewVersionChange`: in Trino, `CREATE OR REPLACE VIEW ... SECURITY INVOKER` and `COMMENT ON COLUMN`; in Spark, `CREATE OR REPLACE VIEW`.
- Queries, `COMMENT ON VIEW`, rename and drop keep working. Configure the trusted engine to change these views again.

Lakekeeper compares keys without regard to upper and lower case, and accepts only the exact key: a key that differs from an owner property only in upper and lower case (e.g. `Trino.Run-As-Owner` for `trino.run-as-owner`) is rejected, even from the trusted engine. Engines read the exact key, so such a key would not change whom the view runs as.

### Check Existing Views

Views that already exist can carry a `trino.run-as-owner` value that a client other than Trino wrote. When you set up the trusted engine, check every view that has this property, and make sure that each owner is the user who should own the view:

- Trino's `information_schema.views` does not show view properties. List the views of each namespace and read the properties of each view, for example with the `list_views` and `load_view` functions of PyIceberg 0.12 or later.
- If the [audit log](./logging.md) was enabled, search it for `create_view` records whose `properties` contain `trino.run-as-owner`, and for `commit` records on views whose `updated_properties` or `removed_properties` contain it. Check the records whose actor is not the service identity of Trino.

To correct the owner of a view, run `CREATE OR REPLACE VIEW ... SECURITY DEFINER` in Trino as the user who should own it. The view keeps its id and the grants on it. With the [OPA bridge](./opa.md#replacing-tables-and-views), this user needs `change_security_mode` on the view. Drop the view and create it again only if this is not possible: this loses the grants on the view.

## Security Considerations

### Delegated Execution

When a DEFINER view references a table, the table load happens with the **owner's** permissions. This is flagged as "delegated execution" in authorization checks. Authorization backends can use this flag to apply different policies — for example, skipping permission-inspection rights that would normally be required.

A DEFINER view runs its own SQL with the owner's rights. A view that it references runs as that view's owner if it is DEFINER, and as the calling user if it is INVOKER, see [Chained Views](#chained-views). So, where Lakekeeper sees the calling user or with the OPA bridge, whoever can change a referenced view cannot read data with the outer owner's rights. Checks below an INVOKER view are not delegated.

### Metadata vs. execution: `get_metadata` and `select`

Views expose two authorization checkpoints:

- `get_metadata` (control-plane) — listing the view, reading its definition, returning it from `loadView`.
- `select` (data-plane) — executing the view to produce rows, including traversing a DEFINER chain into the underlying table.

`get_metadata` and `select` are **distinct actions** that an authorizer evaluates independently. By convention, a principal who can `select` a view can also `get_metadata` on it (someone allowed to query must also be allowed to inspect); the reverse does not hold. How you grant each is authorizer-specific — see the [OpenFGA](./authorization-openfga.md) or [Cedar](./authorization-cedar.md) docs for the concrete grant names.

**When does the split matter in practice?**

- **View with no downstream objects** (e.g. `SELECT 1`). No referenced-by chain is built, so `select` is never checked via the chain. `get_metadata` on the view is enough for `loadView` (reading the SQL). `select` is only required if the engine actually executes the view (which it does via the OPA bridge, see below).
- **INVOKER view referencing other objects.** The referenced-by chain emits `select` on the view. Downstream objects are checked against the **caller**. The caller needs `select` on the view to traverse the chain plus `read_data` / `write_data` on downstream tables.
- **DEFINER view.** Same `select`-on-view requirement for the caller, but the objects it references are checked against the **owner**. `select` on the view is what gates the caller from entering the owner's context without explicit permission. This is also the boundary where the instance-admin `get_metadata` bypass stops short — admins cannot traverse a DEFINER chain they weren't granted `select` on.
- **OPA bridge.** When the engine is fronted by the [OPA bridge](./opa.md), the bridge issues a `select` check on the view for every query that returns data (via Trino's `SelectFromColumns`). Under the OPA bridge, `select` on the view is always required to read data. Inside a DEFINER view of another user, the bridge allows a view only if it is a DEFINER view itself, see [Views inside DEFINER views](./opa.md#views-inside-definer-views).

The data-plane / control-plane split lets policies differentiate the two dimensions — for example, the instance-admin bypass applies to `get_metadata` but not to `select`, matching the carve-out that already excludes `read_data` / `write_data` on tables. Operator-style identities can list and manage views they have no explicit access to, but cannot execute them through a referenced-by chain without `select` on that view.

Every view in a referenced-by chain is checked for *both* actions. The target of `loadView` is checked only for `get_metadata`.

### Owner Property Integrity

The owner property on a view is critical for security. Lakekeeper ensures that:

- Only matched trusted engines can **set or remove** the owner property, and change the current version of a view that has it.
- Adding or removing the owner property needs the `change_security_mode` permission, see [Property Protection](#property-protection).
- The owner string is resolved to a user in the engine's Identity Provider.
- If the owner cannot be resolved, the request fails with a clear error.

### Audit Trail

When [audit logging](./configuration.md#audit-logging) is enabled, the audit log records each `loadTable` and `loadView` request with the calling user, the loaded table or view, and the decision. It does not list the checks along the view chain. If a check that ran with a DEFINER view owner's permissions is refused, the error in the record says so.
