# View Security

Lakekeeper supports **DEFINER** and **INVOKER** security models for views, enabling catalogs to make context-aware authorization decisions when query engines load tables through view chains.

## Background

When a query engine executes a query that references a view, the engine sends a `loadTable` (or `loadView`) request to the catalog with a `referenced-by` query parameter. This parameter contains the chain of views through which the table is being accessed. Lakekeeper uses this chain to decide **which user's permissions** to check at each step.

This feature is based on the [Apache Iceberg referenced-by specification](https://github.com/apache/iceberg/pull/13810).

## INVOKER vs DEFINER

### INVOKER (default)

With the INVOKER security model, the **calling user's** permissions are checked at every step in the view chain. This is the default behavior — if a view does not have an owner property set, it is treated as INVOKER.

**Example:** User Alice queries a view that references a table.

```text
Alice --> View (INVOKER) --> Table
                |                |
          Check: Alice     Check: Alice
```

Alice must have permission to access both the view and the underlying table.

### DEFINER

With the DEFINER security model, the **view owner's** permissions are used for resources downstream of the DEFINER view. This allows view owners to grant access to underlying data without granting direct table access.

**Example:** User Alice queries a DEFINER view owned by Bob that references a table.

```text
Alice --> View (DEFINER, owner=Bob) --> Table
                |                          |
          Check: Alice               Check: Bob
```

Alice needs permission to access the view, but the **table** is checked against **Bob's** permissions. Alice does not need direct access to the table.

### Chained Views

Views can reference other views, creating chains. The security model is evaluated at each step, and DEFINER views switch the "current user" for all subsequent checks.

**Example:** A chain with mixed security models.

```text
Alice --> View1 (DEFINER, owner=Bob) --> View2 (INVOKER) --> View3 (DEFINER, owner=Carol) --> Table
              |                              |                            |                     |
        Check: Alice                   Check: Bob                   Check: Bob            Check: Carol
```

- **View1** is checked as Alice (the calling user).
- **View2** is INVOKER, but we are already in Bob's delegated context (from View1 being DEFINER), so it is checked as Bob.
- **View3** is DEFINER with owner Carol — from this point on, Carol's permissions are used.
- The **Table** is checked as Carol.

## How It Works

When a trusted engine sends a `loadTable` or `loadView` request with the `referenced-by` query parameter, Lakekeeper:

1. Resolves all views and tables in the chain.
2. Determines the security model (DEFINER or INVOKER) for each view by checking the configured owner property (e.g. `trino.run-as-owner`).
3. Walks the chain from entry point to target, switching the "current user" at each DEFINER boundary.
4. Checks permissions for the correct user at each step in a single batch authorization call.
5. Returns the result only if all checks pass.

Without a trusted engine, the `referenced-by` parameter is ignored and only the calling user's permissions on the target resource are checked (standard behavior).

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
LAKEKEEPER__TRUSTED_ENGINES__TRINO__IDENTITIES__OIDC__AUDIENCES=[trino]
```

### Creating DEFINER Views

Once trusted engines are configured, only matched engines can set the owner property on views. In Trino, DEFINER views are created with:

```sql
CREATE VIEW my_schema.my_view
SECURITY DEFINER
AS SELECT * FROM my_schema.my_table;
```

Trino automatically sets the `trino.run-as-owner` property on the view with the creating user as the owner.

!!! warning "Enabling trusted engines with existing views"

    When you enable trusted engines in an existing deployment, any views that **already** have the owner property set (e.g. `trino.run-as-owner`) will **immediately** be treated as DEFINER views. Lakekeeper will start checking permissions against the owner specified in that property.

    **Before enabling**, audit your existing views:

    ```sql
    -- In Trino, check for views with the owner property
    SELECT * FROM my_catalog.information_schema.views;
    ```

    Ensure that:

    1. The owner values in existing view properties are valid users in your identity provider.
    2. Those owners have appropriate permissions on the underlying tables.
    3. You have tested the authorization chain in a non-production environment first.

    If an owner referenced in a view property does not exist or lacks permissions, queries through that view will fail once trusted engines are enabled.

### Property Protection

Once a trusted engine is configured, the owner property (e.g. `trino.run-as-owner`) becomes **protected**: only requests from a matched engine can set or remove it. This prevents privilege escalation — without this protection, any user who can commit to a view could set themselves as the DEFINER owner and gain access to tables they shouldn't see.

Non-engine requests that attempt to modify a protected property receive a `403 Forbidden` error with type `ProtectedPropertyModification`.

## Security Considerations

### Delegated Execution

When a user accesses a table through a DEFINER view, the table load happens with the **owner's** permissions. This is flagged as "delegated execution" in authorization checks and audit logs. Authorization backends can use this flag to apply different policies — for example, skipping permission-inspection rights that would normally be required.

### Owner Property Integrity

The owner property on a view is critical for security. Lakekeeper ensures that:

- Only matched trusted engines can **set or remove** the owner property.
- The owner string is resolved to a user in the engine's Identity Provider.
- If the owner cannot be resolved, the request fails with a clear error.

### Audit Trail

All authorization decisions in the referenced-by chain are logged when [audit logging](./configuration.md#audit-logging) is enabled. The audit log includes:

- Which user was checked at each step.
- Whether the check was a delegated execution.
- The full view chain that was evaluated.
