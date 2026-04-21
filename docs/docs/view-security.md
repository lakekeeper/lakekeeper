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

Matching is **scoped to the IdP** the token was issued by. Each engine's `IDENTITIES` block is keyed per IdP (e.g. `IDENTITIES__OIDC__...`), and a token is only tested against the block matching its own IdP. Within that block, a request is matched when **either** its audience appears in `AUDIENCES` **or** its subject appears in `SUBJECTS`. Use `SUBJECTS` when the IdP does not mint a distinguishing audience — for example, to trust a specific service-account's subject UUID directly:

```bash
LAKEKEEPER__TRUSTED_ENGINES__TRINO__IDENTITIES__OIDC__SUBJECTS=["<trino-service-account-subject>"]
```

**What happens when a request is not matched as a trusted engine:**

- `loadTable` / `loadView` requests that include a `referenced-by` parameter are **silently ignored** with respect to that parameter — the load still succeeds, but the DEFINER chain is not resolved and permissions are evaluated against the caller only. This is logged at debug level; no error is returned.
- Only **commits that actually attempt to set or remove a protected owner property** (`create-view` or `commit-view` writing `trino.run-as-owner`) are rejected with `403 ProtectedPropertyModification`. An ignored `referenced-by` on a load does **not** trigger this error.

!!! note "When using the OPA bridge"

    The [OPA bridge](./opa.md) authenticates to Lakekeeper with its own client credentials to evaluate permission checks. We recommend it runs under a **dedicated** Keycloak client with narrower privileges than the Trino catalog client — not a shared one.

    If the OPA bridge issues `loadTable` / `loadView` requests with `referenced-by` on behalf of Trino, its client must be matched as a trusted engine so DEFINER chains are resolved rather than ignored. Add its service-account subject under `SUBJECTS` (or its audience under `AUDIENCES` if your IdP mints distinguishing audiences). Permission-check traffic itself is not gated by trusted-engine status — the `ProtectedPropertyModification` rejection only applies to DDL that writes a protected owner property, not to the OPA bridge's routine check calls.

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

Protection is **case-insensitive on the match, case-sensitive on the accepted value**: a property key that differs from the configured owner property only in casing (e.g. `Trino.Run-As-Owner` when the admin configured `trino.run-as-owner`) is also rejected. Most engines read the owner property with fixed casing, so a case variant would silently have no effect on the security model while appearing to set it — only the exact key configured by the admin is accepted.

## Security Considerations

### Delegated Execution

When a user accesses a table through a DEFINER view, the table load happens with the **owner's** permissions. This is flagged as "delegated execution" in authorization checks and audit logs. Authorization backends can use this flag to apply different policies — for example, skipping permission-inspection rights that would normally be required.

### Metadata vs. execution: `get_metadata` and `select`

Views expose two authorization checkpoints:

- `get_metadata` (control-plane) — listing the view, reading its definition, returning it from `loadView`.
- `select` (data-plane) — executing the view to produce rows, including traversing a DEFINER chain into the underlying table.

`get_metadata` and `select` are **distinct actions** that an authorizer evaluates independently. By convention, a principal who can `select` a view can also `get_metadata` on it (someone allowed to query must also be allowed to inspect); the reverse does not hold. How you grant each is authorizer-specific — see the [OpenFGA](./authorization-openfga.md) or [Cedar](./authorization-cedar.md) docs for the concrete grant names.

**When does the split matter in practice?**

- **View with no downstream objects** (e.g. `SELECT 1`). No referenced-by chain is built, so `select` is never checked via the chain. `get_metadata` on the view is enough for `loadView` (reading the SQL). `select` is only required if the engine actually executes the view (which it does via the OPA bridge, see below).
- **INVOKER view referencing other objects.** The referenced-by chain emits `select` on the view. Downstream objects are checked against the **caller**. The caller needs `select` on the view to traverse the chain plus `read_data` / `write_data` on downstream tables.
- **DEFINER view.** Same `select`-on-view requirement for the caller, but downstream objects are checked against the **owner**. `select` on the view is what gates the caller from entering the owner's context without explicit permission. This is also the boundary where the instance-admin `get_metadata` bypass stops short — admins cannot traverse a DEFINER chain they weren't granted `select` on.
- **OPA bridge.** When the engine is fronted by the [OPA bridge](./opa.md), the bridge issues a `select` check on the view for every query that returns data (via Trino's `SelectFromColumns`). Under the OPA bridge, `select` on the view is always required to read data.

The data-plane / control-plane split lets policies differentiate the two dimensions — for example, the instance-admin bypass applies to `get_metadata` but not to `select`, matching the carve-out that already excludes `read_data` / `write_data` on tables. Operator-style identities can list and manage views they have no explicit access to, but cannot execute them through a referenced-by chain without `select` on that view.

Every view in a referenced-by chain is checked for *both* actions. The target of `loadView` is checked only for `get_metadata`.

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
