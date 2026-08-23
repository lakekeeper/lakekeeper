---
description: "Run Lakekeeper's authorization on OpenFGA for hierarchical, inheritable permissions across warehouses, namespaces, tables and views."
---

# Authorization with OpenFGA

Lakekeeper can use [OpenFGA](https://openfga.dev) to store and evaluate permissions. OpenFGA provides bi-directional inheritance, which is key for managing hierarchical namespaces in modern lakehouses. For query engines like Trino, Lakekeeper's OPA bridge translates OpenFGA permissions into Open Policy Agent (OPA) format. See the [OPA Bridge Guide](./opa.md) for details.

Check the [Authorization Configuration](./configuration.md#authorization) for setup details.

Choose OpenFGA when you want permissions managed at runtime — admins and object owners handing out access through the UI and API. For policy-as-code with conditions on attributes, see [Cedar](./authorization-cedar.md); the [Overview](./authorization.md#choose-an-authorizer) compares the two.

!!! note "Minimum OpenFGA version"
    **OpenFGA v1.11 or later is required.** The bootstrap and `lakekeeper openfga reconcile` paths use OpenFGA's idempotent-write semantics (`on_duplicate: ignore` / `on_missing: ignore`), introduced in v1.11. Earlier versions will fail with `cannot write a tuple which already exists` during a re-bootstrap or reconcile run. We test against v1.14.

    The server's `OPENFGA_MAX_TUPLES_PER_WRITE` must be at least **100** (its default). A grant diff is applied as one write of up to 100 tuples, and Lakekeeper cannot see the server's setting — a smaller cap fails those writes at runtime.

## Grants

The default permission model is focused on collaborating on data. Permissions are additive. The underlying OpenFGA model is defined in [`schema.fga` on GitHub](https://github.com/lakekeeper/lakekeeper/blob/main/authz/openfga/). The following grants are available:

| Entity    | Grant                                                            |
|-----------|------------------------------------------------------------------|
| server    | admin, operator                                                  |
| project   | project_admin, security_admin, data_admin, role_creator, tag_creator, describe, select, create, modify |
| warehouse | ownership, pass_grants, manage_grants, manage_tags, describe, select, create, modify |
| namespace | ownership, pass_grants, manage_grants, manage_tags, describe, select, create, modify |
| table     | ownership, pass_grants, manage_grants, manage_tags, describe, select, modify  |
| view      | ownership, pass_grants, manage_grants, manage_tags, describe, select, modify  |
| generic table | ownership, pass_grants, manage_grants, manage_tags, describe, select, modify |
| role      | assignee, ownership                                              |
| tag       | ownership, apply                                                 |

### Ownership

Owners of objects have all rights on the specific object. When principals create new objects, they automatically become owners of these objects. This enables powerful self-service scenarios where users can act autonomously in a (sub-)namespace. By default, Owners of objects are also able to access grants on objects, which enables them to expand the access to their owned objects to new users. Enabling [Managed Access](#managed-access) for a Warehouse or Namespace removes the `grant` privilege from owners.

### Server: Admin

A `server`'s `admin` role is the most powerful role (apart from `operator`) on the server. In order to guarantee auditability, this role can list and administrate all Projects, but does not have access to data in projects. While the `admin` can assign himself the `project_admin` role for a project, this assignment is tracked by `OpenFGA` for audits. `admin`s can also manage all projects (but no entities within it), server settings and users.

### Server: Operator

The `operator` has unrestricted access to all objects in Lakekeeper. It is designed to be used by technical users (e.g., a Kubernetes Operator) managing the Lakekeeper deployment.

### Project: Security Admin

A `security_admin` in a project can manage all security-related aspects, including grants and ownership for the project and all objects within it. However, they cannot modify or access the content of any object, except for listing and browsing purposes.

### Project: Data Admin

A `data_admin` in a project can manage all data-related aspects, including creating, modifying, and deleting objects within the project. They can delegate the `data_admin` role they already hold (for example to team members), but they do not have general grant or ownership administration capabilities.

### Project: Admin

A `project_admin` in a project has the combined responsibilities of both `security_admin` and `data_admin`. They can manage all security-related aspects, including grants and ownership, as well as all data-related aspects, including creating, modifying, and deleting objects within the project.

### Project: Role Creator

A `role_creator` in a project can create new roles within it. This role is essential for delegating the creation of roles without granting broader administrative privileges.

### Project: Tag Creator

A `tag_creator` in a project can create new governance tag definitions within it, without holding broader administrative privileges — the tag analogue of `role_creator`. Managing an *existing* definition (update, delete, delegating who may apply it) is not conferred by `tag_creator`; it keys off the definition's `ownership` or `security_admin`. See [Tags](#tags).

### Describe

The `describe` grant allows a user to view metadata and details about an object without modifying it. This includes listing objects and viewing their properties. The `describe` grant is inherited down the object hierarchy, meaning if a user has the `describe` grant on a higher-level entity, they can also describe all child entities within it. The `describe` grant is implicitly included with the `select`, `create`, and `modify` grants.

### Select

The `select` grant allows a user to read data from an object, such as tables or views. This includes querying and retrieving data. The `select` grant is inherited down the object hierarchy, meaning if a user has the `select` grant on a higher-level entity, they can select all views and tables within it. The `select` grant implicitly includes the `describe` grant.

### Create

The `create` grant allows a user to create new objects within an entity, such as tables, views, or namespaces. The `create` grant is inherited down the object hierarchy, meaning if a user has the `create` grant on a higher-level entity, they can also create objects within all child entities. The `create` grant implicitly includes the `describe` grant.

### Modify

The `modify` grant allows a user to change the content or properties of an object, such as updating data in tables or altering views. The `modify` grant is inherited down the object hierarchy, meaning if a user has the `modify` grant on a higher-level entity, they can also modify all child entities within it. The `modify` grant implicitly includes the `select` and `describe` grants.

### Pass Grants

The `pass_grants` grant allows a user to pass their own privileges to other users. This means that if a user has certain permissions on an object, they can grant those same permissions to others. However, the `pass_grants` grant does not include the ability to pass the `pass_grants` privilege itself.

### Manage Grants

The `manage_grants` grant allows a user to manage all grants on an object, including creating, modifying, and revoking grants. This also includes `manage_grants` and `pass_grants`.

### Manage Tags

The `manage_tags` grant allows a user to attach and detach governance tags on an object (warehouse, namespace, table, view, or generic table) and its columns. It is **independent of `modify`** — a separation-of-duties choice, so a data steward can classify objects without holding data or schema-modification rights. `manage_tags` inherits down the object hierarchy. Attaching or detaching a *specific* tag additionally requires the `apply` grant on that tag definition (see [Tags](#tags)).

## Managing grants through the grants API

The grants above can be managed through two APIs:

- [`/management/v1/.../grants`](./grants.md) — the authorizer-independent surface, recommended for new consoles and scripts.
- `/management/v1/permissions/...` — the older, OpenFGA-specific API.

Under OpenFGA these are **two views of one set of tuples**: a grant written through either is visible through both, and there is nothing to migrate. Note that only the `/grants` path emits typed `GrantCreated`/`GrantRevoked` events; the `/permissions` path never has.

Because a grant *is* a tuple, a few behaviors differ from deployments that store grants in the catalog database:

- **No grantor.** A tuple has nowhere to record who wrote it. Grant listings publish no grantor under any authorizer, so this costs nothing at the API — but it does mean the `grant_created` audit event is the only record of who granted a privilege here. `created-at` is the tuple's write timestamp.
- **User ids are taken as given.** OpenFGA does not check that a user exists, so granting to a mistyped user id succeeds silently — and granting *before* a user's first login works, taking effect when they register. Role ids are checked: a `/grants` write below the server rejects a role outside the project with `GrantRoleNotInProject`. Server grants have no project to check against and skip that lookup. Deployments that store grants in the catalog database reject an unknown user with `GrantUserNotFound`.
- **Grant events are never deduplicated.** Grant events assert state rather than transitions and may repeat under every authorizer (see [Logging](./logging.md#audit-logs)). A tuple write additionally reports nothing about prior state, so here no no-op can be suppressed at all: re-applying an unchanged diff emits the full set of events every time. Deployments that store grants in the catalog database skip events for no-ops; this one cannot.
- **Unknown privileges cannot be revoked.** A privilege outside the model has no relation, so there is no tuple to remove and no `can_grant_…` relation to check — the request is refused with `403`. Nothing revocable is withheld; just do not mix unknown privileges into a diff with real ones.
- **The project-scoped listing is not supported.** `GET /management/v1/grants` reports `GrantListingNotImplemented` (501) here. Tuples are indexed by object, so "everything one principal holds in this project" has no index to answer it from: it would mean reading the store a level at a time and resolving every object back to its project through the hierarchy, producing one unpageable response sized by the deployment rather than by the request. Rather than ship that, the arm refuses. Read one resource's grants from `GET .../{resource}/grants` — those page normally and take `principalUser`/`principalRole` — or query OpenFGA directly, where the tuples are. Deployments that keep grants in the catalog database answer the project-scoped listing normally.
- **`pageSize` is clamped to 100.** The authorizer's read caps a page there, whatever the deployment maximum is. Follow the continuation token rather than raising the page size.
- **A privilege the model no longer defines disappears from listings.** It is stored as a relation, and a relation the model does not define has no name to report — so such a grant is invisible *and*, per the point above, unrevocable through the API. `openfga reconcile` does not clean grants. Remove the tuple in OpenFGA directly, or keep the relation in the model until its grants are gone.
- **Applying below the warehouse under an assumed role is refused** with `GrantNotSupported`: managed access has no public userset below the warehouse, so the request cannot be evaluated for a role. The `/permissions` assignments API applies the same restriction.
- **Grants outlive the resources they name.** When a table, namespace or warehouse is deleted, its tuples are removed afterwards on a best-effort basis rather than in the same transaction — and deleting a warehouse removes only the warehouse's own grants, not those on the namespaces and tabulars inside it. Leftover tuples are invisible to listings, because the path back to the project is gone, and are never reclaimed. Deployments that keep grants in the catalog database remove them by foreign key with the resource.
- **Per-resource listings page normally, but an empty page is not the end.** A resource's grants come from one `Read` of that object, which pages with a continuation token like any other listing. Non-privilege tuples on the same object are filtered out *after* the page is fetched, so a page can come back short or empty while more grants remain. Follow the token until it is absent; do not stop on a short page.

## Tags

Governance tags are project-scoped definitions, each represented in the model as a `lakekeeper_catalog_tag` object parented to its project. A [`tag_creator`](#project-tag-creator) creates definitions and becomes the definition's `ownership`, which — together with a project `security_admin` — allows updating it, deleting it, and delegating who may apply it. The directly-assignable `apply` grant lets a principal attach and detach that specific tag ("may apply *this* tag") without owning the definition.

Attaching a tag to a resource requires **both** `apply` on the tag definition **and** `manage_tags` on the target resource. Detaching requires the same pair, so a governance tag cannot be stripped from a resource with target-side rights alone.

## Inheritance

- **Top-Down-Inheritance**: Permissions in higher up entities are inherited to their children. For example if the `modify` privilege is granted on a `warehouse` for a principal, this principal is also able to `modify` any namespaces, including nesting ones, tables and views within it.
- **Bottom-Up-Inheritance**: Permissions on lower entities, for example tables, inherit basic navigational privileges to all higher layer principals. For example, if a user is granted the `select` privilege on table `ns1.ns2.table_1`, that user is implicitly granted limited list privileges on `ns1` and `ns2`. Only items in the direct path are presented to users. If `ns1.ns3` would exist as well, a list on `ns1` would only show `ns1.ns2`.

## Managed Access

Managed access is a feature designed to provide stricter control over access privileges within Lakekeeper. It is particularly useful for organizations that require a more restrictive access control model to ensure data security and compliance.

In some cases, the default ownership model, which grants all privileges to the creator of an object, can be too permissive. This can lead to situations where non-admin users unintentionally share data with unauthorized users by granting privileges outside the scope defined by administrators. Managed access addresses this concern by removing the `grant` privilege from owners and centralizing the management of access privileges.

With managed access, admin-like users can define access privileges on high-level container objects, such as warehouses or namespaces, and ensure that all child objects inherit these privileges. This approach prevents non-admin users from granting privileges that are not authorized by administrators, thereby reducing the risk of unintentional data sharing and enhancing overall security.

Managed access combines elements of Role-Based Access Control (RBAC) and Discretionary Access Control (DAC). While RBAC allows privileges to be assigned to roles and users, DAC assigns ownership to the creator of an object. By integrating managed access, Lakekeeper provides a balanced access control model that supports both self-service analytics and data democratization while maintaining strict security controls.

Managed access can be enabled or disabled for warehouses and namespaces using the UI or the `../managed-access` Endpoints. Managed access settings are inherited down the object hierarchy, meaning if managed access is enabled on a higher-level entity, it applies to all child entities within it.

### Moving namespaces under managed access

Moving a namespace requires `manage_grants` at **both** ends — on the namespace being moved and on the destination parent (or on the warehouse, when moving to the root) — *plus* the ordinary write privilege at each end: `modify` on the namespace being moved, `create` on the destination. `manage_grants` alone is not sufficient at either end. Re-parenting is a grant-shaped operation rather than an ordinary write — every principal holding `select`, `modify`, or `describe` at the destination immediately gains it on the moved namespace and everything inside it, through inheritance and without an assignment being recorded. Requiring grant authority at the source means the move confers nothing you could not already have granted directly; requiring it at the destination stops a namespace being populated and granted somewhere permissive and then moved in, issuing grants in a managed subtree that you could never have issued there.

Two consequences are worth knowing:

* Managed access removes `manage_grants` from *owners*, so ownership alone does not let someone move a namespace out of a managed subtree — which is the point, since moving it out would restore their ability to grant on it. A principal **directly assigned** `manage_grants` on that namespace still holds it — the direct assignment is unaffected by managed access — and can move the namespace provided it also holds `modify` there; ownership supplies that, so an owner who was additionally granted `manage_grants` directly can still move out.
* `can_move` is the conjunction of `manage_grants` and `modify`, so it is strictly stronger than `can_delete`: anyone who may move a namespace may also delete it, but not the reverse. A `security_admin` with no write access cannot restructure the hierarchy, and a principal granted only `modify` cannot either — moving needs both.

## Best Practices

We recommend separating access to data from the ability to grant privileges. To achieve this, the `security_admin` and `data_admin` roles divide the responsibilities of the initial `project_admin`, who has the authority to perform tasks in both areas.

## OpenFGA in Production

When deploying OpenFGA in production environments, ensure you follow the [OpenFGA Production Checklist](https://openfga.dev/docs/best-practices/running-in-production).

Lakekeeper includes [Query Consistency](https://openfga.dev/docs/interacting/consistency) specifications with each authorization request to OpenFGA. For most operations, `MINIMIZE_LATENCY` consistency provides optimal performance while maintaining sufficient data consistency guarantees.

For medium to large-scale deployments, we strongly recommend enabling caching in OpenFGA and increasing the database connection pool limits. These optimizations significantly reduce database load and improve authorization latency. Configure the following environment variables in OpenFGA (written for version 1.14). You may increase the number of connections further if your database deployment can handle additional connections:

```sh
OPENFGA_DATASTORE_MAX_OPEN_CONNS=200
OPENFGA_DATASTORE_MAX_IDLE_CONNS=100
OPENFGA_CACHE_CONTROLLER_ENABLED=true
OPENFGA_CHECK_QUERY_CACHE_ENABLED=true
OPENFGA_CHECK_ITERATOR_CACHE_ENABLED=true
```

## Reconciling OpenFGA against the catalog

The Postgres catalog is the source of truth for *which objects exist* (projects, warehouses, namespaces, tables, views, roles). OpenFGA stores the **structural hierarchy** between those objects plus all permissions (grants, ownership, role assignments). Under normal operation Lakekeeper keeps the two in sync on every API call.

Drift can still happen — for example after a backup/restore where Postgres and OpenFGA were snapshotted at different times, after pointing Lakekeeper at a fresh OpenFGA store, or after a rare bug. The `lakekeeper openfga reconcile` subcommand rebuilds the structural hierarchy in OpenFGA from the Postgres catalog.

```sh
# Add any hierarchy edges the catalog implies but OpenFGA is missing.
# Purely additive; never deletes. Safe default.
lakekeeper openfga reconcile --mode add-missing

# Same plus delete structural tuples the catalog contradicts.
lakekeeper openfga reconcile --mode add-and-delete-drift

# Show the diff without writing anything.
lakekeeper openfga reconcile --mode add-and-delete-drift --dry-run
```

### What reconcile touches

Reconcile only operates on the **structural** parts of the OpenFGA store: the parent/child edges between server, projects, warehouses, namespaces, tables, views, and roles. Ownership tuples, grants, role assignments, bootstrap admin tuples, and authorization-model bookkeeping are left alone. Tuples whose endpoints both refer to objects that *don't* exist in the catalog are also untouched — there is no anchor by which to interpret them.

### Operational notes

- Run during a low-traffic window. Reconcile does **not** stop API writes; concurrent renames/creates/deletes can produce transient inconsistencies that self-heal on the next run.
- A Postgres advisory lock prevents two reconciles from running at once. The second invocation fails fast with the lock key in the error message; you can confirm a held lock with `SELECT * FROM pg_locks WHERE locktype = 'advisory'`.
- Use `--dry-run` first when you intend to delete drift.

## Switching to OpenFGA or replacing the store

OpenFGA can be enabled, or its store replaced, on an already-bootstrapped Lakekeeper. Reconcile rebuilds the **structural hierarchy** from the catalog — but the initial server `admin` / `operator` tuple, ownership records, grants, and role assignments are **not** stored in the catalog and cannot be reconstructed from it. Lakekeeper's `/management/v1/bootstrap` endpoint runs only once per catalog by design; the `lakekeeper reopen-bootstrap` CLI re-opens it for cases like this without touching `server_id`, catalog data, or existing OpenFGA tuples.

### Procedure

1. Stand up an OpenFGA deployment.
2. Configure Lakekeeper for OpenFGA (`LAKEKEEPER__AUTHZ_BACKEND=openfga` plus the `LAKEKEEPER__OPENFGA__*` settings).
3. Run `lakekeeper migrate` once. This installs the OpenFGA authorization model in the configured store.
4. Run `lakekeeper openfga reconcile --mode add-missing` to seed the structural hierarchy from the catalog into OpenFGA.
5. Re-open bootstrap and seed the initial admin/operator via the API:

    ```sh
    lakekeeper reopen-bootstrap --yes
    ```

    This flips `server.open_for_bootstrap` back to `true`. The `server_id` is preserved, so the hierarchy tuples written in step 4 remain valid.

6. Open the Lakekeeper UI and complete the bootstrap flow as the intended initial admin (or operator) — same path as a fresh deploy.
7. From that admin/operator account, recreate the role assignments and grants you need through the management API. If you exported tuples from the previous OpenFGA store, you can also selectively reimport them with the [fga CLI](https://github.com/openfga/cli) — reconcile leaves non-structural tuples alone.

Switching *away* from OpenFGA (for example to Cedar) is not covered by reconcile and generally requires a new Lakekeeper instance.

> Instance admins are useful as a parallel safety net while the OpenFGA store has no admin tuples: they can still manage projects, warehouses, namespaces, and tables. They do **not** confer data-plane access (`ReadData`, `WriteData`, view `Select`) and they **cannot** write to the OpenFGA permission-management endpoints — see [Instance Admins](./instance-admins.md).
