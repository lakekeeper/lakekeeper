---
description: "Attach governance tags such as pii or sensitivity=restricted to warehouses, namespaces, tables, views, generic tables and columns for classification, discovery and, with Cedar in Lakekeeper Plus, access control."
---

# Governance Tags

Governance tags let you attach a controlled vocabulary of labels — `pii`, `sensitivity=restricted`, `deprecated` — to catalog objects (Warehouses, Namespaces, Tables, Views, Generic Tables, and columns). They are metadata for classification and discovery. With [Cedar](authorization-cedar.md#tag-based-access-control)<span class="lkp"></span> in Lakekeeper Plus, policies can also use them for access control. Under OpenFGA, tags do not change access decisions.

!!! warning "Preview"
    This API is in preview and may change in a backward-incompatible way in a future release.

!!! note "Not Iceberg snapshot tags"
    These are **management-plane** governance tags, applied through the `/management/v1/...` API. They are unrelated to Iceberg's native table-snapshot *tags* (named snapshot references created with `ALTER TABLE ... CREATE TAG`), which live in table metadata and are managed through the data-plane catalog API.

## Concepts

A tag has two parts:

- A **tag definition** — the project-scoped vocabulary entry: a name, what kinds of value it may carry, and which object types it may be applied to. Defined once per project.
- A **tag attachment** — a definition applied to a specific object, optionally with a value. The same definition can be attached to many objects.

Separating the two means the set of allowed tags is governed centrally, while applying them can be delegated per definition.

## Tag definitions

Create a definition by `POST`ing to `/management/v1/tag-definition`:

```json
{
  "name": "sensitivity",
  "description": "Data sensitivity level",
  "scope": ["table", "column"],
  "value-kind": "enumerated",
  "allowed-values": ["public", "internal", "restricted"]
}
```

- **`name`** — unique within the project, case-insensitively. `.` is a hierarchy delimiter (e.g. `pii.classification`); a name may not begin or end with `.` or contain empty segments. The names `system` and `lakekeeper`, and names that start with `system.` or `lakekeeper.`, are reserved.
- **`scope`** — the object types the definition may be attached to: any of `warehouse`, `namespace`, `table`, `view`, `generic-table`, `column`. Attaching to an out-of-scope object type is rejected.
- **`value-kind`** — how values are constrained (see below).

### Value kinds

| `value-kind` | Meaning | Value on apply |
|---|---|---|
| `marker` | Presence-only; the tag carries no value (e.g. `pii`, `deprecated`). | Must be omitted. |
| `free-text` | An arbitrary string, up to 256 characters. | Required. |
| `enumerated` | One of a fixed `allowed-values` set (case-sensitive, exact match). | Required; must be an allowed value. |

`allowed-values` is required (non-empty) for `enumerated` definitions and must be omitted otherwise. Each allowed value is likewise capped at 256 characters.

### Evolving a definition

`POST /management/v1/tag-definition/{id}` updates a definition, with guardrails that keep already-applied tags valid. The body must contain `name` and the full `scope`. Omitting `description` removes it.

- **`scope` can only be widened** — the new scope must contain every currently configured type. Removing an object type from `scope` is rejected, even if no object of that type carries the tag.
- **`add-allowed-values`** adds to an enumerated definition's set; existing values are never removed.
- **`value-kind` is immutable.**

`DELETE /management/v1/tag-definition/{id}` removes a definition, but only once no object references it — detaching all attachments first is required.

### Who may define tags

- Creating definitions requires the project's **tag-creator** capability (or a project security admin). This delegates vocabulary creation without granting broader administrative rights.
- Managing an *existing* definition — updating, deleting, or delegating who may apply it — keys off the definition's **ownership** (or a project security admin), not tag-creator.

See [Authorization](authorization.md) for the capability model.

## Applying tags to objects

Attach a tag with `PUT .../tags/{tag_name}`, carrying the value in the body (omit it for a `marker`):

```http
PUT /management/v1/warehouse/{warehouse_id}/table/{table_id}/tags/sensitivity
{ "value": "restricted" }
```

The full set of attachment endpoints, one per object type:

| Object | Path |
|---|---|
| Warehouse | `/management/v1/warehouse/{warehouse_id}/tags/{tag_name}` |
| Namespace | `/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/tags/{tag_name}` |
| Table | `/management/v1/warehouse/{warehouse_id}/table/{table_id}/tags/{tag_name}` |
| Column | `/management/v1/warehouse/{warehouse_id}/table/{table_id}/column/{column_name}/tags/{tag_name}` |
| View | `/management/v1/warehouse/{warehouse_id}/view/{view_id}/tags/{tag_name}` |
| Generic Table | `/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/tags/{tag_name}` |

Each supports `PUT` (attach or update the value) and `DELETE` (detach). `GET` on the same path without `/{tag_name}` lists the object's tags. Objects are addressed by UUID; tags and columns by name.

**Applying is idempotent.** Re-applying the same value is a no-op — it does not change the attachment's timestamp. Applying a *different* value updates it in place.

### Column tags

In the path, give the column name. For a nested field, join the names with `.`, for example `address.city`. Lakekeeper stores the tag on the column's field ID, so the tag stays when the column is renamed. `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/column-tags` lists every tagged column of a table with its tags in one call.

### Who may apply tags

Attaching a tag to an object requires **both**:

- the **manage-tags** capability on the target object, or on its table for a column. It is independent of write rights, so classification can be separated from data ownership.
- the **apply** capability on the tag definition — a per-definition delegation point for "who may apply *this* tag".

## Effective (inherited) tags

Tags applied high in the hierarchy apply to everything beneath. Pass `?effective=true` to any `GET .../tags` endpoint to get an object's **effective** tags — its own tags plus those inherited from its ancestors:

```http
GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/tags?effective=true
```

- **Inheritance runs down the containment chain**: a Table/View/Generic Table inherits from its Namespace chain and Warehouse; a Namespace inherits from parent Namespaces and the Warehouse.
- **Most-specific wins**: if the same definition is attached at several levels, the nearest attachment's value is returned; the rest are shadowed.
- **Columns are direct-only** — a column's effective tags are exactly its own; it does not inherit its table's tags.
- Each returned tag carries an **`inherited-from`** field naming the ancestor it came from; it is absent for the object's own (direct) tags.

With Cedar in Lakekeeper Plus<span class="lkp"></span>, policies read these effective tags directly; see [Tag-Based Access Control](authorization-cedar.md#tag-based-access-control).

Reading an object's tags requires permission to see the object's metadata (`describe` in OpenFGA). Effective tags are gated only on your access to the **queried object**: an inherited tag is part of that object's effective governance, so anyone who can read the object's tags sees them, values included. Granting someone `describe` on a sub-object therefore also exposes the tags it inherits from above.

## Finding where a tag is used

To list every object a definition is attached to, use the reverse lookup:

```http
GET /management/v1/tag-definition/{tag_definition_id}/attachments?value=restricted
```

Results are paginated and can be filtered by `value`, `targetType`, `warehouseId`, `createdAfter` and `createdBefore`. Tags on [soft-deleted](concepts.md#soft-deletion) objects are not listed.

The reverse lookup is gated more strictly than reading one object's tags — it requires the definition's **owner** or a project **security admin**, because enumerating every object a tag touches is a broad disclosure.

!!! note "Direct attachments only"
    The reverse lookup returns objects the definition is attached to **directly**. It does not expand inheritance, so it will not list objects that merely *inherit* the tag from an ancestor. A completeness sweep ("which objects are effectively `pii`?") must also account for inherited tags via the effective-tags view above.

## API reference

The exact request/response schemas for your running version are in the interactive Swagger UI at `/swagger-ui/#/` and in the [Management API reference](api/management.md). Under OpenFGA, the relations behind these capabilities are detailed in [Tags in the OpenFGA model](authorization-openfga.md#tags); under Cedar they are decided by policy.
