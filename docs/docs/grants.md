---
description: "Grant, list and revoke a single privilege for one principal on one resource with Lakekeeper's authorizer-independent Grants API."
---

# Grants API

A grant gives one **principal** (a user or a role) one named **privilege** on one **resource**: *Alice may `select` on this warehouse*. You hand them out, list them, and take them back, one at a time. See [Authorization](./authorization.md#grants-privileges-and-roles) for how grants relate to your authorizer's model.

!!! warning "Preview"
    This API is in preview and may change in a backward-incompatible way in a future release.

## One API, every authorizer

This is the authorizer-independent way to manage permissions: the same endpoints and the same request shape whichever [authorizer](./authorization.md#choose-an-authorizer) you run. What differs is what your authorizer does with them — and not every part of the API is supported everywhere.

| | [OpenFGA](./authorization-openfga.md) | [Cedar](./authorization-cedar.md)<span class="lkp"></span> | AllowAll |
|---|---|---|---|
| Grant and revoke | Yes | Yes | Recorded, never enforced |
| List a resource's grants | Yes | Yes | Yes |
| List everything one principal holds | No — `501` | Yes | Yes |
| List or revoke a whole subtree | No — `501` | Yes | Yes |
| Enforced | Yes | Yes, through policies | **No** |

Cedar keeps grants in the catalog database and enforces them through policies: the built-in policies make each grant confer its privilege, and policies you author can extend that. AllowAll records and lists grants faithfully while permitting every request regardless — use it for development, never to express a security posture.

Under OpenFGA a grant *is* the same relationship tuple the older `/management/v1/permissions/...` API writes, so the two are views of one set of permissions with nothing to migrate between them. Prefer this API for new scripts and integrations.

### Privilege names come from your authorizer

Which privileges exist, and what each one reaches, is defined by the authorizer — `select` under OpenFGA, `get_metadata` under AllowAll. Fetch the names rather than hardcoding them; a name the server does not know is rejected:

```http
GET /management/v1/grants/grantable-privileges
```

For what each name means and how far it reaches — whether `select` implies `describe`, whether a warehouse grant covers the tables inside it — see your authorizer's own page: [Grants in the OpenFGA model](./authorization-openfga.md#grants), or [Cedar](./authorization-cedar.md).

## Granting access

Grant and revoke in one request, against the resource you are granting on:

```http
POST /management/v1/warehouse/{warehouse_id}/grants
```

```json
{
  "writes": [
    { "privilege": "select", "principal": { "user": "oidc~alice" } }
  ]
}
```

Grant to a **role** rather than to individual users wherever you can — one grant plus role membership beats one grant per person.

Granting requires the authority to hand that privilege on, on that resource. Your authorizer decides who has it; under OpenFGA see [Grants](./authorization-openfga.md#grants).

!!! note "The instance-admin bypass does not apply to grant writes"
    Handing out permissions is not covered by the [instance-admin bypass](./instance-admins.md). Grant writes always go through the configured authorizer, which decides the outcome.

## Revoking access

Put the same entry under `deletes`:

```json
{
  "deletes": [
    { "privilege": "modify", "principal": { "role": "0198e0f4-3f6e-7c31-8c7d-9b7b8f2a1d44" } }
  ]
}
```

- **Applying is idempotent.** Granting twice creates one grant; revoking a grant nobody holds is not an error. The whole request lands atomically.
- Success is `204`: every entry now holds, and every revoked entry no longer does.
- At most **100 entries** per request, `writes` and `deletes` combined.
- Concurrent changes to the same resource's grants can answer `409`. The request applied nothing — retry it.

## Where you can grant

The same request shape works at every level:

| Resource | Path |
|---|---|
| Server | `/management/v1/server/grants` |
| Project | `/management/v1/project/grants` |
| Warehouse | `/management/v1/warehouse/{warehouse_id}/grants` |
| Namespace | `/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants` |
| Table | `/management/v1/warehouse/{warehouse_id}/table/{table_id}/grants` |
| View | `/management/v1/warehouse/{warehouse_id}/view/{view_id}/grants` |
| Generic table | `/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/grants` |
| Tag definition | `/management/v1/tag-definition/{tag_definition_id}/grants` |

`GET` on any of these lists the grants held there; `POST` applies a diff.

## Finding out what you may grant

`.../actions` does not report whether you may hand a privilege on — that is a separate right. Ask the resource:

```http
GET /management/v1/warehouse/{warehouse_id}/grants/grantable-privileges
```

Every privilege the level has is returned, each marked `allowed` for you. Add `principalUser` or `principalRole` to ask on someone else's behalf, which requires permission to read that resource's grants.

## Reviewing who has access

### On one resource

```http
GET /management/v1/warehouse/{warehouse_id}/grants
GET /management/v1/warehouse/{warehouse_id}/grants?principalUser=oidc~alice
```

Reading a resource's grants requires permission to read them there — with one exception.

### Your own access

Narrowing to **yourself** needs no such permission, only permission to see the resource. That lets a console show someone what they hold next to what they could ask for.

"Yourself" is the principal the request acts *as*. Belonging to a role does not make that role's grants yours. Under `X-Assume-Role` the request acts as that role, so narrowing to it is a self-read.

The server-level listing follows the same rule and is the only way to read your own server grants.

### Everything one principal holds in a project

```http
GET /management/v1/grants?principalUser=oidc~alice
GET /management/v1/grants?principalRole=<role_id>
```

Name exactly one of the two. To read every grant on a single resource, use that resource's own listing instead.

Asking about another principal requires the project's grant-read permission; asking about yourself is free. Server grants belong to no project and are excluded — use `GET /management/v1/server/grants`.

!!! warning "Not available under OpenFGA"
    OpenFGA stores permissions per object, so it cannot answer this without reading its whole store and returns `GrantListingNotImplemented` (501). Read one resource's grants from its own endpoint, or query OpenFGA directly. Deployments that keep grants in the catalog database answer it normally.

To answer "who can do what in this project", walk the resources you care about and read each one's grants. There is no whole-project export endpoint.

### Everything held under a namespace or warehouse

```http
GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/subtree
GET /management/v1/warehouse/{warehouse_id}/grants/subtree
```

One paginated call lists a whole branch of the catalog: the resource you address, every namespace beneath it, and the tables, views and generic tables inside those. A subtree contains its root, so the addressed resource's own grants are part of the answer; pass `includeRootLevel=false` for strictly-below.

Filter by `principalUser` or `principalRole`, by `createdBefore`, or by `privilege` and `resourceType` — repeat those two for each value you want, and name every kind: `resourceType=table` matches tables only. A privilege your authorizer does not know, or a `resourceType` that cannot occur under the root, is a `400` — so an empty page always means the access truly is absent.

A namespace-rooted call reads up to **5,000 namespaces**. Beyond that it is refused with `GrantSubtreeTooLarge`, naming the size it would have read — address a namespace further down, or use the warehouse-rooted form, which an index answers in page-proportional time whatever the warehouse holds. A read that spans few namespaces but very many grants is stopped by the database after ten seconds with `GrantSubtreeReadTimeout`; these calls hold a write-pool connection for their duration, so size that pool accordingly in large catalogs.

Reading a subtree takes its own permission on the root, stronger than reading one resource's grants: it covers the whole answer, so pages come back full — follow `next-page-token` until it disappears. Under OpenFGA it comes with `manage_grants`; under Cedar your policies decide who holds it.

Each response carries `as-of`, the instant it was read at, and the page token pins it: every page of one walk reads under the instant page one bound, with nothing to carry by hand. A `createdBefore` that disagrees with a token is refused — changing the window means starting a new listing.

!!! warning "Subtree grant-read is an enumeration capability"
    A subtree listing returns resources, principals and privileges the caller may not otherwise know exist. That reach is exactly what the permission grants — hand it out as deliberately as grant administration itself.

## Clearing a subtree

Revoke the matching grants under a namespace — or a whole warehouse — in one bounded request:

```http
POST /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/subtree/revoke
POST /management/v1/warehouse/{warehouse_id}/grants/subtree/revoke
```

```json
{
  "principal": { "user": "oidc~contractor" },
  "privilege": ["select"],
  "allow-partial": true
}
```

The body takes the same filters as the listing, except that a revoke always includes soft-deleted tabulars. An empty filter means *every grant under here*.

- **One call is not the whole operation.** There is no continuation token: revoked grants are gone, so repeating the same request takes the next batch. Repeat until `has-more` is `false`.
- **`has-more: false` means "nothing further was visible", not "the subtree is clear."** A grant whose transaction committed after a call began can carry a timestamp below that call's ceiling and outlive the loop. Where clearing must be demonstrated rather than attempted, re-run the operation with a fresh ceiling and confirm it removes nothing.
- **Pass `created-before` back on every later call.** The first response reports the ceiling it used; echoing it is what makes the loop terminate. Grants created after the operation began are deliberately left alone — to keep someone out, revoke the parent grant and let inheritance do it.
- **A request matching more than `limit` grants is refused outright** with `GrantRevokeBatchTooLarge`, and nothing is removed, unless you set `allow-partial`. Each call is its own transaction; the operation as a whole is not atomic, and the flag is how you say you know that.
- `limit` defaults to and is capped at **1000** grants per call.
- **There is no self-exemption.** Your own grants are revoked like anyone else's, including the one your authority to run the operation flows through — so a default revoke at a root you administer can leave you unable to make the next call. `include-root-level: false` is the way to keep the administration plane: it holds every grant on the addressed resource, yours included, and clears everything beneath it.
- **The root's own grants go too**, because a grant on a container confers access to everything beneath it — a revoke that skipped them would leave standing the access it names. Set `include-root-level: false` to keep them and clear only what lies below.
- **Grants on tables that were created but never committed** are in scope, like those on soft-deleted ones: the grant names a real table id and outlives the staging.
- **`dry-run: true` answers with the batch and removes nothing.** The response's `preview` lists every grant the live call would take, read and authorized the same way. A dry run is never refused for size; `has-more: true` is how you learn the live call needs `allow-partial`.
- Grants on **soft-deleted** tables and views are removed too. An undrop would otherwise restore the table together with the access you believed you had taken away.
- **Direct grants only.** Revoking `alice`'s grants leaves everything alice holds through a role, through an ancestor resource, or through a tag definition's own grants. A tag definition belongs to the project, not to the subtree, even when the tag is attached to tables in it.
- Every removed grant produces its own `grant_revoked` audit record.

A revoke needs two permissions on the resource you addressed: the subtree grant-read the listing takes — reading the batch is a read — and a dedicated subtree-revoke permission answering for the whole batch at once. Both reach downward by definition: holding one at a container means holding it over everything beneath, and an authorizer that cannot promise that must refuse these endpoints. A refusal removes nothing. Under OpenFGA both come with `manage_grants`; under Cedar your policies decide who holds them.

To preview a revoke, send the same body with `dry-run: true`, then pass the response's `created-before` on the live call — it removes exactly what the dry run showed, less anything revoked in between.

!!! warning "Not available under OpenFGA"
    Subtree operations need grants indexed by container, which an authorizer storing permissions per object cannot provide. Those return `SubtreeGrantsNotImplemented` (501). Deployments that keep grants in the catalog database answer normally.

## What grants cannot do

Grants say what a principal *may* do. They cannot express deny rules, conditions such as time windows or IP ranges, or row filters and column masks. A listing also shows **direct** grants only — see [Direct grants are not effective permissions](./authorization.md#direct-grants-are-not-effective-permissions).

## Reference

**Lifecycle.**

- Creating a resource can grant the creator privileges on it, if the authorization backend says creation confers them.
- Deleting a resource revokes its grants with it. Under OpenFGA this covers the deleted resource itself but not everything inside a deleted warehouse — see [grants outlive the resources they name](./authorization-openfga.md#managing-grants-through-the-grants-api).
- Deleting a user revokes their grants, so a re-created user id never inherits access.
- Grants removed *with* their principal or resource are not audited individually: deleting a user emits one revocation record per grant, but deleting a role or a resource does not. The deletion of the role or resource is the audit record, and the grants on it are gone by definition. Only explicit revocations through the grants API produce per-grant records.
- Soft-deleted tables and views keep their grants until expiration, so an undrop restores the access that was there before. They stay visible on the resource's own listing and are left out of the project-scoped listing until the table is restored.
- Deactivating a warehouse hides its children's grants but not its own, so an administrator can still audit and revoke at the warehouse level.

**Privilege categories.** Each entry from `grantable-privileges` carries a `display-name`, a `description`, and a `category` for grouping a picker:

| Category | Contains |
|---|---|
| `metadata` | reading an object's definition, and attaching tags |
| `read` | reading data |
| `write` | changing an object and its contents |
| `create` | creating objects inside it |
| `security` | ownership and the right to hand privileges on |
| `administration` | the coarse built-in roles, such as `project_admin` |

**Auditing.** Grants are current state, not history: a listing tells you what access exists, never how it came to exist, and **no grantor is published**. Who granted a privilege is answered by the `grant_created` audit record, per grant — see [Logging](./logging.md#operational-audit-events). Deleting a resource removes its grants without emitting one; the resource's own deletion record is the trace.

**Switching authorizers means re-granting, not migrating.** A grant recorded under one authorizer confers nothing under another, because the privilege names belong to the authorizer.

**Under OpenFGA.** A grant is a relationship tuple, which produces a handful of behavioural differences — paging, principal validation, event emission. See [Managing grants through the grants API](./authorization-openfga.md#managing-grants-through-the-grants-api).

**API reference.** Every endpoint, parameter and error is in the [Management API reference](./api/management.md).
