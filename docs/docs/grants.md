# Permissions & Grants

A grant gives one **principal** (a user or a role) one named **privilege** on one **resource**: *Alice may `select` on this warehouse*. Grants are the enumerable, click-driven half of authorization — you list them, hand them out, and take them back, one at a time.

The two words are used strictly throughout the API: a *privilege* is only ever the name of a capability (`select`), while a *grant* is only ever the assignment of one to a principal on a resource. So `.../grants` collections hold assignments, and `.../grants/grantable-privileges` asks which names may appear in them.

!!! warning "Preview"
    This API is in preview and may change in a backward-incompatible way in a future release.

## Grants and the authorizer

The configured [Authorizer](./authorization.md) decides every request; grants are records it decides from. The `/management/v1/.../grants` API works under **every** authorizer — that is what distinguishes it from the OpenFGA-specific `/management/v1/permissions/...` API, which returns 404 elsewhere — but the authorizer, not the API, defines what a grant means:

- **Which privileges exist.** Each authorizer publishes its own vocabulary. Fetch it from `GET /management/v1/grants/grantable-privileges` rather than hardcoding names — sending a name the server does not know is a `400`.
- **What a privilege implies.** Whether `select` includes `describe`, and whether a warehouse grant reaches the tables inside it, is the authorizer's model. Under OpenFGA, see [Grants in the OpenFGA model](./authorization-openfga.md#grants).
- **Where grants live, and whether they are enforced:**

| Authorizer | Vocabulary | Grants stored in | Enforced |
|---|---|---|---|
| OpenFGA | `select`, `modify`, `ownership`, … ([model](./authorization-openfga.md#grants)) | OpenFGA, as the same tuples the `/permissions` API writes | Yes |
| AllowAll | the catalog's own action names (`get_metadata`, `list_namespaces`, …) | catalog database | **No** — recorded and listed faithfully, but every request is permitted regardless |

A custom authorizer chooses its own position on both axes: it may manage grants itself or let the catalog store them, and it publishes its own vocabulary.

!!! warning "Privilege names are not portable between authorizers"
    A grant recorded under one authorizer is meaningless under another: it confers nothing. Switching authorizers means re-granting, not migrating. How the stale grant *appears* depends on where it was stored — the catalog store keeps it and lists it as `"recognized": false`, while grants held by an authorizer that no longer defines the privilege vanish from listings altogether.

## Giving and taking access

One endpoint per resource level applies a `{writes, deletes}` diff:

```http
POST /management/v1/warehouse/{warehouse_id}/grants
```

```json
{
  "writes": [
    { "privilege": "select", "principal": { "user": "oidc~alice" } }
  ],
  "deletes": [
    { "privilege": "modify", "principal": { "role": "0198e0f4-3f6e-7c31-8c7d-9b7b8f2a1d44" } }
  ]
}
```

The same shape works on every level: `server`, `project`, `warehouse`, `namespace`, `table`, `view`, `generic-table`, and `tag-definition`. A grant is identified by its `(principal, privilege, resource)` triple — there is no grant id, so a revocation is the `deletes` half of a diff, not a `DELETE .../grants/{id}`.

- The whole diff lands **atomically**, and applying it twice has the same effect as once.
- Success is `204 No Content`. There is no response body: a `204` means every entry in the diff now holds, and every entry in `deletes` no longer does. Whether an entry was *already* in that state is deliberately not reported — see below.
- At most **100 entries** per request, `writes` and `deletes` combined. The same entry may not appear in both lists, and unknown fields are rejected.
- `writes` must name privileges from the vocabulary; `deletes` need not — a privilege that has left the vocabulary must stay revocable, or its grants would be stuck forever.

Applying a grant requires grant authority on the resource — under OpenFGA, the matching `can_grant_*` relation (via `manage_grants`, or `pass_grants` plus holding the privilege yourself).

!!! note "Instance admins cannot grant"
    The [instance-admin bypass](./authorization.md#instance-admins) does not reach this API. Granting is permission administration, so it always goes through the configured Authorizer — otherwise a leaked operator credential could make any principal an admin. Instance admins provision; a principal holding real grants administers permissions.

!!! note "Why no delta is returned"
    Whether a grant was *newly* created or already held is not something every authorizer can determine. The catalog database computes it inside the apply transaction; an authorizer that owns its grants writes idempotently and cannot see the prior state without a second read it may not be able to make consistently. Reporting it would mean one response field meaning different things on different deployments. Ask `GET .../grants` for what exists now, and read the `grant_created`/`grant_revoked` audit events for what changed. The same reasoning applies to the older `/permissions/…/assignments` diff endpoints, which likewise return `204`.

## Who can do what here?

Every resource level lists the grants held directly on it:

```http
GET /management/v1/warehouse/{warehouse_id}/grants
GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/grants
GET /management/v1/tag-definition/{tag_definition_id}/grants
GET /management/v1/project/grants
GET /management/v1/server/grants
```

Reading a resource's grants requires that resource's grant-read permission — with one exception below.

### What do *I* hold here?

Add `principalUser` or `principalRole` to any listing to narrow it to one principal:

```http
GET /management/v1/warehouse/{warehouse_id}/grants?principalUser=oidc~alice
```

Narrowing to **yourself** needs no grant-read permission — only permission to see the resource, the same requirement its `.../actions` endpoint has. That lets a console show users their own access next to what they could ask for. Narrowing to anyone else, or not narrowing, requires grant-read.

"Yourself" means the principal the request is acting *as*. Being a member of a role does not make that role's grants your own — a role's grants belong to the role, so reading them needs grant-read. The exception is a request made **under an assumed role** (`X-Assume-Role`): it acts as that role, so narrowing to that same role is a self-read. Conversely, while acting under an assumed role you cannot read your own *user* grants without grant-read, because the request is not acting as your user.

The server listing follows the same rule and is the only way to read your own server-level grants.

### Direct grants are not effective permissions

A listing shows grants recorded *on that exact resource*, for *that exact principal*. It does not resolve role membership (a role's grant does not appear under its members) or inheritance (a warehouse grant does not appear in a table's listing, even where the model makes it effective there). To ask what a principal may *effectively* do, use the per-resource `.../actions` endpoints or `POST /management/v1/action/batch-check`.

## What may I grant here?

Grant authority is a right of its own, invisible to `.../actions`:

```http
GET /management/v1/warehouse/{warehouse_id}/grants/grantable-privileges
```

returns the level's **whole** vocabulary, each entry marked `allowed: true|false` for the caller. Render the disallowed entries greyed out rather than omitting them — a silently shortened list reads as a missing privilege. Each entry carries a `display-name`, a `description`, and a `category` for grouping a picker into columns:

| Category | Contains |
|---|---|
| `metadata` | reading an object's definition, and attaching tags |
| `read` | reading data |
| `write` | changing an object and its contents |
| `create` | creating objects inside it |
| `security` | ownership and the right to hand privileges on |
| `administration` | the coarse built-in roles, such as `project_admin` |

All three fields come from the authorizer; treat an unrecognized category as its own group and `null` as ungrouped. Add `principalUser`/`principalRole` to ask on someone else's behalf, which requires the resource's grant-read permission.

## What one principal holds

One listing spans the whole project, for a single principal — "what does alice have here":

```http
GET /management/v1/grants?principalUser=oidc~alice
GET /management/v1/grants?principalRole=<role_id>
```

Exactly one of the two is **required**: a request naming neither is refused with `MissingGrantPrincipal` (400). The listing crosses every resource in the project, so an unnarrowed answer would be sized by the deployment rather than by the question — to read every grant on one resource, use that resource's own listing.

Asking about another principal requires the project's grant-read permission; asking about yourself is free. Server grants belong to no project and are excluded — use `GET /management/v1/server/grants`.

!!! warning "Not available under every authorizer"
    This is the one endpoint on the grants surface whose availability depends on where grants are stored. An authorizer that indexes permissions by resource cannot answer it without reading its whole store, so it reports `GrantListingNotImplemented` (501) instead — **OpenFGA does**. With grants in the catalog database it works and pages normally. `GET /info` reports the configured backend, and every per-resource listing works under every authorizer.

To answer "who can do what in this project", walk the resources you care about and read each one's grants. There is no whole-project export endpoint.

## Reference

**Lifecycle.**

- Deleting a resource revokes its grants with it.
- Deleting a user revokes their grants, so a re-created user id never inherits access. Per-grant `GrantRevoked` events are emitted under the catalog store; an authorizer that owns its grants removes them with the user's other relations, without per-grant events.
- Soft-deleted tables and views keep their grants until expiration, so an undrop restores the access that was there before. They stay visible on the resource's own listing; whether they also appear in the project-scoped listing depends on the authorizer (see below).
- Deactivating a warehouse hides its children's grants (its tables read as absent) but not its own, so an administrator can still audit and revoke at the warehouse level.

**Auditing.** Every grant read and write emits an authorization event, and every change that actually happened emits a typed `GrantCreated` or `GrantRevoked` event carrying the full triple. Both reach the audit log.

Grants are current state, not history: a listing tells you what access exists, never how it came to exist. In particular **no grantor is published** — who granted a privilege is a question about a past event, and the `grant_created` audit event is where it is answered, per grant. Retain those events for as long as you need to answer it; once a grant is revoked its row is gone, and where a `grant_revoked` event was emitted it is the only record the access ever existed. Note that deleting a resource removes its grants without emitting one — the resource's own deletion event is the record. See [Logging](./logging.md) for the full rules.

**Boundaries.** Grants say what a principal *may* do. They cannot express deny rules, conditions (time windows, IP ranges), or row filters and column masks — those belong to a policy engine. Combine grants with roles to keep the grant count manageable: grant to a role once, then manage membership.

**Authorizer differences.** Every endpoint is reachable under every authorizer, with one exception noted in the table; behaviors differ by where grants are stored:

| | Catalog store | OpenFGA |
|---|---|---|
| `created-at` | recorded | the tuple's write time |
| Principal must exist | yes — an unknown user or role is `404 GrantTargetNotFound` | a user is taken as given, so granting to a mistyped id succeeds silently. Roles are still checked, but by the API rather than the store, so a role must exist and be in the project — except at the **server** level, which has no project to check against: there a grant to a nonexistent role succeeds |
| Revoking an unrecognized privilege | works | refused with `403` |
| Paging one resource's grants | normal paging | also pages, but an empty page can still carry a continuation token — non-privilege tuples are filtered out *after* paging, so only an absent token means the end |
| The project-scoped listing `GET /grants` | supported, pages normally | **not supported** — `GrantListingNotImplemented` (501). Tuples are indexed by object, so answering it means reading the store a level at a time; read one resource's grants from its own endpoint, or query OpenFGA directly |
| Applying below the warehouse under an assumed role | works | refused with `GrantNotSupported` — managed access has no public userset below the warehouse, so the request cannot be evaluated for a role. The same restriction the `/permissions` assignments API applies |
| Grant events on user deletion | one `grant_revoked` per grant | none — the authorizer removes the principal's grants with its other relations, without enumerating them |
| No-op grant events | suppressed | not suppressed — grant events are at-least-once under both, but only the catalog store can tell a no-op from a change |
| `pageSize` above 100 on a resource's grants | honoured up to the deployment maximum (default 1000) | clamped to 100 — the authorizer's read caps a page there. Follow the token rather than raising the page size |
| A privilege no longer in the vocabulary | listed with `recognized: false`, and still revocable | dropped from the listing entirely — the privilege is stored as a relation, and one the model does not define has no name to report |
| A grant whose resource is deleted | removed in the same transaction as the resource, by foreign key | removed afterwards, best effort. If that write fails the grant stays: the grant stays and can no longer be revoked, because the resource's own endpoint now answers `404`. `openfga reconcile` does not clean grants |
| Deleting a warehouse | removes the grants on its namespaces and tabulars too | removes only the warehouse's own. Grants inside it are left behind — invisible to listings, since the path back to the project is gone, but never reclaimed |
| Two applies racing on one resource | serialized per resource; a contended apply may be refused with `GrantLockTimeout` (409) and should be retried | one atomic write, so there is no lock to wait on and no conflict to surface |

See [Managing grants through the grants API](./authorization-openfga.md#managing-grants-through-the-grants-api) for the OpenFGA details, including interoperability with the older `/permissions` API.

!!! warning "AllowAll records grants but does not enforce them"
    With authorization disabled, grants are stored and listed faithfully and every request is permitted regardless. Use it for development, never to express a security posture.
