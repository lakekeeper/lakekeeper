---
description: "Configure Lakekeeper's structured JSON logging and RUST_LOG filtering, and understand the fields emitted by the tracing pipeline."
---

# Logging

## Overview

Lakekeeper emits structured JSON logs through the Rust `tracing` ecosystem. All logs include standard fields (`timestamp`, `level`, `message`, `target`) and can be filtered using the `RUST_LOG` environment variable.

## Controlling Log Output

### The RUST_LOG Environment Variable

The `RUST_LOG` variable controls which logs are emitted based on their **level** and **target** (the Rust module that produced the log). This applies to all log types including audit logs, error responses, and general application logs.

**Basic syntax:**

```bash
# Set global minimum level
RUST_LOG=info              # Show INFO, WARN, ERROR
RUST_LOG=debug             # Show DEBUG and above
RUST_LOG=warn              # Show only WARN and ERROR

# Filter by `target`
RUST_LOG=lakekeeper=debug                    # Debug for lakekeeper, nothing else
RUST_LOG=info,lakekeeper=debug               # INFO globally, DEBUG for lakekeeper
RUST_LOG=lakekeeper::service::events=trace   # Trace only the events module
```

For production environments, use `RUST_LOG=info` to avoid excessive log volume while capturing all important operational events. You can optionally reduce noise from verbose dependencies (e.g., `RUST_LOG=info,sqlx=warn`).

### Audit Logs and RUST_LOG

Audit logs are **enabled by default**. They will appear when `RUST_LOG` is set to `info` or higher (since audit logs are emitted at INFO level).

To disable audit logs entirely:

```bash
LAKEKEEPER__AUDIT__TRACING__ENABLED=false
```

**Note:** Audit logs contain PII — user identities, and a caller-supplied `user_agent` string that some clients populate with hostnames or OS usernames. When disabling them, ensure you have alternative mechanisms for compliance and security monitoring.

## Log Types

Lakekeeper produces three types of logs, distinguished by the `event_source` field:

### 1. Audit Logs {#audit-logs}

Authorization events tracking access to catalog resources. **Contains PII** (user identities).

**Identified by:** `"event_source": "audit"`

Audit logs cover two distinct schemas depending on the source of the event:

#### Authorization Events

Emitted for every authz check. Always contain `action`/`actions`, `entity`/`entities`, `actor`, and `decision`.

Discriminate on `operation`, not on the presence of `entity`: a record carrying `operation` belongs to the [operational family](#operational-audit-events) below even when it also carries `action`/`entity`.

**Structure:**

| Field                  | Type            | Description                       |
|------------------------|-----------------|-----------------------------------|
| `event_source`         | String          | Always `"audit"`                  |
| `action` or `actions`  | Object or Array | Operation(s) attempted. Each action is an object with an `action_name` field (e.g., `"read_data"`, `"drop"`, `"create_namespace"`) and optional context fields (e.g., `properties`, `updated-properties`, `removed-properties`). See format below. |
| `entity` or `entities` | Object or Array | Resource(s) accessed, containing `entity_type` and type-specific fields (e.g., `warehouse-id`, `namespace`, `table`) |
| `actor`                | Object          | Who performed the action (see format below) |
| `privilege_source`     | String          | Request-level classification of the caller's privilege: `"authorizer"` (no special privileges — all decisions come from the configured Authorizer backend), `"instance_admin"` (caller listed in `LAKEKEEPER__INSTANCE_ADMINS` — control-plane actions are auto-approved, data-plane actions still go through the Authorizer), or `"internal"` (in-process call — full bypass). This is a property of the request, not of individual entries in the `authorizations` array. See [Instance Admins](./instance-admins.md). |
| `user_agent`           | String or null  | The caller's `User-Agent` request header, recorded verbatim and truncated to 256 bytes. `null` when the request sent no `User-Agent` (or sent one that was not valid text) — for example an in-process call from a background worker. **Client-supplied and unverified** — see below. |
| `decision`             | String          | `"allowed"` or `"denied"` — the rollup decision for the whole event |
| `authorizations`       | Array           | Per-decision breakdown. Always present and non-empty. Each entry is self-contained — see [Per-decision breakdown](#per-decision-breakdown-authorizations) below |
| `idempotency_key`      | String \| null  | The request's `Idempotency-Key`, or `null` when the caller sent none. Present so a retry can be tied to the request that did the work — see [Idempotent replays](#operational-audit-events) |
| `context`              | Object          | Optional. Additional operation context (e.g., `project-id`, `warehouse-name`) |
| `failure_reason`       | Object          | Only on failed events. Single-key object identifying the variant — one of `{"ActionForbidden": []}`, `{"ResourceNotFound": []}`, `{"CannotSeeResource": []}`, `{"InternalAuthorizationError": []}`, `{"InternalCatalogError": []}`, `{"InvalidRequestData": []}`. The empty array is the variant payload. |
| `error`                | Object          | Only on failed events. Contains `type`, `message`, `code`, `error_id`, `stack` |

**Note:** Empty arrays and objects are omitted from the output. For example, if `stack` is empty, the field will not appear in the log.

**`user_agent` is client-supplied and unverified.** It is the `User-Agent` request header, recorded as sent. Any caller can set it to any value, including one that names a different client, and Lakekeeper neither validates it nor cross-checks it against the token. Treat it as a hint — fleet inventory, spotting an unexpected client library, triaging why one client started failing — never as identity, and never as an authorization or attribution input. The authoritative statements of *who* and *as what* are `actor` and `privilege_source` on the same event. A detection rule keyed on `user_agent` can be evaded by changing one header; one keyed on `actor` cannot.

Lakekeeper records the header rather than a parsed client name, so a consumer can classify it however it needs and no information is lost to normalisation. Two consequences worth planning for: values are free-form and some clients embed hostnames or usernames in them, so treat the field as potentially disclosing infrastructure detail; and browsers cannot set `User-Agent`, so requests from the web UI are recorded with the browser's own value.

**Ordering:** An authorization event records the *attempt*, and is dispatched to the audit log before the authorized operation issues its write. An `"allowed"` decision therefore means the caller was permitted to perform the action, not that the action succeeded — if the operation fails afterwards the request returns an error while the authorization event remains in the log. Audit consumers should treat authorization events as attempts rather than as confirmation that state changed.

**Actor Types:**

```json
// Anonymous
{"actor_type": "anonymous"}

// Authenticated user
{"actor_type": "principal", "principal": "oidc~user@example.com"}

// Assumed role
{"actor_type": "assumed-role", "principal": "oidc~user@example.com", "assumed_role": "role-id"}

// Internal system
{"actor_type": "lakekeeper-internal"}
```

**Action Format:**

Each action is a structured object containing the operation name and optional context about the operation:

```json
// Simple action (no context)
{"action_name": "read_data"}

// Action with properties context (e.g., create_namespace)
{"action_name": "create_namespace", "properties": {"location": "s3://bucket/ns", "owner": "alice"}}

// Action with update context (e.g., commit with property changes)
{"action_name": "commit", "updated-properties": {"retention-days": "30"}, "removed-properties": ["staging"]}
```

When only a single action is involved, it appears as the `action` field. When multiple actions are checked the `actions` field contains an array.

**Grant changes (`action_name = "apply_grants"`):**

Applying a grant diff is authorized once for the whole request, so a single `apply_grants` action describes the entire diff:

| Context field | Type   | Description                                                                 |
|---------------|--------|-----------------------------------------------------------------------------|
| `principals`  | Array  | The distinct principals the grants were destined for, each prefixed by kind (`user:oidc~alice`, `role:<uuid>`) |
| `privileges`  | Array  | The distinct privilege names named anywhere in the diff                     |
| `writes`      | String | Number of entries requested as grants, before deduplication                 |
| `deletes`     | String | Number of entries requested as revocations, before deduplication            |

The resource the grants apply to is the event's `entity`, not part of the action.

`principals` and `privileges` are deduplicated, so neither is a per-entry list and neither can be matched positionally against the other — a diff naming two principals and two privileges records both sets, not which pairing was requested. The counts are the request's, so `writes: "3"` with one entry in `principals` means three grants for one principal. Requests are capped at 100 entries, which bounds both lists.

**This records the attempt.** What actually changed is recorded separately, one record per grant, under `operation = "grant_created"` / `"grant_revoked"`. Both are audit-log records; neither is published to the configured event stream (Kafka, NATS, CloudEvents). Read this one for what was asked and whether it was allowed, and those for what took effect.

Because the event records the attempt, a *denied* apply is logged with the same detail as an allowed one: what was asked for, for whom, and on which resource. A refused privilege escalation is attributable to its intended beneficiary, not merely to the caller who attempted it.

```json
// Denied attempt to grant `modify` to two principals
{
  "action_name": "apply_grants",
  "principals": ["role:1f7b…", "user:oidc~alice"],
  "privileges": ["modify"],
  "writes": "2",
  "deletes": "0"
}
```

#### Per-decision breakdown (`authorizations`)

Every authorization event carries an `authorizations` array with **at least one entry**. For ordinary single-check API calls the array has exactly one entry, synthesised from the event's top-level fields. For batch-style endpoints (e.g. `/management/v1/action/batch-check` and the various `get_*_actions` introspection endpoints) the array contains one entry per inner check, in request order.

This means audit consumers can use **one query path** for both single and batch events: iterate `authorizations[]` and read the per-entry `allowed` flag, instead of switching between top-level `decision` and a per-batch breakdown.

Each entry is **self-contained** — it does not require zipping with the top-level fields:

| Field           | Type    | Description                                                                          |
|-----------------|---------|--------------------------------------------------------------------------------------|
| `id`            | String  | Stable identifier for this entry. When the client supplies an `id` on a batch-check input it appears verbatim here, and the API response echoes the same value so the two can be correlated 1:1. When the client omits `id`, the API response omits it too; the audit log instead substitutes the request item's zero-based index as an internal bookkeeping fallback so individual decisions can still be pinpointed in the logs. **Do not assume the API response carries index-based ids — that fallback exists only in audit entries.** Absent on synthesised single-check entries. |
| `for-principal` | Object  | Optional. The principal whose permission was evaluated, when different from the request actor. Shape: `{"user": "..."}` or `{"role": "..."}`. Absent means the request actor itself. |
| `action`        | Object  | Same shape as the top-level `action` field.                                          |
| `entity`        | Object  | Same shape as the top-level `entity` field.                                          |
| `allowed`       | Boolean | The decision for *this* tuple. Absent when no definitive verdict was reached — e.g. on `InternalAuthorizationError`, `InternalCatalogError`, or `InvalidRequestData` failures, where the system never actually evaluated the request. Definitive denials (`ActionForbidden`, `ResourceNotFound`, `CannotSeeResource`) are recorded as `false`. |
| `determined_by` | Array   | Optional; present only when the Authorizer surfaces per-decision diagnostics (some backends, e.g. OpenFGA and allow-all, produce none, and the field is then absent). Each element attributes *this* decision to a factor: a matched **policy** (carrying its identifier, an optional author-supplied name, an effect of `Permit` or `Forbid`, and an optional originating source), or a **system-authority override** (an optional source and human-readable reason) recording that a built-in/system authority tier — rather than a configured policy — determined the allow, e.g. a recovery grant that lets a privileged system role act despite a policy that would otherwise forbid it. Distinct from the top-level `privilege_source`, which classifies the caller rather than individual decisions. |

**Top-level vs. per-entry semantics.** The top-level `actor` always reflects the *API caller* (the bearer token holder); `authorizations[].for-principal` reflects *whose permissions were checked*. For most calls these are the same and `for-principal` is omitted. For introspection endpoints like `GET /lakekeeper/v1/permissions/...?for-user=X` the actor is the caller while every entry's `for-principal` is `X` — both facts are recorded structurally on the same event, no `context.for-user` string needed.

**Examples:**

<details>
<summary>Authorization Succeeded</summary>

```json
{
  "timestamp": "2026-02-15T14:20:50.758690Z",
  "level": "INFO",
  "event_source": "audit",
  "action": {
    "action_name": "create_warehouse",
    "name": "demo"
  },
  "entity": {
    "entity_type": "project",
    "project-id": "00000000-0000-0000-0000-000000000000"
  },
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~94eb1d88-7854-43a0-b517-a75f92c533a5"
  },
  "privilege_source": "authorizer",
  "user_agent": "PyIceberg/0.9.1",
  "decision": "allowed",
  "authorizations": [
    {
      "action": {
        "action_name": "create_warehouse",
        "name": "demo"
      },
      "entity": {
        "entity_type": "project",
        "project-id": "00000000-0000-0000-0000-000000000000"
      },
      "allowed": true
    }
  ],
  "message": "Authorization succeeded event",
  "target": "lakekeeper::service::events::backends::audit"
}
```

</details>

<details>
<summary>Authorization Failed</summary>

```json
{
  "timestamp": "2026-02-15T14:21:10.123456Z",
  "level": "INFO",
  "event_source": "audit",
  "action": {
    "action_name": "drop"
  },
  "entity": {
    "entity_type": "table",
    "warehouse-id": "414b18f0-0a6d-11f1-b2d7-f31430431ca0",
    "namespace": "production",
    "table": "sensitive_data"
  },
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~user@example.com"
  },
  "privilege_source": "authorizer",
  "user_agent": "Trino/476",
  "decision": "denied",
  "authorizations": [
    {
      "action": {
        "action_name": "drop"
      },
      "entity": {
        "entity_type": "table",
        "warehouse-id": "414b18f0-0a6d-11f1-b2d7-f31430431ca0",
        "namespace": "production",
        "table": "sensitive_data"
      },
      "allowed": false
    }
  ],
  "failure_reason": {
    "ActionForbidden": []
  },
  "error": {
    "type": "Forbidden",
    "message": "Insufficient permissions",
    "code": 403,
    "error_id": "01234567-89ab-cdef-0123-456789abcdef"
  },
  "message": "Authorization failed event",
  "target": "lakekeeper::service::events::backends::audit"
}
```

</details>

<details>
<summary>Batch check (introspect_permissions) — multiple inner decisions</summary>

A single `POST /management/v1/action/batch-check` call from `oidc~94eb1d88-…` asking whether `oidc~cfb55bf6-…` may `delete` a warehouse and `read_data` from a table. Top-level `actor` is the caller; each `authorizations[]` entry records the on-behalf-of principal and its individual decision.

```json
{
  "timestamp": "2026-04-07T17:58:34.358975Z",
  "level": "INFO",
  "event_source": "audit",
  "action": {
    "action_name": "introspect_permissions"
  },
  "entities": [
    {
      "entity_type": "warehouse",
      "warehouse-id": "255a8f5c-32ab-11f1-889e-4706b6f66241"
    },
    {
      "entity_type": "table",
      "warehouse-id": "255a8f5c-32ab-11f1-889e-4706b6f66241",
      "namespace": "production",
      "table": "events"
    }
  ],
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~94eb1d88-7854-43a0-b517-a75f92c533a5"
  },
  "privilege_source": "authorizer",
  "user_agent": "curl/8.7.1",
  "decision": "allowed",
  "authorizations": [
    {
      "id": "warehouse-delete",
      "for-principal": {
        "user": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
      },
      "action": {
        "action_name": "delete"
      },
      "entity": {
        "entity_type": "warehouse",
        "warehouse-id": "255a8f5c-32ab-11f1-889e-4706b6f66241"
      },
      "allowed": true
    },
    {
      "id": "1",
      "for-principal": {
        "user": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
      },
      "action": {
        "action_name": "read_data"
      },
      "entity": {
        "entity_type": "table",
        "warehouse-id": "255a8f5c-32ab-11f1-889e-4706b6f66241",
        "namespace": "production",
        "table": "events"
      },
      "allowed": false
    }
  ],
  "message": "Authorization succeeded event",
  "target": "lakekeeper::service::events::backends::audit"
}
```

</details>

#### Operational Audit Events

Emitted for operations that produce no authorization decision of their own — LDAP/directory role resolution and user enrichment, the grants an apply actually wrote, and requests answered from an idempotency record. Use these to audit *what the system did on behalf of a user*, rather than *whether the user was allowed to do something*. Several carry user identity (PII); the per-operation sections below say which.

**Structure:**

| Field          | Type   | Description                                        |
|----------------|--------|----------------------------------------------------|
| `event_source` | String | Always `"audit"`                                   |
| `operation`    | String | Machine-readable name of the operation (e.g., `"ldap_resolve_roles"`) |
| `actor`        | Object | Same shape as authorization events: `{"actor_type": "principal", "principal": "oidc~…"}` |
| `outcome`      | String | Result of the operation. Component-specific; see individual operation docs below |
| `context`      | Object | Optional. Operation-specific metadata (e.g., `provider_id`, `role_count`) |

**Outcomes are not binary allow/deny** — they describe the result of the system operation. No `decision` field is present.

**Grant changes (`operation = "grant_created"` / `"grant_revoked"`):**

Emitted after the change is committed, one event per grant the backend reported as applied. `outcome` is always `success`: the event asserts the state the apply left behind, not that the grant differed from what was there before.

These are the confirmed counterpart to the `apply_grants` authorization event. The authorization event records the *attempt* and deduplicates principals and privileges into separate lists, so it cannot say which principal received which privilege; these events carry the full triple, one per grant:

| Context field  | Description                                                                 |
|----------------|-----------------------------------------------------------------------------|
| `principal`    | Who holds the grant, as `{"user": "…"}` or `{"role": "…"}`                   |
| `privilege`    | The privilege name, verbatim from the authorizer's vocabulary                |
| `resource_type`| `server`, `project`, `warehouse`, `namespace`, `table`, `view`, `generic-table` or `tag-definition` |
| `resource_id`  | The exact resource. Absent for `server` grants, which have no id            |
| `warehouse_id` | The containing warehouse, for warehouse-scoped resources only               |

Grants are hard-deleted and keep no history, so a `grant_revoked` event is the only lasting trace of the revocation. It is not proof the access existed: these events report post-apply state, so revoking a grant nobody held can emit one too. Retain them if you need to answer who held what, when.

**These events do not cover every way a grant disappears.** Two paths remove grants without emitting one:

- **Deleting the resource.** Dropping a warehouse, namespace, table, view or tag definition removes its grants in the database directly; the removal is never seen as individual grants, so no event is emitted. The resource's own deletion event is the record.
- **Deleting a user, under an authorizer that owns its grants.** The authorizer removes the principal's grants along with its other relations, without enumerating them. Where grants live in the catalog database, user deletion *does* emit one event per revoked grant.

So a `grant_created` event with no matching `grant_revoked` does **not** imply the grant is still held. To determine current access, read `GET .../grants`; use these events for attribution and change history, not as a ledger you can replay to a current balance.

**These events assert state, not transitions, and may repeat.** A `grant_created` means *this grant is now in effect as of this request* — not that it did not exist before. A `grant_revoked` means *this grant is now not in effect*. Applying the same diff twice can therefore emit the same events twice, and revoking a grant nobody held can emit a `grant_revoked`.

**Delivery is best-effort, after the fact.** Listeners are invoked once the change is committed, so a listener that fails, or a process that stops between the commit and the dispatch, loses the record — the failure is logged and not retried. The grant itself still stands. Treat a missing event as possible rather than impossible, and do not use these events as the authoritative account of what changed.

That is deliberate: whether a grant was *already* held is not something every authorizer can determine, while the state after a successful apply is unambiguous under all of them. Make consumers idempotent — key on the `(principal, privilege, resource)` triple rather than counting events. Where grants live in the catalog database the server can tell a real change from a no-op and will skip the event, but that is an optimisation you should not depend on.

**Idempotent replays (`operation = "idempotent_replay"`):**

Emitted when a request carrying an `Idempotency-Key` was answered from the stored record instead of being executed. `outcome` is always `replayed`.

These records also carry the top-level `action`, `entity`, `privilege_source` and `user_agent` fields of an authorization event, so a retry reads with the same queries as the request that did the work — join the two on `idempotency_key`, which every *authorization* record carries too. The operational records above (`grant_created`, `ldap_resolve_roles`) do not carry it.

| Field             | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `idempotency_key` | The key whose record served the request                                     |
| `action`          | The action the retry asked for, in the same shape as an authorization event  |
| `entity`          | The target the retry named, in the same shape as an authorization event      |

**`action` and `entity` are what the retry asked for, not what the original request did.** A record is matched on warehouse, key and endpoint — never on the target or the request parameters ([Idempotency](./configuration.md#idempotency)). A key reused on the same endpoint against a different table produces a record naming that table, which was not touched; a retry sent with `purgeRequested=true` after an original without it is recorded as a purging drop. Read these as what was asked and answered from a record, not as evidence of what happened.

**A replay record does not establish that its actor was ever authorized for that entity.** The replay is served before authorization runs, so any authenticated caller holding the key can produce one — including one with no grants in that warehouse. The key cannot cause a mutation: a replay executes nothing.

**Audit records carry live idempotency keys**, on authorization records as well as these, for as long as `idempotency-key-lifetime` plus the grace period. A reader of the audit log can replay those keys to a 204 and mint further records; treat audit-log access accordingly.

**Seven endpoints emit this marker**, and no others: `dropTable`, `dropView`, `dropNamespace` (recorded as `action_name = "delete"`), `dropGenericTable`, `renameTable`, `renameView`, `renameGenericTable`. They answer 204 and serve the replay before authorizing, so no `decision` is recorded — the mutation already happened, and denying the retry would report a failure for a completed operation.

**Replays of the other idempotent endpoints are not marked, and do not look like the original request.** `createTable`, `registerTable`, `createNamespace`, `updateNamespaceProperties`, `replaceView` and `createGenericTable` re-derive their response body by loading it, so a retry emits the authorization record of that *load* — `action_name = "get_metadata"` on the entity — and no record for the create or update; `updateTable` emits both `commit` and `get_metadata`; `commitTransaction` authorizes before it detects the replay, so its retry is indistinguishable from a first execution. In all of these, `idempotency_key` is the only field that identifies a retry.

These records are audit-log only. Like the grant records above, they are never published to the configured event stream (Kafka, NATS, CloudEvents), and delivery is best-effort.

**LDAP role resolution (`operation = "ldap_resolve_roles"`):**

| `outcome`        | When emitted                                              |
|------------------|-----------------------------------------------------------|
| `success`        | User found and role list resolved (possibly empty after mapping) |
| `user_not_found` | No LDAP entry matched the search filter for this subject  |
| `no_roles`       | User entry exists but the group-membership attribute is absent |
| `ambiguous_user` *(since 0.12.2)* | LDAP search matched more than one entry for the subject; request errors out |
| `dn_no_match` *(since 0.12.2)* | `Branching` mode with `else.mode = none`: the user DN did not match `branch_if_user_dn_matches`; empty role list returned |

*Since 0.12.2*, every `ldap_resolve_roles` context carries a `mode` field describing which resolution path was active. Possible values:

| `mode`                  | Meaning                                                                                                 |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| `search`                | Stand-alone Search-mode resolution                                                                      |
| `attribute`             | Stand-alone Attribute-mode resolution                                                                   |
| `branching`             | Branching mode, but no branch decision was reached (e.g. `user_not_found`)                              |
| `branch_then`           | Branching mode `then` branch ran (DN matched the regex)                                                 |
| `branch_else_attribute` | Branching mode `else.mode = attribute` ran (DN did not match)                                           |
| `branch_else_none`      | Branching mode `else.mode = none` (only emitted alongside `outcome = "dn_no_match"`)                    |

**PII in context fields.** `filter` (substituted with the user's subject), `user_dn`, and `principal` are PII. `provider_id`, `attribute`, `pattern`, `role_count`, `count`, and `mode` are not.

**Examples:**

<details>
<summary>Roles resolved successfully</summary>

```json
{
  "timestamp": "2026-03-05T09:12:34.000000Z",
  "level": "INFO",
  "event_source": "audit",
  "operation": "ldap_resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~j791840@corp.example.com"
  },
  "outcome": "success",
  "context": {
    "provider_id": "my-ldap",
    "role_count": 3,
    "mode": "search"
  },
  "message": "LDAP role resolution complete",
  "target": "lakekeeper_role_provider::role_provider::ldap"
}
```

</details>

<details>
<summary>User not found in LDAP</summary>

```json
{
  "timestamp": "2026-03-05T09:12:34.000000Z",
  "level": "INFO",
  "event_source": "audit",
  "operation": "ldap_resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~unknown@corp.example.com"
  },
  "outcome": "user_not_found",
  "context": {
    "provider_id": "my-ldap",
    "filter": "(&(objectClass=person)(uid=unknown))",
    "mode": "search"
  },
  "message": "LDAP user not found; returning empty role list",
  "target": "lakekeeper_role_provider::role_provider::ldap"
}
```

</details>

<details>
<summary>Ambiguous user (multiple matches)</summary>

```json
{
  "timestamp": "2026-03-05T09:12:34.000000Z",
  "level": "INFO",
  "event_source": "audit",
  "operation": "ldap_resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~alice@corp.example.com"
  },
  "outcome": "ambiguous_user",
  "context": {
    "provider_id": "my-ldap",
    "filter": "(&(objectClass=person)(uid=alice))",
    "count": 2,
    "mode": "attribute"
  },
  "message": "LDAP search matched multiple entries; cannot resolve principal unambiguously",
  "target": "lakekeeper_role_provider::role_provider::ldap"
}
```

</details>

<details>
<summary>Branching mode: user DN did not match (else.mode = none)</summary>

```json
{
  "timestamp": "2026-03-05T09:12:34.000000Z",
  "level": "INFO",
  "event_source": "audit",
  "operation": "ldap_resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~svc-account@corp.example.com"
  },
  "outcome": "dn_no_match",
  "context": {
    "provider_id": "my-ldap",
    "user_dn": "CN=svc-account,OU=Services,DC=corp,DC=example,DC=com",
    "pattern": "OU=(?<tenant>[^,]+),OU=Tenants,",
    "mode": "branch_else_none"
  },
  "message": "branching DN regex did not match; explicit no-roles outcome",
  "target": "lakekeeper_role_provider::role_provider::ldap"
}
```

</details>

**Role resolution (`operation = "resolve_roles"`):**

| `outcome`                | When emitted                                      |
|--------------------------|---------------------------------------------------|
| `no_provider_applicable` | No configured role provider matched this user.    |
| `roles_resolved`         | At least one role was resolved. Disabled by default — enable with `LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_ROLE_ASSIGNMENTS=true`. The `context` contains `role_count`, the full `roles` list, and `sources` showing where each provider's roles came from (`fresh`, `cache_hit`, `stale_fallback`, or `in_request`). |
| `error`                  | A matched provider failed to resolve roles (e.g. LDAP connection error). The request proceeds with an empty role set. |

The `no_provider_applicable` outcome is enabled by default and can be controlled via `LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_UNHANDLED_USERS`. A `no_provider_applicable` outcome for a user that you expect to be covered indicates a misconfigured domain filter or a missing provider. Set the variable to `false` to suppress these events if some users are intentionally not covered.

The `roles_resolved` outcome is **disabled by default** because it fires on every authenticated request and contains the full list of resolved role names. Enable it temporarily to debug role-provider configuration — do not leave it on in production.

The `error` outcome always fires when role resolution fails. It is accompanied by a general application warning in the non-audit log stream (without PII).

<details>
<summary>No provider applicable</summary>

```json
{
  "timestamp": "2026-03-07T10:00:00.000000Z",
  "level": "INFO",
  "event_source": "audit",
  "operation": "resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~unknown@other-domain.com"
  },
  "outcome": "no_provider_applicable",
  "context": {
    "providers_checked": ["ldap-prod"]
  },
  "message": "No role provider handled user; user will have no provider-assigned roles"
}
```

</details>

<details>
<summary>Roles resolved (debug)</summary>

```json
{
  "timestamp": "2026-03-07T10:00:01.000000Z",
  "level": "INFO",
  "event_source": "audit",
  "operation": "resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~alice@corp.example.com"
  },
  "outcome": "roles_resolved",
  "context": {
    "role_count": 2,
    "roles": ["my-ldap~devs", "my-ldap~admins"],
    "sources": {"my-ldap": "cache_hit", "oidc": "in_request"}
  },
  "message": "Resolved role assignments for user"
}
```

</details>

**Role assignment cache (`operation = "cached_role_provider"`):**

| `outcome`             | When emitted                                                            |
|-----------------------|-------------------------------------------------------------------------|
| `stale_cache_fallback` | One or more providers failed to refresh; stale DB-cached roles are returned instead. The `context.provider_ids` field lists the affected providers. |

This outcome is always accompanied by a WARN-level general log (without PII) and indicates a transient connectivity issue with the role provider (e.g. LDAP unavailable). The user receives their last-known roles rather than an error.

<details>
<summary>Stale cache fallback</summary>

```json
{
  "timestamp": "2026-03-07T11:30:00.000000Z",
  "level": "INFO",
  "event_source": "audit",
  "operation": "cached_role_provider",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~user@corp.example.com"
  },
  "outcome": "stale_cache_fallback",
  "context": {
    "provider_ids": ["ldap-prod"]
  },
  "message": "stale provider(s) failed to refresh; serving cached roles"
}
```

</details>

**jq filters for operational audit events:**

```bash
# All LDAP resolution events
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "ldap_resolve_roles")'

# Users not found in LDAP (misconfigured filter or unknown principals)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .outcome == "user_not_found")'

# Successful resolutions for a specific user
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "ldap_resolve_roles" and .actor.principal == "oidc~user@example.com")'

# Users not matched by any role provider
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .outcome == "no_provider_applicable")'

# Stale cache fallbacks (role provider unreachable, last-known roles served)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .outcome == "stale_cache_fallback")'
```

### 2. Error Response Logs

HTTP error responses returned to clients. **Does not contain PII.**

**Identified by:** `"event_source": "error_response"`

**Structure:**

| Field          | Type   | Description                                        |
|----------------|--------|----------------------------------------------------|
| `event_source` | String | Always `"error_response"`                          |
| `error`        | Object | Contains `type`, `code`, `message`, `error_id`, `stack`, `source` |

**Note:** Empty arrays are omitted. If `stack` or `source` are empty, they will not appear in the log.

**Example:**

```json
{
  "timestamp": "2026-02-15T14:22:15.456789Z",
  "level": "ERROR",
  "event_source": "error_response",
  "error": {
    "type": "TableNotFound",
    "code": 404,
    "message": "Table 'my_table' not found in namespace 'production'",
    "error_id": "01234567-89ab-cdef-0123-456789abcdef",
    "stack": ["Additional context here"],
    "source": ["Caused by: ..."]
  },
  "message": "Internal server error response",
  "target": "iceberg_ext::catalog::rest::error"
}
```

**Note:** For 5xx errors, the `stack` and `source` fields are logged but hidden from the HTTP response body for security.

### 3. General Application Logs

Standard operational and debug logs from Lakekeeper. No `event_source` field.

**Example:**

```json
{
  "timestamp": "2026-02-15T14:20:42.425131Z",
  "level": "INFO",
  "message": "Authorization model for version 4.3 found in OpenFGA store lakekeeper. Model ID: 01KHGMK6TQKN1AVMWX16E37AD1",
  "target": "openfga_client::migration"
}
```

## Additional Configuration

### Extended Debug Logs

Include source file locations and line numbers in logs:

```bash
LAKEKEEPER__DEBUG__EXTENDED_LOGS=true
```

This is useful for debugging but increases log size.

## Filtering Logs

Use `jq` to filter structured JSON logs. Lakekeeper outputs non-JSON content during startup (ASCII art banner, version info), so standard `jq` will fail. Use `jq -R 'fromjson?'` to handle mixed output:

- `-R` reads each line as raw text instead of expecting JSON
- `fromjson?` attempts to parse each line as JSON, silently skipping non-JSON lines (the `?` suppresses errors)

```bash
# Only audit logs
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit")'

# Failed authorizations
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .decision == "denied")'

# Error responses
cat logs.json | jq -R 'fromjson? | select(.event_source == "error_response")'

# Specific user activity
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .actor.principal == "oidc~user@example.com")'

# Specific table access
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .entity.table == "my_table")'

# Any individual denied decision (single-check OR a denied entry inside a batch event)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.authorizations // [])[]; .allowed == false))'

# Permissions checked on behalf of a specific user (introspection / batch-check)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.authorizations // [])[]; .["for-principal"].user == "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"))'

# Every grant change, allowed or refused
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .action.action_name == "apply_grants")'

# Which client libraries are calling, and how often (remember: caller-supplied, unverified)
cat logs.json | jq -R -r 'fromjson? | select(.event_source == "audit") | .user_agent // "(none sent)"' | sort | uniq -c | sort -rn

# Refused attempts to grant privileges TO a specific principal (not by them)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .action.action_name == "apply_grants" and any((.action.principals // [])[]; . == "user:oidc~alice") and any((.authorizations // [])[]; .allowed == false))'
```

## Best Practices

1. **Separate Audit Logs**: Route logs with `event_source=audit` to a secure, long-term storage system for compliance.

2. **PII Handling**: Audit logs contain user identities. Apply appropriate access controls and retention policies.

3. **Error IDs**: Every error has a unique `error_id`. Use this to correlate client-side errors with server logs.

4. **Log Aggregation**: In production, use a centralized logging system (ELK, Loki, Splunk) to collect and analyze logs from all Lakekeeper instances.

5. **Alerts**: Set up alerts for:
   - Multiple `decision=denied` events from the same principal
   - High rates of `event_source=error_response` with 5xx codes
   - Access to sensitive resources outside business hours

## Related Topics

- [Authentication](./authentication.md) - Configure identity providers
- [Authorization](./authorization.md) - Set up permission management  
- [Configuration](./configuration.md) - Complete configuration reference
