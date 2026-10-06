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

Every audit record is emitted on the fixed target `lakekeeper::audit`, whichever part of the catalog produced it. That name is a stable handle chosen for filtering; it is not a Rust module path and does not move when the code is reorganised.

```bash
# Audit records only, nothing else from the catalog
RUST_LOG=warn,lakekeeper::audit=info

# Everything at INFO except audit records
RUST_LOG=info,lakekeeper::audit=warn
```

Select them by that target and nothing else. A directive naming a Rust module path — anything beginning `lakekeeper::service` — selects no audit record. It still selects the ordinary log lines under that path, which is what such a directive is for, so an audit filter written that way yields no audit records and no error. The catalog prints a warning to standard error at start-up when it finds one in `RUST_LOG`.

Filtering this way stops the record reaching the log. Routing records **after** they are emitted is a different job: match on `event_source`, not on the target, because the `target` key itself is added by the log subscriber.

To disable audit logs entirely:

```bash
LAKEKEEPER__AUDIT__TRACING__ENABLED=false
```

**Note:** Audit logs contain PII — user identities, and a caller-supplied `user_agent` string that some clients populate with hostnames or OS usernames. When disabling them, ensure you have alternative mechanisms for compliance and security monitoring.

## Log Types

Lakekeeper produces four types of logs. The first three are structured and are distinguished by their `event_source` field; general application logs carry no `event_source` at all, which is what identifies them:

### 1. Audit Logs {#audit-logs}

Authorization events tracking access to catalog resources. **Contains PII** (user identities).

**Identified by:** `"event_source": "audit"`

Every audit record names its own shape in `record_type`, and that field is what a consumer routes on. Three shapes exist: `authorization`, `replay` and `operation`. The machine-readable description of all three is the [audit log schema](audit/schema.md).

#### Format version and stability {#audit-format}

Every `event_source: "audit"` record carries `audit_format`, a `MAJOR.MINOR` string. It is emitted unconditionally on every audit record, in every configuration.

`event_source` and `audit_format` are stable contracts. `event_source: "audit"` will not be renamed or repurposed, and `audit_format` will not change shape or type.

**MINOR** is bumped when fields are added and nothing existing changes. Additive fields may appear at any minor version, so consumers must ignore unknown keys.

**MAJOR** is bumped when an existing field is renamed, retyped, or structurally moved — including a scalar becoming an object, an object becoming an array, or a key changing case or separator. Every major bump is called out in the release notes.

**Values are open, except four sets:** Several fields carry a value from a fixed vocabulary — `action_name`, `entity_type`, `actor_type`, `outcome`, `operation`, `privilege_source`, `resource_type`, `failure_reason`, and the kinds inside an action's `update_kinds` list. New values may appear in any of them at any version, including a patch release of the catalog, because a new action or a new entity kind is new capability rather than a changed format. **Treat a value you do not recognise as opaque: log it, route it to a default branch, and do not fail on it.** What will not happen without a MAJOR bump is an existing value being renamed or removed, so a consumer that matches on the values it knows and ignores the rest keeps working.

Four sets are closed by what they mean: `decision`, `root_level`, `privilege_scope`, and a policy's `effect` under `determined_by`. The kinds of `determined_by` entry, its `type`, are closed the same way. A new value in any of these is a MAJOR change.

In the [schema](audit/schema.md) every value set has its own definition, and a field holding one of its values points at it. An open set lists its values under `x-audit-values`, so a validator accepts a value added after your copy of the schema was generated. A closed set lists them as an `enum`, which a validator enforces.

A new *key* is a different matter: it is a new field, so it raises MINOR and appears in the release notes. Ignore keys you do not recognise, as the MINOR rule above already says, but you will not meet one without a version to explain it. The [schema](audit/schema.md) marks which sets are which — `x-audit-kind: "enum"` for a set of values, `x-audit-kind: "keys"` for a set of keys.

That promise covers the values Lakekeeper itself emits. Other components write to the same log and name their own values, and `audit_format` does not govern those — whoever ships the component does. The `emitters` object names every component whose names a record carries, each with that component's own version.

#### Two version numbers, and what each one governs {#audit-emitter}

Every record carries two versions, and they answer different questions.

```json
{
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "2.1"
  },
  "time": "2026-02-15T14:20:50.758690Z",
  "operation": "ldap_resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~alice"
  },
  "outcome": "success",
  "context": {
    "provider_id": "ldap",
    "role_count": 3,
    "mode": "attribute"
  }
}
```

The record's shape — that it has a `record_type`, an `actor`, an `outcome` and a `context` — is Lakekeeper's, and `audit_format` governs it. What fills `operation`, `outcome` and `context` belongs to the products in `emitters`, and each product's version there governs what that product put there.

`audit_format` governs **the whole record's shape**: which top-level fields exist, how they nest, and the objects and value sets Lakekeeper itself defines. Every record carries it, whoever produced the record.

`emitters` says **which products this record carries something of, and what each contributes**. It is an object with one key per product: the one that assembled the record, plus any other whose vocabulary supplied a name in it. The key is the product's name, in `lower_snake_case`. The value is that product's own format version, `MAJOR.MINOR`, and it governs that product's vocabulary, its `context` keys, and any record shape it defines. It moves on that product's release cycle, not on Lakekeeper's.

Most records name one product. A record names two when a component that plugs into Lakekeeper — an authorizer, say — supplied an action name or a `context` key on a record Lakekeeper assembled. That is why the field can hold more than one key: the record is then governed by two release cycles at once, and it says so.

**Match on `audit_format` for the whole log, and read the version of the product you care about from its key in `emitters`: `.emitters.lakekeeper`, `.emitters.lakekeeper_plus`.** A record without that key carries nothing of that product. Both can move independently: an addition to a product's own vocabulary moves only that product's entry, and a change to the record's overall shape moves only `audit_format`.

!!! warning "The two numbers are not the same field written twice"
    For records Lakekeeper alone produces, `emitters.lakekeeper` happens to equal `audit_format`, because one project governs both. That equality is a property of that one key, not of the format. Any other product carries a different number, and a consumer that compares whichever it first encountered will route those records wrongly and silently. Compare the one whose scope you mean.

Every object, field and set of values a record can carry is described in the [audit log schema](audit/schema.md), generated from the emitting code. The sections below describe what the records mean and show examples.

Compare versions by splitting on `.` and comparing each half as an integer. Do not compare the string lexically: `"1.10"` sorts *before* `"1.9"`. In `jq`, that is `select((.audit_format | split(".") | map(tonumber)) >= [1, 9])`. Routing on the major alone — `.audit_format | split(".") | .[0]` — is the safe default.

##### Which release ships which version

`audit_format` is a promise about **released** builds. A release raises it at most once however many changes it carries, and a major change absorbs every minor change made alongside it — so the number tells you how badly you are affected, not how many commits touched the log. What actually changed is listed in full in the release notes.

On an unreleased build — `main`, a `rel-*` branch, or anything built from source between releases — `audit_format` names the version the *next* release will carry, and that build may not yet emit all of it. Two unreleased builds can therefore declare the same version while emitting different records. If you parse audit records, pin to a release.

Patch releases never change the audit log format, so every `0.14.x` emits what `0.14.0` emitted.

| Lakekeeper release | `audit_format` |
| ------------------ | -------------- |
| 0.14.0             | `1.0`          |
| 0.13.x and earlier | not emitted    |

##### Not covered by `audit_format`

The following keys are added by the log subscriber (`tracing-subscriber`), not by Lakekeeper's audit code, and are **not** covered by `audit_format`. Their presence, spelling, order and content can change with a dependency upgrade, with no version bump:

- `timestamp`
- `level`
- `message`
- `target`
- `span`
- `spans`
- `filename`
- `line_number`

Under the default binary configuration `span` is suppressed and `filename` / `line_number` appear only when extended debug logs are enabled. Note also that `message` precedes `event_source` in the flattened output. Do not build detection or routing rules on any of these keys — match on `event_source` instead.

One of them is worth a word. The `target` **key** belongs to the subscriber, but the **value** on an audit record is chosen by Lakekeeper and fixed at `lakekeeper::audit`, which is what makes `RUST_LOG` filtering possible. Use it to decide what gets emitted; use `event_source` to decide what to do with what arrives.

**Match on full paths, and do not flatten the record to leaf names.** A key is named for its position: the same name at two paths identifies two different things, and flattening merges them into one field where the later value wins. Some of these differ in type as well as meaning, so a consumer that survives the merge still reads `null` where it expected a value.

| Name | Paths | Meaning |
|---|---|---|
| `name` | `actions[].name` · `authorizations[].determined_by[].name` | A resource name · a policy name |
| `type` | `authorizations[].determined_by[].type` · `error.type` | The kind of deciding factor · the error type |
| `principal` | `actor.principal` · `context.principal` · `actions[].principal` | Who acted (string) · who holds the grant (object) · who a subtree request reaches (string) |

The other repeated names are safe to merge because they mean the same thing wherever they sit: an entity or an action appears both at the top level and inside `authorizations[]`, so `entity_type`, `table` and `warehouse_id` say one thing at either path. `error_id` sits under `context` on an admission record and under `error` on a denied authorization, and means the same in both. `message` is the exception to read carefully — the subscriber writes one at the top of every line, and an admission record carries another under `context`, describing why that gate refused. They are different things at different paths. The rule is the same either way: address a field by its path.

**How names are spelled.** Everything this log names itself is `lower_snake_case` — the record's own fields, every key inside an `entity`, an `action` or a `context`, and every value drawn from a fixed set. This is the same spelling every other Lakekeeper log line uses, so one rule covers the whole log. **A hyphen means the vocabulary belongs to somewhere else**, and there are three: `entity_type` and `resource_type` read `generic-table` and `tag-definition`, which the management API spells that way; `update_kinds` carries Iceberg's own table-update action names such as `add-schema`; and the objects inside `determined_by` are the management API's shape, so one parser reads a `/check` response and an audit record alike. Values that are data the request carried — a name, an id, a location — are whatever the caller sent.

#### Authorization Events

Emitted for every authz check. Always contain `actions`, `entities`, `actor` and `decision`.

Discriminate on `record_type`, which every record carries and which is the only field meant for the purpose. A record is `authorization`, `replay` or `operation`.

**Structure:**

| Field                  | Type            | Description                       |
|------------------------|-----------------|-----------------------------------|
| `event_source`         | String          | Always `"audit"`                  |
| `record_type`          | String          | Always `"authorization"` for this shape. The field every record carries and the one to route on. |
| `emitters`             | Object          | Every product this record carries something of, keyed by name, with the version of what each contributes: `{"lakekeeper": "1.0"}`. See [Two version numbers](#audit-emitter). |
| `request_id`           | String          | The request this record belongs to: the `x-request-id` the caller sent, whatever its form, or the id Lakekeeper generated and returned in that response header. The same value is on the request's log lines and on the CloudEvents it publishes. |
| `time`                 | String          | When the request was decided, in UTC, as RFC 3339 with microseconds: `2026-02-15T14:20:50.758690Z`. Taken when the decision is made, so it can precede the subscriber's `timestamp`, which is when the line was written. |
| `actions`              | Array           | Operation(s) attempted, always an array however many there are. Each action is an object with an `action_name` field (e.g., `"read_data"`, `"drop"`, `"create_namespace"`) and optional context fields describing what the caller requested. See [Action Format](#action-format) below. |
| `entities`             | Array           | Resource(s) accessed, always an array however many there are. Each entity contains `entity_type` and type-specific fields (e.g., `warehouse_id`, `namespace`, `table`) |
| `actor`                | Object          | Who performed the action (see format below) |
| `privilege_source`     | String          | Request-level classification of the caller's privilege: `"authorizer"` (no special privileges — all decisions come from the configured Authorizer backend), `"instance_admin"` (caller listed in `LAKEKEEPER__INSTANCE_ADMINS` — control-plane actions are auto-approved, data-plane actions still go through the Authorizer), or `"internal"` (in-process call — full bypass). This is a property of the request, not of individual entries in the `authorizations` array. See [Instance Admins](./instance-admins.md). |
| `user_agent`           | String          | The caller's `User-Agent` request header, recorded verbatim and truncated to 256 bytes. Absent when the request sent no `User-Agent` (or sent one that was not valid text) — for example an in-process call from a background worker. **Client-supplied and unverified** — see below. |
| `break_glass`          | String          | Optional; present only when the caller sent the `x-break-glass` request header with a value that was non-empty after trimming, which nearly no request does. The reason the caller stated for marking the request an emergency override, recorded as sent (undecodable bytes replaced) and truncated to 256 bytes. **Client-supplied and unverified** — see below. |
| `decision`             | String          | `"allowed"` or `"denied"` — the rollup decision for the whole event |
| `authorizations`       | Array           | Per-decision breakdown. Always present; empty only when the request named nothing to check, such as an empty batch check. Each entry is self-contained — see [Per-decision breakdown](#per-decision-breakdown-authorizations) below |
| `idempotency_key`      | String          | The request's `Idempotency-Key`. Absent when the caller sent none. Present so a retry can be tied to the request that did the work — see [Idempotent replays](#operational-audit-events) |
| `context`              | Object          | Optional. What the handler recorded about the request beyond its action and its entity. Its keys come from a set this product declares, not from the handler's name. A value is a string, a flag, a count, a list, a map, or the object a shaped key declares; the [schema](audit/schema.json) states the type of every key on the `context` object's own definition; a product plugged in on top states its keys the same way in its own schema. Absent when the request contributed none. See [Context fields](#audit-context-fields) below. |
| `failure_reason`       | String          | Only on failed events. One of `action_forbidden`, `resource_not_found`, `cannot_see_resource`, `internal_authorization_error`, `internal_catalog_error`, `invalid_request_data`. |
| `error`                | Object          | Only on failed events. Contains `type`, `message`, `code`, `error_id`, `stack` |

**Note:** Empty arrays and objects are omitted from the output. For example, if `stack` is empty, the field will not appear in the log.

**`user_agent` is client-supplied and unverified.** It is the `User-Agent` request header, recorded as sent. Any caller can set it to any value, including one that names a different client, and Lakekeeper neither validates it nor cross-checks it against the token. Treat it as a hint — fleet inventory, spotting an unexpected client library, triaging why one client started failing — never as identity, and never as an authorization or attribution input. The authoritative statements of *who* and *as what* are `actor` and `privilege_source` on the same event. A detection rule keyed on `user_agent` can be evaded by changing one header; one keyed on `actor` cannot.

Lakekeeper records the header rather than a parsed client name, so a consumer can classify it however it needs and no information is lost to normalisation. Two consequences worth planning for: values are free-form and some clients embed hostnames or usernames in them, so treat the field as potentially disclosing infrastructure detail; and browsers cannot set `User-Agent`, so requests from the web UI are recorded with the browser's own value.

**`break_glass` records a claim, not a grant.** Any caller can send `x-break-glass`, and Lakekeeper's built-in authorizers ignore it — on its own the header changes no decision, so the field appearing does not mean the request received anything it would otherwise have been denied. What the request actually got is `decision` and `authorizations`; a pluggable Authorizer that does act on the header reports that in `determined_by` (see [Per-decision breakdown](#per-decision-breakdown-authorizations)). The value is free-form text supplied by the caller, so treat it as a stated reason to correlate against a ticket, never as an authenticated fact. Because it is trivial to send, the presence of `break_glass` on an unexpected principal is worth alerting on.

**Ordering:** An authorization event records the *attempt*, and is handed to the audit dispatcher before the authorized operation issues its write. An `"allowed"` decision therefore means the caller was permitted to perform the action, not that the action succeeded — if the operation fails afterwards the request returns an error while the authorization event remains in the log. Audit consumers should treat authorization events as attempts rather than as confirmation that state changed. Dispatch is asynchronous and best-effort — the event is handed to a background task, which may or may not have reached the sink by the time the write commits — so this is not a write-ahead log, and the absence of a later operational event is not proof that a change was rolled back.

**What counts as a denial.** A request can be refused by the authorizer, or by a rule the authorizer has no say over. The two are recorded differently, and the dividing line is whether the refusal is a *deliberate decision about this action on this resource*:

- **Deliberate refusals are denials.** A catalog-managed (`system`) or provider-managed role that cannot be modified, a reserved tag definition, a warehouse whose spec is locked — these are recorded as `decision: "denied"` with `failure_reason: action_forbidden`, exactly like a missing permission, and not as a bare error with no authorization record. That some of them can never be satisfied by *any* caller does not change this: `action_forbidden` says the action was not permitted, not that a grant was missing. How many records a request produces depends on where the rule is decidable: the role and tag-definition guards are decided alongside the authorizer's own check, so such a request produces exactly one verdict, while the warehouse spec-lock can only be decided after the authorization event has been emitted, so it adds a `"denied"` record after the `"allowed"` one — see the note on counting denials below.
- **Write failures are not denials.** A duplicate name, a tag definition still in use, a backend error — these happen *after* authorization has already succeeded. They leave the `"allowed"` record standing and are not logged a second time as an authorization outcome. Reconcile them against the operational event for the change, which will be absent.

A write failure never appears as a denial. But `decision` alone is not a refusal filter: `"denied"` is stamped on *every* authorization-failed record, including the ones where no verdict was reached — a catalog or authorizer outage during the check surfaces as `decision: "denied"` with a `5xx`. To select actual refusals, filter on the reason as well:

```jq
select(.decision == "denied" and .failure_reason as $r
       | $r == "action_forbidden" or $r == "resource_not_found" or $r == "cannot_see_resource")
```

Equivalently, `authorizations[].allowed == false`. Note also that a single request can produce both an `"allowed"` and a `"denied"` record when a rule can only be decided partway through the request, after the authorization event has already been emitted — the warehouse spec-lock guard does this — so count denials per request, not per record.

**Actor Types:**

```json
// Anonymous
{"actor_type": "anonymous"}

// Authenticated user
{"actor_type": "principal", "principal": "oidc~user@example.com"}

// Assumed role
{"actor_type": "assumed_role", "principal": "oidc~user@example.com", "assumed_role": {"role_id": "…", "provider_id": "…", "source_id": "…"}}

// Internal system
{"actor_type": "lakekeeper_internal"}
```

| Field          | Type   | Description                                                                                       |
|----------------|--------|---------------------------------------------------------------------------------------------------|
| `actor_type`   | String | `"anonymous"`, `"principal"`, `"assumed_role"`, or `"lakekeeper_internal"`. Always present. Open, like the other value sets — see [Format version and stability](#audit-format). |
| `principal`    | String | The authenticated principal. Present for `principal` and `assumed_role`.                           |
| `assumed_role` | Object | The role being acted as, with `role_id`, `provider_id` and `source_id`. Present for `assumed_role`. |

**Principal references.** Where a principal is named as a *target* rather than as the caller — `authorizations[].for_principal`, and `context.principal` on grant events — it is a single-key object: `user` for a user, `role` for a role. For example `{"user": "oidc~alice"}` or `{"role": "<uuid>"}`.

**`principal` is the sharpest case of the path rule above.** `actor.principal` is a string naming who acted, `context.principal` is an object naming who holds a grant (`{"user": "oidc~alice"}`), and `actions[].principal` is a string naming who a subtree-grant request reaches (`"every"`, `"user:oidc~alice"`, `"role:<uuid>"`). A query on the bare name answers a different question depending on which record it meets; one written against `principal.user` returns nothing where the value is a string.

**Context fields** {#audit-context-fields}

The handler adds to the `context` object of an authorization event when the request carries something worth recording that is not an action or an entity. The keys are a closed set — the ones below, each typed in the [schema](audit/schema.json) — and the handler chooses which of them this request warrants; only those appear, and the object is omitted entirely when there are none. A product plugged in on top declares keys of its own, which appear in the same object and in that product's schema.

| Key                        | Description                                                                                  |
|----------------------------|----------------------------------------------------------------------------------------------|
| `invoked_by`               | The higher-level operation this authorization was performed on behalf of, when the check is not directly caused by the API call — currently `register_table_overwrite`, for the drop authorized as part of overwriting a registered table |
| `self_provisioning`        | `true` when a user record was created by the authenticated caller for themselves rather than by an administrator. A boolean, always present on that endpoint — do not read its presence as `true` |
| `self_read`                | `true` when the caller is reading their own grants rather than another principal's. A boolean, always present on the endpoints that set it — do not read its presence as `true` |
| `queue_name`               | The task queue the request addressed                                                          |
| `entity_id`                | Identifier of the entity the task acts on                                                     |

New keys may be added at any minor version, so consumers must not assume this list is closed. On operational audit events the `context` object is a different, per-operation structure — see [Operational Audit Events](#operational-audit-events).

**Entity Format:**

Each entity is an object with an `entity_type` and the identifying fields for that type. `entity_type` is one of `server`, `project`, `warehouse`, `namespace`, `table`, `view`, `task`, `role`, `user`, `generic-table` or `tag`. As with the other value sets, this list may gain entries at any version — see [Format version and stability](#audit-format).

Which of the following fields appear depends on the entity type and on what the request supplied — a field is omitted rather than emitted empty. Every value is a string.

| Field                | Description                                                        |
|----------------------|--------------------------------------------------------------------|
| `server_id`          | The server                                                         |
| `project_id`         | The containing project                                             |
| `warehouse_id`       | The containing warehouse                                           |
| `namespace`          | Namespace name, dot-joined for nested namespaces                   |
| `namespace_id`       | Namespace identifier                                               |
| `table`              | Table name, qualified by its namespace                             |
| `table_id`           | Table identifier                                                   |
| `table_location`     | Storage location of the table                                      |
| `view`               | View name, qualified by its namespace                              |
| `view_id`            | View identifier                                                    |
| `generic_table`      | Generic-table name, qualified by its namespace                     |
| `generic_table_id`   | Generic-table identifier                                           |
| `task_id`            | Task identifier                                                    |
| `role_id`            | Role identifier                                                    |
| `role_source_id`     | Identifier of the role in its originating source                   |
| `role_provider_id`   | Identifier of the provider the role was resolved from              |
| `user_id`            | User identifier                                                    |
| `tag_definition_id`  | Tag-definition identifier                                          |

Entities are always in the `entities` array, whatever their number: a single-entity check carries a one-element array.

**Action Format** {#action-format}

Each action is a structured object containing the operation name and optional context about the operation:

```json
// Simple action (no context)
{"action_name": "read_data"}

// Action with properties context (e.g., create_namespace)
{"action_name": "create_namespace", "properties": {"location": "s3://bucket/ns", "owner": "alice"}}

// Action with update context (e.g., commit with property changes)
{"action_name": "commit", "updated_properties": {"retention-days": "30"}, "removed_properties": ["staging"]}
```

Actions are always in the `actions` array, whatever their number: a single-action check carries a one-element array.

Commit actions carry two further context fields when the commit names them: `target_refs`, the branch or tag references the commit targets, and `update_kinds`, the kinds of update the commit contains. Both are arrays of strings, and each is present whether or not the commit named any — `[]` says the commit named none.

Which context fields appear depends on the action, and the [schema](audit/schema.json) says which ones each action can carry: `ActionRecord` holds one `if`/`then` per action, matching on `action_name` and listing that action's fields with their types. Read it if you build a reader per action rather than inferring the pairing from traffic. A field is listed there because it *can* appear. A field holding a list or a map is always present once the action has it, empty (`[]`, `{}`) when the request supplied nothing — so you read "none" from the value, not from the key being missing. A field whose value the request simply did not supply is absent instead, because there is nothing to report; and a flag — `force`, `purge`, `recursive`, `dry_run`, `allow_partial` — is a JSON **boolean** that is always present once the action has it, so you branch on its value and never on its presence. An action the schema names in no branch is one that carries no context fields, or one newer than the schema you hold.

| Context field           | Type   | Emitted by                        | Description                                             |
|-------------------------|--------|-----------------------------------|---------------------------------------------------------|
| `name`                  | String | create actions                    | The name the client asked to create                     |
| `properties`            | Object | create actions                    | Client-supplied properties, verbatim. Keys are arbitrary — this is user data, not part of the audit format |
| `updated_properties`    | Object | property updates                  | The properties being set, verbatim                      |
| `removed_properties`    | Array  | property updates                  | The property keys being removed                         |
| `table_id`              | String | table creation                    | The table id the client requested                       |
| `generic_table_id`      | String | generic-table creation            | The generic-table id the client requested               |
| `format`                | String | generic-table creation            | The requested table format                              |
| `base_location`         | String | generic-table creation            | The requested storage location                          |
| `project_id`            | String | project creation                  | The project id the client requested                     |
| `force`                 | Boolean | delete and drop actions          | `true` when the client asked to force the operation     |
| `purge`                 | Boolean | delete and drop actions          | `true` when the client asked to purge the data          |
| `recursive`             | Boolean | delete actions                   | `true` when the client asked for a recursive delete     |
| `target_refs`           | Array  | commits                           | The branch or tag references the commit targets         |
| `source`                | Array  | accepting a moved namespace       | The namespace path the entity is being moved from        |
| `destination`           | Array  | move actions                      | The namespace path the entity is being moved to          |
| `update_kinds`          | Array  | commits                           | The kinds of update the commit contains                 |
| `requested_provider_id` | String | role creation, source-system updates | The role provider the client named                   |
| `requested_source_id`   | String | role creation, source-system updates | The source identifier the client named               |

New context fields may be added at any minor version, so consumers must not assume this list is closed. Note also that the values are client-*requested* inputs: an authorization event records the attempt, so a `table_id` here is what the caller asked for, not necessarily what was created.

**Grant changes (`action_name = "apply_grants"`):**

Applying a grant diff is authorized once for the whole request, so a single `apply_grants` action describes the entire diff:

| Context field | Type   | Description                                                                 |
|---------------|--------|-----------------------------------------------------------------------------|
| `principals`  | Array  | The distinct principals the grants were destined for, each prefixed by kind (`user:oidc~alice`, `role:<uuid>`) |
| `privileges`  | Array  | The distinct privilege names named anywhere in the diff                     |
| `writes`      | Integer | Number of entries requested as grants, before deduplication                |
| `deletes`     | Integer | Number of entries requested as revocations, before deduplication           |

The resource the grants apply to is the event's `entities` entry, not part of the action.

`principals` and `privileges` are deduplicated, so neither is a per-entry list and neither can be matched positionally against the other — a diff naming two principals and two privileges records both sets, not which pairing was requested. The counts are the request's, so `writes: "3"` with one entry in `principals` means three grants for one principal. Requests are capped at 100 entries, which bounds both lists.

**This records the attempt.** What actually changed is recorded separately, one record per grant, under `operation = "grant_created"` / `"grant_revoked"`. Both are audit-log records; neither is published to the configured event stream (Kafka, NATS, CloudEvents). Read this one for what was asked and whether it was allowed, and those for what took effect.

Because the event records the attempt, a *denied* apply is logged with the same detail as an allowed one: what was asked for, for whom, and on which resource. A refused privilege escalation is attributable to its intended beneficiary, not merely to the caller who attempted it.

```json
// Denied attempt to grant `modify` to two principals
{
  "action_name": "apply_grants",
  "principals": ["role:1f7b…", "user:oidc~alice"],
  "privileges": ["modify"],
  "writes": 2,
  "deletes": 0
}
```

**Subtree grants (`action_name = "read_subtree_grants"` / `"revoke_subtree_grants"`):**

Both are authorized once at the subtree root for the whole batch, so one action describes it. The root is the event's `entities` entry, not part of the action.

Six fields are the **scope** — the same value the authorizer is asked with, so the record and the decision describe one request. They are emitted together or not at all: a request that names no scope, which is the base-capability form, carries none of them.

| Context field    | Type   | Description                                                                 |
|------------------|--------|------------------------------------------------------------------------------|
| `dry_run`        | Boolean | `true` when the call only reports what it would do. A dry run changes nothing, so a record carrying `true` is not evidence of a revocation. A dry-run revoke is recorded as `revoke_subtree_grants` carrying `true`; a `read_subtree_grants` record from a subtree listing reads `false` |
| `resource_types` | Array  | The resource kinds the request reaches. Always at least one, and always a subset of the kinds the addressed resource covers |
| `root_level`     | String | `included` when the addressed resource's own grants are in range, `excluded` when only those beneath it are. Closed — see [Format version and stability](#audit-format) |
| `principal`      | String | Whose grants are in range: `every`, or one principal prefixed by kind (`user:oidc~alice`, `role:<uuid>`) |
| `privilege_scope` | String | `every` when the request reaches every privilege a matching grant can carry — including privileges this server no longer publishes — and `only` when it names a set. Closed — see [Format version and stability](#audit-format) |
| `narrowed_privileges` | Array | The privileges named when `privilege_scope` is `only`. Emitted as `[]` when it is `every`, because the widest case has no list to expand into: read `privilege_scope` first, and do not read this array alone as the whole answer |

`revoke_subtree_grants` carries three more, describing the filter rather than the reach:

| Context field    | Type   | Description                                                                 |
|------------------|--------|------------------------------------------------------------------------------|
| `privileges`     | Array  | The distinct privilege names the revocation was narrowed to. Emitted as `[]` when the request named none, which means every privilege |
| `allow_partial`  | Boolean | `true` when the client asked the revocation to proceed despite grants it could not revoke |
| `created_before` | String | Optional. RFC 3339 timestamp; only grants created before it were in range   |

Only `created_before` is omitted, when the request does not narrow on it. `privileges` is emitted as `[]` and `allow_partial` as a boolean, both always present. Do not infer a field's behaviour here from another field's; read each row.

**These are the filters, not the outcome.** The action records what the caller asked for and whether they were allowed it; it does not say which grants matched. What actually changed is recorded separately, one record per grant, under `operation = "grant_revoked"` — and for `dry_run` requests, nothing is.

#### Per-decision breakdown (`authorizations`)

Every authorization event carries an `authorizations` array. For ordinary single-check API calls the array has exactly one entry, synthesised from the event's top-level fields. For `/management/v1/action/batch-check` the array contains one entry per inner check, in request order. A request that names nothing to check, such as a batch check with `checks: []` or a transaction commit with no table changes, records an empty array, and `decision` still carries the outcome. The `get_*_actions` introspection endpoints are **not** in that group: they emit a single synthesised entry for the `introspect_permissions` action, whatever the answer contains — the actions a principal holds are in the response body, not in this array.

This means audit consumers can use **one query path** for both single and batch events: iterate `authorizations[]` and read the per-entry `allowed` flag, instead of switching between top-level `decision` and a per-batch breakdown.

Each entry is **self-contained** — it does not require zipping with the top-level fields:

| Field           | Type    | Description                                                                          |
|-----------------|---------|--------------------------------------------------------------------------------------|
| `id`            | String  | Stable identifier for this entry. When the client supplies an `id` on a batch-check input it appears verbatim here, and the API response echoes the same value so the two can be correlated 1:1. When the client omits `id`, the API response omits it too; the audit log instead substitutes the request item's zero-based index as an internal bookkeeping fallback so individual decisions can still be pinpointed in the logs. **Do not assume the API response carries index-based ids — that fallback exists only in audit entries.** Absent on synthesised single-check entries. |
| `for_principal` | Object  | Optional. The principal whose permission was evaluated, when different from the request actor. Shape: `{"user": "..."}` or `{"role": "..."}`. Absent means the request actor itself. |
| `action`        | Object  | One action, in the same shape as an element of the top-level `actions` array.        |
| `entity`        | Object  | One entity, in the same shape as an element of the top-level `entities` array.       |
| `allowed`       | Boolean | Whether this tuple was permitted. `false` means the request was definitively refused for this tuple — by the authorizer, or by a resource-identity guard the authorizer has no say over (see [What counts as a denial](#authorization-events)); `error.type` distinguishes the two, and a guard refusal leaves `determined_by` empty. Absent when no definitive verdict was reached — e.g. on `internal_authorization_error`, `internal_catalog_error`, or `invalid_request_data` failures, where the system never actually evaluated the request. Definitive denials (`action_forbidden`, `resource_not_found`, `cannot_see_resource`) are recorded as `false`. |
| `determined_by` | Array   | Always present, and empty when the Authorizer surfaces no per-decision diagnostics (some backends, e.g. OpenFGA and allow-all, produce none, and the array is then `[]`). Each element attributes *this* decision to a factor: a matched **policy** (carrying its identifier, an optional author-supplied name, an effect of `permit` or `forbid`, and an optional originating source), or a **system-authority override** (an optional source and human-readable reason) recording that a built-in/system authority tier — rather than a configured policy — determined the allow, e.g. a recovery grant that lets a privileged system role act despite a policy that would otherwise forbid it; or an **admission gate** (the gate's name and an optional check) recording that the gate would refuse this user at admission, so the request is denied whatever the policies say. Distinct from the top-level `privilege_source`, which classifies the caller rather than individual decisions. The same factors are returned to callers of `POST /management/v1/action/batch-check`, spelled `determined-by` there. |

Each `determined_by` element is a flat object whose `type` field names the kind of factor. The same shape is returned by `POST /management/v1/action/batch-check`, where the field is spelled `determined-by`, so one parser reads both.

| `type`             | Field       | Type   | Description                                                                               |
|--------------------|-------------|--------|-------------------------------------------------------------------------------------------|
| `policy`           | `policy-id` | String | Authorizer-assigned identifier of the policy. Always present.                             |
| `policy`           | `effect`    | String | `permit` or `forbid`. Always present.                                                     |
| `policy`           | `name`      | String | Author-supplied policy name. Absent when the author provided none. Not guaranteed unique. |
| `policy`           | `source`    | String | Opaque origin of the policy. Absent when the producer cannot attribute one.               |
| `system-authority` | `source`    | String | Opaque identifier of the built-in authority tier. Absent when none can be attributed.     |
| `system-authority` | `reason`    | String | Human-facing reason the tier applied. Absent when the producer gives none.                |
| `admission-gate`   | `gate`      | String | Name of the admission gate that would refuse the user. Always present.                    |
| `admission-gate`   | `check`     | String | The check within that gate that decided. Absent when the gate names none.                 |

```json
{"type": "policy", "policy-id": "policy-42", "name": "deny-stale-namespaces", "effect": "forbid", "source": "cedar"}
```

An absent field is left out rather than written as `null`, here as everywhere else in a record.

**Top-level vs. per-entry semantics.** The top-level `actor` always reflects the *API caller* (the bearer token holder); `authorizations[].for_principal` reflects *whose permissions were checked*. For most calls these are the same and `for_principal` is omitted. For introspection endpoints like `GET /lakekeeper/v1/permissions/...?for-user=X` the actor is the caller while every entry's `for_principal` is `X` — both facts are recorded structurally on the same event, no `context.for-user` string needed.

**Examples:**

<details>
<summary>Authorization Succeeded</summary>

```json
{
  "timestamp": "2026-02-15T14:20:50.758690Z",
  "level": "INFO",
  "message": "Authorization succeeded event",
  "target": "lakekeeper::audit",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "authorization",
  "emitters": {
    "lakekeeper": "1.0"
  },
  "request_id": "019684ff-0000-7000-8000-000000000005",
  "time": "2026-02-15T14:20:50.758690Z",
  "actions": [
    {
      "action_name": "create_warehouse",
      "name": "demo"
    }
  ],
  "entities": [
    {
      "entity_type": "project",
      "project_id": "00000000-0000-0000-0000-000000000000"
    }
  ],
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
        "project_id": "00000000-0000-0000-0000-000000000000"
      },
      "allowed": true,
      "determined_by": []
    }
  ]
}
```

</details>

<details>
<summary>Authorization Failed</summary>

```json
{
  "timestamp": "2026-02-15T14:21:10.123456Z",
  "level": "INFO",
  "message": "Authorization failed event",
  "target": "lakekeeper::audit",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "authorization",
  "emitters": {
    "lakekeeper": "1.0"
  },
  "request_id": "019684ff-0000-7000-8000-000000000005",
  "time": "2026-02-15T14:21:10.123456Z",
  "actions": [
    {
      "action_name": "drop"
    }
  ],
  "entities": [
    {
      "entity_type": "table",
      "warehouse_id": "414b18f0-0a6d-11f1-b2d7-f31430431ca0",
      "namespace": "production",
      "table": "sensitive_data"
    }
  ],
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
        "warehouse_id": "414b18f0-0a6d-11f1-b2d7-f31430431ca0",
        "namespace": "production",
        "table": "sensitive_data"
      },
      "allowed": false,
      "determined_by": []
    }
  ],
  "failure_reason": "action_forbidden",
  "error": {
    "type": "Forbidden",
    "message": "Insufficient permissions",
    "code": 403,
    "error_id": "01234567-89ab-cdef-0123-456789abcdef",
    "stack": []
  }
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
  "message": "Authorization succeeded event",
  "target": "lakekeeper::audit",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "authorization",
  "emitters": {
    "lakekeeper": "1.0"
  },
  "request_id": "019684ff-0000-7000-8000-000000000005",
  "time": "2026-04-07T17:58:34.358975Z",
  "actions": [
    {
      "action_name": "introspect_permissions"
    }
  ],
  "entities": [
    {
      "entity_type": "warehouse",
      "warehouse_id": "255a8f5c-32ab-11f1-889e-4706b6f66241"
    },
    {
      "entity_type": "table",
      "warehouse_id": "255a8f5c-32ab-11f1-889e-4706b6f66241",
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
      "for_principal": {
        "user": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
      },
      "action": {
        "action_name": "delete"
      },
      "entity": {
        "entity_type": "warehouse",
        "warehouse_id": "255a8f5c-32ab-11f1-889e-4706b6f66241"
      },
      "allowed": true,
      "determined_by": []
    },
    {
      "id": "1",
      "for_principal": {
        "user": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
      },
      "action": {
        "action_name": "read_data"
      },
      "entity": {
        "entity_type": "table",
        "warehouse_id": "255a8f5c-32ab-11f1-889e-4706b6f66241",
        "namespace": "production",
        "table": "events"
      },
      "allowed": false,
      "determined_by": []
    }
  ]
}
```

</details>

#### Operational Audit Events

Emitted for operations that produce no authorization decision of their own — LDAP/directory role resolution and user enrichment, the grants an apply actually wrote, admission decisions, and requests answered from an idempotency record. Use these to audit *what the system did on behalf of a user*, rather than *whether the user was allowed to do something*. Several carry user identity (PII); the per-operation sections below say which.

**Structure:**

| Field          | Type   | Description                                        |
|----------------|--------|----------------------------------------------------|
| `event_source` | String | Always `"audit"`                                   |
| `request_id`   | String | The request the operation belongs to, as on authorization records. Absent for an operation no request triggered, such as a background role sync |
| `time`         | String | When the operation happened, in UTC, as on authorization records |
| `operation`    | String | Machine-readable name of the operation (e.g., `"ldap_resolve_roles"`) |
| `actor`        | Object | Same shape as authorization events, and the same four shapes: `{"actor_type": "principal", "principal": "oidc~…"}` is the common one, but `assumed_role` adds a nested `assumed_role` object, while `anonymous` and `lakekeeper_internal` carry `actor_type` alone. Read `actor_type` before reading `principal` — the four shapes and their fields are tabulated under [Authorization Events](#authorization-events). Which shapes a given operation can produce depends on how it obtains the actor. `admission_decided` and the grant records (`grant_created`, `grant_revoked`) render the request's resolved actor, so any of the shapes can appear. On the grant records, the `assumed_role` of an assumed-role caller is the role that holds the grant; on `admission_decided`, which is written before the `x-assume-role` check, it is the role the request asked to assume, not yet authorized — for a caller acting as a role the `assumed_role` object is where the acting identity is, and `principal` alone under-reports it. Only operations that name a user directly rather than taking it from the request are always `principal` |
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
| `resource_type`| `server`, `project`, `warehouse`, `namespace`, `table`, `view`, `generic-table` or `tag-definition`. Open, like the other value sets — see [Format version and stability](#audit-format) |
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

**Admission rejections (`operation = "admission_decided"`):**

Emitted when an [admission gate](./admission.md) refuses a request. Gates run after authentication, before the `x-assume-role` check and before any handler. For an assumed-role caller, `actor.assumed_role` is the role the request asked to assume, which has not been authorized yet. A gate refusal is the only record of the request: no `assume_role` authorization record accompanies it.

`outcome` is one of:

- `forbidden` — the gate denied the caller (`403`).
- `unavailable` — the gate could not reach an upstream it needs, so it failed closed (`503`).

This record names the principal. The error response does not, because responses are PII-free by contract. Without this record the log would say a request was refused, but never whose.

| Context field | Description |
|---------------|-------------|
| `gate`        | Which gate decided. Useful when you run more than one |
| `denied_by`   | The gate's rule that decided it. Absent when the gate named none, as a fail-closed rejection does |
| `status`      | `403` or `503`, as a JSON **number** rather than a string, matching `error.code` on authorization events |
| `error_type`  | The gate's error type, e.g. `ExternalEnforceForbidden` |
| `message`     | The gate's own wording, as the caller received it. What separates two rejections sharing an `error_type` — the same gate failing closed on a missing precondition rather than on an unreachable upstream. Not to be confused with the envelope `message` at the top level of every log line |
| `error_id`    | The id the caller received in the response body. Use it to match a user's report to this record |

This is the **only** record of a rejection. The generic [error-response log](#2-error-response-logs) is skipped for it, because that line would repeat the decision without the principal.

A gate that fails closed also logs a `WARN` on the general stream, with `gate`, `error_type`, `error_id`, `request_id`, and `cause` (what failed, if the gate reported one). That line names no principal. `cause` appears nowhere else: not in the audit record, not in the response. Alert on the gate's metrics rather than on `ERROR` lines.

Admitted requests produce no record here. The authorization events that follow show what the caller then did.

**Admission role checks (`operation = "admission_enforce_check"`):**

Emitted by the external-enforce gate when the control plane refuses one role but still admits the request. `outcome` is `role_withheld`. The request runs with fewer privileges, and nothing later in the request reports that. Context carries `gate`, `check`, `role` and `cache_ttl_secs`.

There is one record per answer from the control plane, not one per request. The gate caches each answer, and requests served from that cache add no record. So every record is something the control plane actually said, and `cache_ttl_secs` tells you how long the requests after it may have relied on that answer. For the per-request rate, use `lakekeeper_admission_enforce_decisions_total`.

`actor` has the same shape here as in `admission_decided`, including the assumed role. The two records join on it directly.

**Idempotent replays (`record_type` = `replay`):**

Emitted when a request carrying an `Idempotency-Key` was answered from the stored record instead of being executed. Identified by `record_type: "replay"`; it carries no `operation` or `outcome`.

These records also carry the top-level `actions`, `entities`, `privilege_source` and `user_agent` fields of an authorization event, so a retry reads with the same queries as the request that did the work — join the two on `idempotency_key`, which every *authorization* record carries too. The operational records above (`grant_created`, `ldap_resolve_roles`) do not carry it.

| Field             | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `request_id`      | The request that was answered from the record                               |
| `time`            | When the request was answered, in UTC                                       |
| `idempotency_key` | The key whose record served the request                                     |
| `actions`         | The actions the retry asked for, in the same shape as an authorization event |
| `entities`        | The targets the retry named, in the same shape as an authorization event     |

**`actions` and `entities` are what the retry asked for, not what the original request did.** A record is matched on warehouse, key and endpoint — never on the target or the request parameters ([Idempotency](./configuration.md#idempotency)). A key reused on the same endpoint against a different table produces a record naming that table, which was not touched; a retry sent with `purgeRequested=true` after an original without it is recorded as a purging drop. Read these as what was asked and answered from a record, not as evidence of what happened.

**A replay record does not establish that its actor was ever authorized for that entity.** The replay is served before authorization runs, so any authenticated caller holding the key can produce one — including one with no grants in that warehouse. The key cannot cause a mutation: a replay executes nothing.

**Audit records carry live idempotency keys**, on authorization records as well as these. A reader of the audit log can replay those keys to a 204 and mint further records; treat audit-log access accordingly. `idempotency-key-lifetime` sets the earliest a record becomes eligible for deletion; keyed traffic decides when that deletion runs, because cleanup rides on a small fraction of keyed requests. A record stays replayable until then, so a quiet deployment keeps its keys live until traffic resumes.

**Seven endpoints emit this marker**, and no others: `dropTable`, `dropView`, `dropNamespace` (recorded as `action_name = "delete"`), `dropGenericTable`, `renameTable`, `renameView`, `renameGenericTable`. They answer 204 and serve the replay before authorizing, so no `decision` is recorded — the mutation already happened, and denying the retry would report a failure for a completed operation.

**Replays of the other idempotent endpoints are not marked, and do not look like the original request.** `createTable`, `registerTable`, `createNamespace`, `updateNamespaceProperties`, `replaceView` and `createGenericTable` re-derive their response body by loading it, so a retry emits the authorization record of that *load* — `action_name = "get_metadata"` on the entity — and no record for the create or update; `updateTable` emits both `commit` and `get_metadata`. For these the replay is a different authorization event from the original, and `idempotency_key` is what ties the two together.

**`commitTransaction` is the exception: nothing marks its replay.** It authorizes before it detects the replay, so the retry emits the same `commit` record as a first execution, carrying the same `idempotency_key`. Only the pair of records sharing one key shows that a retry happened — neither record does on its own.

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
  "message": "LDAP role resolution complete",
  "target": "lakekeeper::audit",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "1.0"
  },
  "time": "2026-03-05T09:12:34.000000Z",
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
  }
}
```

</details>

<details>
<summary>User not found in LDAP</summary>

```json
{
  "timestamp": "2026-03-05T09:12:34.000000Z",
  "level": "INFO",
  "message": "LDAP user not found; returning empty role list",
  "target": "lakekeeper::audit",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "1.0"
  },
  "time": "2026-03-05T09:12:34.000000Z",
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
  }
}
```

</details>

<details>
<summary>Ambiguous user (multiple matches)</summary>

```json
{
  "timestamp": "2026-03-05T09:12:34.000000Z",
  "level": "INFO",
  "message": "LDAP search matched multiple entries; cannot resolve principal unambiguously",
  "target": "lakekeeper::audit",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "1.0"
  },
  "time": "2026-03-05T09:12:34.000000Z",
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
  }
}
```

</details>

<details>
<summary>Branching mode: user DN did not match (else.mode = none)</summary>

```json
{
  "timestamp": "2026-03-05T09:12:34.000000Z",
  "level": "INFO",
  "message": "branching DN regex did not match; explicit no-roles outcome",
  "target": "lakekeeper::audit",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "1.0"
  },
  "time": "2026-03-05T09:12:34.000000Z",
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
  }
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
  "message": "No role provider handled user; user will have no provider-assigned roles",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "1.0"
  },
  "time": "2026-03-07T10:00:00.000000Z",
  "operation": "resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~unknown@other-domain.com"
  },
  "outcome": "no_provider_applicable",
  "context": {
    "providers_checked": [
      "ldap-prod"
    ]
  }
}
```

</details>

<details>
<summary>Roles resolved (debug)</summary>

```json
{
  "timestamp": "2026-03-07T10:00:01.000000Z",
  "level": "INFO",
  "message": "Resolved role assignments for user",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "1.0"
  },
  "time": "2026-03-07T10:00:01.000000Z",
  "operation": "resolve_roles",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~alice@corp.example.com"
  },
  "outcome": "roles_resolved",
  "context": {
    "role_count": 2,
    "roles": [
      "my-ldap~devs",
      "my-ldap~admins"
    ],
    "sources": {
      "my-ldap": "cache_hit",
      "oidc": "in_request"
    }
  }
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
  "message": "stale provider(s) failed to refresh; serving cached roles",
  "event_source": "audit",
  "audit_format": "1.0",
  "record_type": "operation",
  "emitters": {
    "lakekeeper_plus": "1.0"
  },
  "time": "2026-03-07T11:30:00.000000Z",
  "operation": "cached_role_provider",
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~user@corp.example.com"
  },
  "outcome": "stale_cache_fallback",
  "context": {
    "provider_ids": [
      "ldap-prod"
    ]
  }
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

# Who was refused admission to this instance, and by which rule
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "admission_decided") | {actor: .actor.principal, outcome, rule: .context.denied_by}'

# An admission gate failing closed (an upstream outage, not a denial)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "admission_decided" and .outcome == "unavailable")'

# Resolve a user's reported error id to the decision behind it
cat logs.json | jq -R 'fromjson? | select(.context.error_id == "<error-id>")'

# Roles the control plane withheld, by principal
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .outcome == "role_withheld") | {actor: .actor.principal, check: .context.check, role: .context.role}'
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

### 3. Validation Check Logs

Errors raised while validating a warehouse's storage profile or credentials, emitted when the error is not marked to skip logging. A 4xx-class failure is the caller's to fix and is logged at INFO; a 5xx is the server's and is logged at ERROR, with the stack stripped from what the caller receives. Both carry the same fields.

**Identified by:** `"event_source": "validation_check"`

| Field   | Type   | Description                                                             |
| ------- | ------ | ----------------------------------------------------------------------- |
| `check` | String | Name of the validation check that failed                                |
| `error` | String | The underlying error, `Debug`-formatted — not structured, and not a stable contract |

The `error_id` on both is the id the caller was handed, so a user's report resolves to the logged detail.

Unlike audit logs, this source carries **no format version** and no stability guarantee: `error` is a `Debug` rendering whose content may change at any release. Use it for support correlation, not for automated parsing.

### 4. General Application Logs

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
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.entities // [])[]; .table == "my_table"))'

# Any individual denied decision (single-check OR a denied entry inside a batch event)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.authorizations // [])[]; .allowed == false))'

# Permissions checked on behalf of a specific user (introspection / batch-check)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.authorizations // [])[]; .["for_principal"].user == "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"))'

# Every grant change, allowed or refused
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.actions // [])[]; .action_name == "apply_grants"))'

# Which client libraries are calling, and how often (remember: caller-supplied, unverified)
cat logs.json | jq -R -r 'fromjson? | select(.event_source == "audit") | .user_agent // "(none sent)"' | sort | uniq -c | sort -rn

# Refused attempts to grant privileges TO a specific principal (not by them)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.actions // [])[]; .action_name == "apply_grants" and any((.principals // [])[]; . == "user:oidc~alice")) and any((.authorizations // [])[]; .allowed == false))'
```

## Best Practices

1. **Separate Audit Logs**: Route logs with `event_source=audit` to a secure, long-term storage system for compliance.

2. **PII Handling**: Audit logs contain user identities. Apply appropriate access controls and retention policies.

3. **Error IDs**: Every error has a unique `error_id`. Use this to correlate client-side errors with server logs.

4. **Log Aggregation**: In production, use a centralized logging system (ELK, Loki, Splunk) to collect and analyze logs from all Lakekeeper instances.

5. **Alerts**: Set up alerts for:
   - Multiple `decision=denied` events from the same principal
   - High rates of `event_source=error_response` with 5xx codes. Admission rejections do not appear here: they are recorded once as an `admission_decided` audit event. Alert on `lakekeeper_admission_gate_duration_seconds{outcome="unavailable"}` instead, or on the `WARN` a failing-closed gate emits
   - Access to sensitive resources outside business hours

## Related Topics

- [Authentication](./authentication.md) - Configure identity providers
- [Authorization](./authorization.md) - Set up permission management  
- [Configuration](./configuration.md) - Complete configuration reference
