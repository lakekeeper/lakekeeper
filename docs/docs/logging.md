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

Audit logs are **enabled by default**. They are emitted at INFO level, so they appear when `RUST_LOG` lets INFO through.

Every audit record is emitted on the fixed target `lakekeeper::audit`, whatever part of the catalog produced it. This name is stable. It is not a module path and does not change between releases.

```bash
# Audit records only, nothing else from the catalog
RUST_LOG=warn,lakekeeper::audit=info

# Everything at INFO except audit records
RUST_LOG=info,lakekeeper::audit=warn
```

Select audit records by this target only. A directive on a module path, such as anything starting with `lakekeeper::service`, selects no audit records. It still selects ordinary log lines under that path, so the mistake gives no error, just no audit records. Lakekeeper prints a warning to standard error at start-up when it finds such a directive in `RUST_LOG`.

`RUST_LOG` decides which records are written. To route records **after** they are written, match on `event_source`, not on `target`: the `target` key is added by the log subscriber, not by the audit log.

To disable audit logs entirely:

```bash
LAKEKEEPER__AUDIT__TRACING__ENABLED=false
```

**Note:** Audit logs contain PII: user identities, and a caller-supplied `user_agent` string that some clients fill with host names or OS user names. If you disable them, make sure you have another way to meet compliance and security monitoring needs.

### User Emails on Audit Records {#audit-user-emails}

Audit records name users by their principal id. To also put their email on the record, set:

```bash
LAKEKEEPER__AUDIT__TRACING__INCLUDE_USER_EMAIL=true
```

It is off by default. When on, an `email` key appears next to the user it belongs to: on `actor` for `principal` and `assumed_role` actors, on the user form of `authorizations[].for_principal`, on the users in an `apply_grants` action's `principals` and in a subtree action's `principal`, and on the user form of `context.principal` on grant records. Roles, `anonymous` and `lakekeeper_internal` actors never carry one.

The email comes from the caller's token when the token is that user's and carries an email claim (`email`, or `upn` or `preferred_username` when they hold an address). Otherwise it comes from the user's record in the catalog, through the [user cache](./configuration.md#caching), so a user named on every request costs one database read per cache lifetime.

On `admission_decided` records, which are written while the request is still being admitted, the actor's email comes from the token only.

The email is best-effort. It is absent, never `null`, when it is not known: the user has no record or no email, was deleted, or the lookup failed. A lookup never fails or delays a request or a record. On the records written when a user is deleted, for that user's revoked grants, the email is absent: the deletion has already removed it.

**An email is metadata, not identity.** Emails are not unique and can change. Correlate on `principal`, `user` and `role`, never on `email`.

**Note:** With this setting, audit logs hold users' email addresses. An email stays in the log after the user is deleted from the catalog, so plan retention and access for the log accordingly.

### Role Sources on Audit Records {#audit-role-sources}

A role on an audit record carries, next to its id, the provider it comes from and its id there: `provider_id` and `source_id`. With these a reader can find the role at its provider, for example the LDAP group behind it. This applies to every role a record names: `actor.assumed_role`, `authorizations[].for_principal`, `context.principal` on grant records, `principals` on `apply_grants`, and `principal` on subtree grant requests.

```json
{"role": "1f7b…", "provider_id": "corporate-ldap", "source_id": "engineering"}
```

`provider_id` is always included. `source_id` is included by default. Some providers let it be a free-form name, so Lakekeeper cannot rule out that it holds personal data. To leave `source_id` out of every record, set:

```bash
LAKEKEEPER__AUDIT__TRACING__INCLUDE_ROLE_SOURCE_ID=false
```

Except on `actor.assumed_role`, both are best-effort: absent when the role no longer exists or the lookup failed. They are read through the role cache, so a role named on many records costs one database read per cache lifetime. Correlate on the role's id: a role's source does not change, but its id is what every record carries.

## Log Types

Lakekeeper produces four types of logs. The first three are identified by their `event_source` field. General application logs have no `event_source` field.

### 1. Audit Logs {#audit-logs}

A record of who tried to do what in the catalog, and of actions the system took for a user. **Contains PII** (user identities).

**Identified by:** `"event_source": "audit"`

Every audit record names its shape in `record_type`. Route on this field. There are three shapes:

- `authorization`: an access check and its decision. See [Authorization Events](#authorization-events).
- `replay`: a retried request answered from a stored idempotency record. See [Idempotent Replay Events](#replay-events).
- `operation`: something the system did for a user, such as resolving roles or writing a grant. See [Operational Audit Events](#operational-audit-events).

The [audit log schema](audit/schema.md) describes all three in machine-readable form.

#### Format version and stability {#audit-format}

Every audit record carries `audit_format`, a `MAJOR.MINOR` string. It is present on every audit record, in every configuration.

`event_source` and `audit_format` are stable. `event_source: "audit"` will not be renamed or reused, and `audit_format` will not change its shape or type.

- **MINOR** goes up when fields are added and nothing existing changes. Ignore keys you do not know.
- **MAJOR** goes up when an existing field is renamed, changes type, or moves. This includes a scalar becoming an object, an object becoming an array, or a key changing case or separator. Every major change is called out in the release notes.

**Open value sets.** Most fields with a fixed list of values are open: `action_name`, `entity_type`, `actor_type`, `record_type`, `outcome`, `operation`, `privilege_source`, `resource_type`, `failure_reason`, and the entries of an action's `update_kinds` list. A new value may appear in any release, including a patch release, without a version change. **Treat a value you do not know as opaque: log it, send it to a default branch, and do not fail.** An existing value is renamed or removed only in a MAJOR version.

**Closed value sets.** These sets are fixed: `decision`, `privilege_scope`, `root_level`, a policy's `effect` under `determined_by`, and the kind of a `determined_by` entry (its `type`). A new value in any of them is a MAJOR change.

In the [schema](audit/schema.md), every value set has its own definition. An open set lists its values under `x-audit-values`, so a validator accepts a value newer than your copy of the schema. A closed set lists its values as an `enum`, which a validator enforces.

**New keys.** A new key is a new field. It raises MINOR and appears in the release notes. In the [schema](audit/schema.md) every key is a property of its object, with its type and description.

These promises cover the names Lakekeeper itself emits. Other products can add names to the same records. `audit_format` does not cover those; the `emitters` object names each product and its own version.

#### Two version numbers {#audit-emitter}

Every record carries two kinds of version, and they answer different questions.

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

- `audit_format` governs **the shape of the whole record**: which top-level fields exist, how they nest, and the objects and value sets Lakekeeper defines. Every record carries it, whoever produced the record.
- `emitters` names **every product that contributed to the record**, with a version for what each one contributed. It is an object with one key per product. The key is the product name in `lower_snake_case`: `lakekeeper`, `lakekeeper_plus`. The value is that product's own format version, a `MAJOR.MINOR` string such as `{"lakekeeper": "1.0"}`. It governs that product's names, its `context` keys, and any record shape it defines, and it follows that product's release cycle.

In the example above, the record's shape (`record_type`, `actor`, `outcome`, `context`) is governed by `audit_format`. The values of `operation` and `outcome` and the keys in `context` come from Lakekeeper+, so `emitters.lakekeeper_plus` governs them.

Most records name one product. A record names two when a plug-in component, such as an authorizer, added an action name or a `context` key to a record Lakekeeper built.

**Match on `audit_format` for the whole log. Read the version of the product you care about from its key: `.emitters.lakekeeper` or `.emitters.lakekeeper_plus`.** If the key is missing, the record carries nothing from that product. The two versions change independently.

!!! warning "The two numbers are not the same field written twice"
    For records only Lakekeeper produced, `emitters.lakekeeper` equals `audit_format`. That is true for that one key only. Other products carry their own numbers. Always compare the version whose scope you mean.

Compare versions by splitting on `.` and comparing each part as an integer. Do not compare the strings: `"1.10"` sorts *before* `"1.9"`. In `jq`: `select((.audit_format | split(".") | map(tonumber)) >= [1, 9])`. Routing on the major part alone (`.audit_format | split(".") | .[0]`) is the safe default.

##### Which release ships which version

`audit_format` is a promise about **released** builds. A release raises it at most once. If a release has both major and minor changes, the version shows only the major change. The release notes list every change.

A build between releases (`main`, a `rel-*` branch, or a build from source) carries the version the *next* release will have, and may not yet emit all of it. Two such builds can show the same version and emit different records. If you parse audit records, use a release.

Patch releases never change the audit log format: every `x.y.z` emits what `x.y.0` emitted.

Each row holds from its release up to the release in the next row.

| From Lakekeeper release | `audit_format` |
| ----------------------- | -------------- |
| (earlier)               | not emitted    |

##### Not covered by `audit_format`

The log subscriber adds these keys around every record. They are **not** covered by `audit_format`, and their presence, spelling, order and content can change with a dependency upgrade:

- `timestamp`
- `level`
- `message`
- `target`
- `span`
- `spans`
- `filename`
- `line_number`

By default `span` is not written, and `filename` / `line_number` appear only with [extended debug logs](#extended-debug-logs). Do not build detection or routing rules on these keys. Match on `event_source`.

One exception: the `target` **key** belongs to the subscriber, but its **value** on an audit record is always `lakekeeper::audit`. Use it in `RUST_LOG` to decide what is written. Use `event_source` to decide what to do with what arrives.

**Match on full paths. Do not flatten a record to leaf names.** The same key name at two paths can mean two different things, sometimes with different types. Flattening merges them and the later value wins.

| Name | Paths | Meaning |
|---|---|---|
| `name` | `actions[].name` · `authorizations[].determined_by[].name` | A resource name · a policy name |
| `type` | `authorizations[].determined_by[].type` · `error.type` | The kind of deciding factor · the error type |
| `principal` | `actor.principal` · `context.principal` · `actions[].principal` | Who acted (string) · who holds the grant (object) · whose grants a subtree request reaches (object) |
| `source` | `actions[].source` · `authorizations[].determined_by[].source` | The namespace path an entity moves from (array) · where a policy came from (string) |
| `message` | `message` · `context.message` | The subscriber's log message · on an admission record, the gate's own text |

Other repeated names mean the same thing at every path. `authorizations[].action` and `authorizations[].entity` have the same shape as the entries of `actions` and `entities`, so `entity_type`, `action_name`, `table` or `warehouse_id` mean the same inside and outside `authorizations[]`. `error_id` sits under `context` on an admission record and under `error` on a denied authorization, and means the same in both.

**Spelling.** Every name this log defines is `lower_snake_case`: record fields, keys inside an entity, an action or a `context`, and values from a fixed set. **A hyphen means the name comes from another vocabulary.** There are three:

- `entity_type` and `resource_type` use the management API spelling: `generic-table`, `tag-definition`.
- `update_kinds` uses Iceberg's table-update names, such as `add-schema`.
- The objects inside `determined_by` use the management API shape (`policy-id`), so one parser reads both a `/check` response and an audit record.

Values that come from the request, such as a name, an id or a location, are whatever the caller sent.

#### Authorization Events

Written for every authorization check. Every one carries `actions`, `entities`, `actor`, `decision` and `authorizations`. `record_type` is `"authorization"`.

**Structure:**

| Field                  | Type            | Description                       |
|------------------------|-----------------|-----------------------------------|
| `event_source`         | String          | Always `"audit"`                  |
| `audit_format`         | String          | Version of the record's shape, `MAJOR.MINOR`. See [Format version and stability](#audit-format). |
| `record_type`          | String          | Always `"authorization"` for this shape. Route on this field. |
| `emitters`             | Object          | Every product that contributed to the record, with the version of its contribution: `{"lakekeeper": "1.0"}`. See [Two version numbers](#audit-emitter). |
| `request_id`           | String          | The request this record belongs to: the `x-request-id` the caller sent, in whatever form, or the id Lakekeeper generated and returned in that response header. The same value is on the request's other log lines and on the CloudEvents it publishes. |
| `time`                 | String          | When the request was decided, in UTC, RFC 3339 with microseconds: `2026-02-15T14:20:50.758690Z`. It can be earlier than the subscriber's `timestamp`, which is when the line was written. |
| `actions`              | Array           | The operations attempted. Always an array, even with one entry. Each entry has an `action_name` (e.g. `"read_data"`, `"drop"`, `"create_namespace"`) and optional fields describing what the caller asked for. See [Action Format](#action-format). |
| `entities`             | Array           | The resources accessed. Always an array, even with one entry. Each entry has an `entity_type` and fields for that type (e.g. `warehouse_id`, `namespace`, `table`). See [Entity Format](#entity-format). |
| `actor`                | Object          | Who made the request. See [Actor Types](#actor-types). |
| `privilege_source`     | String          | What kind of privilege the caller had for this request: `"authorizer"` (no special privileges; every decision comes from the configured Authorizer), `"instance_admin"` (caller is listed in `LAKEKEEPER__INSTANCE_ADMINS`; control-plane actions are approved automatically, data-plane actions still go through the Authorizer), or `"internal"` (an in-process call; no checks). This describes the request, not the individual entries in `authorizations`. See [Instance Admins](./instance-admins.md). |
| `user_agent`           | String          | The caller's `User-Agent` request header, as sent, cut to 256 bytes. Absent when the request had no `User-Agent`, or one that was not valid text (for example an in-process call from a background worker). **Set by the client and not verified.** See below. |
| `break_glass`          | String          | Present only when the caller sent the `x-break-glass` header with a non-empty value (after trimming). Almost no request does. The reason the caller gave for marking the request an emergency override, as sent (invalid bytes replaced), cut to 256 bytes. **Set by the client and not verified.** See below. |
| `decision`             | String          | `"allowed"` or `"denied"`: the overall decision for the whole record. |
| `authorizations`       | Array           | One entry per decision. Always present. Empty only when the request named nothing to check, such as an empty batch check. See [Per-decision breakdown](#per-decision-breakdown-authorizations). |
| `idempotency_key`      | String          | The request's `Idempotency-Key`. Absent when the caller sent none. Use it to link a retry to the request that did the work. See [Idempotent Replay Events](#replay-events). |
| `context`              | Object          | Optional. Extra facts about the request that are not an action or an entity. Absent when there are none. See [Context fields](#audit-context-fields). |
| `failure_reason`       | String          | Only on failed records. One of `action_forbidden`, `resource_not_found`, `cannot_see_resource`, `internal_authorization_error`, `internal_catalog_error`, `invalid_request_data`. |
| `error`                | Object          | Only on failed records. Contains `type`, `message`, `code`, `error_id` and `stack`. `stack` is always present, `[]` when there is nothing to show. |

**`user_agent` is set by the client and not verified.** Any caller can set any value, including one that names a different client. Lakekeeper does not check it against the token. Use it as a hint, for example to see which client libraries call you, or why one client started failing. Never use it as identity, or for authorization or attribution. Who made the request is in `actor` and `privilege_source`. A detection rule on `user_agent` can be avoided by changing one header; a rule on `actor` cannot.

The header is recorded as sent, not parsed into a client name. Values are free-form, and some clients put host names or user names in them, so treat the field as possibly sensitive. Browsers cannot set `User-Agent`, so requests from the web UI show the browser's own value.

**`break_glass` records a claim, not a grant.** Any caller can send `x-break-glass`. Lakekeeper's built-in authorizers ignore it, so its presence does not mean the request got anything extra. What the request actually got is in `decision` and `authorizations`. A pluggable Authorizer that does act on the header reports this in `determined_by` (see [Per-decision breakdown](#per-decision-breakdown-authorizations)). The value is free text from the caller: match it against a ticket, never trust it as fact. Because it is easy to send, `break_glass` on an unexpected principal is worth an alert.

**An authorization record records the attempt.** It is written before the operation makes its change. `"allowed"` means the caller was permitted to try, not that the change succeeded. If the operation fails afterwards, the request returns an error and the `"allowed"` record stays in the log. Records are written in the background, on a best-effort basis. The log is not a write-ahead log, and a missing operation record does not prove a change was rolled back.

**What counts as a denial.** A request can be refused by the authorizer, or by a rule outside the authorizer. Both are recorded the same way when the refusal is a *decision about this action on this resource*:

- **Refusals by rule are denials.** For example: a catalog-managed (`system`) or provider-managed role that cannot be changed, a reserved tag definition, or a warehouse whose spec is locked. These are recorded as `decision: "denied"` with `failure_reason: action_forbidden`, like a missing permission. Some of these can never be allowed for anyone; `action_forbidden` still applies, because it means "not permitted", not "missing grant". The role and tag-definition rules are checked together with the authorizer, so the request gets one record. The warehouse spec lock can only be checked later, so the request gets an `"allowed"` record followed by a `"denied"` one.
- **Write failures are not denials.** A duplicate name, a tag definition still in use, a backend error: these happen *after* authorization succeeded. The `"allowed"` record stays, and no second authorization record is written. The missing operation record for the change shows that it did not happen.

`decision` alone does not separate refusals from outages. Every failed authorization is marked `"denied"`, including when no verdict was reached; for example, an outage of the catalog or authorizer during the check gives `decision: "denied"` with a `5xx` code. To select real refusals, also filter on the reason:

```jq
select(.decision == "denied" and .failure_reason as $r
       | $r == "action_forbidden" or $r == "resource_not_found" or $r == "cannot_see_resource")
```

The same set is selected by `authorizations[].allowed == false`. Because one request can produce both an `"allowed"` and a `"denied"` record (see the warehouse spec lock above), count denials per request, not per record.

**Actor Types** {#actor-types}

```json
// Anonymous
{"actor_type": "anonymous"}

// Authenticated user
{"actor_type": "principal", "principal": "oidc~user@example.com"}

// Authenticated user, with user emails enabled
{"actor_type": "principal", "principal": "oidc~94eb1d88-7854-43a0-b517-a75f92c533a5", "email": "alice@example.com"}

// Assumed role
{"actor_type": "assumed_role", "principal": "oidc~user@example.com", "assumed_role": {"role_id": "…", "provider_id": "…", "source_id": "…"}}

// Internal system
{"actor_type": "lakekeeper_internal"}
```

| Field          | Type   | Description                                                                                       |
|----------------|--------|---------------------------------------------------------------------------------------------------|
| `actor_type`   | String | `"anonymous"`, `"principal"`, `"assumed_role"`, or `"lakekeeper_internal"`. Always present. |
| `principal`    | String | The authenticated principal. Present for `principal` and `assumed_role`.                           |
| `assumed_role` | Object | The role being acted as, with `role_id`, `provider_id`, and `source_id` unless [role source ids](#audit-role-sources) are turned off. Present for `assumed_role`. |
| `email`        | String | The principal's email, for `principal` and `assumed_role`. Only with [user emails](#audit-user-emails) enabled, and only when known. |

**Principal references.** Where a principal is the *target* rather than the caller (`authorizations[].for_principal`, and `context.principal` on grant records), it is an object with one key: `user` for a user, `role` for a role. For example `{"user": "oidc~alice"}` or `{"role": "<uuid>"}`. With [user emails](#audit-user-emails) enabled, the user form can also carry `email`: `{"user": "oidc~alice", "email": "alice@example.com"}`. The role form carries `provider_id` and `source_id` when they are known, `source_id` unless [role source ids](#audit-role-sources) are turned off: `{"role": "<uuid>", "provider_id": "corporate-ldap", "source_id": "engineering"}`.

**`principal` has three meanings, depending on its path.** `actor.principal` is a string naming who acted. `context.principal` is an object naming who holds a grant (`{"user": "oidc~alice"}`). `actions[].principal` is an object naming the one principal whose grants a subtree request reaches (`{"user": "oidc~alice"}`), present only when `principal_scope` is `one`. A query on `principal.user` finds nothing in `actor`, where the value is a string.

**Context fields** {#audit-context-fields}

The `context` object of an authorization record holds facts about the request that are not an action or an entity. Lakekeeper defines the keys below; the [schema](audit/schema.json) gives the type of each on the `HandlerContext` definition. Only the keys that apply to the request appear, and the object is left out when none apply. A product on top of Lakekeeper can add its own keys, described in that product's schema.

| Key                        | Description                                                                                  |
|----------------------------|----------------------------------------------------------------------------------------------|
| `invoked_by`               | The larger operation this check was made for, when the API call did not cause it directly. Currently only `register_table_overwrite`: the drop checked when a registered table is overwritten. |
| `self_provisioning`        | Boolean. `true` when the caller created a user record for themselves, not an administrator. Always present on that endpoint, so do not read its presence as `true`. |
| `self_read`                | Boolean. `true` when the caller reads their own grants, not another principal's. Always present on the endpoints that set it, so do not read its presence as `true`. |
| `queue_name`               | The task queue the request addressed.                                                         |
| `entity_id`                | Identifier of the entity the task acts on.                                                    |

New keys may be added in any minor version. On operation records, `context` has a different, per-operation layout; see [Operational Audit Events](#operational-audit-events).

**Entity Format** {#entity-format}

Each entity has an `entity_type` and the fields that identify it. `entity_type` is one of `server`, `project`, `warehouse`, `namespace`, `table`, `view`, `task`, `role`, `user`, `generic-table` or `tag`.

Which fields appear depends on the entity type and on what the request gave. A field with no value is left out, never written empty. Every value is a string.

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

**Action Format** {#action-format}

Each action has an `action_name` and, for some actions, fields describing what the caller asked for:

```json
// Simple action (no context)
{"action_name": "read_data"}

// Action with properties context (e.g., create_namespace)
{"action_name": "create_namespace", "properties": {"location": "s3://bucket/ns", "owner": "alice"}}

// Action with update context (e.g., commit with property changes)
{"action_name": "commit", "updated_properties": {"retention-days": "30"}, "removed_properties": ["staging"]}
```

Which fields an action can carry is listed in the [schema](audit/schema.json): the `ActionRecord` definition has one `if`/`then` branch per `action_name`, listing that action's fields and their types. An action with no branch carries no extra fields, or is newer than your copy of the schema.

When a field appears:

- **Lists and maps** are always present once the action has them. They are empty (`[]`, `{}`) when the request gave nothing. Read "none" from the empty value, not from a missing key.
- **Flags** (`force`, `purge`, `recursive`, `dry_run`, `allow_partial`) are JSON booleans, always present once the action has them. Branch on the value, not on presence.
- **Other values** are absent when the request did not give them.

| Context field           | Type   | Description                                             |
|-------------------------|--------|---------------------------------------------------------|
| `name`                  | String | The name the client asked to create                     |
| `properties`            | Object | Properties the client sent, as sent. The keys are user data, not part of the audit format |
| `updated_properties`    | Object | The properties being set, as sent                       |
| `removed_properties`    | Array  | The property keys being removed                         |
| `table_id`              | String | The table id the client asked for                       |
| `generic_table_id`      | String | The generic-table id the client asked for               |
| `format`                | String | The requested table format                              |
| `base_location`         | String | The requested storage location                          |
| `project_id`            | String | The project id the client asked for                     |
| `force`                 | Boolean | `true` when the client asked to force the operation     |
| `purge`                 | Boolean | `true` when the client asked to purge the data          |
| `recursive`             | Boolean | `true` when the client asked for a recursive delete     |
| `target_refs`           | Array  | Commit only. The branch or tag references the commit targets; `[]` when it names none |
| `source`                | Array  | Where the entity is being moved from. For a namespace, its current full path. For a table, view or generic table, the path of the namespace it leaves |
| `destination`           | Array  | Where the entity is being moved to. For a namespace, its full new path. For a table, view or generic table, the path of the destination namespace, without the new name |
| `update_kinds`          | Array  | Commit only. The kinds of update the commit contains; `[]` when it names none |
| `requested_provider_id` | String | The role provider the client named                   |
| `requested_source_id`   | String | The source identifier the client named               |

New fields may be added in any minor version. The values are what the client *asked for*: a `table_id` here is the id requested, not necessarily the one created.

**Grant changes (`action_name = "apply_grants"`):**

A grant change request is checked once as a whole, so one `apply_grants` action describes all of it:

| Context field | Type   | Description                                                                 |
|---------------|--------|-----------------------------------------------------------------------------|
| `principals`  | Array  | The distinct principals the grants are for, each `{"user": "…"}` or `{"role": "…"}` like `context.principal` on grant records; a user with `email` when [user emails](#audit-user-emails) are enabled and it is known |
| `privileges`  | Array  | The distinct privilege names in the request                                 |
| `writes`      | Integer | Number of grant entries requested, before removing duplicates              |
| `deletes`     | Integer | Number of revocation entries requested, before removing duplicates         |

The resource the grants apply to is the record's `entities` entry.

`principals` and `privileges` have duplicates removed, so you cannot pair them up by position. A request for two principals and two privileges shows both lists, not which pairs were asked for. The counts are the request's own, so `writes: 3` with one entry in `principals` means three grants for one principal. A request has at most 100 entries.

**This records the attempt.** What actually changed is recorded separately, one record per grant, as `operation = "grant_created"` or `"grant_revoked"`. Neither kind is published to the configured event stream (Kafka, NATS, CloudEvents). Read `apply_grants` for what was asked and whether it was allowed; read the grant records for what took effect.

A *denied* `apply_grants` has the same detail as an allowed one: what was asked, for whom, on which resource. So a refused privilege escalation shows who it was meant to benefit, not only who tried it.

```json
// Denied attempt to grant `modify` to two principals
{
  "action_name": "apply_grants",
  "principals": [{"role": "1f7b…"}, {"user": "oidc~alice"}],
  "privileges": ["modify"],
  "writes": 2,
  "deletes": 0
}
```

**Subtree grants (`action_name = "read_subtree_grants"` / `"revoke_subtree_grants"`):**

Both are checked once, at the root of the subtree, for the whole batch, so one action describes it. The root is the record's `entities` entry.

`GET /management/v1/grants` about another principal records `read_subtree_grants` on the project, with the same scope fields; its `principal_scope` is always `one`. About yourself it records `get_metadata`.

Six fields describe the **scope**, plus `principal` when the scope names one principal. It is the same scope the authorizer is asked about. The six fields appear together or not at all; a request without a scope carries none of them.

| Context field    | Type   | Description                                                                 |
|------------------|--------|------------------------------------------------------------------------------|
| `dry_run`        | Boolean | `true` when the call only reports what it would do. A dry run changes nothing, so `true` is not evidence of a revocation. A dry-run revoke is recorded as `revoke_subtree_grants` with `true`; a `read_subtree_grants` record from a subtree listing shows `false` |
| `resource_types` | Array  | The resource kinds the request reaches. At least one, and only kinds the addressed resource covers |
| `root_level`     | String | `included` when the addressed resource's own grants are in range, `excluded` when only those below it are. Closed set |
| `principal_scope` | String | `every` when the grants of every principal are in range, `one` when `principal` names the one. Closed set |
| `principal`      | Object | The one principal whose grants are in range, `{"user": "…"}` or `{"role": "…"}`; a user with `email` when [user emails](#audit-user-emails) are enabled and it is known. Present only when `principal_scope` is `one` |
| `privilege_scope` | String | `every` when the request reaches every privilege a matching grant can have, including privileges this server no longer lists; `only` when it names a set. Closed set |
| `narrowed_privileges` | Array | The privileges named when `privilege_scope` is `only`. `[]` when it is `every`, so read `privilege_scope` first |

`revoke_subtree_grants` has three more fields, describing the filter:

| Context field    | Type   | Description                                                                 |
|------------------|--------|------------------------------------------------------------------------------|
| `privileges`     | Array  | The distinct privilege names the revocation is limited to. Always present; `[]` means every privilege |
| `allow_partial`  | Boolean | `true` when the client asked the revocation to go ahead even if some grants cannot be revoked. Always present |
| `created_before` | String | Optional. RFC 3339 timestamp; only grants created before it are in range. Absent when the request does not filter on it |

**These are the filters, not the result.** The record shows what the caller asked for and whether it was allowed, not which grants matched. What actually changed is recorded separately, one record per grant, as `operation = "grant_revoked"`. A dry run records none.

#### Per-decision breakdown (`authorizations`)

Every authorization record has an `authorizations` array:

- A normal single-check API call: one entry, built from the record's top-level fields.
- `/management/v1/action/batch-check`: one entry per check, in request order.
- A request with nothing to check, such as a batch check with `checks: []` or a transaction commit with no table changes: an empty array. `decision` still holds the result.
- The `get_*_actions` endpoints: one entry for the `introspect_permissions` action. The actions the principal holds are in the response body, not in this array.

So one query works for single and batch records: go through `authorizations[]` and read each entry's `allowed`.

Each entry stands on its own; you do not need to combine it with the top-level fields:

| Field           | Type    | Description                                                                          |
|-----------------|---------|--------------------------------------------------------------------------------------|
| `id`            | String  | Identifier of this entry. When the client gives an `id` on a batch-check input, it appears here as sent, and the API response returns the same value. When the client gives none, the API response has no id, and the audit entry uses the item's zero-based position instead. **The API response never carries position-based ids; only the audit entry does.** Absent on single-check entries. |
| `for_principal` | Object  | Optional. The principal whose permission was checked, when it is not the caller: `{"user": "..."}` or `{"role": "..."}`, the user form with `email` when [user emails](#audit-user-emails) are enabled and it is known. Absent means the caller. |
| `action`        | Object  | One action, in the same shape as an entry of `actions`.                              |
| `entity`        | Object  | One entity, in the same shape as an entry of `entities`.                             |
| `allowed`       | Boolean | Whether this check was permitted. `false` means a definite refusal, by the authorizer or by a rule outside it (see [What counts as a denial](#authorization-events)); `error.type` tells which, and a refusal by rule has an empty `determined_by`. Absent when no verdict was reached, as with `internal_authorization_error`, `internal_catalog_error` or `invalid_request_data`. On a record denied with `action_forbidden`, `resource_not_found` or `cannot_see_resource`, every entry is normally `false`; an endpoint that records its own checks gives each entry its own verdict, so a denied record can have an entry with `true`. |
| `determined_by` | Array   | What decided this entry. Always present; `[]` when the authorizer reports nothing (OpenFGA and allow-all never do). Each element is one of the kinds below. This is about one decision; `privilege_source` is about the caller. `POST /management/v1/action/batch-check` returns the same elements, under the name `determined-by`. |

Each `determined_by` element is a flat object. Its `type` names the kind:

- `policy`: a matched policy.
- `system-authority`: a built-in authority, not a configured policy, decided the allow. For example, a recovery grant that lets a privileged system role act despite a policy that would forbid it.
- `admission-gate`: an admission gate would refuse this user, so the request is denied whatever the policies say.

| `type`             | Field       | Type   | Description                                                                               |
|--------------------|-------------|--------|-------------------------------------------------------------------------------------------|
| `policy`           | `policy-id` | String | Authorizer-assigned identifier of the policy. Always present.                             |
| `policy`           | `effect`    | String | `permit` or `forbid`. Always present.                                                     |
| `policy`           | `name`      | String | Name the policy author gave. Absent when there is none. Not guaranteed unique.            |
| `policy`           | `source`    | String | Opaque origin of the policy. Absent when unknown.                                         |
| `system-authority` | `source`    | String | Opaque identifier of the built-in authority. Absent when unknown.                         |
| `system-authority` | `reason`    | String | Readable reason the authority applied. Absent when none is given.                         |
| `admission-gate`   | `gate`      | String | Name of the admission gate that would refuse the user. Always present.                    |
| `admission-gate`   | `check`     | String | The check within that gate that decided. Absent when the gate names none.                 |

```json
{"type": "policy", "policy-id": "policy-42", "name": "deny-stale-namespaces", "effect": "forbid", "source": "cedar"}
```

A field with no value is left out, never written as `null`. This holds everywhere in a record.

**Caller and checked principal.** The top-level `actor` is always the *caller* (the bearer token holder). `authorizations[].for_principal` is *whose permissions were checked*. Usually they are the same and `for_principal` is left out. For a call like `GET /lakekeeper/v1/permissions/...?for-user=X`, the actor is the caller and every entry's `for_principal` is `X`.

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

#### Idempotent Replay Events {#replay-events}

Written when a request with an `Idempotency-Key` was answered from the stored result instead of being run again. `record_type` is `"replay"`. There is no `operation`, `outcome` or `decision`.

A replay record also carries `actions`, `entities`, `privilege_source` and `user_agent`, like an authorization record, so the same queries work on both. Join a replay to the original request on `idempotency_key`, which authorization records also carry. Operation records (such as `grant_created` or `ldap_resolve_roles`) do not carry it.

| Field             | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `event_source`    | Always `"audit"`                                                            |
| `audit_format`    | Version of the record's shape, `MAJOR.MINOR`                                |
| `record_type`     | Always `"replay"` for this shape                                            |
| `emitters`        | Every product that contributed to the record, as on authorization records   |
| `request_id`      | The request that was answered from the stored result                        |
| `time`            | When the request was answered, in UTC                                       |
| `idempotency_key` | The key whose stored result answered the request                            |
| `actions`         | The actions the retry asked for, in the same shape as on an authorization record |
| `entities`        | The targets the retry named, in the same shape as on an authorization record |

**`actions` and `entities` show what the retry asked for, not what the original request did.** A stored result is matched on warehouse, key and endpoint only, not on the target or the parameters (see [Idempotency](./configuration.md#idempotency)). If a key is reused on the same endpoint for a different table, the record names that table, which was not touched. A retry sent with `purgeRequested=true` after an original without it is recorded as a purging drop. Read these fields as what was asked, not as evidence of what happened.

**A replay record does not show that the actor was ever authorized.** The stored result is returned before authorization runs, so any authenticated caller with the key can produce one, even one with no grants in the warehouse. A replay never changes anything.

**Audit records contain live idempotency keys**, on authorization records as well as on replays. Anyone who can read the audit log can replay those keys, get a 204, and create more records. Protect access to the audit log accordingly. `idempotency-key-lifetime` sets the earliest time a stored result can be deleted. Deletion runs only as part of a small share of keyed requests, so on a quiet deployment keys stay live until traffic picks up again.

**Only seven endpoints write replay records:** `dropTable`, `dropView`, `dropNamespace` (recorded as `action_name = "delete"`), `dropGenericTable`, `renameTable`, `renameView`, `renameGenericTable`. They answer 204 and return the stored result before authorization, so no `decision` is recorded. The change already happened, and denying the retry would report a failure for a completed operation. `renameTable`, `renameView` and `renameGenericTable` record a rename into another namespace as `action_name = "move"`, on replays as on their authorization records.

**Retries on the other idempotent endpoints are not marked, and look different from the original.** `createTable`, `registerTable`, `createNamespace`, `updateNamespaceProperties`, `replaceView` and `createGenericTable` build their response by loading the entity. A retry therefore writes the authorization record of that *load* (`action_name = "get_metadata"` on the entity) and no record for the create or update. `updateTable` writes both `commit` and `get_metadata`. Use `idempotency_key` to link these to the original.

**`commitTransaction` retries are not marked at all.** It authorizes before it detects the retry, so the retry writes the same `commit` record as the first run, with the same `idempotency_key`. Only two records sharing a key show that a retry happened.

Replay records appear only in the audit log. Like grant records, they are never published to the configured event stream (Kafka, NATS, CloudEvents). Delivery is best-effort.

#### Operational Audit Events

Written for things the system does for a user that involve no authorization decision: role resolution through LDAP or other providers, the grants actually written, and admission decisions. Use them to see *what the system did for a user*, not *whether the user was allowed to do something*. `record_type` is `"operation"`. Several carry user identities (PII); each section below says which.

**Structure:**

| Field          | Type   | Description                                        |
|----------------|--------|----------------------------------------------------|
| `event_source` | String | Always `"audit"`                                   |
| `audit_format` | String | Version of the record's shape, `MAJOR.MINOR`       |
| `record_type`  | String | Always `"operation"` for this shape                |
| `emitters`     | Object | The product that wrote the record, with the version of its contribution. `{"lakekeeper": "1.0"}` for grant and `admission_decided` records; `{"lakekeeper_plus": "1.0"}` for records written by Lakekeeper+, such as role resolution |
| `request_id`   | String | The request the operation belongs to, as on authorization records. Absent when no request caused the operation, such as a background role sync |
| `time`         | String | When the operation happened, in UTC, as on authorization records |
| `operation`    | String | Name of the operation (e.g. `"ldap_resolve_roles"`) |
| `actor`        | Object | Same shape as on authorization records (see [Actor Types](#actor-types)). Read `actor_type` before `principal`. See below for which shapes each operation can have |
| `outcome`      | String | Result of the operation. Values depend on the operation; see the sections below |
| `context`      | Object | Optional. Details specific to the operation (e.g. `provider_id`, `role_count`) |

**Outcomes are not allow/deny.** They describe the result of a system operation. There is no `decision` field.

**Which actor shapes appear.** `admission_decided`, `grant_created` and `grant_revoked` use the request's actor, so any of the four shapes can appear. On grant records, the `assumed_role` of an assumed-role caller is the role that holds the grant. On `admission_decided`, which is written before the `x-assume-role` check, `assumed_role` is the role the request *asked* to assume, not yet authorized. For a caller acting as a role, the acting identity is in `assumed_role`; `principal` alone does not show it. Operations that name a user directly, not taken from the request, always use `principal`.

**Grant changes (`operation = "grant_created"` / `"grant_revoked"`):**

Written after the change is committed, one record per grant the backend reports as applied. `outcome` is always `success`. The record states the grant's state after the change, not that it differs from before.

These records confirm what an `apply_grants` authorization recorded as an attempt. The `apply_grants` record lists principals and privileges separately and cannot say who got what; these records give each full grant:

| Context field  | Description                                                                 |
|----------------|-----------------------------------------------------------------------------|
| `principal`    | Who holds the grant, as `{"user": "…"}` or `{"role": "…"}`; the user form with `email` when [user emails](#audit-user-emails) are enabled and it is known |
| `privilege`    | The privilege name, as the authorizer names it                              |
| `resource_type`| `server`, `project`, `warehouse`, `namespace`, `table`, `view`, `generic-table` or `tag-definition` |
| `resource_id`  | The exact resource. Absent for `server` grants, which have no id            |
| `warehouse_id` | The containing warehouse, for resources inside a warehouse only             |

Revoked grants are deleted with no history, so a `grant_revoked` record is the only lasting trace of a revocation. If you need to answer "who held what, and when", keep these records.

**State, not transitions.** `grant_created` means *this grant is in effect after this request*, not that it was new. `grant_revoked` means *this grant is not in effect after this request*. Applying the same change twice can write the same records twice, and revoking a grant nobody held can write a `grant_revoked`. Not every authorizer can tell whether a grant was already held. Make consumers idempotent: key on the `(principal, privilege, resource)` triple, not on record counts. Where grants live in the catalog database, Lakekeeper skips records for changes that did nothing, but do not depend on this.

**Not every grant removal is recorded.** Two paths remove grants without a `grant_revoked` record:

- **Deleting the resource.** Dropping a warehouse, namespace, table, view or tag definition removes its grants directly in the database. The deletion record of the resource is the only record.
- **Deleting a user, when the authorizer stores the grants.** The authorizer removes the user's grants with its other data, without listing them. Where grants live in the catalog database, deleting a user *does* write one record per revoked grant.

So a `grant_created` with no matching `grant_revoked` does **not** mean the grant is still held. For current access, call `GET .../grants`. Use these records for attribution and change history, not to rebuild the current state.

**Delivery is best-effort.** Records are written after the commit. If a listener fails, or the process stops between commit and dispatch, the record is lost; the failure is logged and not retried. The grant itself still stands. Expect that a record can be missing, and do not treat these records as the authoritative list of changes.

**Admission rejections (`operation = "admission_decided"`):**

Written when an [admission gate](./admission.md) refuses a request. Gates run after authentication, before the `x-assume-role` check and before any handler. For an assumed-role caller, `actor.assumed_role` is the role the request asked to assume, not yet authorized. No `assume_role` authorization record is written for the request.

`outcome` is one of:

- `forbidden`: the gate denied the caller (`403`).
- `unavailable`: the gate could not reach a service it needs, so it refused the request to be safe (`503`).

This record names the principal. The error response does not, because responses never contain PII.

| Context field | Description |
|---------------|-------------|
| `gate`        | Which gate decided. Useful when you run more than one |
| `denied_by`   | The gate's rule that decided. Absent when the gate names none, as when it fails closed |
| `status`      | `403` or `503`, as a JSON **number**, like `error.code` on authorization records |
| `error_type`  | The gate's error type, e.g. `ExternalEnforceForbidden` |
| `message`     | The gate's own text, as the caller received it. It tells apart two rejections with the same `error_type`, for example a gate failing closed on a missing setting versus an unreachable service. Not the same as the top-level `message` of every log line |
| `error_id`    | The id the caller received in the response body. Use it to match a user's report to this record |

This is the **only** record of a rejection. The [error-response log](#2-error-response-logs) is not written for it.

A gate that fails closed also logs a `WARN` line in the general log, with `gate`, `error_type`, `error_id`, `request_id`, and `cause` (what failed, if the gate reports it). That line names no principal. `cause` appears nowhere else: not in the audit record, not in the response. Alert on the gate's metrics, not on `ERROR` lines.

Admitted requests write no record here. The authorization records that follow show what the caller did.

**Admission role checks (`operation = "admission_enforce_check"`):**

Written by the external-enforce gate when the control plane refuses one role but still admits the request. `outcome` is `role_withheld`. The request runs with fewer privileges, and nothing else in the request reports this. `context` has `gate`, `check`, `role` and `cache_ttl_secs`.

There is one record per answer from the control plane, not per request. The gate caches each answer, and requests served from the cache write no record. `cache_ttl_secs` tells you how long later requests may have used that answer. For the per-request rate, use the `lakekeeper_admission_enforce_decisions_total` metric.

`actor` has the same shape here as in `admission_decided`, including the assumed role, so the two records can be joined on it.

**LDAP role resolution (`operation = "ldap_resolve_roles"`):**

| `outcome`        | When written                                              |
|------------------|-----------------------------------------------------------|
| `success`        | User found and roles resolved (the list can be empty after mapping) |
| `user_not_found` | No LDAP entry matched the search filter for this user     |
| `no_roles`       | The user entry exists but has no group-membership attribute |
| `ambiguous_user` | The LDAP search matched more than one entry for the user; the request fails |
| `dn_no_match`    | `Branching` mode with `else.mode = none`: the user DN did not match `branch_if_user_dn_matches`; an empty role list is returned |

Every `ldap_resolve_roles` context has a `mode` field naming the resolution path used:

| `mode`                  | Meaning                                                                                                 |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| `search`                | Search mode                                                                                             |
| `attribute`             | Attribute mode                                                                                          |
| `branching`             | Branching mode, but no branch was chosen (e.g. `user_not_found`)                                        |
| `branch_then`           | Branching mode, `then` branch (the DN matched the pattern)                                              |
| `branch_else_attribute` | Branching mode, `else.mode = attribute` (the DN did not match)                                          |
| `branch_else_none`      | Branching mode, `else.mode = none` (only with `outcome = "dn_no_match"`)                                |

Other context fields: `provider_id`, `role_count` (number of roles resolved), `count` (number of matching entries, on `ambiguous_user`), `filter` (the search filter with the user's subject filled in), `user_dn`, `attribute`, `pattern` (the DN pattern in branching mode) and `principal`.

**PII in context fields.** `filter`, `user_dn` and `principal` are PII. `provider_id`, `attribute`, `pattern`, `role_count`, `count` and `mode` are not.

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

| `outcome`                | When written                                      |
|--------------------------|---------------------------------------------------|
| `no_provider_applicable` | No configured role provider matched this user. `context.providers_checked` lists the providers tried. |
| `roles_resolved`         | At least one role was resolved. Off by default; turn on with `LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_ROLE_ASSIGNMENTS=true`. `context` has `role_count`, the full `roles` list, and `sources`: where each provider's roles came from (`fresh`, `cache_hit`, `stale_fallback` or `in_request`). |
| `error`                  | A matched provider failed to resolve roles (e.g. an LDAP connection error). The request continues with no roles. |

`no_provider_applicable` is on by default and controlled by `LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_UNHANDLED_USERS`. If it appears for a user you expect to be covered, a domain filter is wrong or a provider is missing. If some users are not meant to be covered, set the variable to `false` to stop these records.

`roles_resolved` is **off by default** because it is written on every authenticated request and lists every resolved role name. Turn it on only for a while, to debug role-provider settings. Do not leave it on in production.

`error` is always written when role resolution fails. A warning without PII is also written to the general log.

<details>
<summary>No provider applicable</summary>

```json
{
  "timestamp": "2026-03-07T10:00:00.000000Z",
  "level": "INFO",
  "message": "No role provider handled user; user will have no provider-assigned roles",
  "target": "lakekeeper::audit",
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
  "target": "lakekeeper::audit",
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

| `outcome`             | When written                                                            |
|-----------------------|-------------------------------------------------------------------------|
| `stale_cache_fallback` | One or more providers failed to refresh, so older roles cached in the database are used. `context.provider_ids` lists the affected providers. |

A `WARN` line without PII is also written to the general log. This outcome points to a short-lived connection problem with the role provider (e.g. LDAP unreachable). The user gets their last known roles, not an error.

<details>
<summary>Stale cache fallback</summary>

```json
{
  "timestamp": "2026-03-07T11:30:00.000000Z",
  "level": "INFO",
  "message": "stale provider(s) failed to refresh; serving cached roles",
  "target": "lakekeeper::audit",
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

**jq filters for operation records:**

```bash
# All LDAP resolution events
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "ldap_resolve_roles")'

# Users not found in LDAP (wrong filter or unknown principals)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .outcome == "user_not_found")'

# Successful resolutions for a specific user
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "ldap_resolve_roles" and .actor.principal == "oidc~user@example.com")'

# Users not matched by any role provider
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .outcome == "no_provider_applicable")'

# Stale cache fallbacks (role provider unreachable, last-known roles served)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .outcome == "stale_cache_fallback")'

# Who was refused admission to this instance, and by which rule
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "admission_decided") | {actor: .actor.principal, outcome, rule: .context.denied_by}'

# An admission gate failing closed (a service outage, not a denial)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .operation == "admission_decided" and .outcome == "unavailable")'

# Find the record behind an error id a user reported
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

# Denied authorizations (includes outages during the check; see "What counts as a denial")
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .decision == "denied")'

# Error responses
cat logs.json | jq -R 'fromjson? | select(.event_source == "error_response")'

# Specific user activity
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and .actor.principal == "oidc~user@example.com")'

# Specific table access
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.entities // [])[]; .table == "my_table"))'

# Any denied decision, single check or inside a batch
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.authorizations // [])[]; .allowed == false))'

# Permissions checked for a specific user (introspection / batch-check)
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.authorizations // [])[]; .["for_principal"].user == "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"))'

# Every grant change, allowed or refused
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.actions // [])[]; .action_name == "apply_grants"))'

# Which client libraries call, and how often (set by the client, not verified)
cat logs.json | jq -R -r 'fromjson? | select(.event_source == "audit") | .user_agent // "(none sent)"' | sort | uniq -c | sort -rn

# Refused attempts to grant privileges TO a specific principal
cat logs.json | jq -R 'fromjson? | select(.event_source == "audit" and any((.actions // [])[]; .action_name == "apply_grants" and any((.principals // [])[]; .user == "oidc~alice")) and any((.authorizations // [])[]; .allowed == false))'
```

## Best Practices

1. **Separate Audit Logs**: Send logs with `event_source=audit` to secure, long-term storage for compliance.

2. **PII Handling**: Audit logs contain user identities and live idempotency keys. Restrict access and set retention policies.

3. **Error IDs**: Every error has a unique `error_id`. Use this to correlate client-side errors with server logs.

4. **Log Aggregation**: In production, use a centralized logging system (ELK, Loki, Splunk) to collect and analyze logs from all Lakekeeper instances.

5. **Alerts**: Set up alerts for:
   - Multiple `decision=denied` events from the same principal
   - High rates of `event_source=error_response` with 5xx codes. Admission rejections are not in this log; they appear only as `admission_decided` audit records. For gates failing closed, alert on `lakekeeper_admission_gate_duration_seconds{outcome="unavailable"}` or on the `WARN` line the gate writes
   - Access to sensitive resources outside business hours

## Related Topics

- [Authentication](./authentication.md) - Configure identity providers
- [Authorization](./authorization.md) - Set up permission management  
- [Configuration](./configuration.md) - Complete configuration reference
