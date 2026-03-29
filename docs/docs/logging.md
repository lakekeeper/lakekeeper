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

**Note:** Audit logs contain PII. When disabling them, ensure you have alternative mechanisms for compliance and security monitoring.

## Log Types

Lakekeeper produces three types of logs, distinguished by the `event_source` field:

### 1. Audit Logs

Authorization events tracking access to catalog resources. **Contains PII** (user identities).

**Identified by:** `"event_source": "audit"`

Audit logs cover two distinct schemas depending on the source of the event:

#### Authorization Events

Emitted for every authz check. Always contain `action`/`actions`, `entity`/`entities`, `actor`, and `decision`.

**Structure:**

| Field                  | Type            | Description                       |
|------------------------|-----------------|-----------------------------------|
| `event_source`         | String          | Always `"audit"`                  |
| `action` or `actions`  | Object or Array | Operation(s) attempted. Each action is an object with an `action_name` field (e.g., `"read_data"`, `"drop"`, `"create_namespace"`) and optional context fields (e.g., `properties`, `updated-properties`, `removed-properties`). See format below. |
| `entity` or `entities` | Object or Array | Resource(s) accessed, containing `entity_type` and type-specific fields (e.g., `warehouse-id`, `namespace`, `table`) |
| `actor`                | Object          | Who performed the action (see format below) |
| `decision`             | String          | `"allowed"` or `"denied"`         |
| `context`              | Object          | Optional. Additional operation context (e.g., `project-id`, `warehouse-name`) |
| `failure_reason`       | String          | Only on failed events. One of: `ActionForbidden`, `ResourceNotFound`, `CannotSeeResource`, `InternalAuthorizationError`, `InternalCatalogError`, `InvalidRequestData` |
| `error`                | Object          | Only on failed events. Contains `type`, `message`, `code`, `error_id`, `stack` |

**Note:** Empty arrays and objects are omitted from the output. For example, if `stack` is empty, the field will not appear in the log.

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

**Examples:**

<details>
<summary>Authorization Succeeded</summary>

```json
{
  "timestamp": "2026-02-15T14:20:50.758690Z",
  "level": "INFO",
  "event_source": "audit",
  "action": {
    "action_name": "introspect_permissions"
  },
  "entity": {
    "entity_type": "warehouse",
    "warehouse-id": "414b18f0-0a6d-11f1-b2d7-f31430431ca0"
  },
  "actor": {
    "actor_type": "principal",
    "principal": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
  },
  "decision": "allowed",
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
  "decision": "denied",
  "failure_reason": "ActionForbidden",
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

#### Operational Audit Events

Emitted for non-authz operations that touch user identity (PII) — such as LDAP/directory role resolution and user enrichment. Use these to audit *what the system fetched on behalf of a user*, rather than *whether the user was allowed to do something*.

**Structure:**

| Field          | Type   | Description                                        |
|----------------|--------|----------------------------------------------------|
| `event_source` | String | Always `"audit"`                                   |
| `operation`    | String | Machine-readable name of the operation (e.g., `"ldap_resolve_roles"`) |
| `actor`        | Object | Same shape as authorization events: `{"actor_type": "principal", "principal": "oidc~…"}` |
| `outcome`      | String | Result of the operation. Component-specific; see individual operation docs below |
| `context`      | Object | Optional. Operation-specific metadata (e.g., `provider_id`, `role_count`) |

**Outcomes are not binary allow/deny** — they describe the result of the system operation. No `decision` field is present.

**LDAP role resolution (`operation = "ldap_resolve_roles"`):**

| `outcome`        | When emitted                                              |
|------------------|-----------------------------------------------------------|
| `success`        | User found and role list resolved (possibly empty after mapping) |
| `user_not_found` | No LDAP entry matched the search filter for this subject  |
| `no_roles`       | User entry exists but has no group memberships configured |

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
    "role_count": 3
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
    "filter": "(&(objectClass=person)(uid=unknown))"
  },
  "message": "LDAP user not found; returning empty role list",
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
