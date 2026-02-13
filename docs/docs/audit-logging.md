# Audit Logging

## Overview

Lakekeeper automatically generates audit logs for all authorization events in your catalog. These logs provide a comprehensive audit trail for security monitoring, compliance, and debugging. Audit logs capture both successful and failed authorization attempts, allowing you to track who accessed what data and when.

Audit logs are emitted through Lakekeeper's standard logging infrastructure and can be collected and analyzed using your existing log aggregation tools.

## Identifying Audit Logs

All audit log entries include a special field that distinguishes them from other log messages:

```json
{
  "event_source": "audit",
  ...
}
```

This field allows you to filter and route audit logs separately from operational logs in your log aggregation system (e.g., ELK Stack, Splunk, Grafana Loki).

## Log Structure

Audit logs are structured JSON entries containing the following fields:

### Core Fields

| Field           | Type   | Description |
|-----------------|--------|-----|
| `event_source`  | String | Always set to `"audit"` for audit logs. Use this field to identify and filter audit log entries. |
| `action`        | String | The operation that was attempted (e.g., `read_data`, `create_namespace`, `drop`, `update_storage`). |
| `entity`        | Object | Details about the resource being accessed. The structure varies depending on the resource type (table, namespace, warehouse, etc.). Common fields include `warehouse_id`, `namespace`, `table`, etc. |
| `actor`         | Object | Identifies who attempted the action. See [Actor Structure](#actor-structure) below. |
| `request_id`    | String | Unique identifier (UUID) for the request. Can be used to correlate audit logs with other system logs. |
| `extra_context` | Object | Additional contextual information relevant to the specific operation. May include project IDs, warehouse names, or other relevant metadata. |

### Authorization Failed Events

When an authorization check fails, the following additional fields are included:

| Field             | Type   | Description |
|-------------------|--------|-----|
| `failure_reason`  | String | Why the authorization failed. Possible values: `ActionForbidden`, `ResourceNotFound`, `CannotSeeResource`, `InternalAuthorizationError`, `InternalCatalogError`, `InvalidRequestData`. |
| `error`           | Object | Detailed error information including error type, message, code, and stack trace. |

### Actor Structure

The `actor` field identifies who performed the operation. It can take one of the following forms:

**Anonymous User:**
```json
{
  "type": "anonymous"
}
```

**Authenticated User:**
```json
{
  "type": "principal",
  "principal": "oidc~user@example.com"
}
```

**Assumed Role:**
```json
{
  "type": "role",
  "principal": "oidc~user@example.com",
  "assumed-role": "00000000-0000-0000-0000-000000000001"
}
```

The `Principal` value follows the format `<idp>~<subject>` where:
- `idp` is the identity provider (e.g., `oidc` for OpenID Connect, `kubernetes` for Kubernetes service accounts)
- `subject` is the user identifier from the identity provider

## Example Audit Logs

The following examples show the general structure of audit logs. Note that the exact structure of the `entity` field varies depending on the resource type being accessed.

### Successful Authorization

```json
{
  "timestamp": "2026-02-13T10:23:45.123Z",
  "level": "INFO",
  "event_source": "audit",
  "action": "read_data",
  "entity": {
    "warehouse_id": "550e8400-e29b-41d4-a716-446655440000",
    "namespace": ["production", "sales"],
    "table": "customer_orders"
  },
  "actor": {
    "type": "principal",
    "principal": "oidc~analyst@company.com"
  },
  "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "extra_context": {
    "project_id": "00000000-0000-0000-0000-000000000000"
  },
  "message": "Authorization succeeded event"
}
```

### Failed Authorization

```json
{
  "timestamp": "2026-02-13T10:25:12.456Z",
  "level": "INFO",
  "event_source": "audit",
  "action": "drop",
  "entity": {
    "warehouse_id": "550e8400-e29b-41d4-a716-446655440000",
    "namespace": ["production", "finance"],
    "table": "sensitive_data"
  },
  "actor": {
    "type": "principal",
    "principal": "oidc~contractor@external.com"
  },
  "request_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "failure_reason": "ActionForbidden",
  "error": {
    "type": "Forbidden",
    "message": "Insufficient permissions to drop table",
    "code": 403,
    "stack": []
  },
  "extra_context": {
    "project_id": "00000000-0000-0000-0000-000000000000"
  },
  "message": "Authorization failed event"
}
```

## Authorization Failure Reasons

The `failure_reason` field provides specific information about why an authorization check failed:

| Reason                        | Description |
|-------------------------------|-----|
| `ActionForbidden`             | The user is authenticated and the resource exists, but the user lacks permission to perform the requested action. |
| `ResourceNotFound`            | The requested resource does not exist in the catalog. |
| `CannotSeeResource`           | The resource exists but the user lacks permission to view it. |
| `InternalAuthorizationError`  | The authorization backend service (e.g., OpenFGA, Cedar) encountered an error. |
| `InternalCatalogError`        | An internal catalog error occurred before the authorization check could be completed. |
| `InvalidRequestData`          | The client provided malformed or invalid data that prevented the authorization check from completing. |

## Consuming Audit Logs

### Log Aggregation

Audit logs are emitted through Lakekeeper's standard logging output. To collect them:

1. Configure your log aggregation system to collect logs from Lakekeeper pods/containers
2. Filter for entries where `event_source = "audit"` 
3. Route audit logs to a dedicated index or storage for compliance and security monitoring

### Common Use Cases

**Security Monitoring:**
- Track failed authorization attempts to identify potential security threats
- Monitor access patterns to sensitive tables and namespaces
- Detect unusual activity or privilege escalation attempts

**Compliance & Auditing:**
- Maintain audit trails for regulatory compliance (GDPR, SOX, HIPAA, etc.)
- Track who accessed specific data and when
- Generate reports on data access patterns

**Debugging:**
- Investigate permission issues by examining failed authorization events
- Use `request_id` to correlate audit logs with application logs
- Analyze `extra_context` for additional debugging information

### Example Queries

**Filter audit logs only (using jq):**
```bash
cat lakekeeper.log | jq 'select(.event_source == "audit")'
```

**Find all failed authorization attempts:**
```bash
cat lakekeeper.log | jq 'select(.event_source == "audit" and .failure_reason != null)'
```

**Track access to a specific table:**
```bash
cat lakekeeper.log | jq 'select(.event_source == "audit" and .entity.table == "sensitive_data")'
```

**Monitor a specific user's activity:**
```bash
cat lakekeeper.log | jq 'select(.event_source == "audit" and .actor.principal == "oidc~user@example.com")'
```

## Best Practices

1. **Centralize Audit Logs**: Send audit logs to a dedicated log aggregation system for long-term retention and analysis.

2. **Set Up Alerts**: Configure alerts for suspicious patterns such as:
   - Multiple failed authorization attempts from the same user
   - Access attempts to sensitive resources outside business hours
   - Privilege escalation or role assumption events

3. **Retention Policies**: Establish appropriate retention policies based on your compliance requirements. Many regulations require audit logs to be retained for several years.

4. **Access Control**: Restrict access to audit logs themselves to authorized security and compliance personnel only.

5. **Regular Reviews**: Periodically review audit logs to identify unusual access patterns or potential security issues.

6. **Performance Considerations**: Audit logging is lightweight and enabled by default. However, in high-throughput environments, ensure your log aggregation system can handle the volume.

## Configuration

Audit logging is disabled by default to reduce log volume in production. To enable audit logging, set the following environment variable:

```bash
LAKEKEEPER__AUDIT__TRACING__ENABLED=true
```

Audit events are logged at the `INFO` level through Lakekeeper's standard logging infrastructure. To see audit logs:

1. Enable audit logging with the environment variable above
2. Ensure your `RUST_LOG` level is set to `info` or lower (e.g., `RUST_LOG=info` or `RUST_LOG=debug`)
3. Audit logs will appear in structured JSON format by default

### Additional Logging Configuration

You can further customize log output:

- `RUST_LOG`: Controls the log level for all components (e.g., `RUST_LOG=info,lakekeeper=debug`)
- `LAKEKEEPER__DEBUG__EXTENDED_LOGS=true`: Includes file names and line numbers in log entries for debugging

See the [Configuration Guide](./configuration.md) for more details on logging configuration.

## Related Topics

- [Authentication](./authentication.md) - Configure identity providers
- [Authorization](./authorization.md) - Set up permission management
- [Configuration](./configuration.md) - General Lakekeeper configuration
- [Production Checklist](./production.md) - Production deployment recommendations
