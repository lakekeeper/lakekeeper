# Admission Gates <span class="lkp"></span> {#admission-gates}

An **admission gate** makes a coarse allow/deny decision about an *already-authenticated* request **before it reaches any handler** — distinct from the per-resource [Authorizer](./authorization.md). Use one to consult an external control-plane entitlement service, suspend a tenant or principal, or reject revoked tokens.

Gates run on every authenticated request, in order, after the actor and instance-admin status are resolved; the first rejection wins. A gate returns either a terminal `403 Forbidden` or — when it fails closed because an upstream it depends on is unreachable — a `503` with a `Retry-After`.

The gate seam itself ([`AdmissionGate`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/admission.rs)) is a Rust trait; see [Customize](./customize.md) to implement your own. This page documents the **external enforce-endpoint gate** that ships ready-to-configure with Lakekeeper Plus.

## When to use it

This gate is for deployments whose IdP issues **broad, non-instance-scoped tokens**, where a separate service — not the token — is authoritative for whether the caller may use *this* Lakekeeper instance. After authentication, the gate asks that service, per caller, and either lets the request through or rejects it.

If your tokens already carry the entitlement (claims, roles, audience), you don't need this — use [authentication](./authentication.md) and [authorization](./authorization.md).

## How it works

The gate evaluates one or more named **checks** against a configured enforce endpoint. **Each check carries its own complete request body** (any JSON shape), so the gate imposes no schema on the enforce API — you decide what the endpoint sees. Each check is a single `POST`; the decision is the HTTP status:

| Upstream status                            | `kind = "gating"`                       | `kind = "role_granting"`      |
| ------------------------------------------ | --------------------------------------- | ----------------------------- |
| `2xx`                                      | admit + grant role                      | grant role                    |
| `403` (exactly)                            | **reject request (`403`)**              | withhold role, continue       |
| any other status, timeout, network error  | **`503` + `Retry-After`** (fail closed) | same — `503` fail closed      |

Only an exact `403` is read as an authoritative deny. Every other non-`2xx` status — including other `4xx` (e.g. `400`, `401`, `404`, `429`) and any `5xx` — is treated as the endpoint being unable to give a verdict, so the gate fails closed with a `503`. This is deliberate: a misconfigured or malfunctioning enforce endpoint must never silently admit. If your endpoint signals "denied" with a status other than `403`, map it to `403` on its side.

On admit, each passing check contributes its role to the request's admission roles, consumed by authorization downstream.

- **Operator-defined body.** A check's `body` is arbitrary JSON. The only substitutions the gate makes are the request-derived placeholders `{{subject}}` (the token `sub`) and `{{idp_id}}` inside string values; everything else is sent literally. Unknown placeholders are rejected at startup. The gate models no "actions"/"resource" concepts — those are just whatever you write in the body.
- **IdP-scoped.** The gate only governs tokens from the configured `idp_id`; tokens from any other identity provider are admitted untouched.
- **Cached.** Decisions are cached in memory per `(subject, check)` for `cache_ttl_secs`. Both allow *and* deny are cached, so a denied-but-authenticated caller triggers at most one upstream call per TTL and cannot amplify load; transient `5xx`/timeout results are never cached.
- **Fail closed.** Anything other than `2xx`/`403` becomes a `503` with `Retry-After`.
- **Token relay is opt-in.** The caller's bearer token is forwarded only when `auth` is `forward_caller_token`; otherwise the endpoint is reached with the static `headers` only. A forwarded token goes over the configured URL (use TLS) and is never logged.

## Configuration

The gate is **disabled unless an `[admission_enforce]` block is present**. Like [Cedar derivations](./authorization-cedar.md) and [role providers](./configuration.md#role-provider), this is nested config, so it is configured via a TOML file with full environment-variable parity — point `LAKEKEEPER__ADMISSION_ENFORCE_FILE` at a TOML file, and/or set `LAKEKEEPER__ADMISSION_ENFORCE__*` variables on top.

### `[admission_enforce]`

| Key                            | Required | Default | Description                                                       |
| ------------------------------ | -------- | ------- | ----------------------------------------------------------------- |
| `endpoint`                     | yes      | —       | Enforce endpoint URL (`POST`). Validated at startup.              |
| `idp_id`                       | yes      | —       | Only govern tokens from this IdP; others are admitted untouched.  |
| `role_provider_id`             | yes      | —       | Provider namespace for the synthesized admission roles.           |
| `cache_ttl_secs`               | no       | `60`    | TTL for cached allow/deny decisions.                              |
| `cache_max_entries`            | no       | `10000` | Max cached decisions.                                             |
| `request_timeout_secs`         | no       | `5`     | Per-request timeout.                                              |
| `connect_timeout_secs`         | no       | `2`     | Connection timeout.                                               |
| `unavailable_retry_after_secs` | no       | `5`     | `Retry-After` returned on the fail-closed `503`.                  |
| `headers`                      | no       | `{}`    | Extra static headers sent on every call (e.g. a service API key). |
| `auth`                         | no       | _none_  | How to authenticate (see below). Omit to send no `Authorization`. |
| `checks`                       | yes      | —       | Named map of checks (at least one). See below.                   |

### `[admission_enforce.auth]`

Omit to forward no token. Currently one scheme — relay the caller's bearer token:

```toml
[admission_enforce.auth]
type = "forward_caller_token"   # sent as `Authorization: Bearer <caller token>`
```

When set, the request **must** carry a bearer token; the gate fails closed if it is absent.

### `[admission_enforce.checks.<name>]`

Each check is keyed by a name you choose (used in the cache key and logs). Use lowercase with `_`/`-` only, so env vars can express it.

| Key                | Required | Default          | Description                                                            |
| ------------------ | -------- | ---------------- | --------------------------------------------------------------------- |
| `kind`             | yes      | —                | `gating` (a `403` rejects the request) or `role_granting` (`403` withholds the role). |
| `body`             | yes      | —                | This check's complete request body (any JSON). `{{subject}}`/`{{idp_id}}` are substituted; everything else is literal. |
| `role_source_id`   | yes      | —                | Source id of the admission role granted when the check passes.        |
| `role_provider_id` | no       | gate-level value | Override the role provider namespace for this check.                  |

## Example

```toml
[admission_enforce]
endpoint         = "https://control-plane.internal/v1/authorize"
idp_id           = "oidc"
role_provider_id = "control-plane"
cache_ttl_secs   = 60

[admission_enforce.auth]
type = "forward_caller_token"

# A `403` here rejects the request outright.
[admission_enforce.checks.instance-access]
kind           = "gating"
role_source_id = "instance-access"
[admission_enforce.checks.instance-access.body]
subject      = "{{subject}}"
resource     = "102befc3-424d-479e-b1f7-bb47c1e1a1a2"
resourceType = "project"
actions      = ["workflows.instance.read"]

# A `403` here only withholds the role; the request still proceeds.
[admission_enforce.checks.workflow-editor]
kind           = "role_granting"
role_source_id = "workflow-editor"
[admission_enforce.checks.workflow-editor.body]
subject      = "{{subject}}"
resource     = "102befc3-424d-479e-b1f7-bb47c1e1a1a2"
resourceType = "project"
actions      = ["workflows.instance.update", "workflows.instance.delete"]
```

The body shape is entirely yours — a different enforce API (e.g. OPA-style `{"input": {...}}`) is just a different `body`, with no code change:

```toml
[admission_enforce.checks.can-read.body.input]
user   = "{{subject}}"
tenant = "acme"
verb   = "read"
```

### Configuring via environment variables

Every field has full env↔TOML parity. The example above is equivalent to:

```bash
LAKEKEEPER__ADMISSION_ENFORCE__ENDPOINT='https://control-plane.internal/v1/authorize'
LAKEKEEPER__ADMISSION_ENFORCE__IDP_ID='oidc'
LAKEKEEPER__ADMISSION_ENFORCE__ROLE_PROVIDER_ID='control-plane'
LAKEKEEPER__ADMISSION_ENFORCE__AUTH__TYPE='forward_caller_token'
LAKEKEEPER__ADMISSION_ENFORCE__CHECKS__INSTANCE_ACCESS__KIND='gating'
LAKEKEEPER__ADMISSION_ENFORCE__CHECKS__INSTANCE_ACCESS__ROLE_SOURCE_ID='instance-access'
LAKEKEEPER__ADMISSION_ENFORCE__CHECKS__INSTANCE_ACCESS__BODY__SUBJECT='{{subject}}'
LAKEKEEPER__ADMISSION_ENFORCE__CHECKS__INSTANCE_ACCESS__BODY__RESOURCETYPE='project'
LAKEKEEPER__ADMISSION_ENFORCE__CHECKS__INSTANCE_ACCESS__BODY__ACTIONS='[workflows.instance.read]'
```

!!! note "Arrays in a body via env vars"
    A literal JSON array in a body must use figment's inline form — `BODY__ACTIONS='[a,b]'`, not repeated keys. TOML files use ordinary `actions = ["a", "b"]`. This is the one place the two forms differ, so prefer a TOML file when bodies contain arrays.

As with [role providers](./configuration.md#role-provider), the two approaches combine: load non-sensitive config from the file and inject secrets (e.g. a service API key in `headers`) via env vars on top.
