# Authorization

## Overview

Authentication verifies *who* you are, while authorization determines *what* you can do.

Authorization can only be enabled if Authentication is enabled. Please check the [Authentication Docs](./authentication.md) for more information.

Lakekeeper currently supports the following Authorizers:

* **AllowAll**: A simple authorizer that allows all requests. This is mainly intended for development and testing purposes.
* **OpenFGA**: A fine-grained authorization system based on the CNCF project [OpenFGA](https://openfga.dev). OpenFGA requires an additional OpenFGA service to be deployed (this is included in our self-contained examples and our helm charts). See the [Authorization with OpenFGA](./authorization-openfga.md) guide for details.
* **Cedar**<span class="lkp"></span>: An enterprise-grade policy-based authorization system based on [Cedar](https://cedarpolicy.com). The Cedar authorizer is built into Lakekeeper and requires no additional external services. See the [Authorization with Cedar](./authorization-cedar.md) guide for details.
* **Custom**: Lakekeeper supports custom authorizers via the `Authorizer` trait.

Check the [Authorization Configuration](./configuration.md#authorization) for setup details.

Whichever authorizer is configured, direct permissions are managed through one API: see [Permissions & Grants](./grants.md) for granting, revoking, and reviewing access.

## Instance Admins

*Available since Lakekeeper 0.12.1.*

**Instance admins** are principals listed directly in Lakekeeper's static
configuration. They bypass the configured Authorizer for administrative
actions, so they can always manage the catalog — even if the Authorizer
itself is broken or misconfigured.

The typical instance admin is an automation account: a Kubernetes Operator
reconciling Lakekeeper resources, for example, or an infrastructure admin
responsible for operating the deployment. Without this mechanism, common
failure modes would lock everyone out — for instance, deleting the last
OpenFGA admin tuple, or deploying a Cedar policy that denies everything.

### Scope

Instance admins bypass authorization for **control-plane** operations:

- Bootstrap.
- Project, role, warehouse, namespace management.
- Table / view metadata operations, including `GetMetadata`, `Commit`,
  `Drop`, `Rename`, property changes.
- User management.

Instance admins do **not** bypass authorization for:

- **Data-plane operations** — `CatalogTableAction::ReadData`,
  `CatalogTableAction::WriteData`, and `CatalogViewAction::Select` still
  route through the configured Authorizer. If the instance admin does not
  hold the relevant grants, reads and writes of table row data (and
  execution of views via the referenced-by chain) are denied. In the default
  OpenFGA model `Select` and `GetMetadata` resolve to the same underlying
  grant, so ordinary users see no behavioural change — the two exist as
  distinct actions so that the bypass carve-out can exclude `Select`.
- **Role assumption** (`x-assume-role` header) — an instance admin must act
  with their own identity. Assuming a role opts into that role's narrower
  scope.
- **Handing out permissions** — writes through the
  [`/management/v1/.../grants`](./grants.md) API under every Authorizer, and
  through the endpoints the active Authorizer exposes itself (for example
  `/management/v1/permissions/...` under OpenFGA; Cedar exposes its own set).
  These always go through the Authorizer's own grant-check path, so an instance
  admin cannot directly make Alice a `project_admin`, and
  `grants/grantable-privileges` reports them as able to grant nothing — that
  being the truth. Ongoing permission administration stays with a principal
  that holds real grants in the configured Authorizer.

    *Reading* permissions is not restricted. A grant listing is a control-plane
    read like any other, so an instance admin can audit who holds what. The
    split is deliberate: disclosure to an operator who can already read every
    other record is not an escalation, whereas writing a grant is.

This split keeps a leaked operator credential from being trivially used
either to exfiltrate data or to escalate arbitrary principals to admin.

### Configuration

Set `LAKEKEEPER__INSTANCE_ADMINS` to a **TOML inline array** of user IDs. For
simple string arrays this is syntactically identical to a JSON array:

```yaml
# e.g. in a Kubernetes deployment's env block
env:
  - name: LAKEKEEPER__INSTANCE_ADMINS
    value: '["kubernetes~eb952f26-3a1a-4020-bcb4-3f7d43049284","oidc~alice"]'
```

Each entry is a Lakekeeper user ID of the form `<idp_id>~<subject>`. The
`idp_id` matches the identifier of a configured Authenticator (for example,
`kubernetes` or `oidc`). The `subject` is the resolved subject claim — for
Kubernetes ServiceAccount tokens that is the service account's `uid` (as
returned by the `TokenReview` API, e.g.
`eb952f26-3a1a-4020-bcb4-3f7d43049284`); for OIDC it is whatever the
configured subject claim produces.

A bare string (e.g. `oidc~alice`) is **rejected** — even a single admin must
be wrapped in brackets: `["oidc~alice"]`. The indexed-variable pattern that
some other config systems accept (`LAKEKEEPER__INSTANCE_ADMINS__0=...`) is
**not** supported.

### Operational notes

- **Not a recovery mechanism.** If OpenFGA is unreachable or the authn layer
  is misconfigured such that the instance admin's identity cannot be
  resolved, the bypass does not engage. Instance admins are for day-to-day
  operator access, not break-glass recovery.
- **Rotation.** The admin list is read once at process startup. Adding or
  removing an admin requires a redeploy. This is intentional: the mechanism
  is a deployment-config concern, not a runtime one.
- **Audit.** Authorization events include a `privilege_source` field
  indicating how the decision was reached: `"internal"` (in-process call),
  `"instance_admin"` (config-granted bypass), or `"authorizer"`
  (configured Authorizer backend decision). See the
  [Logging guide](./logging.md#audit-logs-and-rust_log) for the event
  schema.
- **Role-assumed requests.** Setting `x-assume-role` on a request from an
  instance admin drops the bypass for that request — the effective scope is
  whatever the assumed role holds.
- **Permission administration.** Because instance admins cannot write
  grants or permission assignments, day-to-day management of them is done by
  a human (or service) principal that holds real grants in the configured
  Authorizer — under OpenFGA, one bootstrapped through it. The operator use
  case is provisioning (creating projects/warehouses, initial bootstrap), not
  ongoing user administration. A fresh deployment gets its first grant-holder
  without one: bootstrap makes the bootstrapping principal a server admin (or
  operator), and creating a project makes its creator that project's admin.
