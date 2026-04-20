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

## Instance Admins

*Available since Lakekeeper 0.12.1.*

"Instance admins" are principals granted privileged access via deployment
configuration rather than through the selected Authorizer. They exist so that
deployment-time operators — typically a Kubernetes Operator reconciling
Lakekeeper resources — can always administer the catalog, even if users
misconfigure their authz backend (for example by deleting the last OpenFGA
admin tuple or deploying a restrictive Cedar policy that denies everything).

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
- **Permission-management endpoints** exposed by the active Authorizer
  (for example `/management/v1/permissions/...` under OpenFGA; Cedar
  exposes its own set) — the instance-admin bypass does **not** apply to
  these. Writes go through the Authorizer's own grant-check path, so an
  instance admin cannot directly make Alice a `project_admin`. Ongoing
  permission administration stays with a principal that holds real grants
  in the configured Authorizer.

This split keeps a leaked operator credential from being trivially used
either to exfiltrate data or to escalate arbitrary principals to admin.

### Configuration

Set `LAKEKEEPER__INSTANCE_ADMINS` to a **TOML inline array** of user IDs. For
simple string arrays this is syntactically identical to a JSON array:

```yaml
# e.g. in a Kubernetes deployment's env block
env:
  - name: LAKEKEEPER__INSTANCE_ADMINS
    value: '["kubernetes~system:serviceaccount:lakekeeper:operator","oidc~alice"]'
```

Each entry is a Lakekeeper user ID of the form `<idp_id>~<subject>`. The
`idp_id` matches the identifier of a configured Authenticator (for example,
`kubernetes` or `oidc`). The `subject` is the resolved subject claim — for
Kubernetes ServiceAccount tokens that is
`system:serviceaccount:<namespace>:<sa-name>`; for OIDC it is whatever the
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
- **Permission administration.** Because instance admins cannot write to
  the OpenFGA permission-management endpoints, day-to-day management of
  role grants and assignments is done by a human (or service) principal
  that was bootstrapped through OpenFGA. The operator use case is
  provisioning (creating projects/warehouses, initial bootstrap), not
  ongoing user administration.
