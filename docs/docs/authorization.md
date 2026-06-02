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

## Role assignments

Roles are granted to users through the role-assignments API:

- `POST /management/v1/role-assignments` — assign a role to a user. Idempotent;
  returns `200` with the assignment.
- `DELETE /management/v1/role-assignments/{user_id}/{role_id}` — revoke. Idempotent
  (revoking an absent assignment is a no-op); returns `204`.
- `GET /management/v1/role-assignments?userId=… | roleId=…` — list assignments.
  Exactly one of `userId` or `roleId` must be supplied; neither or both returns
  `400 RoleAssignmentFilterRequired`. Paginated via an opaque `pageToken`: page
  until the response has no `nextPageToken` — a page may be shorter than
  `pageSize` (or empty) yet still carry a token (under OpenFGA, project scoping
  is applied after each page read).

### Storage is authorizer-specific

The endpoint routes through the active Authorizer, which is the single source of
truth for assignments:

- Under **OpenFGA**, assignments are stored as relationship tuples. This API
  models only `(user, role)` **assignee** membership: an assign/revoke writes or
  removes exactly the same `assignee` tuple the legacy
  `/management/v1/permissions/role/{role_id}/assignments` endpoint would. It does
  **not** fully replace that endpoint, which exposes the complete `RoleAssignment`
  relation — including role **ownership** (and role-to-role assignees). Managing
  ownership still requires the legacy endpoint.
- Under **Cedar** and **AllowAll**, assignments are stored as rows in the
  `role_assignment` Postgres table.

There is **no dual-write**: under OpenFGA the `role_assignment` table is not
written by this endpoint.

### Only catalog-managed roles are manually assignable

Only roles whose provider is `lakekeeper` (the default) or `system` may be
assigned or revoked here. A role owned by an external role provider (LDAP/OIDC
sync today, SCIM in future) is rejected with `409 RoleNotManuallyAssignable` —
its assignments are owned by the provider and would be overwritten on the next
sync.

### The user must already exist

Assigning to a `user_id` with no user record returns `404 RoleAssignmentUserNotFound`.
This endpoint does not create users — provision them first via login/JIT,
role-provider sync, or the user API. (Revoke does not perform this check.)

### Authorization

Writes require the `manage_role_assignments` action on the role. List requires
`read_role_assignments` — on the role when filtering by `role_id`, or on the user
when filtering by `user_id`. `INSTANCE_ADMINS` bypass these checks (used by
control-plane provisioning).

### Cross-authorizer caveats

"Which roles does a user have" must be queried through the active Authorizer;
there is no canonical cross-authorizer read. Switching Authorizers
(OpenFGA ↔ Cedar) is not runtime-portable — existing assignments live in the
previous Authorizer's store and would require a separate one-time migration tool
(out of scope).

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
