---
description: "Policy-as-code authorization for Lakekeeper Plus with Cedar: declarative policies with attribute conditions, evaluated without an external service."
---

# Authorization with Cedar { #authorization-with-cedar .lkp }

!!! important "Using the Correct Cedar Schema Version"
    Always use the Cedar schema version that exactly matches your Lakekeeper deployment when developing policies. Schema mismatches can cause policy validation failures or unexpected authorization behavior. Download the schema from the Lakekeeper UI (Lakekeeper Plus 0.11.2+) or retrieve it via the `/management/v1/permissions/cedar/schema` endpoint.

<a href="../api/lakekeeper.cedarschema" download class="md-button md-button--primary">
  :material-download: Download Cedar Schema
</a>

[Cedar](https://docs.cedarpolicy.com/) is an enterprise-grade, policy-based authorization system built into Lakekeeper that requires no external services. Cedar uses a declarative policy language to define access controls, making it ideal for organizations that prefer infrastructure-as-code approaches to authorization management.

Check the [Authorization Configuration](./configuration.md#authorization) for configuration options.

!!! note "Policies decide, and grants feed them"
    Cedar decides from policies. Lakekeeper Plus also keeps [grants](./grants.md), handed out at runtime through the Grants API. Its predefined policies turn grants into access inside projects; server actions need the five server-grant `permit`s from the schema. Your own policies can read grants as `resource.principal_privileges`. Switch the predefined policies off with `LAKEKEEPER__CEDAR__PREDEFINED_POLICIES_ENABLED=false`. Initial access comes from your policy source, or from [Instance Admins](./instance-admins.md).

## How it Works

Lakekeeper uses the built-in Cedar Authorizer to evaluate whether a request is allowed. Each Cedar authorization request consists of three components:

1. **Principal**: The entity performing the request. Example: `Lakekeeper::User::"oidc~peter"` ("oidc~" prefix indicates users from the OIDC identity provider)
1. **Action**: The operation being performed. Example: `Lakekeeper::Action::"CommitTable"`
1. **Resource**: The target of the action. Example: `transactions` table in namespace `finance` (`Lakekeeper::Table::<warehouse-id>/<table-id>`)

To evaluate authorization requests, Cedar requires the following information:

1. **Policies**: Define which principals can perform which actions on which resources. Policies are provided via files (`LAKEKEEPER__CEDAR__POLICY_SOURCES__LOCAL_FILES`) or Kubernetes ConfigMaps (`LAKEKEEPER__CEDAR__POLICY_SOURCES__K8S_CM`). See [Policy Examples](#policy-examples) below.
1. **Entities**: Application data Cedar uses to make authorization decisions, such as tables (including name, ID, warehouse, namespace, properties, etc.). Lakekeeper automatically provides all required entities (Tables, Generic Tables, Namespaces, Warehouses, etc.) for each decision. The user's roles are included too: roles from the token (when `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured), from configured role providers such as LDAP, and [roles managed in Lakekeeper](#roles-managed-in-lakekeeper). You can also provide users and roles yourself—see [External Entity Management](#external-entity-management).
1. **Context**: Transient request-specific data related to an action. For example, the `table_properties_updates` field is available when checking `Lakekeeper::Action::"CommitTable"`. Context is handled internally by Lakekeeper and requires no configuration.
1. **Schema**: Defines entity types recognized by the application. Lakekeeper uses a built-in schema (downloadable above) that can be customized via `LAKEKEEPER__CEDAR__SCHEMA_*` environment variables. We recommend schema customization only for advanced use cases.

Most deployments only need to configure `LAKEKEEPER__CEDAR__POLICY_SOURCES__*` and optionally `LAKEKEEPER__OPENID_ROLES_CLAIM` if role information is available in user tokens.

Generic (non-Iceberg) tables are a first-class resource in Cedar too: they have their own `Lakekeeper::GenericTable` entity and a parallel set of action groups — `GenericTableActions`, `GenericTableDescribeActions`, `GenericTableSelectActions` and `GenericTableModifyActions` — that mirror the regular `Table` actions. Use them in policies exactly as you would the `Table` equivalents.

## RBAC and ABAC Support

Cedar supports both Role-Based Access Control (RBAC) and Attribute-Based Access Control (ABAC). RBAC grants permissions based on `Lakekeeper::Role` entities, while ABAC uses resource attributes — such as Table, View, and Namespace properties, or the [governance tags](#tag-based-access-control) in effect on an object — for authorization decisions. See the ABAC examples in [Policy Examples](#policy-examples) below for more information.

## Role Matching with `project_roles`

Every `Lakekeeper::User` entity carries a `project_roles` attribute — a flat set of records holding the user's role memberships. At actions inside a project these are the user's roles in that object's project. At server actions they are the user's groups (see [Role scope at server actions](#role-scope-at-server-actions)):

```
principal.project_roles  →  Set<{provider_id: String, source_id: String}>
```

Lakekeeper fills this set for you: roles from the token (when `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured), from configured role providers, from [admission gates](./admission.md), and [roles managed in Lakekeeper](#roles-managed-in-lakekeeper), including every role they are nested in. At server actions it holds only the user's groups. In external entity mode (`EXTERNALLY_MANAGED_USER_AND_ROLES=true`) you fill it yourself in the entity JSON file.

The `Lakekeeper::User` entity also carries `provider_id` and `source_id` attributes identifying the user's own authentication provider and their ID within it:

| Attribute                      | Example value                                  | Description |
|--------------------------------|------------------------------------------------|-----|
| `provider_id`                  | `"oidc"`                                       | Authentication provider of the user |
| `source_id`                    | `"2f268e8b-8cc1-4edd-a9df-87d69f7e9deb"`       | User's ID within the provider |
| `project_roles`   | `[{provider_id: "oidc", source_id: "admins"}]` | Role memberships as `{provider_id, source_id}` records: roles from token claims, role providers (e.g. LDAP), admission gates and roles managed in Lakekeeper, resolved in the object's project. At server actions: the user's groups. |
| `global_role_ids` | `["admins", "developers"]`                     | The `source_id` of each of the user's groups, as a plain `Set<String>`. Lakekeeper roles are not in it. Only populated when `LAKEKEEPER__CEDAR__GLOBAL_ROLE_IDS_ENABLED=true`. See below. |

The `Lakekeeper::User` entity also exposes an optional `email` attribute extracted from the authentication token. Email uniqueness is not enforced — two distinct users may share an email.

### When to use `project_roles` vs `global_role_ids` vs `principal in Role::...`

| Scenario                                                         | Recommended approach |
|------------------------------------------------------------------|-----------|
| Roles come from OIDC/token claims or a role provider (e.g. LDAP) | `principal.project_roles.contains({provider_id: "oidc", source_id: "my-group"})` |
| Directory group names are unique across all your providers       | `principal.global_role_ids.contains("my-group")` *(requires `GLOBAL_ROLE_IDS_ENABLED`)* |
| Roles are managed in Lakekeeper (via the management API)         | At actions inside a project, `principal.project_roles.contains({provider_id: "lakekeeper", source_id: "analysts"})`, or `principal in Lakekeeper::Role::"<project-id>/lakekeeper~analysts"` for one project's role. At server actions a Lakekeeper role does not count: name a group, or grant the users. See [Roles managed in Lakekeeper](#roles-managed-in-lakekeeper) |
| Roles come from an external entities file                        | Either approach works; `project_roles` is simpler |

`project_roles` matches by provider and role name alone, with no project ID. `principal in Lakekeeper::Role::...` needs the project ID, which is inconvenient to embed in policy files. For groups, `project_roles` is also the form that works at every action, server actions included.

`global_role_ids` holds the names of your directory groups: the roles your identity and role providers assign (token claims, LDAP, Entra ID, Okta), without the provider prefix. It simplifies policies when those names are unique across your providers (e.g. a single LDAP server or OIDC provider). Roles managed in Lakekeeper belong to one project and are named by whoever creates them, so they are not included: creating a role can never make someone match a `global_role_ids` check. Enable it with `LAKEKEEPER__CEDAR__GLOBAL_ROLE_IDS_ENABLED=true`; when disabled the attribute is always an empty set.

### Roles managed in Lakekeeper

Roles you create through the management API (`POST /management/v1/role`) belong to the `lakekeeper` provider. Their `source_id` is the `source-id` you give when creating the role, or the role's own id if you give none. Under Cedar the API creates and rebinds only `lakekeeper` roles, so a role the API creates can never pass for a directory group. Assign users to them, and nest roles inside other roles, with `POST /management/v1/role/{role_id}/members`. Whoever joins a role holds its grants, so changing a role's members is granting: the predefined policies let `manage_grants` on the role's project create roles, add and remove members, rename and delete roles, while `describe` is enough to read them. With `LAKEKEEPER__CEDAR__EXTERNALLY_MANAGED_USER_AND_ROLES=true` the API creates no roles and answers `400 CreateRolesNotSupported`: declare every role in your entities file instead — see [External Entity Management](#external-entity-management).

At actions inside a project, a user holds every role they are assigned to and every role those are nested in, at any depth, so both ways of naming a role match its indirect members too. At server actions neither matches a Lakekeeper role; see [Role scope at server actions](#role-scope-at-server-actions).

```cedar
// The `analysts` role of the object's project (never at server actions).
principal.project_roles.contains({provider_id: "lakekeeper", source_id: "analysts"})

// The `analysts` role of one specific project.
principal in Lakekeeper::Role::"<project-id>/lakekeeper~analysts"
```

Things to know:

- Name a role by its `source_id`. The role's display name is not available to Cedar, and a role created with a `source-id` of its own cannot be named by its id.
- A `source_id` names a role only together with its `provider_id`: `analysts` in `lakekeeper` and `analysts` in `ldap` are different roles. Wherever a policy reads a `source_id` — `resource.source_id` on a role action, or `context.requested_source_id` — check the matching `provider_id` too.
- At actions inside a project, `project_roles` names a role of the project the request is decided in. A request about a resource is decided in that resource's project: the one `x-project-id` names or, for a catalog request without it, the project of the warehouse it addresses.
- Changing a role's `source_id` through the source-system endpoint changes its name in Cedar: policies naming the old `source_id` stop matching it.
- `global_role_ids` does not include these roles, and resource property tags (`role:` / `role-full:`) cannot reference them.
- When a user acts as a role with `x-assume-role`, the principal is that role. `principal in Lakekeeper::Role::"…"` still matches the roles it is nested in. A role has no `project_roles`: guard that read with `principal is Lakekeeper::User`, which limits the policy to users. At server actions, a request under `x-assume-role` is denied: send it without the header to act as yourself.

### Role scope at server actions

A user holds two kinds of role:

- **Groups** come from a directory (LDAP, Entra ID, Okta), from token claims, or from an [admission gate](./admission.md).
- **Lakekeeper roles** are roles managed in Lakekeeper and `system` roles.

What a user carries depends on the action:

- **Actions inside a project** (project, warehouse and below): every role the user holds in that object's project, groups and Lakekeeper roles, with every role those are nested in.
- **Server actions** (every action on `Lakekeeper::Server`, user management included): the user's groups, in `project_roles` and `global_role_ids`. They are the same for every `x-project-id`. `roles` is empty; `principal in Role::"..."` and `request_project` work only at actions inside a project.

A Lakekeeper role does not count at server actions: name a group, or give the users [server grants](#server-grants). Act as yourself: a request with `x-assume-role` is denied at server actions. When Lakekeeper loads or reloads its policies, the server-action check refuses a policy that would stop fewer users at a server action, such as a `forbid` that names a `Role` id, and the log shows the fix. A `permit` that requires a Lakekeeper role loads with a warning. The check does not run when users and roles are externally managed.

#### Name groups with the flat form

The flat form works at every action. A `Lakekeeper::Role::"<project-id>/<provider>~<source-id>"` id names a group in one project and matches no user at server actions.

```cedar
permit (
  principal is Lakekeeper::User,
  action in [Lakekeeper::Action::"ListUsers", Lakekeeper::Action::"UpdateUsers", Lakekeeper::Action::"DeleteUsers"],
  resource is Lakekeeper::Server
)
when { principal.project_roles.contains({provider_id: "ldap", source_id: "user-admins"}) };
```

#### Server grants

Grant a user a privilege on the server ([`/management/v1/server/grants`](./grants.md#where-you-can-grant)), and load these five permits, one per privilege. No predefined policy decides server actions, so without them a server grant does nothing at the server itself. Inside projects the [predefined policies](#predefined-policies) already count it:

```cedar
permit (principal, action in Lakekeeper::Action::"ServerDescribeActions", resource is Lakekeeper::Server)
when { resource.principal_privileges.direct.describe };

permit (principal, action in Lakekeeper::Action::"ServerCreateActions", resource is Lakekeeper::Server)
when { resource.principal_privileges.direct.create };

permit (principal, action in Lakekeeper::Action::"ServerModifyActions", resource is Lakekeeper::Server)
when { resource.principal_privileges.direct.manage };

permit (principal, action in Lakekeeper::Action::"ServerGrantActions", resource is Lakekeeper::Server)
when { resource.principal_privileges.direct.manage_grants };

permit (principal, action == Lakekeeper::Action::"ReadServerGrants", resource is Lakekeeper::Server)
when { resource.principal_privileges.direct.read_grants };
```

Server grants go to users only. A role belongs to a project, so a role holding a server grant would hand server-wide authority to whoever manages that project's role members. To give a team server access, name its group with the flat form above. If nobody can reach server administration yet, an identity from [`LAKEKEEPER__INSTANCE_ADMINS`](./instance-admins.md) can set the first grant.

#### Forbid a group

A `forbid` on a group with an unconstrained `action` holds at every action, server actions included. It also stops its members from assuming a role:

```cedar
forbid (principal is Lakekeeper::User, action, resource)
when { principal.project_roles.contains({provider_id: "ldap", source_id: "contractors"}) };
```

Name `AssumeRole` too in a `forbid` on fewer actions.

A decision about another user — a grant's grantee, or the subject of a permission check — sees that user's directory groups. With [persisted token roles](./configuration.md#token-role-provider) it also sees the token groups last stored for them. Admission groups come only with the user's own requests. To decide about users who are not signed in, use a role provider (LDAP, Entra ID, Okta).

#### Fix a `forbid` that names a group by its `Role` id

Split it in two: keep the original for actions inside projects, and add a copy for server actions that uses the flat form. A `Role` id in the scope moves into the `when`:

```cedar
forbid (principal in Lakekeeper::Role::"my-project/ldap~contractors", action, resource)
when { !(resource is Lakekeeper::Server) };

forbid (principal, action, resource is Lakekeeper::Server)
when { (principal is Lakekeeper::User && principal.project_roles.contains({provider_id: "ldap", source_id: "contractors"})) };
```

Split a `permit` whose exception names the group the same way: the copy keeps the exception, the actions and the other conditions. If the group's provider is no longer configured, the flat form matches nobody; name a group the members still hold.

To keep any policy away from server actions, add `!(resource is Lakekeeper::Server)` to its `when`. Use this condition, not a `principal has request_project` test: the server-action check refuses a `forbid` that reaches server actions with that test.

#### Act as yourself at server actions

A request with `x-assume-role` is denied every server action, except on the caller's own user record. A permission check about a role is denied there too, and `/management/v1/permissions/cedar/resolve-entities` answers `400` when asked about a role at the server. Every user can update and delete their own user record: Lakekeeper allows that before any policy is evaluated.

!!! tip "The server-action check"
    When Lakekeeper loads or reloads its policies, the server-action check reads each policy that can decide a server action. It refuses a `forbid`, or a `permit`'s exception, that names something no user has at server actions, such as a `Role` id, a Lakekeeper role, `roles` or `request_project`, because it would stop fewer users there, or nobody. The log names the policy and shows the fix. For a policy that decides only server actions, the fix can replace a test of a user's group by `Role` id in place with the group's flat form. For a policy that also decides actions inside projects, the fix adds `!(resource is Lakekeeper::Server)` to its `when` to keep it for actions inside projects, and a policy for server actions that names a group with the flat form or reads a server grant. A `permit` that requires such a name, or a `forbid` exception that names one, still loads, with a warning. For a group's `Role` id, the warning comes only when the policy decides no action inside a project. With externally managed users and roles, the check does not run. A refused reload keeps the policies already loaded; see [Policy and Entity Management](#policy-and-entity-management).

For externally managed users and roles, see [External Entity Management](#external-entity-management).

The project list is decided per project: each project is decided with your roles in that project, as a request naming it with `x-project-id` would be, and `IncludeProjectInList` decides whether it shows up.

### Policy example

```cedar
// Grant namespace/table/view access to users whose token contains the
// "warehouse-1-admins" group from the OIDC provider.
permit (
    principal is Lakekeeper::User,
    action in
        [Lakekeeper::Action::"NamespaceActions",
         Lakekeeper::Action::"TableActions",
         Lakekeeper::Action::"ViewActions"],
    resource
)
when {
    resource.warehouse.name == "wh-1" &&
    principal.project_roles.contains(
        {provider_id: "oidc", source_id: "warehouse-1-admins"}
    )
};
```

!!! tip "Monitoring role providers"
    Role provider availability is tracked via Prometheus metrics (`lakekeeper_role_provider_up`, `lakekeeper_role_provider_get_roles_duration_seconds`), emitted per `provider_id` for providers with an external backend such as LDAP. The built-in OIDC token provider does no external lookup, so it reports neither — with `persist_token_roles` it surfaces only through `lakekeeper_role_provider_sync_errors_total` on a failed catalog write. Lakekeeper deliberately excludes role provider health from the pod liveness probe — an unreachable provider causes graceful fallback to cached roles from Postgres rather than a pod restart. See [Monitoring — Role Provider Metrics](./monitoring.md#role-provider-metrics) for details and alerting guidance.

!!! tip "Debugging role assignments"
    To see which roles are resolved for each user, temporarily set `LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_ROLE_ASSIGNMENTS=true`. This emits an audit event listing every resolved role name after each request. The event is noisy and contains PII — disable it after debugging. See [Logging — Operational Audit Events](./logging.md) for the event schema and example output.

### Policy example — `global_role_ids`

Use this simpler form when all your role providers are server-wide and use unique group names (e.g. a single LDAP directory). Requires `LAKEKEEPER__CEDAR__GLOBAL_ROLE_IDS_ENABLED=true`.

```cedar
// Grant access to users who are members of the "data-engineers" group,
// regardless of which provider that group came from.
permit (
    principal is Lakekeeper::User,
    action in
        [Lakekeeper::Action::"NamespaceActions",
         Lakekeeper::Action::"TableActions",
         Lakekeeper::Action::"ViewActions"],
    resource
)
when {
    resource.warehouse.name == "my-warehouse" &&
    principal.global_role_ids.contains("data-engineers")
};
```

### Property-based `global_role_ids` matching

When `GLOBAL_ROLE_IDS_ENABLED` is set, both `User` and `ResourcePropertyValue` expose `global_role_ids` as plain `Set<String>`. This enables provider-agnostic property-based access control — no need to align provider prefixes between the user's roles and the property tag references:

```cedar
// Grant read access when the user shares any role with the table's access_read tag.
// Works regardless of whether roles come from OIDC, LDAP, or any other provider.
permit (
    principal is Lakekeeper::User,
    action in [Lakekeeper::Action::"TableSelectActions"],
    resource is Lakekeeper::Table
)
when {
    resource.properties.hasTag("access_read") &&
    resource.properties.getTag("access_read").global_role_ids.containsAny(principal.global_role_ids)
};
```

## Predefined policies

The predefined policies turn [grants](./grants.md) into access inside projects. No predefined policy decides server actions: a server grant reaches the projects beneath it, and the server itself needs the [server grants](#server-grants) permits. The predefined policies are on by default. `LAKEKEEPER__CEDAR__PREDEFINED_POLICIES_ENABLED=false` switches all of them off. A `forbid` of yours overrides them like any other `permit`.

A project or a warehouse can switch single predefined policies on or off for itself (`ToggleProjectPredefinedPolicy` and `ToggleWarehousePredefinedPolicy` under [Cedar Policy Actions](#cedar-policy-actions)). A policy is in force for a warehouse only when it is on for both the warehouse and its project. Policies for projects, tags and roles are switched per project only. Once a project or warehouse has switched any policy, a policy added in a later release starts out off there.

`GET /management/v1/permissions/cedar/project/predefined-policies` and `GET /management/v1/permissions/cedar/warehouse/{warehouse_id}/predefined-policies` list each policy's id, its description and whether it is on. Ids look like `predefined-grants-table-select`, and the description says in one sentence what the policy allows.

### What each privilege allows

A grant on an object reaches everything beneath it: server, project, warehouse, namespace, then tables, views and generic tables. A project grant also reaches the project's tags. Policies read this as `principal_privileges.inherited`. Grants reach downward only: a grant on a table does not make its namespace or warehouse visible. A stronger privilege includes the weaker ones on the same ladder: `describe` < `select` < `write` < `manage` on tables, views and generic tables, `describe` < `create` < `manage` on containers, and `describe` < `apply` < `manage` on tags (see [How action groups are nested](#how-action-groups-are-nested)).

| Privilege | Granted on | Reaches children | What it allows |
|---|---|---|---|
| `describe` | Every level | Yes | `<Level>DescribeActions`: see the object and its metadata, and list what is in it. On a tag, `ReadTag`. |
| `select` | Server down to table, view, generic table | Yes | `TableSelectActions`, `ViewSelectActions`, `GenericTableSelectActions`: read data and metadata. |
| `write` | Server down to table, view, generic table | Yes | `TableWriteActions`, `ViewWriteActions`, `GenericTableWriteActions`: write and commit data, and read it. |
| `create` | Server, project, warehouse, namespace | Yes | `<Level>CreateActions`: create warehouses, namespaces, tables, views and generic tables inside the object, and everything `describe` allows there. |
| `manage` | Every level | Yes | `<Level>ModifyActions`: full control of the object, including deleting it, and everything the weaker privileges allow. This covers tagging the object, `CreateTag` on a project, and `TagModifyActions` on a tag. |
| `manage_tags` | Server down to table, view, generic table | Yes | `Manage<Level>Tags` and `<Level>DescribeActions` on warehouses, namespaces, tables (column tags too), views and generic tables. No data access. |
| `apply` | Tag | No | `TagApplyActions`: attach the tag, with any of its values, and remove it, and read the tag. |
| `read_grants` | Every level | Yes | `Read<Level>Grants`: see who holds which privilege on the object. On a project, warehouse or namespace also the subtree listing. |
| `pass_grants` | Every level | No | Grant others a privilege the holder holds on the object, and `Read<Level>Grants` on it. |
| `manage_grants` | Every level | Yes | `<Level>GrantActions`: grant and revoke any privilege on the object without holding it, see who holds which privilege on it, and check what another user or role may do there. On a warehouse or namespace also the subtree listing and the subtree revoke. On a project also the subtree listing and the project's roles. |
| `read_policies` | Project, warehouse | Project grant reaches its warehouses | List and read the scope's stored Cedar policies and which predefined policies are switched on there. |
| `manage_policies` | Project, warehouse | Project grant reaches its warehouses | `ProjectCedarPolicyActions` or `WarehouseCedarPolicyActions`: read and write the scope's Cedar policies, and switch and reset its predefined policies. |

Notes:

- **`select` and `write` on a container** act on the tables, views and generic tables beneath it, not on the container. Grant `describe` on the container too, so the holder can reach and list what is in it.
- **`manage` does not administer grants or policies.** Grant `read_grants`, `pass_grants`, `manage_grants`, `read_policies` or `manage_policies` for those.
- **`manage_tags`** tags objects without access to their data. On a server or project it acts only on what is beneath; grant `describe` there too to list warehouses and tags. Attaching or removing a tag also needs `apply` or `manage` on the tag. Policies may read tags to decide access, so treat tagging as a governance right.
- **`apply`** counts only on the tag it is granted on. Attaching the tag also needs `manage_tags` or `manage` on the object.
- **`read_grants`** reads grants and nothing else: no check of what another user or role may do, no grant, no revoke. On a project the subtree listing is everything one user or role holds in it; on a warehouse or namespace it is every grant at and under it.
- **`pass_grants`** counts only on the object it is granted on, and works in the grant direction only: it never revokes. It passes on `describe`, `select`, `write` and `create` (project, warehouse, namespace), `describe`, `select` and `write` (table, view, generic table), or `describe` and `apply` (tag), each only when the holder holds that privilege on the object, directly or from above (`apply` directly). It never passes on `manage`, `manage_tags` or a grant or policy privilege, and gives no check of what others may do. On the server it does nothing without a policy of your own.
- **`manage_grants`** is the only privilege that lets the holder check what another user or role may do (`Introspect<Level>Authorization`). That check runs every policy, including ones the holder cannot read.
- **`read_policies` and `manage_policies`** on a project count for the project itself and reach its warehouses. On a warehouse both also allow `UseWarehouse`.
- **Reading grants** shows that the object exists, even where a policy hides it otherwise. A subtree listing or revoke is decided once, on the project, warehouse or namespace it names, so a `forbid` on the grants of something inside does not shorten it. A subtree revoke removes the holder's own grants too; see [Clearing a subtree](./grants.md#clearing-a-subtree).

### Roles and the project

A privilege cannot be granted on a role. The role policies read the grants on the role's project, including those from the server above it.

- `describe`, `create`, `manage` or `manage_grants` on the project lets the holder read every role in it, with its members, and see which roles a user holds there (`ReadUserRoleAssignments`).
- `manage_grants` on the project also lets the holder create roles (`CreateRole`), list and search them, and check what another user or role may do on a role (`IntrospectRoleAuthorization`). On [roles managed in Lakekeeper](#roles-managed-in-lakekeeper) it adds and removes members, renames and deletes them. A role's members hold its grants, so these are grant administration.
- `manage_grants` on the project also deletes roles from identity and role providers, for example one whose group is gone. If the provider still reports the group, the role is created again at a member's next request, without the deleted role's grants. This does not cover `system` roles.
- No predefined policy allows `AssumeRole` or `UpdateRoleSourceSystem`.

## Property-Based Access Control

Lakekeeper can parse roles and users directly from Table, Namespace, and View properties. This enables a powerful ABAC pattern where access control lists are stored as resource metadata, and Cedar policies grant access based on those lists — without maintaining a separate role-assignment file.

### How Properties Are Exposed to Cedar

Every Table, Namespace, and View entity carries a `properties` attribute of type `ResourceProperties`. This is a Cedar entity with typed tags — one per property key — each holding a `ResourcePropertyValue` record:

```
type ResourcePropertyValue = {
    raw:            String,        // original value as stored
    roles:          Set<Role>,     // parsed Lakekeeper::Role entity references
    users:          Set<User>,     // parsed Lakekeeper::User entity references
    global_role_ids: Set<String>,  // source_id of each parsed role (requires GLOBAL_ROLE_IDS_ENABLED)
}
```

Properties are ordinary Iceberg table/namespace properties — you set them with the same tools you already use. For example, using Spark SQL:

```sql
-- Set access-control properties when creating a table
CREATE TABLE my_catalog.finance.transactions (
    id     BIGINT,
    amount DOUBLE,
    ts     TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'access-owners'  = '["role-full:oidc~data-admins", "user:oidc~alice@example.com"]',
    'access-readers' = '["role:analysts"]'
);

-- Or add/update them on an existing table
ALTER TABLE my_catalog.finance.transactions
SET TBLPROPERTIES (
    'access-readers' = '["role:analysts", "role-full:oidc~reporting-team"]'
);

-- Namespace properties work the same way
ALTER NAMESPACE my_catalog.finance
SET PROPERTIES (
    'access-readers' = '["role-full:oidc~finance-readers"]'
);
```

Keys that start with a configured parse prefix (default: `access-`, `access_`) are automatically parsed into `roles` and `users` sets. All other keys (e.g. `write.metadata.metrics.default-mode`) pass through as plain strings in `.raw` with empty `roles` and `users`.

In a Cedar policy, properties are accessed using Cedar's tag syntax:

```cedar
// Check if a property key exists
resource.properties.hasTag("access-owners")

// Read the raw string value
resource.properties.getTag("access-owners").raw

// Check whether the requesting principal is in the allowed roles
principal in resource.properties.getTag("access-owners").roles

// Check whether the requesting principal is explicitly listed as an allowed user
principal in resource.properties.getTag("access-owners").users

// Check either roles or users
principal in resource.properties.getTag("access-owners").roles ||
principal in resource.properties.getTag("access-owners").users
```

The `principal in <set-of-roles>` check leverages Cedar's entity hierarchy: a user is considered `in` a role if that role appears anywhere in the user's ancestry chain (as established by token claims, role providers, roles managed in Lakekeeper or external entity definitions).

### Access-Control Property Keys

Properties whose key starts with one of the configured **parse prefixes** are treated as **access-control properties**. The default prefixes are `access-` and `access_`; they can be changed or disabled entirely with `LAKEKEEPER__CEDAR__PROPERTY_PARSE_PREFIXES` (see [Configuration](#configuration) below).

Access-control property values must be a JSON array of typed entity references:

| Format                                          | Description                |
|-------------------------------------------------|----------------------------|
| `role:<source-id>`                              | Short form — uses the default role provider. |
| `role-full:<provider>~<source-id>`              | Full form — provider name is explicit. Works with any configured role or identity provider. |
| `role-full:<project-id>/<provider>~<source-id>` | Full form with an explicit project scope. Useful in multi-project setups when referencing a role from a different project. |
| `user:<user-id>`                                | References a specific user by their identity-provider ID (e.g. `user:oidc~alice@example.com`). |

The default provider for the `role:` short form is determined as follows: if a role provider (e.g. LDAP) is configured, its provider ID is used; otherwise, if exactly one identity provider (e.g. OIDC) is registered, it becomes the default. When there are multiple providers and no single default can be determined, you must use the `role-full:` form.

The entire property value is a **JSON-encoded string** containing an array of these references. For example:

```
'["role:analysts", "role-full:oidc~data-admins", "user:oidc~alice@example.com"]'
```

A property with a single entry is still a JSON array, and an empty array (`'[]'`) is valid — it effectively grants access to nobody via that property.

### Configuration

| Environment variable                                      | Default                  | Description |
|-----------------------------------------------------------|--------------------------|-----|
| `LAKEKEEPER__CEDAR__PROPERTY_PARSE_PREFIXES` | `["access_", "access-"]` | List of property key prefixes that trigger entity-reference parsing. Set to `[]` to disable parsing entirely. |

### Error Handling

| Path                                                         | Behavior      |
|--------------------------------------------------------------|---------------|
| **Read** (AuthZ checks for read/describe operations)         | Parse errors in access-prefixed properties are logged as warnings. The property is still visible in Cedar with `raw` set to the original value and empty `roles`/`users` sets. Authorization is not blocked. |
| **Write** (AuthZ checks for create/update/commit operations) | Parse errors in access-prefixed properties cause the request to be **rejected with HTTP 400**. This prevents malformed access-control data from ever being stored. |

!!! tip
    Because malformed access-control values are rejected on write, you can rely on the `roles`/`users` sets being accurate and complete during read-path authorization.

## Tag-Based Access Control

[Governance tags](./tags.md) on warehouses, namespaces, tables, views and generic tables are visible to Cedar policies, so access can follow classification. For example, keep `pii` data from everyone outside a compliance role, or open a namespace to readers once it is tagged `published`.

### How Tags Are Exposed to Cedar

Every Warehouse, Namespace, Table, View and GenericTable entity carries a `lowercase_tags` attribute of type `ResourceTags`. This is a Cedar entity with one tag per governance tag in effect on the object, each holding a `TagValue` record:

```cedar
type TagValue = {
    value:     String,       // the value in effect, as applied; "" for a marker tag
    values:    Set<String>,  // every value this tag has on the object or above it
    inherited: Bool,         // whether `value` comes from a namespace or warehouse above
}
```

The tags are the object's [effective tags](./tags.md#effective-inherited-tags), the same set `?effective=true` returns: its own tags plus those inherited from the namespaces and the warehouse above it. When one tag is applied at several levels, the nearest one wins. Column tags are not included.

Keys are the tag's name in lower case, so `hasTag("pii")` also matches a tag named `PII`. Tag names are unique per project ignoring case, so a rename that only changes case keeps matching. Values keep their case.

```cedar
// The object, or anything above it, is tagged pii
resource.lowercase_tags.hasTag("pii")

// The value in effect
resource.lowercase_tags.hasTag("sensitivity") &&
resource.lowercase_tags.getTag("sensitivity").value == "restricted"

// Applied to this object itself, not inherited
resource.lowercase_tags.hasTag("sensitivity") &&
!resource.lowercase_tags.getTag("sensitivity").inherited
```

Guard every `getTag` with `hasTag`: any key may be missing, and Cedar rejects a policy that reads a tag without checking for it first.

### Rules a Lower Level Cannot Weaken

The nearest value wins, so a tag applied to a table replaces the value the table would inherit. If namespace `finance` is tagged `sensitivity=restricted` and a table inside it `sensitivity=public`, the table's `value` is `public`. Anyone allowed to tag that table could lift a restriction set on the namespace.

When a rule must hold whatever is applied further down, check `values` instead. It keeps every value on the way down, and a lower level cannot remove one:

```cedar
// Restricted data, wherever the restriction was applied, only for the compliance group.
forbid (
    principal,
    action in Lakekeeper::Action::"TableSelectActions",
    resource is Lakekeeper::Table
)
when {
    resource.lowercase_tags.hasTag("sensitivity") &&
    resource.lowercase_tags.getTag("sensitivity").values.contains("restricted")
}
unless {
    principal is Lakekeeper::User &&
    principal.project_roles.contains({provider_id: "oidc", source_id: "compliance"})
};
```

## User Identity Derivations

User derivations let you extract parts of a user's identity (`source_id` or `provider_id`) using regex named capture groups, and expose them as Cedar tags on a `UserDerivedAttributes` sub-entity. This enables policies that match users to resources based on identity patterns — for example, granting a user full access to namespaces that match their username.

### How It Works

Each derivation rule specifies:

- **`source`**: which identity field to match against — `source_id` (the user's subject in the IdP) or `provider_id` (e.g. `oidc`, `kubernetes`)
- **`pattern`**: a regex with [named capture groups](https://docs.rs/regex/latest/regex/#grouping-and-flags) (`(?<name>...)`)
- **`transform`** *(optional)*: a transformation applied to every captured value before it becomes a tag — `none` (default), `lowercase`, or `uppercase`

Every named group that matches a non-empty substring becomes a string tag on the `UserDerivedAttributes` entity. Empty captures are silently skipped.

Because Cedar has no built-in case-insensitive string comparison or `toLowerCase()` function, use `transform = "lowercase"` to normalize captured values so that policies can compare them against known-case literals. If different capture groups need different transforms, define separate derivation entries with distinct regexes.

### Configuration

Derivations are configured as a map under `LAKEKEEPER__CEDAR__USER_DERIVATIONS`. Each key is a human-readable name (used in error messages), and the value specifies `source` and `pattern`.

**Environment variables:**

```sh
# Extract "username" and "domain" from source_id (e.g. "Alice@Example.COM"),
# lowercased so policies can compare against known-case literals.
LAKEKEEPER__CEDAR__USER_DERIVATIONS__EMAIL_PARTS__SOURCE=source_id
LAKEKEEPER__CEDAR__USER_DERIVATIONS__EMAIL_PARTS__PATTERN=^(?<username>[^@]+)@(?<domain>.+)$
LAKEKEEPER__CEDAR__USER_DERIVATIONS__EMAIL_PARTS__TRANSFORM=lowercase

# Extract Kubernetes service account parts from source_id (no transform needed)
LAKEKEEPER__CEDAR__USER_DERIVATIONS__K8S_SA__SOURCE=source_id
LAKEKEEPER__CEDAR__USER_DERIVATIONS__K8S_SA__PATTERN=^system:serviceaccount:(?<namespace>[^:]+):(?<sa_name>.+)$
```

**TOML (file-based config):**

```toml
[cedar.user_derivations.email_parts]
source    = "source_id"
pattern   = "^(?<username>[^@]+)@(?<domain>.+)$"
transform = "lowercase"   # "none" (default), "lowercase", "uppercase"

[cedar.user_derivations.k8s_sa]
source  = "source_id"
pattern = "^system:serviceaccount:(?<namespace>[^:]+):(?<sa_name>.+)$"
```

Regex patterns are compiled once at startup. Invalid patterns cause a startup error with a clear message including the derivation name.

### Accessing Derived Attributes in Policies

Derived attributes are stored on a `UserDerivedAttributes` entity linked from the `User` via the optional `derived_attributes` field. Access tags using Cedar's `hasTag()` and `getTag()` functions:

```cedar
// Guard with `has` since derived_attributes is optional
principal has derived_attributes &&
principal.derived_attributes.hasTag("username") &&
principal.derived_attributes.getTag("username")
```

### Policy Examples

**Grant users full access to their personal namespace in the `dev` warehouse:**

If users authenticate with an OIDC provider where `source_id` is an email (e.g. `Alice@Example.COM`), and you configure a derivation with `transform = "lowercase"` to extract `username`, this policy lets each user perform any action on the namespace resource itself (e.g. list tables, create tables) — but only within the `dev` warehouse. The `lowercase` transform ensures the comparison works regardless of the casing in the IdP's subject claim. It does not automatically grant access to tables, views, or child namespaces within it; those require separate policies:

```cedar
permit(
  principal is Lakekeeper::User,
  action,
  resource is Lakekeeper::Namespace
) when {
  resource.warehouse.name == "dev" &&
  principal has derived_attributes &&
  principal.derived_attributes.hasTag("username") &&
  resource.name == principal.derived_attributes.getTag("username")
};
```

This allows user `alice@example.com` to perform any action on namespace `alice` in warehouse `dev`.

## Entity Hierarchy and Context

For each authorization request, Lakekeeper provides Cedar with the complete entity hierarchy from the requested resource to the server root. This hierarchical context ensures policies have full visibility into the resource's location and relationships.

**Example**: When a user queries table `ns1.ns2.transactions` in warehouse `wh-1` within project `my-project`, Cedar sees the following entities:

- `Lakekeeper::Server::<server-id>` (root)
- `Lakekeeper::Project::"<project-my-project-id>"`
- `Lakekeeper::Warehouse::"<warehouse-wh-1-id>"` (parent: Project)
- `Lakekeeper::Namespace::"<namespace-ns1-id>"` (parent: Warehouse)
- `Lakekeeper::Namespace::"<namespace-ns2-id>"` (parent: ns1)
- `Lakekeeper::Table::"<table-transactions-id>"` (parent: ns2)

This hierarchy allows policies to reference any level in the path — you can grant access based on warehouse names, namespace hierarchies, or specific table properties.

## Entity ID Formats

The following table documents the ID format used for each Cedar entity type. These IDs appear as the `id` field inside `uid` in entity JSON, and as the string literal in policy rules (e.g. `Lakekeeper::User::"oidc~alice"`).

| Entity type                                      | ID format                                   | Example |
|--------------------------------------------------|---------------------------------------------|-----|
| `Lakekeeper::Server`                             | UUIDv7 (auto-assigned, one per deployment)  | `019c192e-cc20-7a13-a1ac-2e3390f81908` |
| `Lakekeeper::Project`                            | String (alphanumeric, hyphens, underscores) | `my-project` or `019c192f-0613-7422-90f1-7dd6b09f033c` |
| `Lakekeeper::Warehouse`                          | UUIDv7 (assigned at warehouse creation)     | `d08dca76-ff69-11f0-9aa6-ab201d553ec5` |
| `Lakekeeper::Namespace`             | UUIDv7 (assigned at namespace creation)     | `019c192f-18c2-7f93-848f-542d8f32bc3c` |
| `Lakekeeper::Table`                              | `<warehouse-uuid>/<table-uuid>`             | `d08dca76-.../019c192f-...` |
| `Lakekeeper::View`                               | `<warehouse-uuid>/<view-uuid>`              | `d08dca76-.../019c192f-...` |
| `Lakekeeper::User`                               | `<provider_id>~<subject_in_idp>`            | `oidc~alice@example.com` |
| `Lakekeeper::Role`                               | `<project-id>/<provider_id>~<source_id>`    | `my-project/oidc~data-admins`, `my-project/lakekeeper~analysts` |
| `Lakekeeper::UserDerivedAttributes` | Same ID as the owning `User` (1:1)          | `oidc~alice@example.com` |

**Notes:**

- User IDs are constructed by Lakekeeper from the token's issuer/provider and the subject claim. For OIDC the format is `oidc~<sub>`.  
- Role IDs combine the project ID, the provider ID, and the role's source ID within that provider.  
- All UUIDs shown in entity JSON are the literal string without braces.

## External Entity Management

**Default Behavior**: Lakekeeper automatically includes the `Lakekeeper::User` entity with information extracted from the user's token. At actions inside a project it also includes a `Lakekeeper::Role` entity for every role the user holds — from the token (when `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured), from role providers, and roles managed in Lakekeeper — enabling role-based policies. At server actions, see [Role scope at server actions](#role-scope-at-server-actions).

**External Management**: To manage users and roles yourself instead, provide them as external entities:

1. Set `LAKEKEEPER__CEDAR__EXTERNALLY_MANAGED_USER_AND_ROLES` to `true`
2. Provide entity definitions via `LAKEKEEPER__CEDAR__ENTITY_JSON_SOURCES*` configurations
3. Ensure your external entities conform to Lakekeeper's Cedar schema

See [Entity Definition Example](#entity-definition-example) below for the JSON format.

In this mode Lakekeeper builds no user or role entities: a user is exactly what your entities file declares, with its attributes (such as `project_roles` and `global_role_ids`) and its `Role` parents, at every action, server actions included. What this page says a user carries at server actions describes the default mode. The `x-assume-role` rule applies the same in every mode. The [server-action check](#role-scope-at-server-actions) does not run in this mode: your entities file decides who a user is at every action.

**Schema Reference**: The Lakekeeper Cedar schema defines all available entity types, attributes, and actions. All entities and policies are validated against this schema on startup and refresh. Download the schema above or view it on [GitHub](https://github.com/lakekeeper/lakekeeper/tree/main/docs/docs/api).

## Policy Examples

The following examples demonstrate common Cedar policy patterns. Unless otherwise noted, examples assume a single-project setup (the project is not restricted). Note that warehouse names are only guaranteed to be unique within a project.

??? example "Allow everything for everyone"
    ```cedar
    permit (
        principal,
        action,
        resource
    );
    ```

??? example "Allow everything for a specific user"
    ```cedar
    permit (
        principal == Lakekeeper::User::"oidc~<user-id>", // Add user name in comment for documentation
        action,
        resource
    );
    ```

??? example "Allow everything for all users in a role/group"

    **Option 1 — using the full Role entity ID**

    The Role ID has the form `<project-id>/<provider_id>~<source_id>`. You can look it up in the Lakekeeper UI or via the management API.

    ```cedar
    permit (
        principal in Lakekeeper::Role::"my-project/oidc~data-engineers",
        action,
        resource
    );
    ```

    **Option 2 — using `project_roles`**
    `project_roles` matches by provider and role name, with no project ID to look up. At project actions it holds the roles of the project the request is decided in; the Role ID in Option 1 names the role of one project. At server actions only Option 2 matches, and only for groups — see [Role scope at server actions](#role-scope-at-server-actions). With externally managed users and roles, both options match what your entities file declares.

    ```cedar
    permit (
        principal is Lakekeeper::User,
        action,
        resource
    )
    when {
        principal.project_roles.contains(
            {provider_id: "oidc", source_id: "data-engineers"}
        )
    };
    ```

??? example "Grant access based on a token-sourced group (project_roles)"

    Use this pattern when roles come from OIDC token claims (configured via `LAKEKEEPER__OPENID_ROLES_CLAIM`). This avoids constructing the full role entity ID (which requires the project ID) and works identically in both token mode and external-entity mode. `project_roles` holds the roles of the project the request is decided in at project actions, and the user's groups at server actions — see [Role scope at server actions](#role-scope-at-server-actions).

    ```cedar
    permit (
        principal is Lakekeeper::User,
        action in
            [Lakekeeper::Action::"NamespaceActions",
             Lakekeeper::Action::"TableActions",
             Lakekeeper::Action::"ViewActions"],
        resource
    )
    when {
        resource.warehouse.name == "my-warehouse" &&
        principal.project_roles.contains(
            {provider_id: "oidc", source_id: "data-engineers"}
        )
    };

    permit (
        principal is Lakekeeper::User,
        action in [Lakekeeper::Action::"WarehouseModifyActions"],
        resource
    )
    when {
        resource.name == "my-warehouse" &&
        principal.project_roles.contains(
            {provider_id: "oidc", source_id: "data-engineers"}
        )
    };
    ```

    The `provider_id` must match the Authenticator ID configured in Lakekeeper (typically `"oidc"`). The `source_id` is the role/group name as it appears in the token claim (without any prefix).

??? example "Allow everything for multiple specific users"
    ```cedar
    permit (
        principal is Lakekeeper::User,
        action,
        resource
    ) when {
        [
            Lakekeeper::User::"oidc~<user-id-1>", // User 1 name for documentation
            Lakekeeper::User::"oidc~<user-id-2>", // User 2 name for documentation
            Lakekeeper::User::"oidc~<user-id-3>"  // User 3 name for documentation
        ].contains(principal)
    };
    ```

??? example "Basic server and project permissions for all authenticated users"
    ```cedar
    permit (
        principal,
        action in [
            Lakekeeper::Action::"ProjectDescribeActions", // Applies to all projects unless resource is restricted
        ],
        resource
    );
    ```

??? example "Read and write access to a namespace and all its contents (recursive)"
    ```cedar
    permit (
        principal == Lakekeeper::User::"oidc~<user-id>",
        action in
            [Lakekeeper::Action::"NamespaceModifyActions",
            Lakekeeper::Action::"TableModifyActions",
            Lakekeeper::Action::"ViewModifyActions"],
        resource
    ) when {
        ( resource is Lakekeeper::Warehouse && resource.name == "dev" ) ||
        ( resource is Lakekeeper::Namespace && resource.warehouse.name == "dev" && resource.name == "finance.revenue" ) ||
        ( resource is Lakekeeper::Table && resource.warehouse.name == "dev" && resource.namespace.name like "finance.revenue*" ) || // Include sub-namespaces via wildcard
        ( resource is Lakekeeper::View && resource.warehouse.name == "dev" && resource.namespace.name like "finance.revenue*" )
    };
    ```

??? example "Read access to a warehouse and all its contents for a group"

    **Option 1 — full Role ID:**

    ```cedar
    permit (
        principal in Lakekeeper::Role::"my-project/oidc~warehouse-readers",
        action in
            [
                Lakekeeper::Action::"WarehouseDescribeActions",
                Lakekeeper::Action::"NamespaceDescribeActions",
                Lakekeeper::Action::"TableSelectActions",
                Lakekeeper::Action::"ViewSelectActions"
            ],
        resource
    ) when {
        (resource has warehouse && resource.warehouse.name == "dev") ||
        (resource is Lakekeeper::Warehouse && resource.name == "dev")
    };
    ```

    **Option 2 — `project_roles`, no project ID needed:**

    ```cedar
    permit (
        principal is Lakekeeper::User,
        action in
            [
                Lakekeeper::Action::"WarehouseDescribeActions",
                Lakekeeper::Action::"NamespaceDescribeActions",
                Lakekeeper::Action::"TableSelectActions",
                Lakekeeper::Action::"ViewSelectActions"
            ],
        resource
    ) when {
        principal.project_roles.contains({provider_id: "oidc", source_id: "warehouse-readers"}) &&
        ((resource has warehouse && resource.warehouse.name == "dev") ||
         (resource is Lakekeeper::Warehouse && resource.name == "dev"))
    };
    ```

??? example "Read access to a warehouse and all its contents in multi-project setups"
    ```cedar
    permit (
        principal in Lakekeeper::Role::"my-project/oidc~warehouse-readers",
        action in
            [
                Lakekeeper::Action::"WarehouseDescribeActions",
                Lakekeeper::Action::"NamespaceDescribeActions",
                Lakekeeper::Action::"TableSelectActions",
                Lakekeeper::Action::"ViewSelectActions"
            ],
        resource in Lakekeeper::Project::"my-project"
    ) when {
        (resource has warehouse && resource.warehouse.name == "dev") ||
        (resource is Lakekeeper::Warehouse && resource.name == "dev")
    };
    ```

??? example "ABAC: Role-based table access using static role membership"

    This example grants read/write access to tables tagged with an `access-role` property matching the requesting user's role — using traditional RBAC role membership. The `access-role-*` keys use the `access-` prefix so Lakekeeper parses them as entity references; the `.raw` field always stores the original string.

    ```cedar
    @id("abac-role-based-access-marketing-select")
    @description("ABAC: Allow Read access to tables tagged with access-role-select:marketing to the marketing-select role")
    permit (
        principal in Lakekeeper::Role::"my-project/lakekeeper~marketing-select",
        action in Lakekeeper::Action::"TableSelectActions",
        resource is Lakekeeper::Table
    )
    when
    {
        resource.properties.hasTag("access-role-select") &&
        resource.properties.getTag("access-role-select").raw == "marketing"
    };

    @id("abac-role-based-access-marketing-modify")
    @description("ABAC: Allow Modify access to tables tagged with access-role-modify:marketing, but prevent removing or changing the tag itself")
    permit (
        principal in Lakekeeper::Role::"my-project/lakekeeper~marketing-modify",
        action in Lakekeeper::Action::"TableModifyActions",
        resource is Lakekeeper::Table
    )
    when
    {
        resource.properties.hasTag("access-role-modify") &&
        resource.properties.getTag("access-role-modify").raw == "marketing"
    }
    unless
    {
        // Prevent users from removing or changing the access-control tag itself.
        action == Lakekeeper::Action::"CommitTable" &&
        (context.table_properties_removal.contains("access-role-modify") ||
         context.table_properties_updates.hasTag("access-role-modify"))
    };

    @id("abac-role-based-access-marketing-admin")
    @description("ABAC: Allow full Modify access (including changing access tags) to marketing-admin role")
    permit (
        principal in Lakekeeper::Role::"my-project/lakekeeper~marketing-admin",
        action in Lakekeeper::Action::"TableModifyActions",
        resource is Lakekeeper::Table
    )
    when
    {
        resource.properties.hasTag("access-role-modify") &&
        resource.properties.getTag("access-role-modify").raw == "marketing"
    };
    ```

??? example "ABAC: Access control lists stored directly in table properties"

    This is a more advanced ABAC pattern where each table carries its own access control list in an `access-owners` and `access-readers` property. The values are JSON arrays of entity references (roles and/or users), parsed automatically by Lakekeeper.

    **Tag the table** (e.g. via the Iceberg REST API or your ETL pipeline):
    ```
    access-owners  = ["role-full:oidc~data-admins", "user:oidc~alice@example.com"]
    access-readers = ["role:analysts", "role-full:oidc~reporting-team"]
    ```

    **Cedar policies** (no role names are hardcoded — access is determined entirely by table metadata):
    ```cedar
    @id("abac-property-acl-select")
    @description("Allow read access to any table where the principal is listed in the access-readers property")
    permit (
        principal,
        action in Lakekeeper::Action::"TableSelectActions",
        resource is Lakekeeper::Table
    )
    when
    {
        resource.properties.hasTag("access-readers") &&
        (principal in resource.properties.getTag("access-readers").roles ||
         principal in resource.properties.getTag("access-readers").users)
    };

    @id("abac-property-acl-modify")
    @description("Allow write access to any table where the principal is listed in the access-owners property")
    permit (
        principal,
        action in Lakekeeper::Action::"TableModifyActions",
        resource is Lakekeeper::Table
    )
    when
    {
        resource.properties.hasTag("access-owners") &&
        (principal in resource.properties.getTag("access-owners").roles ||
         principal in resource.properties.getTag("access-owners").users)
    }
    unless
    {
        // Owners can modify the table but cannot change the access-control properties themselves.
        // Grant the marketing-admin role a separate policy if escalation is needed.
        action == Lakekeeper::Action::"CommitTable" &&
        (context.table_properties_removal.contains("access-owners") ||
         context.table_properties_removal.contains("access-readers") ||
         context.table_properties_updates.hasTag("access-owners") ||
         context.table_properties_updates.hasTag("access-readers"))
    };
    ```

    !!! tip "Role resolution"
        `principal in resource.properties.getTag("access-readers").roles` uses Cedar's built-in entity hierarchy. A user is considered `in` a role if that role appears as an ancestor in the user entity's parent chain — exactly the same mechanism used for static role-based policies. This means the access control lists stored in table properties work the same for token-extracted roles (`LAKEKEEPER__OPENID_ROLES_CLAIM`), roles from role providers, and externally managed role assignments.

??? example "ABAC: Namespace-level access control inherited by all tables"

    Apply access-control lists at the namespace level so that all tables in the namespace inherit the same restrictions.

    **Tag the namespace**:
    ```
    access-readers = ["role-full:oidc~finance-readers"]
    access-writers = ["role-full:oidc~finance-engineers"]
    ```

    **Cedar policies**:
    ```cedar
    @id("abac-namespace-acl-select")
    @description("Allow read access to tables when the namespace has access-readers listing the principal")
    permit (
        principal,
        action in Lakekeeper::Action::"TableSelectActions",
        resource is Lakekeeper::Table
    )
    when
    {
        resource.namespace.properties.hasTag("access-readers") &&
        (principal in resource.namespace.properties.getTag("access-readers").roles ||
         principal in resource.namespace.properties.getTag("access-readers").users)
    };
    ```

??? example "Recommended permissions for the OPA bridge user"
    ```cedar
    @id("opa-permissions")
    @description("Grant global permission read access to OPA user")
    permit (
        principal == Lakekeeper::User::"oidc~<opa-user-id>", // OPA service account
        action in [
            Lakekeeper::Action::"IntrospectServerAuthorization",
            Lakekeeper::Action::"IntrospectProjectAuthorization",
            Lakekeeper::Action::"IntrospectRoleAuthorization",
            Lakekeeper::Action::"WarehouseDescribeActions",
            Lakekeeper::Action::"IntrospectWarehouseAuthorization",
            Lakekeeper::Action::"NamespaceDescribeActions",
            Lakekeeper::Action::"IntrospectNamespaceAuthorization",
            Lakekeeper::Action::"TableDescribeActions",
            Lakekeeper::Action::"IntrospectTableAuthorization",
            Lakekeeper::Action::"ViewDescribeActions",
            Lakekeeper::Action::"IntrospectViewAuthorization",
        ],
        resource
    );
    ```

## Entity Definition Example

Lakekeeper provides the following entities internally to Cedar: Server, Project, Warehouse, Namespace, Table, View, the requesting User, and a Role for every role the user holds. A request on a table called "my-table" in Namespace "my-namespace" provides the following entities to Cedar:

??? example "Entities provided to Cedar internally"
    ```json
    [
        {
            "uid": {
                "type": "Lakekeeper::Table",
                "id": "d08dca76-ff69-11f0-9aa6-ab201d553ec5/019c192f-18d0-7390-9d90-93facfb8e3d3"
            },
            "attrs": {
                "namespace": {
                    "__entity": {
                        "type": "Lakekeeper::Namespace",
                        "id": "019c192f-18c2-7f93-848f-542d8f32bc3c"
                    }
                },
                "protected": false,
                "warehouse": {
                    "__entity": {
                        "type": "Lakekeeper::Warehouse",
                        "id": "d08dca76-ff69-11f0-9aa6-ab201d553ec5"
                    }
                },
                "name": "transactions",
                "project": {
                    "__entity": {
                        "type": "Lakekeeper::Project",
                        "id": "019c192f-0613-7422-90f1-7dd6b09f033c"
                    }
                }
            },
            "tags": {
                // Table properties are stored as Cedar entity tags.
                // Access-prefixed keys (access- / access_) have roles and users parsed.
                "access-owners": {
                    "raw": "[\"role-full:oidc~data-admins\", \"user:oidc~alice\"]",
                    "roles": [
                        { "__entity": { "type": "Lakekeeper::Role", "id": "019c192f-0613-7422-90f1-7dd6b09f033c/oidc~data-admins" } }
                    ],
                    "users": [
                        { "__entity": { "type": "Lakekeeper::User", "id": "oidc~alice" } }
                    ]
                },
                "description": {
                    "raw": "Financial transactions table",
                    "roles": [],
                    "users": []
                }
            },
            "parents": [
                {
                    "type": "Lakekeeper::Namespace",
                    "id": "019c192f-18c2-7f93-848f-542d8f32bc3c"
                }
            ]
        },
        {
            "uid": {
                "type": "Lakekeeper::Server",
                "id": "019c192e-cc20-7a13-a1ac-2e3390f81908"
            },
            "attrs": {},
            "parents": []
        },
        {
            "uid": {
                "type": "Lakekeeper::Project",
                "id": "019c192f-0613-7422-90f1-7dd6b09f033c"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Lakekeeper::Server",
                    "id": "019c192e-cc20-7a13-a1ac-2e3390f81908"
                }
            ]
        },
        {
            "uid": {
                "type": "Lakekeeper::Warehouse",
                "id": "d08dca76-ff69-11f0-9aa6-ab201d553ec5"
            },
            "attrs": {
                "is_active": true,
                "protected": false,
                "project": {
                    "__entity": {
                        "type": "Lakekeeper::Project",
                        "id": "019c192f-0613-7422-90f1-7dd6b09f033c"
                    }
                },
                "name": "wh-1"
            },
            "parents": [
                {
                    "type": "Lakekeeper::Project",
                    "id": "019c192f-0613-7422-90f1-7dd6b09f033c"
                }
            ]
        },
        {
            "uid": {
                "type": "Lakekeeper::Namespace",
                "id": "019c192f-18c2-7f93-848f-542d8f32bc3c"
            },
            "attrs": {
                "protected": false,
                "warehouse": {
                    "__entity": {
                        "type": "Lakekeeper::Warehouse",
                        "id": "d08dca76-ff69-11f0-9aa6-ab201d553ec5"
                    }
                },
                "project": {
                    "__entity": {
                        "type": "Lakekeeper::Project",
                        "id": "019c192f-0613-7422-90f1-7dd6b09f033c"
                    }
                },
                "name": "my-namespace"
            },
            "tags": {
                "location": {
                    "raw": "s3://tests/075272e23ed548d8bfd722a7a383cd50/019c192f-18c2-7f93-848f-542d8f32bc3c",
                    "roles": [],
                    "users": []
                }
            },
            "parents": [
                {
                    "type": "Lakekeeper::Warehouse",
                    "id": "d08dca76-ff69-11f0-9aa6-ab201d553ec5"
                }
            ]
        },
        {
            "uid": {
                "type": "Lakekeeper::User",
                "id": "oidc~2f268e8b-8cc1-4edd-a9df-87d69f7e9deb"
            },
            "attrs": {
                // Every role the user holds in the table's project, as Role
                // entities — from the token, role providers and roles managed in
                // Lakekeeper, including roles they are nested in. Empty at
                // server actions.
                "roles": [
                    { "__entity": { "type": "Lakekeeper::Role", "id": "019c192f-0613-7422-90f1-7dd6b09f033c/oidc~analysts" } }
                ],
                // The same roles as {provider_id, source_id} records.
                "project_roles": [
                    {"provider_id": "oidc", "source_id": "analysts"}
                ],
                // Names of the roles from identity and role providers; only
                // populated when LAKEKEEPER__CEDAR__GLOBAL_ROLE_IDS_ENABLED=true,
                // otherwise [].
                "global_role_ids": [],
                "provider_id": "oidc",
                "source_id": "2f268e8b-8cc1-4edd-a9df-87d69f7e9deb"
            },
            // Every role in `roles` is a parent, so `principal in Role::"…"` matches.
            "parents": [
                { "type": "Lakekeeper::Role", "id": "019c192f-0613-7422-90f1-7dd6b09f033c/oidc~analysts" }
            ]
        }
    ]
    ```

Lakekeeper can log all entities provided to Cedar for debugging purposes. See the [Cedar Configuration](./configuration.md#cedar) section for details on enabling entity logging.

When `LAKEKEEPER__CEDAR__EXTERNALLY_MANAGED_USER_AND_ROLES` is set to `true`, Lakekeeper excludes User and Role entities from Cedar requests and expects you to provide them externally via `LAKEKEEPER__CEDAR__ENTITY_JSON_SOURCES*` configurations. The following example shows an `entity.json` file defining user-to-role assignments:

```json
[
    {
        "uid": {
            "type": "Lakekeeper::User",
            "id": "oidc~90471f73-e338-4032-9a6b-1e021cc3cb1e"
        },
        "attrs": {
            // Roles the user is a member of.
            // Use the `parents` array (not this set) to establish the hierarchy;
            // keep both in sync.
            "roles": [
                { "__entity": { "type": "Lakekeeper::Role", "id": "data-engineering" } }
            ],
            // Flat set of role identities relevant to the current project.
            // Enables principal.project_roles.contains({provider_id, source_id}) checks.
            // Provide these only in single project setups.
            "project_roles": [
                { "provider_id": "oidc", "source_id": "warehouse-1-admins" }
            ],
            // source_id of each provider-resolved role as plain strings.
            // Required by the schema; use [] when GLOBAL_ROLE_IDS_ENABLED is off.
            "global_role_ids": [],
            // Authentication provider and subject ID of this user.
            "provider_id": "oidc",
            "source_id": "90471f73-e338-4032-9a6b-1e021cc3cb1e"
        },
        "parents": [
            { "type": "Lakekeeper::Role", "id": "data-engineering" }
        ]
    },
    {
        "uid": {
            "type": "Lakekeeper::Role",
            "id": "data-engineering"
        },
        "attrs": {
            "project": {
                "__entity": {
                    "type": "Lakekeeper::Project",
                    "id": "<your-project-id>"
                }
            },
            "provider_id": "entities-file",
            "source_id": "data-engineering"
        },
        "parents": [
            { "type": "Lakekeeper::Role", "id": "warehouse-1-admins" }
        ]
    },
    {
        "uid": {
            "type": "Lakekeeper::Role",
            "id": "warehouse-1-admins"
        },
        "attrs": {
            "project": {
                "__entity": {
                    "type": "Lakekeeper::Project",
                    "id": "<your-project-id>"
                }
            },
            "provider_id": "entities-file",
            "source_id": "warehouse-1-admins"
        },
        "parents": []
    }
]
```

!!! tip "Required User attributes"
    Every `Lakekeeper::User` entity in an external file **must** include `roles`, `project_roles`, `provider_id`, `source_id`, and `global_role_ids`. Omitting any of these will cause a schema validation error on startup. Use `[]` for `global_role_ids` when it is not used or `LAKEKEEPER__CEDAR__GLOBAL_ROLE_IDS_ENABLED` is disabled. Set `project_roles` to `[]` in multi-project setups.

## Policy and Entity Management

**Startup Behavior:**

- All policy and entity files are loaded and validated against the Cedar schema
- If any file is unreadable or invalid, Lakekeeper fails to start with an error

This ensures that authorization policies are always valid before serving requests

**Refresh Behavior:**
Configure automatic policy refresh using `LAKEKEEPER__CEDAR__REFRESH_INTERVAL_SECS` (default: 5 seconds):

1. **Change Detection**: Lightweight checks monitor ConfigMap versions and file timestamps
2. **Reload on Change**: Modified entity or policy files trigger a full reload of all files to guarantee consistency
3. **Atomic Updates**: The in-memory store is only updated if all files reload successfully
4. **Error Handling**: If any reload fails, the previous configuration is retained, an error is logged, and health checks report unhealthy status

This approach ensures that authorization policies remain consistent and that partial updates never compromise security.

## Break-Glass

Cedar policies come from two places. The **server set** comes from files and ConfigMaps (`LAKEKEEPER__CEDAR__POLICY_SOURCES__*`), and only the operator can change it. The **scope sets** are stored in the catalog, one per project and one per warehouse, and a project manages its own through the management API.

A project controls its own scope set, so it can write a `forbid` that denies everyone, including the people who would remove it again. Break-glass is the way out.

A break-glass request is decided by the server set only. Scope policies are not read, so a bad one cannot block the repair. Grants are still read: they sit in an ordinary catalog table that a policy lockout cannot reach, so the grants a project already has start granting access again.

Break-glass allows only what the server set allows. The operator writes the policy that says who may repair a project.

### Sending a Break-Glass Request

Add the `x-break-glass` header, with your reason as its value:

```bash
curl -X POST "https://lakekeeper.example.com/management/v1/permissions/cedar/project/policies" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-project-id: 01943e3d-43c5-7a4e-b6dd-a55c7796d9da" \
  -H "x-break-glass: INC-1234 removing the forbid that locked out the admins" \
  -H "Content-Type: application/json" \
  -d '{"writes": [], "replace": true}'
```

Any non-empty value works. The reason goes into the audit event as `break_glass`, cut off at 256 bytes, so send a ticket reference. See the [Logging guide](./logging.md#audit-logs-and-rust_log).

Break-glass applies only to a user acting as themselves. Requests that assume a role with `x-assume-role`, and permission checks about someone else, are decided the normal way. OpenFGA and allow-all ignore the header: it changes no decision and only shows up in the audit record.

An [instance admin](./instance-admins.md) must give a reason, because their requests skip stored policy and nothing else would record why a policy changed. Without one, applying project or warehouse policies answers `403 CedarInstanceAdminNeedsBreakGlass`. The value `true` does not count as a reason.

### Checking Whether Break-Glass Is Available

Ask `break-glass-status` first. It only reports, and decides nothing:

```bash
curl "https://lakekeeper.example.com/management/v1/permissions/cedar/break-glass-status?project-id=01943e3d-43c5-7a4e-b6dd-a55c7796d9da" \
  -H "Authorization: Bearer $TOKEN"
# {"break-glass-available": true}
```

Anyone signed in can ask, and no permission is needed, because the people who need this answer are the ones being denied. It covers one project and the break-glass path only. To see whether someone is an instance admin, read `is-instance-admin` from `/management/v1/whoami`.

## Cedar Actions

The following tables document all available Cedar actions. Use action groups for broad permissions or individual actions for fine-grained control.

The **Audit log `action_name`** column lists the standardized snake_case identifier that appears in the `action.action_name` field of [audit log events](./logging.md#audit-logs) when that action is checked. For actions shared with OpenFGA (those derived from the authorizer-agnostic `Catalog*Action` enums) the same value is emitted regardless of which authorizer is configured. A dash (`—`) means the action is only reached through a silent backend pre-check (no audit event is emitted under a stable standardized name).

Because the audit `action_name` deliberately omits the resource type (`delete`, `rename`, `get_metadata`, `introspect_authorization`, etc. appear across multiple Cedar action names), use the sibling `entity.entity_type` field on the audit event to pick the right Cedar action. For example, `action_name = "read_data"` with `entity.entity_type = "table"` corresponds to `Lakekeeper::Action::"ReadTableData"`:

```json
{
  "action": { "action_name": "read_data" },
  "entity": {
    "entity_type": "table",
    "warehouse-id": "faac5cb2-5902-11f1-b9a7-1360e98a724d",
    "namespace": "finance",
    "table": "products"
  }
}
```

### How action groups are nested

A policy can name a group in place of every action in it. Each level has a ladder of groups, from the narrowest step to the widest:

| Level | Ladder |
|---|---|
| Server, Project, Warehouse, Namespace | `<Level>DescribeActions` < `<Level>CreateActions` < `<Level>ModifyActions` < `<Level>Actions` |
| Table, View, GenericTable | `<Level>DescribeActions` < `<Level>SelectActions` < `<Level>WriteActions` < `<Level>ModifyActions` < `<Level>Actions` |
| Tag | `TagDescribeActions` < `TagApplyActions` < `TagModifyActions` < `TagActions` |
| Role | `RoleActions` only |

A wider step contains the narrower ones: a permit on `ProjectModifyActions` also allows every create and describe action on the project. Permit the narrowest group that covers what you mean. A `forbid` works the same way.

Grant administration and policy administration stand outside every `<Level>Actions` group. Name them on their own: see [Grant Administration Actions](#grant-administration-actions) and [Cedar Policy Actions](#cedar-policy-actions). `DataPlaneActions` holds the actions that hand out data or data-access credentials: `ReadTableData`, `WriteTableData`, `SelectView`, `ReadGenericTableData` and `WriteGenericTableData`. A `forbid` on it keeps a principal away from data and leaves their metadata access alone.

The **Group** column below names the narrowest group an action is in. An action marked "none" is in no group: a policy reaches it by naming it, or by leaving `action` unconstrained.

### Server Actions

At server actions a user carries their groups; see [Role scope at server actions](#role-scope-at-server-actions).

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `ListUsers` | `list_users` | `ServerDescribeActions` | List the users this server knows about |
| `CreateProject` | `create_project` | `ServerCreateActions` | Create a project |
| `ProvisionUsers` | `provision_users` | `ServerModifyActions` | Create a user record, through the API or at the user's first authenticated request |
| `UpdateUsers` | `update_users` | `ServerModifyActions` | Change what Lakekeeper stores about a user |
| `DeleteUsers` | `delete_users` | `ServerModifyActions` | Remove a user record |

Every user can update and delete their own user record; that needs no policy. Server grants are under [Grant Administration Actions](#grant-administration-actions), and the server's policy sources under [Cedar Policy Actions](#cedar-policy-actions).

### Project Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `GetProjectMetadata` | `get_metadata` | `ProjectDescribeActions` | View project details and configuration |
| `ListWarehouses` | `list_warehouses` | `ProjectDescribeActions` | List all warehouses in the project |
| `IncludeProjectInList` | `include_in_list` | `ProjectDescribeActions` | Include the project in project listings |
| `ListRoles` | `list_roles` | `ProjectDescribeActions` | List all roles in the project |
| `SearchRoles` | `search_roles` | `ProjectDescribeActions` | Search for roles in the project |
| `ListTags` | `list_tags` | `ProjectDescribeActions` | List the project's tag definitions |
| `GetProjectEndpointStatistics` | `get_endpoint_statistics` | `ProjectDescribeActions` | View API usage statistics for the project |
| `GetProjectTaskQueueConfig` | `get_task_queue_config` | `ProjectDescribeActions` | View task queue configuration for the project |
| `GetProjectTasks` | `get_project_tasks` | `ProjectDescribeActions` | List background tasks in the project |
| `CreateWarehouse` | `create_warehouse` | `ProjectCreateActions` | Create a warehouse in the project |
| `DeleteProject` | `delete` | `ProjectModifyActions` | Delete the project |
| `RenameProject` | `rename` | `ProjectModifyActions` | Change the project's name |
| `ModifyProjectTaskQueueConfig` | `modify_task_queue_config` | `ProjectModifyActions` | Update task queue configuration |
| `ControlProjectTasks` | `control_project_tasks` | `ProjectModifyActions` | Manage background tasks (cancel, retry, etc.) |
| `CreateRole` | `create_role` | `ProjectGrantActions` | Create a role in the project |
| `CreateTag` | `create_tag` | `ProjectModifyActions` | Create a tag definition in the project |
| `ReadUserRoleAssignments` | `read_role_assignments` | none | List the roles a user holds in the project (`GET /management/v1/user/{user_id}/roles` and `/roles/transitive`) |

`ProjectCreateActions` covers places to put data. `CreateTag` shapes how the project is governed, so it is in `ProjectModifyActions`. `CreateRole` is in `ProjectGrantActions`, because a role exists to hold grants. Permit either by name to allow it alone.

### Role Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `AssumeRole` | `assume_role` | `RoleActions` | Act as this role for the request. It changes who the caller is, so permit it narrowly |
| `ReadRole` | `read` | `RoleActions` | Read the role, including its members |
| `ReadRoleMetadata` | `read_metadata` | `RoleActions` | Read only the role's name and project |
| `UpdateRole` | `update` | `RoleActions` | Change the role's name or description |
| `DeleteRole` | `delete` | `RoleActions` | Delete the role |
| `ManageRoleAssignments` | `manage_role_assignments` | `RoleActions` | Add or remove the role's members (users or roles) |
| `ReadRoleAssignments` | `read_role_assignments` | `RoleActions` | List the role's members, parents and assignments |
| `UpdateRoleSourceSystem` | `update_source_system` | `RoleActions` | Rebind the role to a different provider and source id |

### Tag Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `ReadTag` | `read` | `TagDescribeActions` | Read a tag definition (name, value kind, allowed values) |
| `ApplyTag` | `apply` | `TagApplyActions` | Attach the tag to an object |
| `RemoveTag` | `remove` | `TagApplyActions` | Detach the tag from an object |
| `UpdateTag` | `update` | `TagModifyActions` | Rename the tag, widen its scope or add values |
| `DeleteTag` | `delete` | `TagModifyActions` | Delete the tag definition |
| `ReadTagAttachments` | `read_attachments` | `TagModifyActions` | List every object the tag is attached to |

Attaching or detaching a tag needs two permits: `ApplyTag` or `RemoveTag` on the tag, and the object's own tag action (`ManageWarehouseTags`, `ManageNamespaceTags`, `ManageTableTags`, `ManageViewTags` or `ManageGenericTableTags`).

### Warehouse Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `UseWarehouse` | `use` | `WarehouseDescribeActions` | Reach the warehouse at all. Every warehouse request checks it first; denied, the warehouse looks absent |
| `ListNamespacesInWarehouse` | `list_namespaces` | `WarehouseDescribeActions` | List namespaces in the warehouse |
| `GetWarehouseMetadata` | `get_metadata` | `WarehouseDescribeActions` | View warehouse configuration and details |
| `GetConfig` | `get_config` | `WarehouseDescribeActions` | Read the Iceberg REST config that clients call when they connect |
| `IncludeWarehouseInList` | `include_in_list` | `WarehouseDescribeActions` | Include the warehouse in warehouse listings |
| `ListDeletedTabulars` | `list_deleted_tabulars` | `WarehouseDescribeActions` | List soft-deleted tables and views |
| `GetTaskQueueConfig` | `get_task_queue_config` | `WarehouseDescribeActions` | View task queue configuration |
| `GetAllTasks` | `get_all_tasks` | `WarehouseDescribeActions` | List all background tasks in the warehouse |
| `ListEverythingInWarehouse` | `list_everything` | `WarehouseDescribeActions` | List everything under the warehouse; the per-item `Include…InList` checks are skipped |
| `GetWarehouseEndpointStatistics` | `get_endpoint_statistics` | `WarehouseDescribeActions` | View API usage statistics for the warehouse |
| `CreateNamespaceInWarehouse` | `create_namespace` | `WarehouseCreateActions` | Create a namespace directly in the warehouse |
| `DeleteWarehouse` | `delete` | `WarehouseModifyActions` | Delete the warehouse |
| `UpdateStorage` | `update_storage` | `WarehouseModifyActions` | Modify storage configuration |
| `UpdateStorageCredential` | `update_storage_credential` | `WarehouseModifyActions` | Update storage credentials |
| `DeactivateWarehouse` | `deactivate` | `WarehouseModifyActions` | Deactivate the warehouse (suspend operations) |
| `ActivateWarehouse` | `activate` | `WarehouseModifyActions` | Activate a deactivated warehouse |
| `RenameWarehouse` | `rename` | `WarehouseModifyActions` | Change the warehouse's name |
| `ModifySoftDeletion` | `modify_soft_deletion` | `WarehouseModifyActions` | Configure soft-deletion settings |
| `ModifyTaskQueueConfig` | `modify_task_queue_config` | `WarehouseModifyActions` | Update task queue configuration |
| `ControlAllTasks` | `control_all_tasks` | `WarehouseModifyActions` | Manage all background tasks |
| `SetWarehouseProtection` | `set_protection` | `WarehouseModifyActions` | Enable or disable deletion protection |
| `SetWarehouseFormatVersionPolicy` | `set_format_version_policy` | `WarehouseModifyActions` | Change the warehouse's Iceberg format-version policy |
| `ManageWarehouseTags` | `manage_tags` | `WarehouseModifyActions` | Attach or detach tags on the warehouse |
| `AcceptMovedNamespaceInWarehouse` | `accept_moved_namespace` | none | Accept a namespace moved in at the warehouse root. Asked together with `CreateNamespaceInWarehouse` |

### Namespace Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `ListEverythingInNamespace` | `list_everything` | `NamespaceDescribeActions` | List everything under the namespace; the per-item `Include…InList` checks are skipped |
| `GetNamespaceMetadata` | `get_metadata` | `NamespaceDescribeActions` | View namespace properties and configuration |
| `IncludeNamespaceInList` | `include_in_list` | `NamespaceDescribeActions` | Include the namespace in namespace listings |
| `ListTables` | `list_tables` | `NamespaceDescribeActions` | List tables in the namespace |
| `ListViews` | `list_views` | `NamespaceDescribeActions` | List views in the namespace |
| `ListGenericTables` | `list_generic_tables` | `NamespaceDescribeActions` | List generic tables in the namespace |
| `ListNamespacesInNamespace` | `list_namespaces` | `NamespaceDescribeActions` | List child namespaces |
| `CreateTable` | `create_table` | `NamespaceCreateActions` | Create a table in the namespace |
| `CreateView` | `create_view` | `NamespaceCreateActions` | Create a view in the namespace |
| `CreateGenericTableInNamespace` | `create_generic_table` | `NamespaceCreateActions` | Create a generic table in the namespace |
| `CreateNamespaceInNamespace` | `create_namespace` | `NamespaceCreateActions` | Create a child namespace |
| `DeleteNamespace` | `delete` | `NamespaceModifyActions` | Delete the namespace |
| `SetNamespaceProtection` | `set_protection` | `NamespaceModifyActions` | Enable or disable deletion protection |
| `ManageNamespaceTags` | `manage_tags` | `NamespaceModifyActions` | Attach or detach tags on the namespace |
| `UpdateNamespaceProperties` | `update_properties` | `NamespaceModifyActions` | Modify namespace properties |
| `MoveNamespace` | `move` | none | Move the namespace to a new path: the source half of a move |
| `AcceptMovedNamespaceInNamespace` | `accept_moved_namespace` | none | Accept a namespace moved in as a child. Asked together with `CreateNamespaceInNamespace` |

A move changes what the moved subtree inherits, so `MoveNamespace` and the two `AcceptMovedNamespaceIn…` actions are in no group. Name them to allow a move. Moving out and moving in are decided separately.

### Table Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `GetTableMetadata` | `get_metadata` | `TableDescribeActions` | View table schema, metadata, and configuration |
| `IncludeTableInList` | `include_in_list` | `TableDescribeActions` | Include the table in table listings |
| `GetTableTasks` | `get_tasks` | `TableDescribeActions` | List background tasks for the table |
| `ReadTableData` | `read_data` | `TableSelectActions` | Read data from the table |
| `WriteTableData` | `write_data` | `TableWriteActions` | Get write credentials for the table |
| `CommitTable` | `commit` | `TableWriteActions` | Commit table changes (data, schema, properties) |
| `DropTable` | `drop` | `TableModifyActions` | Delete the table |
| `RenameTable` | `rename` | `TableModifyActions` | Change the table's name or move it to another namespace |
| `UndropTable` | `undrop` | `TableModifyActions` | Restore a soft-deleted table |
| `ControlTableTasks` | `control_tasks` | `TableModifyActions` | Manage the table's background tasks |
| `SetTableProtection` | `set_protection` | `TableModifyActions` | Enable or disable deletion protection |
| `ManageTableTags` | `manage_tags` | `TableModifyActions` | Attach or detach tags on the table |

`TableWriteActions` lets a loading job write and commit without dropping, renaming or unprotecting the table. A commit covers schema changes too; policies cannot tell the two apart.

### View Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `GetViewMetadata` | `get_metadata` | `ViewDescribeActions` | View the view's definition and metadata |
| `IncludeViewInList` | `include_in_list` | `ViewDescribeActions` | Include the view in view listings |
| `GetViewTasks` | `get_tasks` | `ViewDescribeActions` | List background tasks for the view |
| `SelectView` | `select` | `ViewSelectActions` | Execute the view to produce rows (also required to traverse the view in a `referenced-by` chain) |
| `CommitView` | `commit` | `ViewWriteActions` | Commit a new version of the view (definition, properties) |
| `DropView` | `drop` | `ViewModifyActions` | Delete the view |
| `RenameView` | `rename` | `ViewModifyActions` | Change the view's name or move it to another namespace |
| `UndropView` | `undrop` | `ViewModifyActions` | Restore a soft-deleted view |
| `ControlViewTasks` | `control_tasks` | `ViewModifyActions` | Manage the view's background tasks |
| `SetViewProtection` | `set_protection` | `ViewModifyActions` | Enable or disable deletion protection |
| `ManageViewTags` | `manage_tags` | `ViewModifyActions` | Attach or detach tags on the view |

### Generic Table Actions

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `GetGenericTableMetadata` | `get_metadata` | `GenericTableDescribeActions` | View the generic table's metadata |
| `IncludeGenericTableInList` | `include_in_list` | `GenericTableDescribeActions` | Include the generic table in listings |
| `GetGenericTableTasks` | `get_tasks` | `GenericTableDescribeActions` | List background tasks for the generic table |
| `ReadGenericTableData` | `read_data` | `GenericTableSelectActions` | Read data from the generic table |
| `WriteGenericTableData` | `write_data` | `GenericTableWriteActions` | Get write credentials for the generic table |
| `DropGenericTable` | `drop` | `GenericTableModifyActions` | Delete the generic table |
| `RenameGenericTable` | `rename` | `GenericTableModifyActions` | Change the generic table's name or move it to another namespace |
| `UndropGenericTable` | `undrop` | `GenericTableModifyActions` | Restore a soft-deleted generic table |
| `ControlGenericTableTasks` | `control_tasks` | `GenericTableModifyActions` | Manage the generic table's background tasks |
| `SetGenericTableProtection` | `set_protection` | `GenericTableModifyActions` | Enable or disable deletion protection |
| `ManageGenericTableTags` | `manage_tags` | `GenericTableModifyActions` | Attach or detach tags on the generic table |

### Grant Administration Actions

These actions decide who may read and hand out [grants](./grants.md). Name `GrantActions` (every level), `GrantReadActions` (the read-only part, every level), a `<Level>GrantActions` group or a single action.

| Level | Grant and revoke | Read grants | Ask about another principal |
|---|---|---|---|
| Server | `ManageServerGrants` | `ReadServerGrants` | `IntrospectServerAuthorization` |
| Project | `ManageProjectGrants` | `ReadProjectGrants` | `IntrospectProjectAuthorization` |
| Warehouse | `ManageWarehouseGrants` | `ReadWarehouseGrants` | `IntrospectWarehouseAuthorization` |
| Namespace | `ManageNamespaceGrants` | `ReadNamespaceGrants` | `IntrospectNamespaceAuthorization` |
| Table | `ManageTableGrants` | `ReadTableGrants` | `IntrospectTableAuthorization` |
| View | `ManageViewGrants` | `ReadViewGrants` | `IntrospectViewAuthorization` |
| GenericTable | `ManageGenericTableGrants` | `ReadGenericTableGrants` | `IntrospectGenericTableAuthorization` |
| Tag | `ManageTagGrants` | `ReadTagGrants` | `IntrospectTagAuthorization` |
| Role | — | — | `IntrospectRoleAuthorization` |

- `Manage<Level>Grants` (audit `apply_grants`, group `<Level>GrantActions`): grant and revoke privileges on the object. Each privilege is decided on its own, with `context.privilege`, `context.grantee` and `context.direction`.
- `Read<Level>Grants` (audit `read_grants`, groups `<Level>GrantActions` and `GrantReadActions`): see who holds grants on the object. It also makes the object itself visible.
- `Introspect<Level>Authorization` (groups `<Level>GrantActions` and `GrantReadActions`; `IntrospectRoleAuthorization` is in `GrantReadActions` only): ask what another principal may do on the object. `/management/v1/permissions/cedar/resolve-entities` records it as `introspect_authorization`, at the server, project, warehouse, namespace, table, view and generic-table levels.

Whole-subtree grant administration has its own groups. `SubtreeGrantActions` holds all five actions, under `GrantActions`; `SubtreeGrantReadActions` is also in `GrantReadActions`. The call is decided once, on the project, warehouse or namespace it names; see [Clearing a subtree](./grants.md#clearing-a-subtree).

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `ReadProjectSubtreeGrants` | `read_subtree_grants` | `SubtreeGrantReadActions` | List everything one user or role holds anywhere in the project |
| `ReadWarehouseSubtreeGrants` | `read_subtree_grants` | `SubtreeGrantReadActions` | List every grant at and under the warehouse |
| `RevokeWarehouseSubtreeGrants` | `revoke_subtree_grants` | `SubtreeGrantRevokeActions` | Revoke those grants in bulk. Permanent |
| `ReadNamespaceSubtreeGrants` | `read_subtree_grants` | `SubtreeGrantReadActions` | List every grant at and under the namespace |
| `RevokeNamespaceSubtreeGrants` | `revoke_subtree_grants` | `SubtreeGrantRevokeActions` | Revoke those grants in bulk. Permanent |

### Cedar Policy Actions

These actions read and write Cedar policies. Name `CedarPolicyActions` (every level), `CedarPolicyReadActions` (the read-only part, every level), a `<Level>CedarPolicyActions` group or a single action. The `manage_policies` privilege maps to `ProjectCedarPolicyActions` and `WarehouseCedarPolicyActions`.

| Action | Audit log `action_name` | Group | Description |
|---|---|---|---|
| `ListServerCedarPolicySources` | `list_cedar_policy_sources` | `ServerCedarPolicyActions`, `CedarPolicyReadActions` | List the server's policy sources |
| `ListServerCedarEntitySources` | `list_cedar_entity_sources` | `ServerCedarPolicyActions`, `CedarPolicyReadActions` | List the server's entity sources |
| `ListCedarPoliciesFromServerSources` | `list_cedar_policies_from_server_sources` | `ServerCedarPolicyActions`, `CedarPolicyReadActions` | Read the policies the server's sources provide |
| `EvaluateCedarPolicies` | | `ServerCedarPolicyActions`, `CedarPolicyReadActions` | Try a policy against the engine without saving it. Reserved for a future endpoint |
| `ListProjectCedarPolicies` | `list_cedar_policies` | `ProjectCedarPolicyActions`, `CedarPolicyReadActions` | List the project's policies |
| `GetProjectCedarPolicy` | `get_cedar_policy` | `ProjectCedarPolicyActions`, `CedarPolicyReadActions` | Read one of the project's policies |
| `ApplyProjectCedarPolicies` | `apply_cedar_policies` | `ProjectCedarPolicyActions` | Write the project's policies |
| `ToggleProjectPredefinedPolicy` | `toggle_predefined_policy` | `ProjectCedarPolicyActions` | Switch one predefined policy on or off for the project |
| `ResetProjectPredefinedPolicies` | `reset_predefined_policies` | `ProjectCedarPolicyActions` | Put the project back on the shipped predefined defaults |
| `ListWarehouseCedarPolicies` | `list_cedar_policies` | `WarehouseCedarPolicyActions`, `CedarPolicyReadActions` | List the warehouse's policies |
| `GetWarehouseCedarPolicy` | `get_cedar_policy` | `WarehouseCedarPolicyActions`, `CedarPolicyReadActions` | Read one of the warehouse's policies |
| `ApplyWarehouseCedarPolicies` | `apply_cedar_policies` | `WarehouseCedarPolicyActions` | Write the warehouse's policies |
| `ToggleWarehousePredefinedPolicy` | `toggle_predefined_policy` | `WarehouseCedarPolicyActions` | Switch one predefined policy on or off for the warehouse |
| `ResetWarehousePredefinedPolicies` | `reset_predefined_policies` | `WarehouseCedarPolicyActions` | Put the warehouse back on the shipped predefined defaults |

Every warehouse route checks `UseWarehouse` first, so permit `UseWarehouse` together with `WarehouseCedarPolicyActions`.

### Context-Aware Actions

Some actions include additional context information in authorization requests. This enables ABAC policies to make decisions based on properties being created, updated, or removed—for example, preventing users from modifying specific property keys.

All property contexts use the `ResourceProperties` entity type (same structure as `resource.properties`), giving you access to `.raw`, `.roles`, and `.users` on each property entry — including parsed role/user references in access-prefixed keys.

| Action                                    | Context fields                   |
|-------------------------------------------|----------------------------------|
| `CreateProject`                           | `project_name?: String`, `project_id?: String` |
| `CreateWarehouse`                         | `warehouse_name?: String`        |
| `CreateRole`                              | `role_name?: String`, `requested_provider_id?: String`, `requested_source_id?: String` |
| `CreateTag`                               | `tag_name?: String` |
| `UpdateRoleSourceSystem`                  | `requested_provider_id?: String`, `requested_source_id?: String` |
| `CreateNamespaceInWarehouse`              | `namespace_name?: String`, `initial_namespace_properties: ResourceProperties` |
| `CreateNamespaceInNamespace` | `namespace_name?: String`, `initial_namespace_properties: ResourceProperties` |
| `CreateTable`                             | `table_name?: String`, `table_id?: String`, `initial_table_properties: ResourceProperties` |
| `CreateView`                              | `view_name?: String`, `initial_view_properties: ResourceProperties` |
| `CreateGenericTableInNamespace`           | `generic_table_name?: String`, `generic_table_id?: String`, `format?: String`, `base_location?: String`, `initial_generic_table_properties: ResourceProperties` |
| `DeleteNamespace`                         | `force: Bool`, `purge: Bool`, `recursive: Bool` |
| `MoveNamespace`                           | `destination: String`, `force: Bool` |
| `AcceptMovedNamespaceInWarehouse`, `AcceptMovedNamespaceInNamespace` | `source: String` |
| `DropTable`, `DropView`                   | `force: Bool`, `purge: Bool` |
| `UpdateNamespaceProperties`               | `namespace_properties_updates: ResourceProperties`, `namespace_properties_removal: Set<String>` |
| `CommitTable`                             | `table_properties_updates: ResourceProperties`, `table_properties_removal: Set<String>` |
| `CommitView`                              | `view_properties_updates: ResourceProperties`, `view_properties_removal: Set<String>` |
| `Manage<Level>Grants`                     | `privilege: <Level>Privilege`, `grantee: Grantee`, `direction: GrantDirection` |
| `Read…SubtreeGrants`, `Revoke…SubtreeGrants` | `subtree?: SubtreeGrantScope` |
| `Toggle…PredefinedPolicy`                 | `policy_id: String`, `enabled: Bool` |

**Example**: Prevent a table from being created with an `access-owners` property that doesn't include at least one owner from the `oidc~data-governance` role:

```cedar
forbid (
    principal,
    action == Lakekeeper::Action::"CreateTable",
    resource is Lakekeeper::Namespace
)
when {
    context.initial_table_properties.hasTag("access-owners") &&
    !(Lakekeeper::Role::"<project-id>/oidc~data-governance"
        in context.initial_table_properties.getTag("access-owners").roles)
};
```
