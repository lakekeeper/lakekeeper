# Authorization with Cedar <span class="lkp"></span> {#authorization-with-cedar}

!!! important "Using the Correct Cedar Schema Version"
    Always use the Cedar schema version that exactly matches your Lakekeeper deployment when developing policies. Schema mismatches can cause policy validation failures or unexpected authorization behavior. Download the schema from the Lakekeeper UI (Lakekeeper Plus 0.11.2+) or retrieve it via the `/management/v1/permissions/cedar/schema` endpoint.

<a href="api/lakekeeper.cedarschema" download class="md-button md-button--primary">
  :material-download: Download Cedar Schema
</a>

[Cedar](https://docs.cedarpolicy.com/) is an enterprise-grade, policy-based authorization system built into Lakekeeper that requires no external services. Cedar uses a declarative policy language to define access controls, making it ideal for organizations that prefer infrastructure-as-code approaches to authorization management.

Check the [Authorization Configuration](./configuration.md#authorization) for configuration options.

## How it Works

Lakekeeper uses the built-in Cedar Authorizer to evaluate whether a request is allowed. Each Cedar authorization request consists of three components:

1. **Principal**: The entity performing the request. Example: `Lakekeeper::User::"oidc~peter"` ("oidc~" prefix indicates users from the OIDC identity provider)
1. **Action**: The operation being performed. Example: `Lakekeeper::Action::"CommitTable"`
1. **Resource**: The target of the action. Example: `transactions` table in namespace `finance` (`Lakekeeper::Table::<warehouse-id>/<table-id>`)

To evaluate authorization requests, Cedar requires the following information:

1. **Policies**: Define which principals can perform which actions on which resources. Policies are provided via files (`LAKEKEEPER__CEDAR__POLICY_SOURCES__LOCAL_FILES`) or Kubernetes ConfigMaps (`LAKEKEEPER__CEDAR__POLICY_SOURCES__K8S_CM`). See [Policy Examples](#policy-examples) below.
1. **Entities**: Application data Cedar uses to make authorization decisions, such as tables (including name, ID, warehouse, namespace, properties, etc.). Lakekeeper automatically provides all required entities (Tables, Namespaces, Warehouses, etc.) for each decision. User roles are also included if present in the user's token and `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured. For scenarios where role information isn't available in tokens, you can provide external entities—see [External Entity Management](#external-entity-management).
1. **Context**: Transient request-specific data related to an action. For example, the `table_properties_updates` field is available when checking `Lakekeeper::Action::"CommitTable"`. Context is handled internally by Lakekeeper and requires no configuration.
1. **Schema**: Defines entity types recognized by the application. Lakekeeper uses a built-in schema (downloadable above) that can be customized via `LAKEKEEPER__CEDAR__SCHEMA_*` environment variables. We recommend schema customization only for advanced use cases.

Most deployments only need to configure `LAKEKEEPER__CEDAR__POLICY_SOURCES__*` and optionally `LAKEKEEPER__OPENID_ROLES_CLAIM` if role information is available in user tokens.

## RBAC and ABAC Support
Cedar supports both Role-Based Access Control (RBAC) and Attribute-Based Access Control (ABAC). RBAC grants permissions based on `Lakekeeper::Role` entities, while ABAC uses resource attributes — such as Table, View, and Namespace properties — for authorization decisions. See the ABAC examples in [Policy Examples](#policy-examples) below for complete implementations.

## Property-Based Access Control

Lakekeeper can parse roles and users directly from Table, Namespace, and View properties. This enables a powerful ABAC pattern where access control lists are stored as resource metadata, and Cedar policies grant access based on those lists — without maintaining a separate role-assignment file.

### How Properties Are Exposed to Cedar

Every Table, Namespace, and View entity carries a `properties` attribute of type `ResourceProperties`. This is a Cedar entity with typed tags — one per property key — each holding a `ResourcePropertyValue` record:

```
type ResourcePropertyValue = {
    raw:   String,        // original value as stored
    roles: Set<Role>,     // parsed Lakekeeper::Role entity references
    users: Set<User>,     // parsed Lakekeeper::User entity references
}
```

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

The `principal in <set-of-roles>` check leverages Cedar's entity hierarchy: a user is considered `in` a role if that role appears anywhere in the user's ancestry chain (as established by OIDC token claims or external entity definitions).

### Access-Control Property Keys

Properties whose key starts with one of the configured **parse prefixes** are treated as **access-control properties**. The default prefixes are `access-` and `access_`; they can be changed or disabled entirely with `LAKEKEEPER__CEDAR__PROPERTY_PARSE_PREFIXES` (see [Configuration](#configuration) below).

Access-control property values must be a JSON array of typed entity references:

| Format                                            | Description              |
|---------------------------------------------------|--------------------------|
| `role:<source-id>`                                | Short form — uses the default identity provider. Requires exactly one Authenticator to be configured. |
| `role-full:<provider>~<source-id>`                | Full form — provider name is explicit. Works with any configured identity provider. |
| `role-full:<project-uuid>/<provider>~<source-id>` | Full form with an explicit project scope. Useful in multi-project setups when referencing a role from a different project. |
| `user:<user-id>`                                  | References a specific user by their identity-provider ID (e.g. `user:oidc~alice@example.com`). |

The `provider` in `role-full:` must match one of the configured Authenticator IDs. When there is exactly one OIDC provider, `role:` (short form) automatically resolves to it; when there are multiple, you must use the full form.

**Example property values** (tagged on a Table or Namespace):

```
access-owners  = ["role-full:oidc~data-admins", "user:oidc~alice@example.com"]
access-readers = ["role:analysts", "role-full:oidc~reporting-team"]
```

All other properties (keys that do **not** match any parse prefix) are stored verbatim in `.raw`; their `.roles` and `.users` sets are always empty.

### Configuration

| Environment variable                                      | Default                  | Description |
|-----------------------------------------------------------|--------------------------|-----|
| <nobr>`LAKEKEEPER__CEDAR__PROPERTY_PARSE_PREFIXES`</nobr> | `["access_", "access-"]` | List of property key prefixes that trigger entity-reference parsing. Set to `[]` to disable parsing entirely. |

### Error Handling

| Path                                                         | Behavior      |
|--------------------------------------------------------------|---------------|
| **Read** (AuthZ checks for read/describe operations)         | Parse errors in access-prefixed properties are logged as warnings. The property is still visible in Cedar with `raw` set to the original value and empty `roles`/`users` sets. Authorization is not blocked. |
| **Write** (AuthZ checks for create/update/commit operations) | Parse errors in access-prefixed properties cause the request to be **rejected with HTTP 400**. This prevents malformed access-control data from ever being stored. |

!!! tip
    Because malformed access-control values are rejected on write, you can rely on the `roles`/`users` sets being accurate and complete during read-path authorization.

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

## External Entity Management

**Default Behavior**: Lakekeeper automatically includes `Lakekeeper::User` entities with information extracted from user tokens. When `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured, Lakekeeper also provides `Lakekeeper::Role` entities, enabling role-based policies.

**External Management**: In scenarios where role information isn't available in tokens, you can manage users and roles externally:

1. Set `LAKEKEEPER__CEDAR__EXTERNALLY_MANAGED_USER_AND_ROLES` to `true`
2. Provide entity definitions via `LAKEKEEPER__CEDAR__ENTITY_JSON_SOURCES*` configurations
3. Ensure your external entities conform to Lakekeeper's Cedar schema

See [Entity Definition Example](#entity-definition-example) below for the JSON format.

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
    ```cedar
    permit (
        principal in Lakekeeper::Role::"<role-id>", // Role id as contained in the user's token
        action,
        resource
    );
    ```

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
    ```cedar
    permit (
        principal in Lakekeeper::Role::"<role-id>",
        action in
            [
                Lakekeeper::Action::"WarehouseDescribeActions",
                Lakekeeper::Action::"NamespaceDescribeActions",
                Lakekeeper::Action::"TableSelectActions",
                Lakekeeper::Action::"ViewDescribeActions"
            ],
        resource
    ) when {
        (resource has warehouse && resource.warehouse.name == "dev") ||
        (resource is Lakekeeper::Warehouse && resource.name == "dev")
    };
    ```

??? example "Read access to a warehouse and all its contents in multi-project setups"
    ```cedar
    permit (
        principal in Lakekeeper::Role::"<role-id>",
        action in
            [
                Lakekeeper::Action::"WarehouseDescribeActions",
                Lakekeeper::Action::"NamespaceDescribeActions",
                Lakekeeper::Action::"TableSelectActions",
                Lakekeeper::Action::"ViewDescribeActions"
            ],
        resource in Lakekeeper::Project::"<id of the project>"
    ) when {
        (resource has warehouse && resource.warehouse.name == "dev") ||
        (resource is Lakekeeper::Warehouse && resource.name == "dev")
    };
    ```

??? example "ABAC: Role-based table access using static role membership"

    This example grants read/write access to tables tagged with an `access-role` property matching the requesting user's role — using traditional RBAC role membership. The `access-role-*` keys use the `access-` prefix so Lakekeeper parses them as entity references; the `.raw` field stores the original string value for backward compatibility.

    ```cedar
    @id("abac-role-based-access-marketing-select")
    @description("ABAC: Allow Read access to tables tagged with access-role-select:marketing to the marketing-select role")
    permit (
        principal in Lakekeeper::Role::"marketing-select",
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
        principal in Lakekeeper::Role::"marketing-modify",
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
        principal in Lakekeeper::Role::"marketing-admin",
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
        `principal in resource.properties.getTag("access-readers").roles` uses Cedar's built-in entity hierarchy. A user is considered `in` a role if that role appears as an ancestor in the user entity's parent chain — exactly the same mechanism used for static role-based policies. This means the access control lists stored in table properties work seamlessly with both token-extracted roles (`LAKEKEEPER__OPENID_ROLES_CLAIM`) and externally managed role assignments.

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
Lakekeeper provides the following entities internally to Cedar: Server, Project, Warehouse, Namespace, Table, View. Additionally, if `LAKEKEEPER__OPENID_ROLES_CLAIM` is set, also User and Roles are provided to Cedar. A request on a table called "my-table" in Namespace "my-namespace" provides the following entities to Cedar:

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
                "roles": []
            },
            "parents": []
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
            "display_name": "machine-user-1"
        },
        "parents": [
            {
                "type": "Lakekeeper::Role",
                "id": "data-engineering"
            }
        ]
    },
    {
        "uid": {
            "type": "Lakekeeper::Role",
            "id": "data-engineering"
        },
        "attrs": {
            "name": "DataEngineering",
            "project": {
                "__entity": {
                    "type": "Lakekeeper::Project",
                    "id": "00000000-0000-0000-0000-000000000000"
                }
            }
        },
        "parents": [
            {
                "type": "Lakekeeper::Role",
                "id": "warehouse-1-admins"
            }
        ]
    }
]
```

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


## Cedar Actions

The following tables document all available Cedar actions. Use action groups for broad permissions or individual actions for fine-grained control.

### Server Actions

| Action                                            | Description              |
|---------------------------------------------------|--------------------------|
| `ListServerCedarEntitySources`                    | List Cedar entity sources configured at server level |
| <nobr>`ListCedarPoliciesFromServerSources`</nobr> | View Cedar policies from server-level sources |
| `ListServerCedarPolicySources`                    | List Cedar policy sources configured at server level |
| `CreateProject`                                   | Create new projects      |
| `UpdateUsers`                                     | Modify user information  |
| `DeleteUsers`                                     | Remove users from the system |
| `ListUsers`                                       | View all users in the system |
| `ProvisionUsers`                                  | Provision new users      |
| `IntrospectServerAuthorization`                   | Check access permissions on the server for **other** users (applies when `identity` parameter doesn't match current user) |

### Project Actions

| Action                                        | Description                  |
|-----------------------------------------------|------------------------------|
| `GetProjectMetadata`                          | View project details and configuration |
| `ListWarehouses`                              | List all warehouses in the project |
| `IncludeProjectInList`                        | Include project in list operations (visibility) |
| `ListRoles`                                   | List all roles in the project |
| `SearchRoles`                                 | Search for roles in the project |
| `GetProjectEndpointStatistics`                | View API usage statistics for the project |
| `GetProjectTaskQueueConfig`                   | View task queue configuration for the project |
| `GetProjectTasks`                             | List background tasks in the project |
| <nobr>`IntrospectProjectAuthorization`</nobr> | Check access permissions on the project for other users |
| `CreateWarehouse`                             | Create new warehouses in the project |
| `DeleteProject`                               | Delete the project           |
| `RenameProject`                               | Change project name          |
| `CreateRole`                                  | Create new roles in the project |
| `ModifyProjectTaskQueueConfig`                | Update task queue configuration |
| `ControlProjectTasks`                         | Manage background tasks (cancel, retry, etc.) |

The following Action Groups are available: `ProjectDescribeActions` (read-only), `ProjectModifyActions` (includes Describe), `ProjectActions` (all)

### Role Actions

| Action                                     | Description                     |
|--------------------------------------------|---------------------------------|
| `AssumeRole`                               | Assume this role (use role's permissions) |
| `DeleteRole`                               | Delete the role                 |
| `UpdateRole`                               | Modify role properties          |
| `ReadRole`                                 | View role details               |
| `ReadRoleMetadata`                         | View role metadata              |
| <nobr>`IntrospectRoleAuthorization`</nobr> | Check access permissions on the role for other users |

The following Action Groups are available: `RoleActions` (all role operations)

### Warehouse Actions

| Action                                          | Description                |
|-------------------------------------------------|----------------------------|
| `UseWarehouse`                                  | Use the warehouse (required for any warehouse operations) |
| `ListNamespacesInWarehouse`                     | List namespaces in the warehouse |
| `GetWarehouseMetadata`                          | View warehouse configuration and details |
| `GetConfig`                                     | Get warehouse configuration for clients |
| `IncludeWarehouseInList`                        | Include warehouse in list operations (visibility) |
| `ListDeletedTabulars`                           | List soft-deleted tables and views |
| `GetTaskQueueConfig`                            | View task queue configuration |
| `GetAllTasks`                                   | List all background tasks in the warehouse |
| `ListEverythingInWarehouse`                     | List all objects (namespaces, tables, views) in warehouse |
| `GetWarehouseEndpointStatistics`                | View API usage statistics for the warehouse |
| <nobr>`IntrospectWarehouseAuthorization`</nobr> | Check access permissions on the warehouse for other users |
| `DeleteWarehouse`                               | Delete the warehouse       |
| `UpdateStorage`                                 | Modify storage configuration |
| `UpdateStorageCredential`                       | Update storage credentials |
| `DeactivateWarehouse`                           | Deactivate the warehouse (suspend operations) |
| `ActivateWarehouse`                             | Activate a deactivated warehouse |
| `RenameWarehouse`                               | Change warehouse name      |
| `ModifySoftDeletion`                            | Configure soft-deletion settings |
| `ModifyTaskQueueConfig`                         | Update task queue configuration |
| `ControlAllTasks`                               | Manage all background tasks |
| `SetWarehouseProtection`                        | Enable/disable deletion protection |
| `CreateNamespaceInWarehouse`                    | Create namespaces directly in the warehouse |

The following Action Groups are available: `WarehouseDescribeActions` (read-only), `WarehouseModifyActions` (includes Describe), `WarehouseActions` (all)

### Namespace Actions

| Action                                          | Description                |
|-------------------------------------------------|----------------------------|
| `ListEverythingInNamespace`                     | List all objects (tables, views, child namespaces) in namespace |
| `GetNamespaceMetadata`                          | View namespace properties and configuration |
| `IncludeNamespaceInList`                        | Include namespace in list operations (visibility) |
| `ListTables`                                    | List tables in the namespace |
| `ListViews`                                     | List views in the namespace |
| `ListNamespacesInNamespace`                     | List child namespaces      |
| <nobr>`IntrospectNamespaceAuthorization`</nobr> | Check access permissions on the namespace for other users |
| `DeleteNamespace`                               | Delete the namespace       |
| `SetNamespaceProtection`                        | Enable/disable deletion protection |
| `CreateTable`                                   | Create tables in the namespace |
| `CreateView`                                    | Create views in the namespace |
| `CreateNamespaceInNamespace`                    | Create child namespaces    |
| `UpdateNamespaceProperties`                     | Modify namespace properties |

The following Action Groups are available: `NamespaceDescribeActions` (read-only), `NamespaceModifyActions` (includes Describe), `NamespaceActions` (all)

### Table Actions

| Action                                      | Description                    |
|---------------------------------------------|--------------------------------|
| `GetTableMetadata`                          | View table schema, metadata, and configuration |
| `IncludeTableInList`                        | Include table in list operations (visibility) |
| `GetTableTasks`                             | List background tasks for the table |
| `ReadTableData`                             | Read data from the table (SELECT queries) |
| <nobr>`IntrospectTableAuthorization`</nobr> | Check access permissions on the table for other users |
| `DropTable`                                 | Delete the table               |
| `WriteTableData`                            | Write data to the table (INSERT, UPDATE, DELETE) |
| `RenameTable`                               | Change table name or move to different namespace |
| `UndropTable`                               | Restore a soft-deleted table   |
| `ControlTableTasks`                         | Manage table background tasks  |
| `SetTableProtection`                        | Enable/disable deletion protection |
| `CommitTable`                               | Commit table changes (schema updates, snapshots) |

*Action Groups*: `TableDescribeActions` (metadata only), `TableSelectActions` (includes Describe + read data), `TableModifyActions` (includes Describe + Select + modifications), `TableActions` (all)

### View Actions

| Action                                     | Description                     |
|--------------------------------------------|---------------------------------|
| `GetViewMetadata`                          | View view definition and metadata |
| `IncludeViewInList`                        | Include view in list operations (visibility) |
| `GetViewTasks`                             | List background tasks for the view |
| <nobr>`IntrospectViewAuthorization`</nobr> | Check access permissions on the view for other users |
| `DropView`                                 | Delete the view                 |
| `RenameView`                               | Change view name or move to different namespace |
| `UndropView`                               | Restore a soft-deleted view     |
| `ControlViewTasks`                         | Manage view background tasks    |
| `SetViewProtection`                        | Enable/disable deletion protection |
| `CommitView`                               | Commit view changes (update definition, properties) |

The following Action Groups are available: `ViewDescribeActions` (metadata only), `ViewModifyActions` (includes Describe + modifications), `ViewActions` (all)

### Context-Aware Actions

Some actions include additional context information in authorization requests. This enables ABAC policies to make decisions based on properties being created, updated, or removed—for example, preventing users from modifying specific property keys.

All property contexts use the `ResourceProperties` entity type (same structure as `resource.properties`), giving you access to `.raw`, `.roles`, and `.users` on each property entry — including parsed role/user references in access-prefixed keys.

| Action                       | Context fields                                |
|------------------------------|-----------------------------------------------|
| `CreateNamespaceInWarehouse` | `initial_namespace_properties: ResourceProperties` |
| `CreateNamespaceInNamespace` | `initial_namespace_properties: ResourceProperties` |
| `CreateTable`                | `initial_table_properties: ResourceProperties` |
| `CreateView`                 | `initial_view_properties: ResourceProperties` |
| `UpdateNamespaceProperties`  | `namespace_properties_updated: ResourceProperties`, `namespace_properties_removed: Set<String>` |
| `CommitTable`                | `table_properties_updates: ResourceProperties`, `table_properties_removal: Set<String>` |
| `CommitView`                 | `view_properties_updates: ResourceProperties`, `view_properties_removal: Set<String>` |

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
