# Authorization

## Overview

Authentication verifies *who* you are, while authorization determines *what* you can do.

Authorization can only be enabled if Authentication is enabled. Please check the [Authentication Docs](./authentication.md) for more information.

Lakekeeper currently supports the following Authorizers:

* **AllowAll**: A simple authorizer that allows all requests. This is mainly intended for development and testing purposes.
* **OpenFGA**: A fine-grained authorization system based on the CNCF project [OpenFGA](https://openfga.dev). Please find more information in the [Authorization with OpenFGA](#authorization-with-openfga) section. OpenFGA requires an additional OpenFGA service to be deployed (this is included in our self-contained examples and our helm charts).
* **Cedar**<span class="lkp"></span>: An enterprise-grade policy-based authorization system based on [Cedar](https://cedarpolicy.com). The cedar authorizer is built into Lakekeeper and requires no additional external services. Please find more information in the [Authorization with Cedar](#authorization-with-cedar) section.
* **Custom**: Lakekeeper supports custom authorizers via the `Authorizer` trait.

## Authorization with OpenFGA

Lakekeeper can use [OpenFGA](https://openfga.dev) to store and evaluate permissions. OpenFGA provides bi-directional inheritance, which is key for managing hierarchical namespaces in modern lakehouses. For query engines like Trino, Lakekeeper's OPA bridge translates OpenFGA permissions into Open Policy Agent (OPA) format. See the [OPA Bridge Guide](./opa.md) for details.

Check the [Authorization Configuration](./configuration.md#authorization) for setup details.

### Grants
The default permission model is focused on collaborating on data. Permissions are additive. The underlying OpenFGA model is defined in [`schema.fga` on GitHub](https://github.com/lakekeeper/lakekeeper/blob/main/authz/openfga/). The following grants are available:

| Entity    | Grant                                                            |
|-----------|------------------------------------------------------------------|
| server    | admin, operator                                                  |
| project   | project_admin, security_admin, data_admin, role_creator, describe, select, create, modify |
| warehouse | ownership, pass_grants, manage_grants, describe, select, create, modify |
| namespace | ownership, pass_grants, manage_grants, describe, select, create, modify |
| table     | ownership, pass_grants, manage_grants, describe, select, modify  |
| view      | ownership, pass_grants, manage_grants, describe, modify          |
| role      | assignee, ownership                                              |


##### Ownership
Owners of objects have all rights on the specific object. When principals create new objects, they automatically become owners of these objects. This enables powerful self-service szenarios where users can act autonomously in a (sub-)namespace. By default, Owners of objects are also able to access grants on objects, which enables them to expand the access to their owned objects to new users. Enabling [Managed Access](#managed-access) for a Warehouse or Namespace removes the `grant` privilege from owners.

##### Server: Admin
A `server`'s `admin` role is the most powerful role (apart from `operator`) on the server. In order to guarantee auditability, this role can list and administrate all Projects, but does not have access to data in projects. While the `admin` can assign himself the `project_admin` role for a project, this assignment is tracked by `OpenFGA` for audits. `admin`s can also manage all projects (but no entities within it), server settings and users.

##### Server: Operator
The `operator` has unrestricted access to all objects in Lakekeeper. It is designed to be used by technical users (e.g., a Kubernetes Operator) managing the Lakekeeper deployment.

##### Project: Security Admin
A `security_admin` in a project can manage all security-related aspects, including grants and ownership for the project and all objects within it. However, they cannot modify or access the content of any object, except for listing and browsing purposes.

##### Project: Data Admin
A `data_admin` in a project can manage all data-related aspects, including creating, modifying, and deleting objects within the project. They can delegate the `data_admin` role they already hold (for example to team members), but they do not have general grant or ownership administration capabilities.

##### Project: Admin
A `project_admin` in a project has the combined responsibilities of both `security_admin` and `data_admin`. They can manage all security-related aspects, including grants and ownership, as well as all data-related aspects, including creating, modifying, and deleting objects within the project.

##### Project: Role Creator
A `role_creator` in a project can create new roles within it. This role is essential for delegating the creation of roles without granting broader administrative privileges.

##### Describe
The `describe` grant allows a user to view metadata and details about an object without modifying it. This includes listing objects and viewing their properties. The `describe` grant is inherited down the object hierarchy, meaning if a user has the `describe` grant on a higher-level entity, they can also describe all child entities within it. The `describe` grant is implicitly included with the `select`, `create`, and `modify` grants.

##### Select
The `select` grant allows a user to read data from an object, such as tables or views. This includes querying and retrieving data. The `select` grant is inherited down the object hierarchy, meaning if a user has the `select` grant on a higher-level entity, they can select all views and tables within it. The `select` grant implicitly includes the `describe` grant.

##### Create
The `create` grant allows a user to create new objects within an entity, such as tables, views, or namespaces. The `create` grant is inherited down the object hierarchy, meaning if a user has the `create` grant on a higher-level entity, they can also create objects within all child entities. The `create` grant implicitly includes the `describe` grant.

##### Modify
The `modify` grant allows a user to change the content or properties of an object, such as updating data in tables or altering views. The `modify` grant is inherited down the object hierarchy, meaning if a user has the `modify` grant on a higher-level entity, they can also modify all child entities within it. The `modify` grant implicitly includes the `select` and `describe` grants.

##### Pass Grants
The `pass_grants` grant allows a user to pass their own privileges to other users. This means that if a user has certain permissions on an object, they can grant those same permissions to others. However, the `pass_grants` grant does not include the ability to pass the `pass_grants` privilege itself.

##### Manage Grants
The `manage_grants` grant allows a user to manage all grants on an object, including creating, modifying, and revoking grants. This also includes `manage_grants` and `pass_grants`.

### Inheritance

* **Top-Down-Inheritance**: Permissions in higher up entities are inherited to their children. For example if the `modify` privilege is granted on a `warehouse` for a principal, this principal is also able to `modify` any namespaces, including nesting ones, tables and views within it.
* **Bottom-Up-Inheritance**: Permissions on lower entities, for example tables, inherit basic navigational privileges to all higher layer principals. For example, if a user is granted the `select` privilege on table `ns1.ns2.table_1`, that user is implicitly granted limited list privileges on `ns1` and `ns2`. Only items in the direct path are presented to users. If `ns1.ns3` would exist as well, a list on `ns1` would only show `ns1.ns2`.

### Managed Access
Managed access is a feature designed to provide stricter control over access privileges within Lakekeeper. It is particularly useful for organizations that require a more restrictive access control model to ensure data security and compliance.

In some cases, the default ownership model, which grants all privileges to the creator of an object, can be too permissive. This can lead to situations where non-admin users unintentionally share data with unauthorized users by granting privileges outside the scope defined by administrators. Managed access addresses this concern by removing the `grant` privilege from owners and centralizing the management of access privileges.

With managed access, admin-like users can define access privileges on high-level container objects, such as warehouses or namespaces, and ensure that all child objects inherit these privileges. This approach prevents non-admin users from granting privileges that are not authorized by administrators, thereby reducing the risk of unintentional data sharing and enhancing overall security.

Managed access combines elements of Role-Based Access Control (RBAC) and Discretionary Access Control (DAC). While RBAC allows privileges to be assigned to roles and users, DAC assigns ownership to the creator of an object. By integrating managed access, Lakekeeper provides a balanced access control model that supports both self-service analytics and data democratization while maintaining strict security controls.

Managed access can be enabled or disabled for warehouses and namespaces using the UI or the `../managed-access` Endpoints. Managed access settings are inherited down the object hierarchy, meaning if managed access is enabled on a higher-level entity, it applies to all child entities within it.

### Best Practices
We recommend separating access to data from the ability to grant privileges. To achieve this, the `security_admin` and `data_admin` roles divide the responsibilities of the initial `project_admin`, who has the authority to perform tasks in both areas.

### OpenFGA in Production
When deploying OpenFGA in production environments, ensure you follow the [OpenFGA Production Checklist](https://openfga.dev/docs/best-practices/running-in-production).

Lakekeeper includes [Query Consistency](https://openfga.dev/docs/interacting/consistency) specifications with each authorization request to OpenFGA. For most operations, `MINIMIZE_LATENCY` consistency provides optimal performance while maintaining sufficient data consistency guarantees.

For medium to large-scale deployments, we strongly recommend enabling caching in OpenFGA and increasing the database connection pool limits. These optimizations significantly reduce database load and improve authorization latency. Configure the following environment variables in OpenFGA (written for version 1.10). You may increase the number of connections further if your database deployment can handle additional connections:

```sh
OPENFGA_DATASTORE_MAX_OPEN_CONNS=200
OPENFGA_DATASTORE_MAX_IDLE_CONNS=100
OPENFGA_CACHE_CONTROLLER_ENABLED=true
OPENFGA_CHECK_QUERY_CACHE_ENABLED=true
OPENFGA_CHECK_ITERATOR_CACHE_ENABLED=true
```

## Authorization with Cedar <span class="lkp"></span> {#authorization-with-cedar}

!!! important "Using the Correct Cedar Schema Version"
    Always use the Cedar schema version that exactly matches your Lakekeeper deployment when developing policies. Schema mismatches can cause policy validation failures or unexpected authorization behavior. Download the schema from the Lakekeeper UI (Lakekeeper Plus 0.11.2+) or retrieve it via the `/management/v1/permissions/cedar/schema` endpoint.

<a href="api/lakekeeper.cedarschema" download class="md-button md-button--primary">
  :material-download: Download Cedar Schema
</a>

[Cedar](https://docs.cedarpolicy.com/) is an enterprise-grade, policy-based authorization system built into Lakekeeper that requires no external services. Cedar uses a declarative policy language to define access controls, making it ideal for organizations that prefer infrastructure-as-code approaches to authorization management.

Check the [Authorization Configuration](./configuration.md#authorization) for configuration options.

### How it Works

Lakekeeper uses the built-in Cedar Authorizer to evaluate whether a request is allowed. Each Cedar authorization request consists of three components:

1. **Principal**: The entity performing the request. Example: `Lakekeeper::User::"oidc~peter"` ("oidc~" prefix indicates users from the OIDC identity provider)
1. **Action**: The operation being performed. Example: `Lakekeeper::Action::"CommitTable"`
1. **Resource**: The target of the action. Example: `transactions` table in namespace `finance` (`Lakekeeper::Table::<warehouse-id>/<table-id>`)

To evaluate authorization requests, Cedar requires the following information:

1. **Policies**: Define which principals can perform which actions on which resources. Policies are provided via files (`LAKEKEEPER__CEDAR__POLICY_SOURCES__LOCAL_FILES`) or Kubernetes ConfigMaps (`LAKEKEEPER__CEDAR__POLICY_SOURCES__K8S_CM`). See [Policy Examples](#policy-examples) below.
1. **Entities**: Application data Cedar uses to make authorization decisions, such as tables (including name, ID, warehouse, namespace, properties, etc.). Lakekeeper automatically provides all required entities (Tables, Namespaces, Warehouses, etc.) for each decision. User roles are also included if present in the user's token and `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured. For scenarios where role information isn't available in tokens, you can provide external entities—see [External Entity Management](#external-entity-management).
1. **Context**: Transient request-specific data related to an action. For example, the `table_properties_updated` field is available when checking `Lakekeeper::Action::"CommitTable"`. Context is handled internally by Lakekeeper and requires no configuration.
1. **Schema**: Defines entity types recognized by the application. Lakekeeper uses a built-in schema (downloadable above) that can be customized via `LAKEKEEPER__CEDAR__SCHEMA_*` environment variables. We recommend schema customization only for advanced use cases.

Most deployments only need to configure `LAKEKEEPER__CEDAR__POLICY_SOURCES__*` and optionally `LAKEKEEPER__OPENID_ROLES_CLAIM` if role information is available in user tokens.

### Entity Hierarchy and Context

For each authorization request, Lakekeeper provides Cedar with the complete entity hierarchy from the requested resource to the server root. This hierarchical context ensures policies have full visibility into the resource's location and relationships.

**Example**: When a user queries table `ns1.ns2.transactions` in warehouse `wh-1` within project `my-project`, Cedar sees the following entities:

- `Lakekeeper::Server::<server-id>` (root)
- `Lakekeeper::Project::"<project-my-project-id>"`
- `Lakekeeper::Warehouse::"<warehouse-wh-1-id>"` (parent: Project)
- `Lakekeeper::Namespace::"<namespace-ns1-id>"` (parent: Warehouse)
- `Lakekeeper::Namespace::"<namespace-ns2-id>"` (parent: ns1)
- `Lakekeeper::Table::"<table-transactions-id>"` (parent: ns2)

This hierarchy allows policies to reference any level in the path — you can grant access based on warehouse names, namespace hierarchies, or specific table properties.

### External Entity Management

**Default Behavior**: Lakekeeper automatically includes `Lakekeeper::User` entities with information extracted from user tokens. When `LAKEKEEPER__OPENID_ROLES_CLAIM` is configured, Lakekeeper also provides `Lakekeeper::Role` entities, enabling role-based policies.

**External Management**: In scenarios where role information isn't available in tokens, you can manage users and roles externally:

1. Set `LAKEKEEPER__CEDAR__EXTERNALLY_MANAGED_USER_AND_ROLES` to `true`
2. Provide entity definitions via `LAKEKEEPER__CEDAR__ENTITY_JSON_SOURCES*` configurations
3. Ensure your external entities conform to Lakekeeper's Cedar schema

See [Entity Definition Example](#entity-definition-example) below for the JSON format.

**Schema Reference**: The Lakekeeper Cedar schema defines all available entity types, attributes, and actions. All entities and policies are validated against this schema on startup and refresh. Download the schema above or view it on [GitHub](https://github.com/lakekeeper/lakekeeper/tree/main/docs/docs/api).


### Policy Examples

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

### Entity Definition Example
Lakekeeper provides the following entities internally to Cedar: Server, Project, Warehouse, Namespace, Table, View. Additionally, if `` is set, also User and Roles are provided to Cedar. A request on a table called "my-table" in Namespace "my-namespace" provides the following entities to Cedar:

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
                "properties": [],
                "name": "transactions",
                "project": {
                    "__entity": {
                        "type": "Lakekeeper::Project",
                        "id": "019c192f-0613-7422-90f1-7dd6b09f033c"
                    }
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
                "name": "my-namespace",
                "properties": [
                    {
                        "key": "location",
                        "value": "s3://tests/075272e23ed548d8bfd722a7a383cd50/019c192f-18c2-7f93-848f-542d8f32bc3c"
                    }
                ]
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

### Policy and Entity Management

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


### Cedar Actions

The following tables document all available Cedar actions. Use action groups for broad permissions or individual actions for fine-grained control.

##### Server Actions

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

##### Project Actions

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

##### Role Actions

| Action                                     | Description                     |
|--------------------------------------------|---------------------------------|
| `AssumeRole`                               | Assume this role (use role's permissions) |
| `DeleteRole`                               | Delete the role                 |
| `UpdateRole`                               | Modify role properties          |
| `ReadRole`                                 | View role details               |
| `ReadRoleMetadata`                         | View role metadata              |
| <nobr>`IntrospectRoleAuthorization`</nobr> | Check access permissions on the role for other users |

The following Action Groups are available: `RoleActions` (all role operations)

##### Warehouse Actions

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

##### Namespace Actions

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

##### Table Actions

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

##### View Actions

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

##### Context-Aware Actions

Some actions include additional context information in authorization requests:

- `CreateNamespaceInWarehouse`, `CreateNamespaceInNamespace`: Include `requested_namespace_properties`
- `CreateTable`: Includes `requested_table_properties`
- `CreateView`: Includes `requested_view_properties`
- `UpdateNamespaceProperties`: Includes `namespace_properties_updated` and `namespace_properties_removed`
- `CommitTable`: Includes `table_properties_updated` and `table_properties_removed`
- `CommitView`: Includes `view_properties_updated` and `view_properties_removed`
