# Authorization

Authorization can only be enabled if Authentication is enabled. Please check the [Authentication Docs](./authentication.md) for more information.

Lakekeeper's default permission model uses the CNCF project [OpenFGA](http://openfga.dev) to store and evaluate permissions. OpenFGA enables a powerful permission model with bi-directional inheritance, essential for managing modern lakehouses with hierarchical namespaces. Our model balances usability and control for administrators. In addition to OpenFGA, Lakekeeper's OPA bridge provides an additional translation layer that allows query engines such as trino to access Lakekeeper's permissions via Open Policy Agent (OPA). Please find more information in the [OPA Bridge Guide](./opa.md).

Please check the [Authorization Configuration](./configuration.md#authorization) for details on enabling Authorization with Lakekeeper.

## Grants
The default permission model is focused on collaborating on data. Permissions are additive. The underlying OpenFGA model is defined in [`schema.fga` on Github](https://github.com/lakekeeper/lakekeeper/blob/main/authz/openfga/v1/schema.fga). The following grants are available:

| Entity    | Grant                                                            |
|-----------|------------------------------------------------------------------|
| server    | admin, operator                                                  |
| project   | project_admin, security_admin, data_admin, role_creator, describe, select, create, modify |
| warehouse | ownership, pass_grants, manage_grants, describe, select, create, modify |
| namespace | ownership, pass_grants, manage_grants, describe, select, create, modify |
| table     | ownership, pass_grants, manage_grants, describe, select, modify  |
| view      | ownership, pass_grants, manage_grants, describe, modify          |
| role      | assignee, ownership                                              |


### Ownership
Owners of objects have all rights on the specific object. When principals create new objects, they automatically become owners of these objects. This enables powerful self-service szenarios where users can act autonomously in a (sub-)namespace. By default, Owners of objects are also able to access grants on objects, which enables them to expand the access to their owned objects to new users. Enabling [Managed Access](#managed-access) for a Warehouse or Namespace removes the `grant` privilege from owners.

### Server: Admin
A `server`'s `admin` role is the most powerful role (apart from `operator`) on the server. In order to guarantee auditability, this role can list and administrate all Projects, but does not have access to data in projects. While the `admin` can assign himself the `project_admin` role for a project, this assignment is tracked by `OpenFGA` for audits. `admin`s can also manage all projects (but no entities within it), server settings and users.

### Server: Operator
The `operator` has unrestricted access to all objects in Lakekeeper. It is designed to be used by technical users (e.g., a Kubernetes Operator) managing the Lakekeeper deployment.

### Project: Security Admin
A `security_admin` in a project can manage all security-related aspects, including grants and ownership for the project and all objects within it. However, they cannot modify or access the content of any object, except for listing and browsing purposes.

### Project: Data Admin
A `data_admin` in a project can manage all data-related aspects, including creating, modifying, and deleting objects within the project. However, they cannot grant privileges or manage ownership.

### Project: Admin
A `project_admin` in a project has the combined responsibilities of both `security_admin` and `data_admin`. They can manage all security-related aspects, including grants and ownership, as well as all data-related aspects, including creating, modifying, and deleting objects within the project.

### Project: Role Creator
A `role_creator` in a project can create new roles within it. This role is essential for delegating the creation of roles without granting broader administrative privileges.

### Describe
The `describe` grant allows a user to view metadata and details about an object without modifying it. This includes listing objects and viewing their properties. The `describe` grant is inherited down the object hierarchy, meaning if a user has the `describe` grant on a higher-level entity, they can also describe all child entities within it. The `describe` grant is implicitly included with the `select`, `create`, and `modify` grants.

### Select
The `select` grant allows a user to read data from an object, such as tables or views. This includes querying and retrieving data. The `select` grant is inherited down the object hierarchy, meaning if a user has the `select` grant on a higher-level entity, they can select all views and tables within it. The `select` grant implicitly includes the `describe` grant.

### Create
The `create` grant allows a user to create new objects within an entity, such as tables, views, or namespaces. The `create` grant is inherited down the object hierarchy, meaning if a user has the `create` grant on a higher-level entity, they can also create objects within all child entities. The `create` grant implicitly includes the `describe` grant.

### Modify
The `modify` grant allows a user to change the content or properties of an object, such as updating data in tables or altering views. The `modify` grant is inherited down the object hierarchy, meaning if a user has the `modify` grant on a higher-level entity, they can also modify all child entities within it. The `modify` grant implicitly includes the `select` and `describe` grants.

### Pass Grants
The `pass_grants` grant allows a user to pass their own privileges to other users. This means that if a user has certain permissions on an object, they can grant those same permissions to others. However, the `pass_grants` grant does not include the ability to pass the `pass_grants` privilege itself.

### Manage Grants
The `manage_grants` grant allows a user to manage all grants on an object, including creating, modifying, and revoking grants. This also includes `manage_grants` and `pass_grants`.

## Inheritance

* **To-Down-Inheritance**: Permissions in higher up entities are inherited to their children. For example if the `modify` privilege is granted on a `warehouse` for a principal, this principal is also able to `modify` any namespaces, including nesting ones, tables and views within it.
* **Bottom-Up-Inheritance**: Permissions on lower entities, for example tables, inherit basic navigational privileges to all higher layer principals. For example, if a user is granted the `select` privilege on table `ns1.ns2.table_1`, that user is implicitly granted limited list privileges on `ns1` and `ns2`. Only items in the direct path are presented to users. If `ns1.ns3` would exist as well, a list on `ns1` would only show `ns1.ns2`.

## Managed Access
Managed access is a feature designed to provide stricter control over access privileges within Lakekeeper. It is particularly useful for organizations that require a more restrictive access control model to ensure data security and compliance.

In some cases, the default ownership model, which grants all privileges to the creator of an object, can be too permissive. This can lead to situations where non-admin users unintentionally share data with unauthorized users by granting privileges outside the scope defined by administrators. Managed access addresses this concern by removing the `grant` privilege from owners and centralizing the management of access privileges.

With managed access, admin-like users can define access privileges on high-level container objects, such as warehouses or namespaces, and ensure that all child objects inherit these privileges. This approach prevents non-admin users from granting privileges that are not authorized by administrators, thereby reducing the risk of unintentional data sharing and enhancing overall security.

Managed access combines elements of Role-Based Access Control (RBAC) and Discretionary Access Control (DAC). While RBAC allows privileges to be assigned to roles and users, DAC assigns ownership to the creator of an object. By integrating managed access, Lakekeeper provides a balanced access control model that supports both self-service analytics and data democratization while maintaining strict security controls.

Managed access can be enabled or disabled for warehouses and namespaces using the UI or the `../managed-access` Endpoints. Managed access settings are inherited down the object hierarchy, meaning if managed access is enabled on a higher-level entity, it applies to all child entities within it.

## Best Practices
We recommend separating access to data from the ability to grant privileges. To achieve this, the `security_admin` and `data_admin` roles divide the responsibilities of the initial `project_admin`, who has the authority to perform tasks in both areas.
