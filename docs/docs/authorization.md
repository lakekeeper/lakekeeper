# Authorization Model
Authorization can only be enabled if Authentication is setup as well. Please check the [Authentication Docs](ToDo) for more information.

Lakekeeper ships with all default permission model that uses the CNCF project [OpenFGA](ToDo) to store and evaluate permissions. Using OpenFGA allows us to implement a powerful permission model with bi-directional inheritance that is required to efficiently manage modern lakehouses with hierarchical namespaces. With our permission model, we try to find the balance between usability and control for administrators. With our default permission model, we follow the following design principals:

* **To-Down-Inheritance**: Permissions in higher up entities are inherited to their children. For example if the `modify` privilege is granted on a `warehouse` for a principal, this principal is also able to `modify` any namespaces, including nesting ones, tables and views within it.
* **Bottom-Up-Inheritance**: Permissions on lower entities, for example tables, inherit basic navigational privileges to all higher layer principals. For example, if a user is granted the `select` privilege on table `ns1.ns2.table_1`, that user is implicitly granted limited list privileges on `ns1` and `ns2`. Only items in the direct path are presented to users. If `ns1.ns3` would exist as well, a list on `ns1` would only show `ns1.ns2`.
* **Ownership**: Owners of objects have all rights on the specific object. When principals create new objects, they automatically become owners of these objects. This enables powerful self-service szenarios where users can act autonomously in a (sub-)namespace. By default, Owners of objects are also able to access grants on objects, which enables them to expand the access to their owned objects to new users. Enabling [Managed Access](Todo) for a Warehouse or Namespace removes the `grant` privilege from owners. 
* **Separation of Data and Security**: We recommend to separate access to data from granting privileges. To enable this, the `security_admin` and `data_admin` roles have been designed to replace the inital `project_admin` that is able to perform tasks from both areas.
* **No all-powerful Admin**: A `server`'s `admin` role is the most powerful role (apart from `operator`) on the server. In order to guarantee auditability, this role can list and administrate all Projects, but doesn't have access to any data in projects. While the `admin` can assign himself the `project_admin` role for a project, this assignment is tracked by `OpenFGA` for audits.

The default permission model is focused on collaborating on data. The following assignments are available:

entity    | permissions
----------|------------------------------------------------------------------------------------------
server    | admin, operator
project   | project_admin, security_admin, data_admin, role_creator, describe, select, create, modify
warehouse | ownership, pass_grants, manage_grants, describe, select, create, modify
namespace | ownership, pass_grants, manage_grants, describe, select, create, modify
table     | ownership, pass_grants, manage_grants, describe, select, modify
view      | ownership, pass_grants, manage_grants, describe, modify
role      | assignee, ownership


## Managed Access

## Customization

