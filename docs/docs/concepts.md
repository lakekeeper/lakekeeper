# Concepts

## Entity Hierarchy

In addition to entities defined in the Apache Iceberg specification or the REST specification (Namespaces, Tables, etc.), Lakekeeper introduces new entities for permission management and multi-tenant setups. The following entities are available in Lakekeeper:

<br>
<figure markdown="span">
  ![Lakekeeper Entity Hierarchy](../../assets/entity-hierarchy-v1.svg){ width="100%" }
  <figcaption>Lakekeeper Entity Hierarchy</figcaption>
</figure>
<br>

Project, Server, User and Roles are entities unknown to the Iceberg Rest Specification.Lakekeeper serves two APIs:

1. The Iceberg REST API is served at endpoints prefixed with `/catalog`. External query engines connect to this API to interact with the Lakekeeper. Lakekeeper also implements the S3 remote signing API which is hosted at `/<warehouse-id>/v1/aws/s3/sign`. ToDo: Swagger
1. The Lakekeeper Management API is served at endpoints prefixed with `/management`. It is used to configure Lakekeeper and manage entities that are not part of the Iceberg REST Catalog specification, such as permissions.

### Server
The Server is the highest entity in Lakekeeper, representing a single instance or a cluster of Lakekeeper pods sharing a common state. Each server has a unique identifier (UUID). By default, this `Server ID` is set to `00000000-0000-0000-0000-000000000000`. It can be changed by setting the `LAKEKEEPER__SERVER_ID` environment variable. We recommend to not set the `Server ID` explicitly, unless multiple Lakekeeper instances share a single Authorization system. The `Server ID` may not be changed after the initial [bootstrapping](mysubheadline) or permissions might not work.

### Project
For single-company setups, we recommend using a single Project setup, which is the default. For multi-project/multi-tenant setups, please check our dedicated guide (ToDo: Link). Unless `LAKEKEEPER__ENABLE_DEFAULT_PROJECT` is explicitly set to `false`, a default project is created during [bootstrapping](bootstrapping)! with the nil UUID.

### Warehouse
Each Project can contain multiple Warehouses. Query engines connect to Lakekeeper by specifying a Warehouse name in the connection configuration.

Each Warehouse is associated with a unique location on object stores. Never share locations between Warehouses to ensure no data is leaked via vended credentials. Each Warehouse stores information on how to connect to its location via a `storage-profile` and an optional `storage-credential`.

Warehouses can be configured to use [Soft-Deletes](ToDo: Link). When enabled, tables are not eagerly deleted but kept in a deleted state for a configurable amount of time. During this time, they can be restored. Please find more information on Soft-Deletes [here](ToDo: Link). Please not that Warehouses and Namespaces cannot be deleted via the `/catalog` API while child objects are present. This includes soft-deleted Tables. A cascade-drop API is added in one of the next releases as part of the `/management` API.

### Namespaces
Each Warehouses can contain multiple Namespaces. Namespaces can be nested and serve as containers for Namespaces, Tables and Views. Using the `/catalog` API, a Namespace cannot be dropped unless it is empty. A cascade-drop API is added in one of the next releases as part of the `/management` API.

### Tables & Views
Each Namespace can contain multiple Tables and Views. When creating new Tables and Views, we recommend to not specify the `location` explicitly. If locations are specified explicitly, the location must be a valid sub location of the `storage-profile` of the Warehouse - this is validated by Lakekeeper upon creation. Lakekeeper also ensures that there are no Tables or Views that use a parent- or sub-folder as their `location` and that the location is empty on creation. These checks are required to ensure that no data is leaked via vended-credentials.


### Users
Lakekeeper is no Identity Provider. The identities of users are exclusively managed via an external Identity Provider to ensure compliance with basic security standards. Lakekeeper does not store any Password / Certificates / API Keys or any other secret that grants access to data for users. Instead, we only store Name, Email and type of users with the sole purpose of providing a convenient search while assigning privileges.

Users can be provisioned to lakekeeper by either of the following endpoints:

* Explicit user creation via the POST `/management/user` endpoint. This endpoint is called automatically by the UI upon login. Thus, users are "searchable" after their first login to the UI.
* Implicit on-the-fly creation when calling GET `/catalog/v1/config` (Todo check). This can be used to register technical users simply by connecting to the Lakekeeper with your favorite tool (i.e. Spark). The initial connection will probably fail because privileges are missing to use this endpoint, but the user is provisioned anyway so that privileges can be assigned before re-connecting.


### Roles
Projects can contain multiple Roles, allowing Roles to be reused in all Warehouses within the Project. Roles can be nested arbitrarily. Roles can be provisioned automatically using the `/management/v1/roles` (Todo check) endpoint or manually created via the UI. We are looking into SCIM support to simplify role provisioning. Please consider upvoting the corresponding [Github Issue](https://github.com/lakekeeper/lakekeeper/issues/497) if this would be of interest to you.


## Soft Deletion

In Lakekeeper, warehouses can enable soft deletion. If soft deletion is enabled for a warehouse, when a table or view is dropped, it is not immediately deleted from the catalog. Instead, it is marked as dropped and a job for its cleanup is scheduled. The table is then deleted after the warehouse specific expiration delay has passed. This will allow for a recovery of tables that have been dropped by accident. "Undropping" a table is only possible if soft-deletes are enabled for a Warehouse.
