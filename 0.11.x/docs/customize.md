# Customize

As Customizability is one of the core features we are missing in other IRC implementations, we try to do things differently. The core implementation of this crate is based on four modules that back the `axum` service router:

* `Catalog` is the interface to the DB backend where Warehouses, Namespaces, Tables and other entities are managed.
* `SecretStore` is the interface to a secure storage for secrets.
* `Authorizer` is the interface to the permission system used by Lakekeeper. It may expose its own APIs.
* `EventPublisher` is the interface to message queues to send change events to.
* `ContractValidator` allows an external system to prohibit changes to tables if, for example, data contracts are violated
* `TaskQueue` is the interface to the task store, used to schedule tasks like soft-deletes

All components come pre-implemented, however we encourage you to write custom implementations, for example to seamlessly grant access to tables via your companies Data Governance solution, or publish events to your very important messaging service.
