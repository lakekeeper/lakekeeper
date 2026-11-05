---
level: none
---

**Registering files that are already in storage is authorized on the warehouse, with the new action `register_table`.**

```text
POST /catalog/v1/{prefix}/namespaces/{namespace}/register
  register_table (warehouse entity)   new, before create_table (namespace entity)
POST /lakekeeper/v1/{prefix}/namespaces/{namespace}/generic-tables
  register_table (warehouse entity)   new, after create_generic_table (namespace entity), only when the location holds files
```

A register now produces an authorization record for `register_table` on the warehouse, allowed or refused, before the `create_table` record on the namespace. If `register_table` is refused, the `create_table` record is also written, as refused. It names the table only by the requested name, because the metadata file is not read.

Creating a generic table on a location that already holds files produces a `register_table` record on the warehouse after the `create_generic_table` record. A refusal has error type `WarehouseActionForbidden`, and its message says that the location holds files. A location without files produces no `register_table` record.

**What to do:** Nothing. To find registers and generic tables created over existing files, match `register_table`.
