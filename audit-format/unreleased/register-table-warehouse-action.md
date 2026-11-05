---
level: none
---

**Registering a table is also authorized on the warehouse, with the new action `register_table`.**

```text
POST /catalog/v1/{prefix}/namespaces/{namespace}/register
  register_table (warehouse entity)   new, before create_table (namespace entity)
```

A register now produces an authorization record for `register_table` on the warehouse, allowed or refused, in addition to the `create_table` record on the namespace. A refused register right ends the request with that record: no `create_table` record follows.

**What to do:** Nothing. To find registers, match `register_table`.
