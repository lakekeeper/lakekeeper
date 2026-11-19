---
level: minor
---

**Registering a view from an existing metadata file is authorized with the new warehouse action `register_view` and with `create_view` on the namespace. The `create_view` action carries the new context key `view_id`.**

```text
POST /catalog/v1/{prefix}/namespaces/{namespace}/register-view
  register_view (warehouse entity)   new, no context
  create_view (namespace entity)     context: name, view_id, properties
```

A register produces an authorization record for `register_view` on the warehouse, allowed or refused, in addition to the `create_view` record on the namespace. `register_view` is checked before the metadata file is read. If `register_view` is refused, the `create_view` record is also written, as refused. It names the view only by the requested name, because the metadata file is not read, so it has no `view_id`.

`view_id` is the view UUID from the registered metadata file, and `properties` are the properties in the file. A `create_view` record of a normal create or a rename has no `view_id`.

If the file holds an engine owner property such as `trino.run-as-owner` and the client is not a trusted engine that owns it, the register writes a refused `create_view` record with error type `ProtectedPropertyModification` after the allowed one.

**What to do:** Accept the key `view_id` in the context of `create_view`. To find registered views, match `register_view`, or `create_view` with `view_id`.
