---
level: minor
---

**Registering a view from an existing metadata file is authorized as `create_view` on the namespace, and the `create_view` action carries the new context key `view_id`.**

```text
POST /catalog/v1/{prefix}/namespaces/{namespace}/register-view
  create_view (namespace entity)   context: name, view_id, properties
```

`view_id` is the view UUID from the registered metadata file. It is present only when the view ID comes from outside, as it does on a register. A `create_view` record of a normal create or a rename has no `view_id`. `properties` are the properties in the file.

If the caller may not see the namespace, the refused record is written before the file is read. It names the view only by the requested name, and it has no `view_id`.

**What to do:** Accept the key `view_id` in the context of `create_view`. To tell a registered view from a created one, match `create_view` with `view_id`.
