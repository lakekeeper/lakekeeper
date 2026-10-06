---
level: major
---

**A field is absent when it has no value, and an empty list or map is written out. Nothing in an audit record is `null`.**

A field the request did not supply is left out. A policy factor without a name, for example, has no `name` key instead of `"name": null`.

An empty list or map is a value and is always written once an action can carry it:

```text
before  {"action_name": "commit"}
after   {"action_name": "commit", "updated_properties": {}, "removed_properties": [], "target_refs": [], "update_kinds": []}
```

This applies to `properties`, `updated_properties`, `removed_properties`, `source`, `destination`, `target_refs` and `update_kinds` in an action, and to `authorizations[].determined_by` and `error.stack`. A flag such as `force`, `purge` or `recursive` is also always written, as `true` or `false`.

**What to do:** read "none" from an empty value, not from a missing key. Keys that are genuinely optional, such as a name or an id, still need a presence check.
