---
level: major
---

**A `context` value has its own JSON type. It is no longer always a string.**

```text
before  "force": "true"   "dry_run": "true"   "writes": "2"
after   "force": true     "dry_run": false    "writes": 2
```

- Booleans: `force`, `purge`, `recursive`, `dry_run` and `allow_partial` in an action, and `self_provisioning` and `self_read` in `context`.
- Numbers: `writes` and `deletes`.
- Strings, lists and maps keep their type.

A product that plugs into Lakekeeper may add `context` keys that hold an object. Its schema gives the object's definition.

**What to do:** compare a flag with `true`, not with `"true"`. Read each key's type from the schema.
