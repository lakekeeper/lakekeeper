---
level: major
---

**A `context` value is no longer always a string.**

Each key carries the JSON type that suits it, and the schema states which for every key:

```
before  "force": "true"   "dry_run": "true"   "writes": "2"   "applied": "{\"created\":1}"
after   "force": true     "dry_run": false    "writes": 2     "applied": {"created": 1}
```

- **Booleans** — `force`, `purge`, `recursive`, `dry_run` and `allow_partial` in an action,
  `self_provisioning` and `self_read` in `context`.
- **Numbers** — `writes` and `deletes`.
- **Objects** — a key may carry a whole object rather than JSON packed into a string. No key
  Lakekeeper declares does, so a plain Lakekeeper deployment sees none; a product that plugs
  in may declare one, and the schema lists such keys under `x-audit-key-shapes`, each pointing
  at the definition of whoever declared it.
- **Strings, lists and maps** are unchanged.

`authorizations[].allowed` was already a boolean and is unchanged.

**What to do:** stop assuming a `context` value is a string. A parser comparing a flag against
`"true"` stops matching; one that coerced `"2"` to a number keeps working. Read the key's type
from the schema, or check it where you read the value.
