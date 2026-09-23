---
level: major
---

The entries of `determined_by`, inside each `authorizations[]` entry, are now written
the same way the management API writes them when it answers a permission check.

```
before  {"Policy": {"policy_id": "p-42", "effect": {"Forbid": []}, "source": "cedar"}}
after   {"type": "policy", "policy-id": "p-42", "effect": "forbid", "source": "cedar"}

before  {"SystemAuthority": {"source": "break-glass", "reason": "lockout recovery"}}
after   {"type": "system-authority", "source": "break-glass", "reason": "lockout recovery"}
```

Three things change together. The kind of factor moves from the object's single key
into a `type` field, and its spelling changes with it: `Policy` becomes `policy` and
`SystemAuthority` becomes `system-authority`. Field names inside become kebab-case,
so `policy_id` becomes `policy-id`. And `effect`, which wrapped its own value in an
object, becomes that value as a lowercase string: `permit` or `forbid`.

One parser now reads both the audit log and the API response. Every field of both
shapes is listed in the audit format reference.
