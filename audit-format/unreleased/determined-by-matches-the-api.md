---
level: major
---

**The entries of `determined_by`, inside each `authorizations[]` entry, are written the
way the management API writes them when it answers a permission check.**

```
before  {"Policy": {"policy_id": "p-42", "effect": {"Forbid": []}, "source": "cedar"}}
after   {"type": "policy", "policy-id": "p-42", "effect": "forbid", "source": "cedar"}

before  {"SystemAuthority": {"source": "break-glass", "reason": "lockout recovery"}}
after   {"type": "system-authority", "source": "break-glass", "reason": "lockout recovery"}
```

The factor kind moves into a `type` field, the field names inside are kebab-case, and
`effect` is a plain string: `permit` or `forbid`.

**What to do:** read `type` to tell the kinds apart. The same code now parses these
entries and a `/check` response; both shapes are in the published schema.
