---
level: major
---

**The entries of `determined_by`, inside each `authorizations[]` entry, are written the way
the management API writes them when it answers a permission check.**

```
before  {"Policy": {"policy_id": "p-42", "effect": {"Permit": []}, "source": "cedar"}}
after   {"type": "policy", "policy-id": "p-42", "effect": "permit", "source": "cedar"}
```

The factor kind moves into a `type` field, the field names inside are kebab-case as the API
spells them, and `effect` is a plain string: `permit` or `forbid`.

A second kind joins it, `{"type": "system-authority", ...}`, recording that a built-in
authority tier rather than a configured policy determined the allow — a recovery grant, for
instance. It carries an optional `source` and `reason`.

**What to do:** read `type` to tell the kinds apart, and route an unrecognised one to a
default branch. The same code now parses these entries and a `/check` response; both shapes
are in the published schema.
