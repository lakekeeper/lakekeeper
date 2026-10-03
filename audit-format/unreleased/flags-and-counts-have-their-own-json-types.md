---
level: major
---

**A flag is a JSON boolean and a count is a JSON number.**

```
before  {"action_name": "drop", "force": "true", "purge": "true"}
after   {"action_name": "drop", "force": true,  "purge": true}

before  {"action_name": "apply_grants", "writes": "2", "deletes": "1"}
after   {"action_name": "apply_grants", "writes": 2,   "deletes": 1}
```

Flags: `force`, `purge`, `recursive`, `dry_run`, `allow_partial` in an action, and
`self_provisioning`, `self_read` in `context`. Counts: `writes`, `deletes`.

A flag is also **always present** once the action has it, so `force` is `false` rather than
missing. A flag that is absent says the operation has no such override at all — deleting a
user cannot be forced — which is a different statement from `false`.

`authorizations[].allowed` was already a boolean and is unchanged. Every `context` value now
reaches the wire in the type the schema declares for its key, whichever of the two `context`
objects it sits in.

**What to do:** read these as booleans and numbers. A parser comparing against `"true"` stops
matching; one that coerced `"2"` to a number keeps working. `if (ctx.purge)` is now safe where
the key is present, and its absence means the operation has no purge.
