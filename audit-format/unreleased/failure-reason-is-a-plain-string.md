---
level: major
---

**`failure_reason` on a denied authorization record is the reason itself, a plain string.**

```text
before  "failure_reason": {"ActionForbidden": []}
after   "failure_reason": "action_forbidden"
```

The set of reasons is unchanged; their spelling changes with every other value this log
owns.

**What to do:** in `jq`, `.failure_reason | keys[0]` becomes `.failure_reason`.
