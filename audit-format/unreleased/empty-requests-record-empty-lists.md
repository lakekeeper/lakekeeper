---
level: major
---

**A request that names nothing to check records empty lists. The made-up `unknown` entry is gone.**

A batch check with `checks: []`, and a transaction commit with no table changes, recorded one `authorizations` entry with `entity_type` `unknown`, and for the commit `action_name` `unknown` too.

```text
before  "authorizations": [{"action": {"action_name": "unknown"}, "entity": {"entity_type": "unknown"}, …}]
after   "actions": [], "entities": [], "authorizations": []
```

`decision` still carries the outcome.

**What to do:** stop matching `unknown` in `entity_type` and `action_name`, and handle an empty `authorizations` list.
