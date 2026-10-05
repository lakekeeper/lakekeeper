---
level: major
---

**A request that names nothing to check records empty lists, not an invented `unknown` row.**

A batch check with `checks: []`, and a transaction commit with no table changes, used to record one made-up entry under `authorizations`. It named an `entity_type` of `unknown`, and for the commit an `action_name` of `unknown` too, neither of which the request named. Both values are gone.

The commit, before and after:

```text
before  "actions": [], "entities": [],
        "authorizations": [{"action": {"action_name": "unknown"}, "entity": {"entity_type": "unknown"}, …}]
after   "actions": [], "entities": [], "authorizations": []
```

`authorizations` is still always present. It is empty only when the request named nothing to check; `decision` still carries the outcome.

**What to do:** stop matching `unknown` in `entity_type` and `action_name`. Where you assumed `authorizations` has at least one entry, handle an empty list.
