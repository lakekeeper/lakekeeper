---
level: major
---

**Top-level `action` and `entity` are gone from authorization and replay records.**
`actions` and `entities` replace them, always arrays and always present, whatever the
element count.

```
before  "action": {…}         or  "actions": [{…}, {…}]
after   "actions": [{…}]

before  .entity.namespace         .action.action_name
after   .entities[0].namespace    .actions[0].action_name
```

**What to do:** repoint every query that goes through the container. Iterate rather than
taking the first element, since a record can carry more than one. The per-decision entries
inside `authorizations[]` need no change: each still carries a singular `action` and
`entity`.
