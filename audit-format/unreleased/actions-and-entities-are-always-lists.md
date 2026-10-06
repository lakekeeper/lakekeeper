---
level: major
---

**Authorization records always carry `actions` and `entities` as lists. The singular `action` and `entity` fields are gone.**

```text
before  "action": {…}            or  "actions": [{…}, {…}]
after   "actions": [{…}]

before  .entity.namespace         .action.action_name
after   .entities[0].namespace    .actions[0].action_name
```

Each entry of `authorizations[]` still carries a singular `action` and `entity`.

**What to do:** read `actions` and `entities` as lists, and iterate: a record can carry more than one.
