---
level: minor
---

**Three operations now record the override that made them destructive.**

Each of these bypasses a refusal, and the record said nothing about it:

```
role delete          + force   bypasses the refusal that protects a role still holding grants,
                               which the delete then revokes
warehouse delete     + force   deletes a protected warehouse, and everything in it
generic-table drop   + force   bypasses the warehouse's soft-deletion window
                     + purge   physically deletes the data files
```

They join the operations that already recorded it — namespace delete, table drop, view drop —
and use the same keys, `force` and `purge`. As on those, the key is present whichever way the
flag went; an action that carries no such key has no such override at all, which is why
deleting a user names neither.

The schema is unchanged: `delete` and `drop` already listed these keys, because other operations
spelling the same `action_name` carry them. What changes is that these three records now carry
them too.

**What to do:** if you alert on destructive operations, these three carried nothing to alert
on. Read the value: `"force": true` is the forced form.
