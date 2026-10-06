---
level: minor
---

**Three more operations record the override that makes them destructive.**

```text
role delete          force   revokes the grants the role still holds
warehouse delete     force   deletes a protected warehouse and everything in it
generic-table drop   force   skips the warehouse's soft-deletion window
                     purge   deletes the data files
```

They use the same keys as namespace delete, table drop and view drop. The key is present whichever way the flag went.

**What to do:** if you alert on destructive operations, include these three. `"force": true` is the forced form.
