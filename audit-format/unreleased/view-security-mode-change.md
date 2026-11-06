---
level: none
---

**A view commit that switches the view between running as its owner and running as the caller is recorded with the new view action `change_security_mode`, in records of its own.**

```text
change_security_mode (view entity)              one record per checked principal, after the commit record
introspect_permissions, operation view-replace  recorded as commit (view entity)
```

A view commit from a trusted engine that adds or removes `trino.run-as-owner`, or the owner property of another configured trusted engine, needs `change_security_mode` on the view. After the `commit` record of the request, Lakekeeper writes one `change_security_mode` record for the identity that sends the commit and, if the commit adds the property, one for the user that the property names, with that user in `for_principal`. Each record has its own decision and the same `request_id`. A refused switch has `decision: denied`, `failure_reason: action_forbidden` and error type `SecurityModeChangeForbidden`, and the view does not change. A commit that keeps the security mode writes no `change_security_mode` record.

The batch-check operation `view-replace` asks whether a user may replace a view. Its decision is recorded as `commit` on the view, even when `change_security_mode` decided it.

**What to do:** Nothing.
