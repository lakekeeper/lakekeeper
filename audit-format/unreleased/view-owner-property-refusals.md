---
level: none
---

**A view create or view commit that the view owner property rules refuse is recorded as a denied authorization.**

```text
create_view (namespace entity)  error type ProtectedPropertyModification
commit (view entity)            error type ProtectedPropertyModification or ProtectedViewVersionChange
```

On a server with authentication, a `create_view` or `commit` that writes `trino.run-as-owner`, or the owner property of a configured trusted engine, is refused unless it comes from a trusted engine that owns that property. A `commit` that changes the current version of a view holding such a property is refused unless it comes from that engine. The records have `decision: denied` and `failure_reason: action_forbidden`. A `ProtectedViewVersionChange` record has the same `request_id` as an allowed `commit` record of the same request.

If that engine changes the query of such a view and does not send the owner property, the commit removes the property. The allowed `commit` record lists `updated_properties` and `removed_properties` as the engine sent them.

**What to do:** Nothing.
