---
level: minor
---

**Every audit record now carries an `event_id`, a UUID that is unique to that record.**

```text
authorization, replay, operation   + event_id
```

Records that share a `request_id`, such as those of a grant change, have different `event_id` values. A line delivered twice carries the same `event_id`, so you can use it to remove duplicates. It is a UUIDv7, so ids sort roughly by time.

**What to do:** Nothing. To remove duplicate records, use `event_id`.
