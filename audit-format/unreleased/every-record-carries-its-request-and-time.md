---
level: major
---

**Every audit record carries `request_id` and `time` at the top level, and the admission record's `context.request_id` is gone.**

```text
before  context.request_id: "019684ff-…"          (admission records only)
after   request_id: "019684ff-…"                   (every record caused by a request)
        time: "2026-03-14T09:26:53.589793Z"        (every record)
```

- `request_id` names the request that caused the record. It is the value of the `x-request-id` response header, and the same value every other log line of that request carries. It is a string: Lakekeeper generates a UUID, and a caller or proxy that sends its own `x-request-id` gets that value back unchanged. It is absent from an operation record no request caused, such as one from a background task.
- `time` is when the event happened, in RFC 3339 in UTC with microseconds. It is set by the server and does not depend on how your log subscriber formats its own `timestamp`.
- The schema's root definition, `AuditRecord`, checks a whole record: it requires `event_source`, `audit_format` and `record_type`, and routes on `record_type` to that shape's definition.

**What to do:** read the request id from `request_id`, not from `context.request_id`. Correlate records with a request through `request_id`. Order or window records by `time`. To validate a whole record, point a validator at the schema document itself.
