---
level: major
---

**A field with no value is left out of the record.** Nothing in an audit record is `null`.

Affected: `user_agent` on authorization and replay records, `idempotency_key` on
authorization records, `denied_by` inside an admission record's `context`, and `name`,
`source` and `reason` inside the entries of `determined_by`.

**What to do:** tolerate these keys being absent wherever you read them unconditionally.
In `jq`, `.user_agent` still yields `null` for a missing key, so a query that only reads
the value needs no change; one that tells `null` from missing does.
