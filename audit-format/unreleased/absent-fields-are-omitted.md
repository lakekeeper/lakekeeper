---
level: major
---

A field with no value is now left out of the record rather than written as `null`.
Nothing in an audit record is `null` any more.

The fields affected are `user_agent` on authorization and replay records,
`idempotency_key` on authorization records, `denied_by` inside the `context` of an
admission record, and `name`, `source` and `reason` inside the entries of
`determined_by`.

Previously these keys were always present and held `null` when there was no value,
which claimed the value was known to be nothing. A consumer that tests for the key's
presence now gets the right answer; one that reads the key unconditionally must
tolerate it being absent. In `jq`, `.user_agent` still yields `null` for an absent
key, so a query that only reads the value needs no change; one that distinguishes
`null` from missing does.
