---
level: minor
---

**A list or a map the request left empty is emitted empty, not left out.**

```
before  {"action_name": "commit"}
after   {"action_name": "commit", "updated_properties": {}, "removed_properties": [],
         "target_refs": [], "update_kinds": []}
```

"The request removed no properties" and "this action has no such field" are different answers,
and only the key's presence can tell them apart. So every list and map an action can carry is
present once that action has it: `properties`, `updated_properties`, `removed_properties`,
`source`, `destination`, `target_refs`, `update_kinds`.

Two fields of the record itself follow the same rule and are now **always present**:

- `authorizations[].determined_by` — `[]` when the authorizer reports no deciding factors.
- `error.stack` — `[]` when the error has no causes.

A value the request simply did not supply is still absent: there is no empty form of a name or
an id, and nothing in this log is ever `null`.

**What to do:** read "nothing" from the empty value rather than from a missing key. If you
branch on a key's presence to mean "none", branch on the value being empty instead. `determined_by`
and `error.stack` can now be read without a presence check.
