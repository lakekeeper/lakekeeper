---
level: major
---

**A field is absent only when there is no value; "none" is written out.**

Nothing in an audit record is `null`. A field the request did not supply is left out:

```text
before  {"decision": "allowed", "user_agent": null, "break_glass": null, "idempotency_key": null}
after   {"decision": "allowed"}
```

An empty list or map is **not** such a field — it is a value, and it is written:

```text
before  {"action_name": "commit"}
after   {"action_name": "commit", "updated_properties": {}, "removed_properties": [],
         "target_refs": [], "update_kinds": []}
```

"The request removed no properties" and "this action has no such field" are different answers,
and only the key's presence tells them apart. So every list and map an action can carry is
present once that action has it: `properties`, `updated_properties`, `removed_properties`,
`source`, `destination`, `target_refs`, `update_kinds`. Two fields of the record itself are
likewise always present — `authorizations[].determined_by` (`[]` when the authorizer reports
no deciding factors) and `error.stack` (`[]` when the error has no causes).

A flag — `force`, `purge`, `recursive` and the rest — follows the same rule: it is written
whichever way it went, so its absence means the operation has no such flag at all.

**What to do:** read "none" from the empty value, not from a missing key. Where you branch on
a key's presence to mean "none", branch on the value being empty instead. `determined_by` and
`error.stack` can now be read without a presence check. Keys that are genuinely optional — a
name, an id — still need one; in `jq`, `.name` yields `null` for a missing key, so a query
that only reads the value needs no change, and one that tells `null` from missing does.
