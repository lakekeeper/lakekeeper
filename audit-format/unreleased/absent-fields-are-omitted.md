---
level: major
---

**A field with no value is left out of the record.** Nothing in an audit record is `null`.

Inside each entry of `determined_by`, `name` and `source` were `null` when the authorizer
gave none. They are now absent.

```
before  {"Policy": {"policy_id": "p-42", "name": null, "effect": {"Permit": []}, "source": null}}
after   {"type": "policy", "policy-id": "p-42", "effect": "permit"}
```

The same rule holds for every optional field added in this release, so nothing a record
carries is ever `null`.

**What to do:** tolerate these keys being absent wherever you read them unconditionally. In
`jq`, `.name` still yields `null` for a missing key, so a query that only reads the value
needs no change; one that tells `null` from missing does.
