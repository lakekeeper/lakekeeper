---
level: major
---

**The entries of `authorizations[].determined_by` have the shape the management API returns from a permission check, and there are two new kinds.**

```text
before  {"Policy": {"policy_id": "p-42", "effect": {"Permit": []}, "source": "cedar"}}
after   {"type": "policy", "policy-id": "p-42", "effect": "permit", "source": "cedar"}
```

- The kind is in `type`. Field names are kebab-case, as in the API. `effect` is `permit` or `forbid`.
- `{"type": "system-authority"}`: a built-in authority, not a configured policy, decided the allow. It may carry `source` and `reason`.
- `{"type": "admission-gate"}`: an admission gate would refuse the user. It carries `gate`, and `check` when the gate names one.

**What to do:** switch on `type`. The same code can parse these entries and a `/check` response.
