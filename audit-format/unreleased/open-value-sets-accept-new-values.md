---
level: none
---

**In the schema, an open value set lists its values under `x-audit-values`, and only the four closed sets use `enum`.**

```text
before  "ActorType": {"type": "string", "enum": ["anonymous", "assumed_role", …]}
after   "ActorType": {"type": "string", "x-audit-values": ["anonymous", "assumed_role", …]}
```

- A validator using the schema accepts a value that a later release adds to an open set. Before, it rejected such a value until you downloaded the newer schema.
- `decision`, `privilege_scope`, `root_level` and a policy's `effect` are closed and keep `enum`. A new value in one of them is a major change.
- A policy's `effect` has its own definition, `PolicyEffect`, with `x-audit-field: effect` and its descriptions in `x-audit-descriptions`.

**What to do:** Nothing. If you generate code from the schema, read an open set's values from `x-audit-values`.
