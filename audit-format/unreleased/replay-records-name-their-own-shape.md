---
level: minor
---

**A served idempotent replay now writes its own audit record, carrying
`record_type: "replay"`.**

A repeated request that the server answers from its idempotency record left no audit trail
before. It now leaves one, naming the actions and entities the original request named, with
the `idempotency_key` that matched.

```
"record_type": "replay",
"idempotency_key": "4f1c…",
"actions": [{"action_name": "create_table"}]
```

It carries no `operation` and no `outcome`: those belong to the operational family, and a
replay decided nothing.

**What to do:** select replays with `record_type == "replay"`. A query that counts
authorization records by `record_type` will see a family it has not seen before.
