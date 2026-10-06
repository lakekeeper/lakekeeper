---
level: minor
---

**Three new kinds of audit record: grant changes, admission decisions and idempotent replays.**

- `operation: "grant_created"` and `"grant_revoked"`: one record per grant an apply actually changed, with `outcome: "success"` and `context` `principal`, `privilege`, `resource_type`, and `resource_id` and `warehouse_id` where they apply.
- `operation: "admission_decided"`: a request an admission gate refused. `outcome` is `forbidden` or `unavailable`; `context` carries `gate`, `status`, `error_type`, `message`, `error_id`, and `denied_by` when the gate named a rule.
- `record_type: "replay"`: a request answered from an idempotency record without being executed. It carries the `actions` and `entities` the request named and the `idempotency_key` that matched, and no `decision`.

**What to do:** nothing, unless you want these records. Select them by `operation` or by `record_type`.
