---
level: none
---

**The audit log has a published JSON Schema: `audit/schema.json` in the documentation, linked from the "Audit Log Schema" page.**

- Point a validator at the document to check a whole record. Its root, `AuditRecord`, requires `event_source`, `audit_format` and `record_type` and selects the shape by `record_type`: `AuthorizationRecord`, `ReplayRecord` or `OperationRecord`.
- Every field and key is a property with its type and description. `ActionRecord` lists, per `action_name`, the keys that action carries.
- A value set the log may extend lists its values under `x-audit-values`, so a validator accepts a value a later release adds. The closed sets `decision`, `privilege_scope`, `root_level` and a policy's `effect` use `enum`.
- A product plugged into Lakekeeper publishes its own schema for what it contributes. `emitters` on a record says which schemas apply.

**What to do:** nothing. Use the schema to validate records or to generate a parser.
