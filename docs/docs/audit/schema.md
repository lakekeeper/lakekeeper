---
description: "The machine-readable JSON Schema of Lakekeeper's audit log: every record shape, every object, every field and every closed set of values."
---

# Audit log schema

[**Download the schema**](schema.json) — JSON Schema, draft 2020-12.

Everything a Lakekeeper audit record can carry is described there: the three record shapes, every nested object, every field with the description written on it, and every closed set of values. It is generated from the emitting code, so it cannot fall behind what the server writes, and it is the document a format change is diffed against.

Use it as you would any JSON Schema: validate captured records against it, generate types for your consumer from it, or read it directly.

For what the records *mean* — which family answers which question, when a field appears, worked examples and `jq` recipes — see [Audit Logs](../logging.md#audit-logs).

## Reading the audit-specific annotations

The schema carries four extension keywords. A generic JSON Schema tool ignores them; they are there so you can navigate the document.

| Keyword | On | Meaning |
|---|---|---|
| `x-audit-emitter` | the document | The product this schema describes, and the version of what it contributes |
| `x-audit-kind` | each definition | `shape` for a whole record, `part` for a nested object, `context` for an operation's own detail, `enum` for a closed set of values |
| `x-audit-record-type` | each `shape` | The `record_type` value that names this shape, which is what a consumer routes on |
| `x-audit-field` | each `enum` | The field whose values these are. Several fields draw from more than one set, so the set alone does not tell you where it is used |

## Versions

A record carries two. `audit_format` governs the record's overall shape and is the version this schema is stamped with. `emitter.format` governs what the named product contributes. See [Two version numbers](../logging.md#audit-emitter).
