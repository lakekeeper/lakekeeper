---
description: "The machine-readable JSON Schema of Lakekeeper's audit log: every record shape, every object, every field and every value set."
---

# Audit log schema

[**Download the schema**](schema.json) (JSON Schema, draft 2020-12).

It describes everything a Lakekeeper audit record can carry: the three record shapes, every nested object, every field with its description, and every value set. It is generated from the code that writes the records, so it matches what the server emits.

For what the records *mean* (when a field appears, worked examples and `jq` recipes), see [Audit Logs](../logging.md#audit-logs).

## One schema per product

This schema describes what **Lakekeeper** contributes to a record. A product running on top of Lakekeeper, such as Lakekeeper+, publishes its own schema for what it contributes, in this directory:

| Product           | Key in `emitters`  | Schema              |
|-------------------|--------------------|---------------------|
| Lakekeeper        | `lakekeeper`       | `schema.json`       |
| Lakekeeper+       | `lakekeeper_plus`  | `schema-plus.json`  |

Each schema is published by its own product's releases. `schema-plus.json` is present from the first Lakekeeper+ release with an audit format.

The keys of `emitters` on a record name the products that contributed to it, so they tell you which schemas apply. Each value is the version of that product's contribution. A record naming two products, for example when an authorizer added an action name or a `context` key to a record Lakekeeper built, is governed by both schemas.

The record's overall shape is always Lakekeeper's, governed by `audit_format`. See [Two version numbers](../logging.md#audit-emitter).

## Validating a record

Point a validator at the file to check a whole record as it appears in the log. The root is `#/$defs/AuditRecord`. It requires `event_source`, `audit_format` and `record_type`, and uses `record_type` to pick the definition for the rest of the record. A record whose `record_type` your copy of the schema does not list is a newer record type. It is checked only against the fields every record shares, and passes.

Each shape fixes its `record_type` with `const`. To generate code, use the shape definitions:

| `record_type`   | Shape                         |
|-----------------|-------------------------------|
| `authorization` | `#/$defs/AuthorizationRecord` |
| `replay`        | `#/$defs/ReplayRecord`        |
| `operation`     | `#/$defs/OperationRecord`     |

A shape describes the record, not the whole log line. Your log subscriber adds its own keys (`timestamp`, `level`, `message`, `target`, `span`, `spans`, `filename`, `line_number`). No shape lists them, and none forbids them, so a captured line validates with them attached. `event_source` and `audit_format`, which every record carries, are in the root definition.

## Value sets

Every value set has its own definition, and a field holding one of its values points to it. A set is open or closed.

**An open set** lists its values under `x-audit-values`, next to `"type": "string"`. A later release may add a value without changing `audit_format`, so a validator only checks that the value is a string. A code generator can still read the list. A value not in the list means the record is newer than your schema, not that it is invalid: send it to a default branch and carry on. A value is renamed or removed only in a major version.

**A closed set** lists its values as an `enum`, and a validator enforces it. The closed sets are `decision`, `privilege_scope`, `root_level`, a policy's `effect`, and the kind (`type`) of a `determined_by` entry. A new value in one of them is a major change.

Three fields can hold values from more than one product: `action_name`, `operation` and `outcome`. They are marked `x-audit-open`. Their values are listed in the definitions of the products that contribute them, which `emitters` on the record names.

## Object keys

The keys of an object are its `properties`, each with its type and description. An action's keys depend on its `action_name`, so `ActionRecord` lists them per action in `allOf` branches. Adding a key is a minor format change. Removing a key or a value is a major change.

## Audit-specific annotations

The schema uses these extra keywords. Generic JSON Schema tools ignore them; they help you navigate the document.

| Keyword | On | Meaning |
|---|---|---|
| `x-audit-emitter` | the document | The product this schema describes, and the version of its contribution |
| `x-audit-kind` | each definition | `shape` for a whole record, `part` for a nested object, `context` for an operation's own details, `enum` for a value set (open or closed) |
| `x-audit-values` | an open value set | The values the set had when the schema was generated. A later release may add more |
| `x-audit-field` | each value set | The field whose values these are. Some fields draw from several sets, so the set alone does not tell you where it is used |
| `x-audit-descriptions` | a value set | What each value means, keyed by value. A value missing from the map has no description |
| `x-audit-open` | a property | The value comes from whichever product wrote the record, so this schema cannot list all possible values |

## Versions

A record carries two kinds of version. `audit_format` governs the record's overall shape and is the version this schema is stamped with. Each value in `emitters` is the version of what that product contributes. See [Two version numbers](../logging.md#audit-emitter).
