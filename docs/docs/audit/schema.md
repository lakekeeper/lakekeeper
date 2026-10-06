---
description: "The machine-readable JSON Schema of Lakekeeper's audit log: every record shape, every object, every field and every closed set of values."
---

# Audit log schema

[**Download the schema**](schema.json) — JSON Schema, draft 2020-12.

Everything a Lakekeeper audit record can carry is described there: the three record shapes, every nested object, every field with the description written on it, and every closed set of values. It is generated from the emitting code, so it cannot fall behind what the server writes, and it is the document a format change is diffed against.

## One schema per product

This document describes what **Lakekeeper** contributes to a record. A deployment running another product on top — Lakekeeper+ — has a second schema describing what that product contributes, published beside this one:

| Product           | Key in `emitters`  | Schema              |
|-------------------|--------------------|---------------------|
| Lakekeeper        | `lakekeeper`       | `schema.json`       |
| Lakekeeper+       | `lakekeeper_plus`  | `schema-plus.json`  |

Both sit in this directory, each published by its own product's release, so `schema-plus.json` is present from the first Lakekeeper+ release that carries an audit format.

Every record names the products it carries something of as the keys of `emitters`, each with the version of what that product contributes. Read the keys to know which schemas apply to the record in front of you, and each value to know which version of that product's half you are reading. A record naming one product needs one schema; a record naming two — an authorizer supplying an action name or a `context` key on a record Lakekeeper assembled — is governed by both at once.

The record's overall shape is always Lakekeeper's, and `audit_format` always governs it. See [Two version numbers](../logging.md#audit-emitter).

## Validating a record

Point a validator at the file and it checks a whole record, as the log line carries it. The document's root is `#/$defs/AuditRecord`: it requires `event_source`, `audit_format` and `record_type`, and routes on `record_type` to the definition of that shape, which describes the rest of the record. A record whose `record_type` this copy of the schema does not list is a newer record type: it is checked against what every record shares, and passes.

Each shape pins its `record_type` with `const`. Point a code generator at the shape definitions:

| `record_type`   | Shape                         |
|-----------------|-------------------------------|
| `authorization` | `#/$defs/AuthorizationRecord` |
| `replay`        | `#/$defs/ReplayRecord`        |
| `operation`     | `#/$defs/OperationRecord`     |

## Value sets

Every value set has its own definition, and a field holding one of its values points at it. A set is open or closed, and the definition says which.

**An open set** lists its values under `x-audit-values`, next to `"type": "string"`. A later release may add a value without moving `audit_format`, so a validator checks only that the value is a string. A code generator can still read the list. A value the list does not contain means the record is newer than this schema, not that it is invalid: route it to a default branch and carry on. A value is renamed or removed only with a major version, so a consumer that matches what it knows keeps working.

**A closed set** lists its values as an `enum`, and a validator enforces it. Four sets are closed by what they mean: `decision`, `privilege_scope`, `root_level` and a policy's `effect`. A new value in one of them is a major change.

Three fields hold a value from more than one product: `action_name`, `operation` and `outcome`. They carry `x-audit-open`, and their values are listed by the definitions of the products that contribute them, which `emitters` on the record names.

A shape describes the record, not the log line. Your log subscriber adds its own keys around it — `timestamp`, `level`, `message`, `target`, `span`, `spans`, `filename`, `line_number` — and no shape lists them, because Lakekeeper does not choose them. No shape forbids them either, so a captured line validates with them still attached. The two keys stamped on every record, `event_source` and `audit_format`, are in the root definition, since every record carries them whatever its shape.

For what the records *mean* — which family answers which question, when a field appears, worked examples and `jq` recipes — see [Audit Logs](../logging.md#audit-logs).

## Reading the audit-specific annotations

The schema carries these extension keywords. A generic JSON Schema tool ignores them; they are there so you can navigate the document.

| Keyword | On | Meaning |
|---|---|---|
| `x-audit-emitter` | the document | The product this schema describes, and the version of what it contributes |
| `x-audit-kind` | each definition | `shape` for a whole record, `part` for a nested object, `context` for an operation's own detail, `enum` for a set of values, open or closed |
| `x-audit-values` | an open `enum` | The values the set held when the schema was generated. A later release may add one |
| `x-audit-field` | each `enum` | The field whose values these are. Several fields draw from more than one set, so the set alone does not tell you where it is used |
| `x-audit-descriptions` | an `enum` | What each value means, keyed by the value. Present for the values that carry a description; a value absent from the map has none |
| `x-audit-open` | a property | The value comes from whichever product wrote the record, so this schema cannot list what it may hold |

A value set lists what one field can hold. An open set may gain a value in any release, and your consumer must treat an unrecognised one as data, not as an error. Adding one is not a format change. A closed set gains a value only in a major version.

The keys of an object are its `properties`, each with its type and description. An action's keys depend on its `action_name`, so `ActionRecord` lists them per action in `allOf` branches. A later release may add a key, and that *is* a format change — a minor one — because the object gains a field. Removing a key or a value is a major change.

## Versions

A record carries two. `audit_format` governs the record's overall shape and is the version this schema is stamped with. Each value in `emitters` is the version governing what that product contributes. See [Two version numbers](../logging.md#audit-emitter).
