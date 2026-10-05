---
description: "The machine-readable JSON Schema of Lakekeeper's audit log: every record shape, every object, every field and every closed set of values."
---

# Audit log schema

[**Download the schema**](schema.json) — JSON Schema, draft 2020-12.

Everything a Lakekeeper audit record can carry is described there: the three record shapes, every nested object, every field with the description written on it, and every closed set of values. It is generated from the emitting code, so it cannot fall behind what the server writes, and it is the document a format change is diffed against.

## One schema per product

This document describes what **Lakekeeper** contributes to a record. A deployment running another product on top — Lakekeeper+ — has a second schema describing what that product contributes, published beside this one:

| Product           | `emitters[].name`  | Schema              |
|-------------------|--------------------|---------------------|
| Lakekeeper        | `lakekeeper`       | `schema.json`       |
| Lakekeeper+       | `lakekeeper-plus`  | `schema-plus.json`  |

Both sit in this directory, each published by its own product's release, so `schema-plus.json` is present from the first Lakekeeper+ release that carries an audit format.

Every record lists the products it carries something of in `emitters`, each with the `format` of what that product contributes. Read the names there to know which schemas apply to the record in front of you, and each entry's `format` to know which version of that product's half you are reading. A record naming one product needs one schema; a record naming two — an authorizer supplying an action name or a `context` key on a record Lakekeeper assembled — is governed by both at once.

The record's overall shape is always Lakekeeper's, and `audit_format` always governs it. See [Two version numbers](../logging.md#audit-emitter).

## Validating a record

Point a validator at the file and it checks a whole record, as the log line carries it. The document's root is `#/$defs/AuditRecord`: it requires `event_source`, `audit_format` and `record_type`, and routes on `record_type` to the definition of that shape, which describes the rest of the record. A record whose `record_type` this copy of the schema does not list is a newer record type: it is checked against what every record shares, and passes.

Each shape pins its `record_type` with `const`. Point a code generator at the shape definitions:

| `record_type`   | Shape                         |
|-----------------|-------------------------------|
| `authorization` | `#/$defs/AuthorizationRecord` |
| `replay`        | `#/$defs/ReplayRecord`        |
| `operation`     | `#/$defs/OperationRecord`     |

## Values outside the list

A field whose values are a closed set points at the definition listing them, so a validator checks the value and a code generator emits an enum for it. That list is what the set held when the schema was generated, and the set is open: a later release may add a value without moving `audit_format`.

**A value the list does not contain means the record is newer than this schema, not that it is invalid.** Route it to a default branch and carry on — the same rule that applies to every value set. What will not happen without a major version is a value being renamed or removed, so a consumer that matches what it knows keeps working.

This has one consequence worth planning for: **a schema validates the value sets as of the version it was generated from.** Validate a record carrying a newer value against an older copy of this document and the validator rejects it — correctly, by its own rules, and wrongly about the record. So keep the schema in step with the server it reads from: download it from the version you run, and download it again when you upgrade. A consumer that routes on values rather than validating them needs none of this.

The exposure is small by construction. Of the value sets this document defines, only a few are linked from a record field, and most of those cannot grow: `decision`, `privilege_scope` and `root_level` are closed by what they mean. The ones that do grow — `entity_type`, `resource_type`, `update_kinds` and `failure_reason` — grow on a release boundary, which is the moment to re-download. The field that gains values most often, `action_name`, is not linked at all.

Three fields are not linked at all, and carry `x-audit-open` instead: `action_name`, `operation` and `outcome`. Their values come from whichever product contributed them — `emitters` names every product a record carries something of — so no single product's schema can list them.

A shape describes the record, not the log line. Your log subscriber adds its own keys around it — `timestamp`, `level`, `message`, `target`, `span`, `spans`, `filename`, `line_number` — and no shape lists them, because Lakekeeper does not choose them. No shape forbids them either, so a captured line validates with them still attached. The two keys stamped on every record, `event_source` and `audit_format`, are in the root definition, since every record carries them whatever its shape.

For what the records *mean* — which family answers which question, when a field appears, worked examples and `jq` recipes — see [Audit Logs](../logging.md#audit-logs).

## Reading the audit-specific annotations

The schema carries seven extension keywords. A generic JSON Schema tool ignores them; they are there so you can navigate the document.

| Keyword | On | Meaning |
|---|---|---|
| `x-audit-emitter` | the document | The product this schema describes, and the version of what it contributes |
| `x-audit-kind` | each definition | `shape` for a whole record, `part` for a nested object, `context` for an operation's own detail, `enum` for a closed set of values, `keys` for a closed set of object keys |
| `x-audit-field` | each `enum` | The field whose values these are. Several fields draw from more than one set, so the set alone does not tell you where it is used |
| `x-audit-keys-of` | each `keys` | The object whose keys these are |
| `x-audit-descriptions` | an `enum` or `keys` | What each name means, keyed by the name. Present for the names that carry a description; a name absent from the map has none |
| `x-audit-key-shapes` | a `keys` set | What sits under a key whose value is an object, keyed by the key and pointing at the definition. Present only where at least one key declares a shape, so its absence means none does |
| `x-audit-open` | a property | The value comes from whichever product wrote the record, so this schema cannot list what it may hold |

`enum` and `keys` are both lists of strings, and they change in different ways.

An `enum` lists what one field can hold. That set is open, as above: a later release may add a value, and your consumer must treat an unrecognised one as data rather than as an error. Adding one is not a format change.

A `keys` set lists the keys of an object, so its members are field names. A later release may add one, and that *is* a format change — a minor one — because the object gains a field. Removing either is a major change.

## Versions

A record carries two. `audit_format` governs the record's overall shape and is the version this schema is stamped with. Each entry in `emitters` carries a `format` governing what that product contributes. See [Two version numbers](../logging.md#audit-emitter).
