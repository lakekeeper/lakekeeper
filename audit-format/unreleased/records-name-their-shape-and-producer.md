---
level: minor
---

**Every audit record carries two new top-level fields, `record_type` and `emitter`.**

`record_type` names the record's shape: `authorization`, `replay` or `operation`. Always
present.

`emitter` names the product that wrote the record and the version of what that product
contributes: `{"name": "lakekeeper", "format": "1.0"}`. A distribution that adds records
of its own, such as Lakekeeper+, stamps its own name and version.

Two versions now appear on every record:

- `audit_format` — the record's overall shape: which top-level fields exist and how they nest
- `emitter.format` — what the named product contributes: its `operation` and `outcome`
  values, and the contents of its `context`

They are equal on Lakekeeper's own records. Do not rely on that.

**What to do:** route on `record_type` rather than on which fields are absent, and match
on whichever of the two versions covers the scope you mean.
