---
level: minor
---

**Every audit record carries two new top-level fields, `record_type` and `emitters`.**

`record_type` names the record's shape: `authorization`, `replay` or `operation`. Always present.

`emitters` names every product the record carries something of, and the version of what each contributes. It is an object: each key is a product's name in `lower_snake_case`, and its value is that product's `MAJOR.MINOR`.

```text
"record_type": "authorization",
"emitters": {"lakekeeper": "1.0"}
```

Most records name one product. A record names two when a component that plugs into Lakekeeper supplies a name in a record Lakekeeper assembled — an authorizer's action name, or a `context` key the component declares. Both products then govern part of that record, and both are keys of `emitters`.

Two versions now appear on every record:

- `audit_format` — the record's overall shape: which top-level fields exist and how they nest.
- a product's value in `emitters` — what that product contributes: its `operation` and `outcome` values, and the `context` keys it declares.

They are equal on a record only Lakekeeper produced. Do not rely on that.

**What to do:** route on `record_type`, not on which fields are absent. Read the version of the product you care about from its key, for example `.emitters.lakekeeper`.
