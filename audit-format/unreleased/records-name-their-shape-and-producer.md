---
level: minor
---

Every audit record now carries two new top-level fields.

`record_type` names the record's shape. It is one of `authorization`, `replay` or
`operation`, and it is always present. Before it existed the three shapes could only
be told apart by which fields were missing.

`emitter` is an object with `name` and `format`, naming the product that produced the
record and the version of what that product contributes. Every record Lakekeeper
itself emits carries `{"name": "lakekeeper", "format": "1.0"}`; a distribution that
adds its own audit records, such as Lakekeeper+, stamps its own name and its own
version there.

Two versions therefore appear on every record, and they answer different questions.
`audit_format` governs the record's overall shape — which top-level fields exist and
how they nest. `emitter.format` governs what the named product contributes: its
`operation` and `outcome` values, and the contents of its `context`. For Lakekeeper's
own records the two happen to be equal, because one project governs both. That is a
property of this one emitter and not of the format: match on the one whose scope you
mean.
