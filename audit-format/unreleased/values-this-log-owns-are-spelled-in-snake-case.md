---
level: major
---

**A value this log owns is spelled `snake_case`, like the names around it.**

`actor_type`:

```text
before  assumed-role     lakekeeper-internal
after   assumed_role     lakekeeper_internal
```

`failure_reason` changes spelling the same way; its own fragment shows the before and after.

The sets are otherwise unchanged: the same reasons, the same actor kinds, in the same
places.

Values this log does not own keep their spelling, so a hyphen in a value means the
vocabulary belongs to someone else:

- `entity_type` and `resource_type` — `generic-table` and `tag-definition`, as the
  management API spells them. A generic table's entity therefore reads
  `"entity_type": "generic-table"` beside the key `generic_table`
- `update_kinds` — Iceberg's table-update action names, such as `add-schema`
- the objects inside `determined_by` — the management API's shape

**What to do:** change the strings you match on in `actor_type` and `failure_reason`.
