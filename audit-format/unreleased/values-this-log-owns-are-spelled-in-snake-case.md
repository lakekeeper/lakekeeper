---
level: major
---

**A value this log owns is spelled `snake_case`, like the names around it.** Two fields
carry values that changed.

`actor_type`:

```
before  assumed-role     lakekeeper-internal
after   assumed_role     lakekeeper_internal
```

`failure_reason`, on a denied authorization record:

```
before  ActionForbidden             ResourceNotFound      CannotSeeResource
        InternalAuthorizationError  InternalCatalogError  InvalidRequestData
after   action_forbidden              resource_not_found      cannot_see_resource
        internal_authorization_error  internal_catalog_error  invalid_request_data
```

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
