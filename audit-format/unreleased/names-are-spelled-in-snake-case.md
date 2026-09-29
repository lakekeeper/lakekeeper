---
level: major
---

**Every name in an audit record is spelled `snake_case`.** Twenty-five keys and one field
carried a hyphen and now carry an underscore.

Inside an `entity`: `generic_table`, `generic_table_id`, `namespace_id`, `project_id`,
`role_id`, `role_provider_id`, `role_source_id`, `server_id`, `table_id`, `table_location`,
`tag_definition_id`, `task_id`, `user_id`, `view_id`, `warehouse_id`

Inside an `action`: `allow_partial`, `created_before`, `dry_run`, `removed_properties`,
`target_refs`, `update_kinds`, `updated_properties`

Inside an authorization record's `context`: `invoked_by`, `self_provisioning`, `self_read`

On an `authorizations[]` entry: `for_principal`

`project_id`, `table_id` and `generic_table_id` were hyphenated inside an `entity` and
underscored inside an `action`. They have one spelling everywhere now.

Two things sit outside this. The objects inside `determined_by` keep the management API's
spelling, field name `policy-id` included, because that shape is quoted from the API so the
same code parses both. And values follow their own rule, covered separately.

**What to do:** rename these in every query. A query on the hyphenated spelling matches
nothing rather than failing, so nothing will error — check for the new names explicitly.
