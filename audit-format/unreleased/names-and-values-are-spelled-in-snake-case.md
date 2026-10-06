---
level: major
---

**Every key and every value this log names itself is spelled `snake_case`, and `failure_reason` is a plain string.**

Keys that carried a hyphen now carry an underscore:

- in an `entity`: `generic_table`, `generic_table_id`, `namespace_id`, `project_id`, `role_id`, `role_provider_id`, `role_source_id`, `server_id`, `table_id`, `table_location`, `tag_definition_id`, `task_id`, `user_id`, `view_id`, `warehouse_id`
- in an action: `allow_partial`, `created_before`, `dry_run`, `removed_properties`, `target_refs`, `update_kinds`, `updated_properties`
- in an authorization record's `context`: `invoked_by`, `self_provisioning`, `self_read`
- on an `authorizations[]` entry: `for_principal`

Values:

```text
actor_type      before  assumed-role                after  assumed_role
                before  lakekeeper-internal         after  lakekeeper_internal
failure_reason  before  {"ActionForbidden": []}     after  "action_forbidden"
```

Every `failure_reason` value follows the same pattern: `action_forbidden`, `resource_not_found`, `cannot_see_resource`, `internal_authorization_error`, `internal_catalog_error`, `invalid_request_data`.

Three sets keep the spelling of the vocabulary they come from: `entity_type` and `resource_type` (`generic-table`, `tag-definition`, as in the management API) and `update_kinds` (Iceberg's update names, such as `add-schema`). The objects in `determined_by` keep the management API's field names, `policy-id` included.

**What to do:** rename these keys and values in every query. A query on the old spelling matches nothing and raises no error, so check for the new names explicitly. In `jq`, `.failure_reason | keys[0]` becomes `.failure_reason`.
