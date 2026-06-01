# Changelog

`MODIFIES_TUPLES` indicates whether existing tuples are modified when migrating from lower versions. If `TRUE`, modifications are done via migration functions passed to the OpenFGA model manager.

`ADDS_TUPLES` indicates whether new tuples are added to the store during the migration.

## `v4.6`

```
MODIFIES_TUPLES: FALSE
ADDS_TUPLES:     FALSE
```

Cumulative changes since `v4.0`. All backwards-compatible: existing tuples authorize the same actions, no tuple rewrites needed.

Types:

- Drop the deprecated `table` and `view` types (superseded by `lakekeeper_table` / `lakekeeper_view` in `v4.0`). Tuples on the old types become orphans but are harmless.
- Add `lakekeeper_generic_table` type for the Generic Table API.

`lakekeeper_view`:

- Add `select` relation, split from `describe`. `describe` now derives from `select` (which derives from `modify`), so existing `modify`/`describe` tuples grant the same effective permissions.
- Add `can_select` action and `can_grant_select` grant action.

`lakekeeper_table` and `lakekeeper_view`:

- Add `can_set_protection`.
- Add `can_get_tasks` and `can_control_tasks`.

`namespace`:

- Replace `table` / `view` with `lakekeeper_generic_table` in the `child` relation type set (alongside `lakekeeper_table` / `lakekeeper_view`).
- Add `can_create_generic_table`, `can_list_generic_tables`, `can_set_protection`.

`warehouse`:

- Add `can_set_protection`, `can_set_format_version_policy`.
- Add `can_get_endpoint_statistics`.
- Add `can_get_all_tasks`, `can_control_all_tasks`.

`project`:

- Add `can_get_endpoint_statistics`.
- Add `can_get_project_tasks`, `can_control_project_tasks`.
- Add `can_get_task_queue_config`, `can_modify_task_queue_config`.
- Tighten `can_grant_data_admin`: now requires `security_admin` (was `data_admin`). Existing tuples are unchanged, but only `security_admin` / server-`admin` can grant `data_admin` going forward.

`role`:

- Broaden `can_read_assignments` to `can_list_roles from project` (was `can_read`).

## `v4.0`

```
MODIFIES_TUPLES: FALSE
ADDS_TUPLES:     TRUE
```

- Adds types `lakekeeper_table` and `lakekeeper_view`. Their definitions are copied from `table` and `view`, however the way these objects are represented changes.
  - For `table` it is `table_id`, for `lakekeeper_table` it is `warehouse_id/table_id`.
  - For `view` it is `view_id`, for `lakekeeper_view` it is `warehouse_id/view_id`.
  - This reflects the change that view and table ids can be re-used across warehouses.
- For each tuple referencing a table or view, the migration adds a new tuple according to the new object representation.
- Types `table` and `view` are deprecated and scheduled for deletion.
