# Changelog

`MODIFIES_TUPLES` indicates whether existing tuples are modified when migrating from lower versions. If `TRUE`, modifications are done via migration functions passed to the OpenFGA model manager.

`ADDS_TUPLES` indicates whether new tuples are added to the store during the migration.

## `v4.1`

```
MODIFIES_TUPLES: FALSE
ADDS_TUPLES:     FALSE
```

- Add `can_get_tasks` and `can_control_tasks` permissions to `table` and `view`.
- Add `can_get_all_tasks` and `can_control_all_tasks` permission to `warehouse`.

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
