---
level: none
---

**A call that skips the soft-deletion window of a warehouse is refused when the caller may not change the soft-deletion settings of that warehouse.**

```text
drop (table, view or generic table)                        force: true
delete (namespace)                                         recursive: true
control_tasks (warehouse)                                  run now or run at, when a soft-deletion task is among the tasks
drop (table), invoked_by: register_table_overwrite         force: true
```

The `control_tasks` record does not say which control or which tasks were asked for.

The refusal is recorded on the call, with `decision: denied`, `failure_reason: action_forbidden` and error type `WarehouseActionForbidden`. The error message names the warehouse action `modify_soft_deletion`. The first, second and fourth case apply in warehouses with soft deletion, the third in every warehouse.

A register with `overwrite` that replaces an active table in a warehouse with soft deletion records the drop of the replaced table with `force: true`, also when the call is allowed. Before, it recorded `force: false`.

**What to do:** If you filter or alert on drops with `force: true`, these now also include the drop of a table that a register with `overwrite` replaces. You can tell these records apart by `invoked_by: register_table_overwrite`.
