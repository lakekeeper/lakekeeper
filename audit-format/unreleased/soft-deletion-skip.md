---
level: none
---

**A call that skips the soft-deletion window of a warehouse also asks `modify_soft_deletion` on that warehouse, which has a record of its own. The call is refused when the caller may not change the soft-deletion settings of that warehouse.**

```text
drop (table, view or generic table)                        force: true
delete (namespace)                                         recursive: true
control_tasks (warehouse)                                  run now or run at, when a soft-deletion task is among the tasks
drop (table), invoked_by: register_table_overwrite         force: true
```

The `control_tasks` record does not say which control or which tasks were asked for. All four cases apply only in warehouses with soft deletion.

When the check of its own action allows such a call, the call writes its record with `decision: allowed`, and then a record of `modify_soft_deletion` on the warehouse. If the caller may not change the soft-deletion settings, this second record has `decision: denied`, `failure_reason: action_forbidden` and error type `WarehouseActionForbidden`, and the call is refused. A call that the check of its own action refuses writes no `modify_soft_deletion` record.

A change of the soft-deletion settings writes the same `modify_soft_deletion` record, and no other authorization record. The `modify_soft_deletion` record of a skip has the same `request_id` as the record of the call.

A register with `overwrite` that replaces an active table with another table (a different table UUID) in a warehouse with soft deletion records the drop of the replaced table with `force: true`, also when the call is allowed. Before, it recorded `force: false`.

**What to do:** To find calls that skip the soft-deletion window, match `modify_soft_deletion` records that share their `request_id` with a record of another action. If you filter or alert on drops with `force: true`, these now also include drops by a register with `overwrite`. You can tell them apart by `invoked_by: register_table_overwrite`.
