---
level: none
---

**A call that skips the soft-deletion window of a warehouse also asks `modify_soft_deletion` on that warehouse, and is refused when the caller may not change the soft-deletion settings of that warehouse.**

```text
drop (table, view or generic table)                        force: true
delete (namespace)                                         recursive: true
control_tasks (warehouse)                                  run now or run at, when a soft-deletion task is among the tasks
drop (table), invoked_by: register_table_overwrite         force: true
```

The `control_tasks` record does not say which control or which tasks were asked for. All four cases apply only in warehouses with soft deletion.

The record of such a call names the warehouse action as an entry of its own: `modify_soft_deletion` in `actions`, the warehouse in `entities` unless it is there already, and one more entry in `authorizations` that pairs the two. The call's own action keeps its entry, paired with its own entity only. If the call is refused, both entries have `allowed: false`, with `failure_reason: action_forbidden` and error type `WarehouseActionForbidden` when the warehouse action was refused. A call that the check of its own action refuses first does not have the `modify_soft_deletion` entry.

A register with `overwrite` that replaces an active table with another table (a different table UUID) in a warehouse with soft deletion records the drop of the replaced table with `force: true`, also when the call is allowed. Before, it recorded `force: false`.

**What to do:** To find calls that skip the soft-deletion window, match `modify_soft_deletion` in `actions`. If a rule reads only the first entry of `actions`, `entities` or `authorizations`, read all entries. If you filter or alert on drops with `force: true`, these now also include drops by a register with `overwrite`. You can tell them apart by `invoked_by: register_table_overwrite`.
