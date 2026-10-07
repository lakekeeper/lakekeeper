---
level: major
---

**Renaming a table, view or generic table into another namespace is recorded as `move`, not `rename`, and namespaces have a new action `accept_moved_tabular`.**

```text
renameTable, renameView, renameGenericTable into another namespace
  before   rename
  after    move   destination
namespace  accept_moved_tabular   source
```

The record of such a rename names `move` on the renamed entity, refused or allowed, on replays as well. `destination` holds the path of the destination namespace; the entity's new name is not part of it. A rename within one namespace is still recorded as `rename`. Whether the namespace changes is decided on the paths in the request, ignoring ASCII case: two paths that differ only in the case of a non-ASCII letter are recorded as `move`.

`accept_moved_tabular` is the check on the destination namespace. `source` holds the path of the namespace the entity is moved from. It appears in records of permission checks that name it, such as `/management/v1/action/batch-check`.

The record of cancelling soft-deletion tasks is unchanged: it still names `control_tasks`. Cancelling such a task now also requires `undrop` on its table, view or generic table, and a refusal is recorded as a denied `control_tasks`.

**What to do:** if you match table, view or generic-table renames on `rename`, match `move` as well.
