---
level: none
---

**The schema lists only the action names an audit record can carry.**

The OpenFGA authorizer's internal relation names, about 220 of them, are no longer published as value sets. No record ever carried them. The three OpenFGA endpoints that record their own action keep their names under `PermissionAction`: `can_get_metadata`, `can_read_assignments` and `can_set_managed_access`.

**What to do:** Nothing. If you generated code from the removed definitions, regenerate it.
