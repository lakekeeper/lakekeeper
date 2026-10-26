---
level: none
---

**Switching a Lakekeeper process into or out of read-only maintenance mode at runtime is recorded with two new actions.**

```text
POST /management/v1/server/maintenance-mode
  mode read-only   set_maintenance_mode_read_only
  mode off         set_maintenance_mode_off
```

Both are recorded on the server entity, allowed or refused. Only instance admins are allowed.

**What to do:** Nothing.
