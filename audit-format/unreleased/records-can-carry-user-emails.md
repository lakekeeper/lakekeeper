---
level: minor
---

**Records can carry the email of the users they name.** Off by default; `LAKEKEEPER__AUDIT__TRACING__INCLUDE_USER_EMAIL=true` turns it on.

```text
actor.email                          the principal's email, for principal and assumed_role actors
authorizations[].for_principal.email  the user's email, for a user subject
actions[].principals[].email         the user's email, for a user an apply_grants request names
context.principal.email              the recipient's email, on grant_created and grant_revoked
```

Each is best-effort: absent when the email is not known, never `null`. Roles, `anonymous` and `lakekeeper_internal` actors never carry one. An email is metadata, not identity: emails are not unique and can change.

**What to do:** correlate on `principal`, `user` and `role`, never on `email`. If you enable the setting, treat the log as holding personal data: an email stays in the log after the user is deleted.
