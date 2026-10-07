---
level: major
---

**A role on a record carries its `provider_id` and `source_id` next to its id.** `provider_id` is always shown; `LAKEKEEPER__AUDIT__TRACING__INCLUDE_ROLE_SOURCE_ID=false` leaves `source_id` out.

```text
before  {"role": "1f7b…"}
after   {"role": "1f7b…", "provider_id": "corporate-ldap", "source_id": "engineering"}
```

This applies wherever a role is named: `authorizations[].for_principal`, `context.principal` on `grant_created` and `grant_revoked`, `principals` on `apply_grants`, and `principal` on `read_subtree_grants` and `revoke_subtree_grants`. Both fields are absent when the role no longer exists or could not be looked up. With the setting off, `source_id` is absent everywhere, including on `actor.assumed_role`, which carries it otherwise.

**What to do:** correlate on the role's id. Read `provider_id` and `source_id` as optional on a role a record names, and `source_id` as optional on `actor.assumed_role`. Some providers let a source id be a free-form name, so it might hold personal data; turn the setting off if that matters for your log.
