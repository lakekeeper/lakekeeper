---
level: none
---

**Replacing the storage credential of a warehouse is recorded with the new action `update_storage_credential`. A change of the storage profile also replaces the credential, so its records carry both actions.**

```text
                                                  before            after
POST …/warehouse/{id}/storage-credential          update_storage    update_storage_credential
POST …/warehouse/{id}/storage/validate-credential update_storage    update_storage_credential
POST …/warehouse/{id}/storage                     update_storage    update_storage, update_storage_credential
POST …/warehouse/{id}/storage/validate-profile    update_storage    update_storage, update_storage_credential
POST …/warehouse/{id}/storage/validate-access     update_storage    update_storage
```

A record with both actions has two entries in `actions` and one entry per action in `authorizations`. If the request is denied, both entries have `allowed: false`, and the `error` message names the action that was refused. If an error stopped the evaluation, neither entry has `allowed`.

**What to do:** If a rule finds credential replacements by `update_storage`, match `update_storage_credential` as well. If a rule reads only the first entry of `actions`, read all entries.
