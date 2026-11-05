---
level: none
---

**Replacing a warehouse's storage credential is recorded as `update_storage_credential`; a storage profile change records `update_storage` and `update_storage_credential`.**

```text
                                                  before            after
POST …/warehouse/{id}/storage-credential          update_storage    update_storage_credential
POST …/warehouse/{id}/storage/validate-credential update_storage    update_storage_credential
POST …/warehouse/{id}/storage                     update_storage    update_storage, update_storage_credential
POST …/warehouse/{id}/storage/validate-profile    update_storage    update_storage, update_storage_credential
POST …/warehouse/{id}/storage/validate-access     update_storage    update_storage
```

A record with both actions has two entries in `actions` and one entry per action in `authorizations`. If the request is denied, both entries have `allowed: false`. If an error stopped the evaluation, neither entry has `allowed`.

**What to do:** If a rule finds credential replacements by `update_storage`, match `update_storage_credential` as well. If a rule reads only the first entry of `actions`, read all entries.
