---
level: minor
---

**Datasets reach the audit log: a new entity type, its actions, and a record for each batch of signed file reads.**

```text
entities[]            entity_type "dataset", with dataset and dataset_id
actions[]             create_dataset (namespace): name, dataset_id, base_location, managed, properties
                      commit, promote, reset, manage_refs, read_data (dataset): target_refs
                      drop (dataset): force, purge
                      move (dataset): destination
operation             "dataset_files_signed", outcome success, forbidden or failed
context               warehouse_id, dataset_id, snapshot_id, access_grant_id, key_count
resource_type         "dataset" on grant records
```

`managed` is a boolean and `key_count` a number. `dataset_id` on a `dataset_files_signed` record is absent when the call named no existing dataset. A `dataset_files_signed` record never carries the signed URLs.

**What to do:** nothing, unless you want these records. Select them by `entity_type`, and the signed reads by `operation`.
