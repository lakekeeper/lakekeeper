-- Canonicalise `tabular.fs_location` to align stored bytes with the
-- canonicalisation contract from `Location::from_str` (#1743). The data
-- rewrite lives in the Rust hook (`canonicalise_fs_location.rs`),
-- which runs after this file inside the same transaction. A
-- duplicate-canonical collision rolls back the upgrade with a
-- unique-violation naming the conflicting rows.


DROP INDEX IF EXISTS tabular_warehouse_id_location_idx;

CREATE UNIQUE INDEX IF NOT EXISTS tabular_warehouse_canonical_uq
    ON tabular (warehouse_id, fs_location text_pattern_ops);
