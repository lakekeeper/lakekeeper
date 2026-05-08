-- Canonicalise `tabular.fs_location` to align stored bytes with the
-- canonicalisation contract from `Location::from_str` (#1743). The data
-- rewrite lives in the Rust hook (`canonicalise_fs_location.rs`),
-- which runs after this file inside the same transaction. A
-- duplicate-canonical collision rolls back the upgrade with a
-- unique-violation naming the conflicting rows.
--
-- The unique index uses the `text_pattern_ops` opclass so prefix LIKE
-- queries (the overlap check in `tabular/mod.rs::create_tabular`) use
-- it directly without depending on the database collation. The
-- constraint spans live AND soft-deleted rows: tables own their
-- canonical location until purge.

DROP INDEX tabular_warehouse_id_location_idx;

CREATE UNIQUE INDEX tabular_warehouse_canonical_uq
    ON tabular (warehouse_id, fs_location text_pattern_ops);
