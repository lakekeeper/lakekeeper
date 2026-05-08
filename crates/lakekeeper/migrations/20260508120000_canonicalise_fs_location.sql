-- ALTER COLUMN TYPE refuses while views depend on the column. Drop in
-- reverse-dependency order, ALTER, recreate.
DROP VIEW active_tables;
DROP VIEW active_views;
DROP VIEW active_tabulars;

-- Byte-canonical comparison: defeats locale-sensitive equality and lets
-- the existing `(warehouse_id, fs_location)` btree serve LIKE 'prefix%'
-- queries.
ALTER TABLE tabular
    ALTER COLUMN fs_location TYPE TEXT COLLATE "C";

CREATE VIEW active_tabulars AS
SELECT t.tabular_id,
    t.namespace_id,
    t.name,
    t.typ,
    t.metadata_location,
    t.fs_protocol,
    t.fs_location,
    t.warehouse_id,
    t.tabular_namespace_name AS namespace_name
   FROM tabular t
     JOIN warehouse w ON t.warehouse_id = w.warehouse_id AND w.status = 'active'::warehouse_status;

CREATE VIEW active_tables AS
SELECT tabular_id AS table_id,
    namespace_id,
    warehouse_id,
    name,
    metadata_location,
    fs_protocol,
    fs_location
   FROM active_tabulars t
  WHERE typ = 'table'::tabular_type;

CREATE VIEW active_views AS
SELECT tabular_id AS view_id,
    namespace_id,
    warehouse_id,
    name,
    metadata_location,
    fs_protocol,
    fs_location
   FROM active_tabulars t
  WHERE typ = 'view'::tabular_type;

-- Replace the old non-unique index with a UNIQUE one. The constraint
-- spans live AND soft-deleted rows: tables own their canonical location
-- until purge.
DROP INDEX tabular_warehouse_id_location_idx;

CREATE UNIQUE INDEX tabular_warehouse_canonical_uq
    ON tabular (warehouse_id, fs_location);
