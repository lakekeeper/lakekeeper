-- Normalized inline schema storage for Iceberg tables (first-class columns).
-- Additive only: creates new tables + the governance-GC trigger. The schema-write freeze
-- trigger and the backfill are applied by the migration hook; dropping table_schema.schema
-- is a follow-up release.

-- Type-kind discriminator. geometry/geography/unknown are released Iceberg v3 types not yet in
-- the pinned iceberg-rust — pre-added so adopting them is a pure-Rust change (the exhaustive
-- match in normalized_schema.rs forces it), avoiding a one-way enum migration later.
CREATE TYPE iceberg_type_kind AS ENUM (
    'boolean','int','long','float','double','decimal','date','time',
    'timestamp','timestamptz','timestamp_ns','timestamptz_ns',
    'string','uuid','fixed','binary','geometry','geography','unknown',
    'variant','struct','list','map');

-- One row per (schema_id x field), content inline (no dedup). How a node attaches to its parent
-- (struct field / list element / map key|value) is derived from the parent's type_kind + ordinal,
-- so it is not stored. Insert/delete-only, hence no created_at/updated_at.
CREATE TABLE schema_field (
    warehouse_id     uuid NOT NULL,
    table_id         uuid NOT NULL,
    schema_id        int  NOT NULL,
    field_id         int  NOT NULL,
    parent_field_id  int,
    ordinal          int NOT NULL,
    name             text NOT NULL,
    required         boolean NOT NULL,
    doc              text,
    type_kind        iceberg_type_kind NOT NULL,
    type_params      jsonb,
    initial_default  jsonb,
    write_default    jsonb,
    -- Membership in the schema's identifier_field_ids set, modeled per-field (not as an
    -- int[]) so it can only reference a real field. No default: a write that omits it fails
    -- loud rather than silently dropping identifier-ness.
    is_identifier    boolean NOT NULL,
    PRIMARY KEY (warehouse_id, table_id, schema_id, field_id),
    FOREIGN KEY (warehouse_id, table_id, schema_id)
        REFERENCES table_schema (warehouse_id, table_id, schema_id) ON DELETE CASCADE
);
CREATE INDEX schema_field_assembly
    ON schema_field (warehouse_id, table_id, schema_id, parent_field_id, ordinal);
CREATE INDEX schema_field_by_field
    ON schema_field (warehouse_id, table_id, field_id);

-- Governance spine: the stable per-column identity tags/masks/lineage FK to.
-- Keyed on field_id (independent of how content is stored).
CREATE TABLE column_identity (
    warehouse_id uuid NOT NULL,
    table_id     uuid NOT NULL,
    field_id     int  NOT NULL,
    PRIMARY KEY (warehouse_id, table_id, field_id),
    FOREIGN KEY (warehouse_id, table_id)
        REFERENCES "table" (warehouse_id, table_id) ON DELETE CASCADE
);

-- Refcount GC: reap a column_identity when its last schema_field row is gone.
-- Correctness rests on OCC-serialized table commits (CAS on tabular.metadata_location), not the
-- FOR UPDATE below (that only orders concurrent GC bodies for deadlock-safety).
-- remove_schemas fires this via an explicit DELETE (reliable transition-table capture); a
-- whole-table drop reaps column_identity via its own "table" cascade, leaving this DELETE a no-op.
CREATE FUNCTION gc_orphaned_columns() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Deterministic lock order (deadlock-safety; see header).
    PERFORM 1 FROM column_identity c
      WHERE (c.warehouse_id, c.table_id, c.field_id) IN
            (SELECT warehouse_id, table_id, field_id FROM removed)
      ORDER BY c.warehouse_id, c.table_id, c.field_id
      FOR UPDATE;
    DELETE FROM column_identity c
      WHERE (c.warehouse_id, c.table_id, c.field_id) IN
            (SELECT warehouse_id, table_id, field_id FROM removed)
        AND NOT EXISTS (
            SELECT 1 FROM schema_field f
            WHERE f.warehouse_id = c.warehouse_id
              AND f.table_id = c.table_id
              AND f.field_id = c.field_id);
    RETURN NULL;
END $$;

CREATE TRIGGER schema_field_gc AFTER DELETE ON schema_field
    REFERENCING OLD TABLE AS removed
    FOR EACH STATEMENT EXECUTE FUNCTION gc_orphaned_columns();
