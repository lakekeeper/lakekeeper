-- Normalized inline schema storage for tables and views: per-field rows (schema_field), a per-field
-- identity table (tabular_field), and a reference-counting GC trigger.

-- Type-kind discriminator. Includes v3 types not yet emitted by the code (geometry/geography/
-- unknown) so adopting them later needs no enum migration.
CREATE TYPE iceberg_type_kind AS ENUM (
    'boolean','int','long','float','double','decimal','date','time',
    'timestamp','timestamptz','timestamp_ns','timestamptz_ns',
    'string','uuid','fixed','binary','geometry','geography','unknown',
    'variant','struct','list','map');

-- One row per distinct field_id in a tabular (deduped across schema versions): a stable per-field
-- anchor that outlives individual schemas. Reaped by the GC trigger below when its last schema_field
-- row is deleted.
CREATE TABLE tabular_field (
    warehouse_id uuid NOT NULL,
    tabular_id   uuid NOT NULL,
    field_id     int  NOT NULL,
    PRIMARY KEY (warehouse_id, tabular_id, field_id),
    FOREIGN KEY (warehouse_id, tabular_id)
        REFERENCES tabular (warehouse_id, tabular_id) ON DELETE CASCADE
);

-- One row per (schema_id, field), content inline; keyed by tabular_id (tables and views alike).
-- A node's role (struct field / list element / map key|value) is derived from the parent's
-- type_kind + ordinal, so it is not stored.
CREATE TABLE schema_field (
    warehouse_id     uuid NOT NULL,
    tabular_id       uuid NOT NULL,
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
    is_identifier    boolean NOT NULL,
    PRIMARY KEY (warehouse_id, tabular_id, schema_id, field_id),
    FOREIGN KEY (warehouse_id, tabular_id)
        REFERENCES tabular (warehouse_id, tabular_id) ON DELETE CASCADE,
    -- Every field row needs a tabular_field anchor. NO ACTION (not RESTRICT/CASCADE): a tabular
    -- drop cascades schema_field via its own FK, so this check must defer to statement end.
    FOREIGN KEY (warehouse_id, tabular_id, field_id)
        REFERENCES tabular_field (warehouse_id, tabular_id, field_id) ON DELETE NO ACTION
);
CREATE INDEX schema_field_assembly
    ON schema_field (warehouse_id, tabular_id, schema_id, parent_field_id, ordinal);
CREATE INDEX schema_field_by_field
    ON schema_field (warehouse_id, tabular_id, field_id);

-- Reap a tabular_field once its last schema_field row is deleted. Fires on the explicit schema_field
-- DELETE (populated transition table); a whole-tabular drop reaps via cascade instead.
CREATE FUNCTION gc_orphaned_columns() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Lock candidates in PK order to avoid deadlocks if two GC statements ever touch the same
    -- tabular concurrently.
    PERFORM 1 FROM tabular_field c
      WHERE (c.warehouse_id, c.tabular_id, c.field_id) IN
            (SELECT warehouse_id, tabular_id, field_id FROM removed)
      ORDER BY c.warehouse_id, c.tabular_id, c.field_id
      FOR UPDATE;
    DELETE FROM tabular_field c
      WHERE (c.warehouse_id, c.tabular_id, c.field_id) IN
            (SELECT warehouse_id, tabular_id, field_id FROM removed)
        AND NOT EXISTS (
            SELECT 1 FROM schema_field f
            WHERE f.warehouse_id = c.warehouse_id
              AND f.tabular_id = c.tabular_id
              AND f.field_id = c.field_id);
    RETURN NULL;
END $$;

CREATE TRIGGER schema_field_gc AFTER DELETE ON schema_field
    REFERENCING OLD TABLE AS removed
    FOR EACH STATEMENT EXECUTE FUNCTION gc_orphaned_columns();
