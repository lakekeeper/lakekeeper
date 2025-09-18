-- Migration to change primary keys of 'tabular', 'table', 'view', and related
-- tables to composite keys including 'warehouse_id'. This allows re-using IDs
-- across different warehouses.
--
-- 0. Cleanup some indices and FK constraints that are not needed anymore.
-- 1. Drop all Foreign Keys that will be affected by Primary Key changes.
-- 2. Migrate 'tabular' and 'view' tables to use composite keys.
-- 3. Migrate all 'view_*' related tables to include 'warehouse_id'.
-- 4. Migrate the 'table' table and all 'table_*' related tables.
-- 5. Update all Primary Keys and indices to their new composite form.
-- 6. Re-establish all Foreign Keys to reference the new composite keys.
-- 7. Recreate views
-- =================================================================================================
-- 0: Cleanup some indices and FK constraints that are not needed anymore.-- =================================================================================================
DROP INDEX IF EXISTS tabular_namespace_id_idx;

DROP INDEX IF EXISTS tabular_typ_tabular_id_idx;

DROP INDEX IF EXISTS tabular_namespace_id_location_idx;

DROP INDEX IF EXISTS tabular_namespace_id_name_idx;

DROP INDEX IF EXISTS view_representation_view_id_version_id_idx;

-- Not required anymore, PK already covers this case
DROP INDEX IF EXISTS view_version_log_view_id_version_id_idx;

-- =================================================================================================
-- 1: Drop all Foreign Key constraints that will be affected by Primary Key changes.
-- =================================================================================================
-- FKs referencing 'tabular'
ALTER TABLE "table"
DROP CONSTRAINT IF EXISTS tabular_ident_fk;

ALTER TABLE "view"
DROP CONSTRAINT IF EXISTS tabular_ident_fk;

-- FKs referencing 'view'
ALTER TABLE current_view_metadata_version
    DROP CONSTRAINT IF EXISTS current_view_metadata_version_view_id_fkey;
ALTER TABLE view_properties
    DROP CONSTRAINT IF EXISTS view_properties_view_id_fkey;
ALTER TABLE view_schema
    DROP CONSTRAINT IF EXISTS view_schema_view_id_fkey;

-- FKs referencing 'view_schema'
ALTER TABLE view_version
    DROP CONSTRAINT IF EXISTS view_version_view_id_schema_id_fkey;

-- FKs referencing 'view_version'
ALTER TABLE current_view_metadata_version
    DROP CONSTRAINT IF EXISTS current_view_metadata_version_view_id_version_id_fkey;
ALTER TABLE view_representation
    DROP CONSTRAINT IF EXISTS view_representation_view_id_view_version_id_fkey;
ALTER TABLE view_version_log
    DROP CONSTRAINT IF EXISTS view_version_log_view_id_version_id_fkey;

ALTER TABLE view_representation
    DROP CONSTRAINT IF EXISTS view_representation_view_id_fkey;

ALTER TABLE view_version
DROP CONSTRAINT IF EXISTS view_version_view_id_fkey;


-- FKs referencing 'table'
ALTER TABLE table_current_schema
    DROP CONSTRAINT IF EXISTS table_current_schema_table_id_fkey;
ALTER TABLE table_default_partition_spec
    DROP CONSTRAINT IF EXISTS table_default_partition_spec_table_id_fkey;
ALTER TABLE table_default_sort_order
    DROP CONSTRAINT IF EXISTS table_default_sort_order_table_id_fkey;
ALTER TABLE table_metadata_log
    DROP CONSTRAINT IF EXISTS table_metadata_log_table_id_fkey;
ALTER TABLE table_partition_spec
    DROP CONSTRAINT IF EXISTS table_partition_spec_table_id_fkey;
ALTER TABLE table_properties
    DROP CONSTRAINT IF EXISTS table_properties_table_id_fkey;
ALTER TABLE table_refs
    DROP CONSTRAINT IF EXISTS table_refs_table_id_fkey;
ALTER TABLE table_schema
    DROP CONSTRAINT IF EXISTS table_schema_table_id_fkey;
ALTER TABLE table_snapshot
    DROP CONSTRAINT IF EXISTS table_snapshot_table_id_fkey;
ALTER TABLE table_snapshot_log
    DROP CONSTRAINT IF EXISTS table_snapshot_log_table_id_fkey;
ALTER TABLE table_sort_order
    DROP CONSTRAINT IF EXISTS table_sort_order_table_id_fkey;
ALTER TABLE table_statistics
    DROP CONSTRAINT IF EXISTS table_statistics_table_id_fkey;
ALTER TABLE partition_statistics
    DROP CONSTRAINT IF EXISTS partition_statistics_table_id_fkey;

-- FKs referencing other 'table_*' tables
ALTER TABLE table_current_schema
    DROP CONSTRAINT IF EXISTS table_current_schema_table_id_schema_id_fkey;
ALTER TABLE table_snapshot
    DROP CONSTRAINT IF EXISTS table_snapshot_table_id_schema_id_fkey;
ALTER TABLE table_default_partition_spec
    DROP CONSTRAINT IF EXISTS table_default_partition_spec_table_id_partition_spec_id_fkey;
ALTER TABLE table_default_sort_order
    DROP CONSTRAINT IF EXISTS table_default_sort_order_table_id_sort_order_id_fkey;
ALTER TABLE table_refs
    DROP CONSTRAINT IF EXISTS table_refs_table_id_snapshot_id_fkey;
ALTER TABLE partition_statistics
    DROP CONSTRAINT IF EXISTS partition_statistics_table_id_snapshot_id_fkey;
ALTER TABLE table_statistics
    DROP CONSTRAINT IF EXISTS table_statistics_table_id_snapshot_id_fkey;

-- =================================================================================================
-- 2: Migrate 'tabular' and 'view' tables
-- =================================================================================================
-- Section 2.1: Migrate the 'tabular' table
ALTER TABLE "tabular" ADD COLUMN warehouse_id UUID;
UPDATE "tabular" tab SET warehouse_id = n.warehouse_id
FROM namespace n WHERE tab.namespace_id = n.namespace_id;
ALTER TABLE "tabular" ALTER COLUMN warehouse_id SET NOT NULL;

-- Section 2.2: Migrate the 'view' table
ALTER TABLE "view" ADD COLUMN warehouse_id UUID;
UPDATE "view" v SET warehouse_id = t.warehouse_id
FROM "tabular" t WHERE v.view_id = t.tabular_id;
ALTER TABLE "view" ALTER COLUMN warehouse_id SET NOT NULL;

-- =================================================================================================
-- 3: Migrate tables related to 'view'
-- =================================================================================================

ALTER TABLE current_view_metadata_version ADD COLUMN warehouse_id UUID;
UPDATE current_view_metadata_version cvmv SET warehouse_id = v.warehouse_id
FROM "view" v WHERE cvmv.view_id = v.view_id;
ALTER TABLE current_view_metadata_version ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE view_properties ADD COLUMN warehouse_id UUID;
UPDATE view_properties vp SET warehouse_id = v.warehouse_id
FROM "view" v WHERE vp.view_id = v.view_id;
ALTER TABLE view_properties ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE view_representation ADD COLUMN warehouse_id UUID;
UPDATE view_representation vr SET warehouse_id = v.warehouse_id
FROM "view" v WHERE vr.view_id = v.view_id;
ALTER TABLE view_representation ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE view_schema ADD COLUMN warehouse_id UUID;
UPDATE view_schema vs SET warehouse_id = v.warehouse_id
FROM "view" v WHERE vs.view_id = v.view_id;
ALTER TABLE view_schema ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE view_version ADD COLUMN warehouse_id UUID;
UPDATE view_version vv SET warehouse_id = v.warehouse_id
FROM "view" v WHERE vv.view_id = v.view_id;
ALTER TABLE view_version ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE view_version_log ADD COLUMN warehouse_id UUID;
UPDATE view_version_log vvl SET warehouse_id = v.warehouse_id
FROM "view" v WHERE vvl.view_id = v.view_id;
ALTER TABLE view_version_log ALTER COLUMN warehouse_id SET NOT NULL;

-- =================================================================================================
-- 4: Migrate the "table" table and its related tables
-- =================================================================================================

-- Section 4.1: Modify the "table" table itself
ALTER TABLE "table" ADD COLUMN warehouse_id UUID;
UPDATE "table" t SET warehouse_id = tab.warehouse_id
FROM tabular tab WHERE t.table_id = tab.tabular_id;
ALTER TABLE "table" ALTER COLUMN warehouse_id SET NOT NULL;

-- Section 4.2: Update tables that DIRECTLY REFERENCE "table"
-- Table: table_current_schema
ALTER TABLE table_current_schema ADD COLUMN warehouse_id UUID;
UPDATE table_current_schema tcs SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tcs.table_id = t.table_id;
ALTER TABLE table_current_schema ALTER COLUMN warehouse_id SET NOT NULL;

-- Other 'table_*' tables follow the same pattern...
ALTER TABLE table_default_partition_spec ADD COLUMN warehouse_id UUID;
UPDATE table_default_partition_spec tdps SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tdps.table_id = t.table_id;
ALTER TABLE table_default_partition_spec ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_default_sort_order ADD COLUMN warehouse_id UUID;
UPDATE table_default_sort_order tdso SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tdso.table_id = t.table_id;
ALTER TABLE table_default_sort_order ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_metadata_log ADD COLUMN warehouse_id UUID;
UPDATE table_metadata_log tml SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tml.table_id = t.table_id;
ALTER TABLE table_metadata_log ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_partition_spec ADD COLUMN warehouse_id UUID;
UPDATE table_partition_spec tps SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tps.table_id = t.table_id;
ALTER TABLE table_partition_spec ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_properties ADD COLUMN warehouse_id UUID;
UPDATE table_properties tp SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tp.table_id = t.table_id;
ALTER TABLE table_properties ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_refs ADD COLUMN warehouse_id UUID;
UPDATE table_refs tr SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tr.table_id = t.table_id;
ALTER TABLE table_refs ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_schema ADD COLUMN warehouse_id UUID;
UPDATE table_schema ts SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE ts.table_id = t.table_id;
ALTER TABLE table_schema ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_snapshot ADD COLUMN warehouse_id UUID;
UPDATE table_snapshot ts SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE ts.table_id = t.table_id;
ALTER TABLE table_snapshot ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_snapshot_log ADD COLUMN warehouse_id UUID;
UPDATE table_snapshot_log tsl SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tsl.table_id = t.table_id;
ALTER TABLE table_snapshot_log ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_sort_order ADD COLUMN warehouse_id UUID;
UPDATE table_sort_order tso SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE tso.table_id = t.table_id;
ALTER TABLE table_sort_order ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE table_statistics ADD COLUMN warehouse_id UUID;
UPDATE table_statistics ts SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE ts.table_id = t.table_id;
ALTER TABLE table_statistics ALTER COLUMN warehouse_id SET NOT NULL;

ALTER TABLE partition_statistics ADD COLUMN warehouse_id UUID;
UPDATE partition_statistics ps SET warehouse_id = t."warehouse_id"
FROM "table" t WHERE ps.table_id = t.table_id;
ALTER TABLE partition_statistics ALTER COLUMN warehouse_id SET NOT NULL;

-- =================================================================================================
-- 5: Modify all Primary Keys and indices to include 'warehouse_id'.
-- =================================================================================================
-- Drop and Add PK for 'tabular'
ALTER TABLE "tabular" 
    DROP CONSTRAINT tabular_pkey,
    ADD CONSTRAINT tabular_pkey PRIMARY KEY (warehouse_id, tabular_id);

-- Drop and Add PK for 'view'
ALTER TABLE "view" 
    DROP CONSTRAINT view_pkey,
    ADD CONSTRAINT view_pkey PRIMARY KEY (warehouse_id, view_id);

-- Drop and Add PKs for 'view_*' tables
ALTER TABLE current_view_metadata_version
    DROP CONSTRAINT current_view_metadata_version_pkey,
    ADD CONSTRAINT current_view_metadata_version_pkey PRIMARY KEY (warehouse_id, view_id);

ALTER TABLE view_properties 
    DROP CONSTRAINT view_properties_pkey, 
    ADD CONSTRAINT view_properties_pkey PRIMARY KEY (warehouse_id, view_id, key);

ALTER TABLE view_representation 
    DROP CONSTRAINT view_representation_pkey, 
    ADD CONSTRAINT view_representation_pkey PRIMARY KEY (warehouse_id, view_representation_id);

ALTER TABLE view_schema 
    DROP CONSTRAINT unique_schema_per_view, 
    ADD CONSTRAINT unique_schema_per_view PRIMARY KEY (warehouse_id, view_id, schema_id);

ALTER TABLE view_version 
    DROP CONSTRAINT unique_version_per_metadata, 
    ADD CONSTRAINT unique_version_per_metadata PRIMARY KEY (warehouse_id, view_id, version_id);

ALTER TABLE view_version_log 
    DROP CONSTRAINT view_version_log_pkey, 
    ADD CONSTRAINT view_version_log_pkey PRIMARY KEY (warehouse_id, view_id, version_id, "timestamp");

ALTER TABLE "table" 
    DROP CONSTRAINT table_pkey,
    ADD CONSTRAINT table_pkey PRIMARY KEY (warehouse_id, table_id);

-- Drop and Add PKs for 'table_*' tables
ALTER TABLE table_current_schema
    DROP CONSTRAINT table_current_schema_pkey,
    ADD CONSTRAINT table_current_schema_pkey PRIMARY KEY (warehouse_id, table_id);
ALTER TABLE table_default_partition_spec
    DROP CONSTRAINT table_default_partition_spec_pkey,
    ADD CONSTRAINT table_default_partition_spec_pkey PRIMARY KEY (warehouse_id, table_id);
ALTER TABLE table_default_sort_order
    DROP CONSTRAINT table_default_sort_order_pkey,
    ADD CONSTRAINT table_default_sort_order_pkey PRIMARY KEY (warehouse_id, table_id);
ALTER TABLE partition_statistics
    DROP CONSTRAINT partition_statistics_pkey,
    ADD CONSTRAINT partition_statistics_pkey PRIMARY KEY (warehouse_id, table_id, snapshot_id);
ALTER TABLE table_metadata_log
    DROP CONSTRAINT table_metadata_log_pkey,
    ADD CONSTRAINT table_metadata_log_pkey PRIMARY KEY (warehouse_id, table_id, sequence_number);
ALTER TABLE table_partition_spec
    DROP CONSTRAINT table_partition_spec_pkey,
    ADD CONSTRAINT table_partition_spec_pkey PRIMARY KEY (warehouse_id, table_id, partition_spec_id);
ALTER TABLE table_properties
    DROP CONSTRAINT table_properties_pkey,
    ADD CONSTRAINT table_properties_pkey PRIMARY KEY (warehouse_id, table_id, key);
ALTER TABLE table_refs
    DROP CONSTRAINT table_refs_pkey,
    ADD CONSTRAINT table_refs_pkey PRIMARY KEY (warehouse_id, table_id, table_ref_name);
ALTER TABLE table_schema
    DROP CONSTRAINT table_schema_pkey,
    ADD CONSTRAINT table_schema_pkey PRIMARY KEY (warehouse_id, table_id, schema_id);
ALTER TABLE table_snapshot
    DROP CONSTRAINT table_snapshot_pkey,
    ADD CONSTRAINT table_snapshot_pkey PRIMARY KEY (warehouse_id, table_id, snapshot_id);
ALTER TABLE table_snapshot_log
    DROP CONSTRAINT table_snapshot_log_pkey,
    ADD CONSTRAINT table_snapshot_log_pkey PRIMARY KEY (warehouse_id, table_id, sequence_number);
ALTER TABLE table_sort_order
    DROP CONSTRAINT table_sort_order_pkey,
    ADD CONSTRAINT table_sort_order_pkey PRIMARY KEY (warehouse_id, table_id, sort_order_id);
ALTER TABLE table_statistics
    DROP CONSTRAINT table_statistics_pkey,
    ADD CONSTRAINT table_statistics_pkey PRIMARY KEY (warehouse_id, table_id, snapshot_id);

-- =================================================================================================
-- 6: Re-Create indices
-- =================================================================================================

-- Business key (non-unique). Multiple dialects possible for same (warehouse_id, namespace_id, name).
CREATE INDEX view_representation_view_id_version_id_idx
    ON view_representation USING btree (warehouse_id, view_id, view_version_id);
-- Index supports create table lookup by location for create table conflict checks
-- and s3_location lookup for remote signing
CREATE INDEX tabular_warehouse_id_location_idx
    ON tabular USING btree (warehouse_id, fs_location);

-- Re-Create this unique index to include warehouse_id. Also makes separate index for tabular_ident_to_id query obsolete
ALTER TABLE tabular
    DROP CONSTRAINT IF EXISTS unique_name_per_namespace_id,
    ADD constraint unique_name_per_namespace_id unique NULLS not distinct (warehouse_id, namespace_id, name, deleted_at);

-- Index required to create FK constraint from 'tabular' to 'namespace'
-- on (warehouse_id, namespace_id)
ALTER TABLE namespace
    DROP CONSTRAINT IF EXISTS unique_namespace_id_per_warehouse,
    ADD CONSTRAINT unique_namespace_id_per_warehouse UNIQUE (warehouse_id, namespace_id);
    
-- =================================================================================================
-- 7: Re-add all Foreign Key constraints with composite keys.
-- =================================================================================================

-- Add FK from 'tabular' to 'namespace', indirectly to 'warehouse'
ALTER TABLE "tabular"
    DROP CONSTRAINT IF EXISTS tabular_warehouse_id_fkey,
    DROP CONSTRAINT IF EXISTS tabular_namespace_id_fkey,
    ADD CONSTRAINT tabular_warehouse_id_namespace_id_fkey
        FOREIGN KEY (warehouse_id, namespace_id) REFERENCES namespace(warehouse_id, namespace_id) ON DELETE CASCADE;

-- Add FKs for 'view' and 'table' to 'tabular'
ALTER TABLE "view" ADD CONSTRAINT tabular_ident_fk
    FOREIGN KEY (warehouse_id, view_id)
    REFERENCES "tabular"(warehouse_id, tabular_id) ON DELETE CASCADE;
ALTER TABLE "table" ADD CONSTRAINT tabular_ident_fk
    FOREIGN KEY (warehouse_id, table_id)
    REFERENCES "tabular"(warehouse_id, tabular_id) ON DELETE CASCADE;

-- Add FKs for 'view_*' tables
ALTER TABLE view_schema ADD CONSTRAINT view_schema_view_id_fkey
    FOREIGN KEY (warehouse_id, view_id) REFERENCES "view"(warehouse_id, view_id) ON DELETE CASCADE;
ALTER TABLE view_version ADD CONSTRAINT view_version_view_id_schema_id_fkey
    FOREIGN KEY (warehouse_id, view_id, schema_id)
    REFERENCES view_schema(warehouse_id, view_id, schema_id) ON DELETE CASCADE;
ALTER TABLE view_properties ADD CONSTRAINT view_properties_view_id_fkey
    FOREIGN KEY (warehouse_id, view_id) REFERENCES "view"(warehouse_id, view_id) ON DELETE CASCADE;
ALTER TABLE view_representation ADD CONSTRAINT view_representation_view_id_view_version_id_fkey
    FOREIGN KEY (warehouse_id, view_id, view_version_id)
    REFERENCES view_version(warehouse_id, view_id, version_id) ON DELETE CASCADE;
ALTER TABLE view_version_log ADD CONSTRAINT view_version_log_view_id_version_id_fkey
    FOREIGN KEY (warehouse_id, view_id, version_id)
    REFERENCES view_version(warehouse_id, view_id, version_id) ON DELETE CASCADE;
ALTER TABLE current_view_metadata_version ADD CONSTRAINT current_view_metadata_version_view_id_fkey
    FOREIGN KEY (warehouse_id, view_id) REFERENCES "view"(warehouse_id, view_id) ON DELETE CASCADE;
ALTER TABLE current_view_metadata_version
ADD CONSTRAINT current_view_metadata_version_view_id_version_id_fkey
    FOREIGN KEY (warehouse_id, view_id, version_id)
    REFERENCES view_version(warehouse_id, view_id, version_id) ON DELETE CASCADE;

-- Add FKs for 'table_*' tables
ALTER TABLE table_metadata_log ADD CONSTRAINT table_metadata_log_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;
ALTER TABLE table_partition_spec ADD CONSTRAINT table_partition_spec_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;
ALTER TABLE table_properties ADD CONSTRAINT table_properties_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;
ALTER TABLE table_refs ADD CONSTRAINT table_refs_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;
ALTER TABLE table_schema ADD CONSTRAINT table_schema_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;
ALTER TABLE table_snapshot_log ADD CONSTRAINT table_snapshot_log_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;
ALTER TABLE table_sort_order ADD CONSTRAINT table_sort_order_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;
ALTER TABLE partition_statistics ADD CONSTRAINT partition_statistics_table_id_fkey
    FOREIGN KEY (warehouse_id, table_id) REFERENCES "table"(warehouse_id, table_id) ON DELETE CASCADE;

-- Add remaining FKs between 'table_*' tables
ALTER TABLE table_current_schema ADD CONSTRAINT table_current_schema_table_id_schema_id_fkey
    FOREIGN KEY (warehouse_id, table_id, schema_id)
    REFERENCES table_schema(warehouse_id, table_id, schema_id) ON DELETE CASCADE;
ALTER TABLE table_default_partition_spec
ADD CONSTRAINT table_default_partition_spec_table_id_partition_spec_id_fkey
    FOREIGN KEY (warehouse_id, table_id, partition_spec_id)
    REFERENCES table_partition_spec(warehouse_id, table_id, partition_spec_id) ON DELETE CASCADE;
ALTER TABLE table_default_sort_order
ADD CONSTRAINT table_default_sort_order_table_id_sort_order_id_fkey
    FOREIGN KEY (warehouse_id, table_id, sort_order_id)
    REFERENCES table_sort_order(warehouse_id, table_id, sort_order_id) ON DELETE CASCADE;
ALTER TABLE table_refs ADD CONSTRAINT table_refs_table_id_snapshot_id_fkey
    FOREIGN KEY (warehouse_id, table_id, snapshot_id)
    REFERENCES table_snapshot(warehouse_id, table_id, snapshot_id) ON DELETE CASCADE;
ALTER TABLE table_snapshot ADD CONSTRAINT table_snapshot_table_id_schema_id_fkey
    FOREIGN KEY (warehouse_id, table_id, schema_id)
    REFERENCES table_schema(warehouse_id, table_id, schema_id) ON DELETE CASCADE;
ALTER TABLE partition_statistics ADD CONSTRAINT partition_statistics_table_id_snapshot_id_fkey
    FOREIGN KEY (warehouse_id, table_id, snapshot_id)
    REFERENCES table_snapshot(warehouse_id, table_id, snapshot_id) ON DELETE CASCADE;
ALTER TABLE table_statistics ADD CONSTRAINT table_statistics_table_id_snapshot_id_fkey
    FOREIGN KEY (warehouse_id, table_id, snapshot_id)
    REFERENCES table_snapshot(warehouse_id, table_id, snapshot_id) ON DELETE CASCADE;

-- =================================================================================================
-- 8: Recreate views
-- =================================================================================================

DROP VIEW IF EXISTS active_tables;
DROP VIEW IF EXISTS active_views;
DROP VIEW IF EXISTS active_tabulars;

CREATE VIEW active_tabulars
AS SELECT t.tabular_id,
    t.namespace_id,
    t.name,
    t.typ,
    t.metadata_location,
    t.fs_protocol,
    t.fs_location,
    t.warehouse_id,
    n.namespace_name
   FROM tabular t
     JOIN namespace n ON t.namespace_id = n.namespace_id
     JOIN warehouse w ON n.warehouse_id = w.warehouse_id AND w.status = 'active'::warehouse_status;

CREATE VIEW active_tables
AS SELECT tabular_id AS table_id,
    namespace_id,
    warehouse_id,
    name,
    metadata_location,
    fs_protocol,
    fs_location
   FROM active_tabulars t
  WHERE typ = 'table'::tabular_type;

CREATE VIEW active_views
AS SELECT tabular_id AS view_id,
    namespace_id,
    warehouse_id,
    name,
    metadata_location,
    fs_protocol,
    fs_location
   FROM active_tabulars t
  WHERE typ = 'view'::tabular_type;
