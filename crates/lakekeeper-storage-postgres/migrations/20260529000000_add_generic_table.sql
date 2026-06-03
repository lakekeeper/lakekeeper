-- Generic tables: catalog support for non-Iceberg formats (Lance, Delta, ...).

ALTER TABLE tabular DROP CONSTRAINT IF EXISTS tabular_check;
ALTER TABLE tabular DROP CONSTRAINT IF EXISTS tabular_metadata_location_check;
ALTER TABLE task DROP CONSTRAINT IF EXISTS task_warehouse_id_check;
ALTER TABLE task DROP CONSTRAINT IF EXISTS task_entity_check;
ALTER TABLE task_log DROP CONSTRAINT IF EXISTS task_log_warehouse_id_check;
ALTER TABLE task_log DROP CONSTRAINT IF EXISTS task_log_entity_check;
ALTER TABLE idempotency_record DROP CONSTRAINT IF EXISTS idempotency_operation_check;

DROP VIEW IF EXISTS active_tables;
DROP VIEW IF EXISTS active_views;
DROP VIEW IF EXISTS active_tabulars;

DROP INDEX IF EXISTS task_warehouse_entity_id_queue_idx;

ALTER TYPE tabular_type RENAME TO tabular_type_old;
CREATE TYPE tabular_type AS ENUM ('table', 'view', 'generic-table');

ALTER TYPE entity_type RENAME TO entity_type_old;
CREATE TYPE entity_type AS ENUM (
    'table', 'view', 'project', 'warehouse',
    'namespace', 'role', 'user', 'server', 'generic-table'
);

-- Combined ALTER TABLE: rewrites the column and validates the new CHECK in
-- one table pass under a single AccessExclusiveLock.
ALTER TABLE tabular
    ALTER COLUMN typ TYPE tabular_type USING typ::text::tabular_type,
    ADD CONSTRAINT tabular_metadata_location_check CHECK (
        (typ = 'view' AND metadata_location IS NOT NULL)
        OR typ IN ('table', 'generic-table')
    );

ALTER TABLE task
    ALTER COLUMN entity_type TYPE entity_type USING entity_type::text::entity_type,
    ADD CONSTRAINT task_warehouse_id_check CHECK (
        (entity_type = 'project' AND warehouse_id IS NULL)
        OR (entity_type IN ('warehouse', 'table', 'view', 'generic-table') AND warehouse_id IS NOT NULL)
    ),
    ADD CONSTRAINT task_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_id IS NULL AND entity_name IS NULL)
        OR (entity_type IN ('table', 'view', 'generic-table') AND entity_id IS NOT NULL AND entity_name IS NOT NULL)
    );

ALTER TABLE task_log
    ALTER COLUMN entity_type TYPE entity_type USING entity_type::text::entity_type,
    ADD CONSTRAINT task_log_warehouse_id_check CHECK (
        (entity_type = 'project' AND warehouse_id IS NULL)
        OR (entity_type IN ('warehouse', 'table', 'view', 'generic-table') AND warehouse_id IS NOT NULL)
    ),
    ADD CONSTRAINT task_log_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_id IS NULL AND entity_name IS NULL)
        OR (entity_type IN ('table', 'view', 'generic-table') AND entity_id IS NOT NULL AND entity_name IS NOT NULL)
    );

DROP TYPE tabular_type_old;
DROP TYPE entity_type_old;

ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-create-generic-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-list-generic-tables';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-load-generic-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-drop-generic-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-rename-generic-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-load-generic-table-credentials';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-get-generic-table-actions';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-get-generic-table-protection';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-set-generic-table-protection';

CREATE TABLE generic_table (
    warehouse_id     UUID NOT NULL,
    generic_table_id UUID NOT NULL,
    format           TEXT NOT NULL
        CHECK (format ~ '^[a-z][a-z0-9_-]{0,63}$'),
    doc              TEXT,
    schema_info      JSONB
        CHECK (schema_info IS NULL OR octet_length(schema_info::text) <= 1048576),
    statistics       JSONB
        CHECK (statistics IS NULL OR octet_length(statistics::text) <= 1048576),
    version          BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ,
    PRIMARY KEY (warehouse_id, generic_table_id),
    FOREIGN KEY (warehouse_id, generic_table_id)
        REFERENCES tabular(warehouse_id, tabular_id) ON DELETE CASCADE
);
SELECT trigger_updated_at_and_version_if_distinct('"generic_table"');

CREATE TABLE generic_table_properties (
    warehouse_id     UUID NOT NULL,
    generic_table_id UUID NOT NULL,
    key              TEXT NOT NULL,
    value            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ,
    PRIMARY KEY (warehouse_id, generic_table_id, key),
    FOREIGN KEY (warehouse_id, generic_table_id)
        REFERENCES generic_table(warehouse_id, generic_table_id) ON DELETE CASCADE
);
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON generic_table_properties
    FOR EACH ROW
    WHEN (old.* IS DISTINCT FROM new.*)
    EXECUTE FUNCTION set_updated_at();

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
  JOIN warehouse w
    ON t.warehouse_id = w.warehouse_id
   AND w.status = 'active'::warehouse_status;

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

CREATE INDEX task_warehouse_entity_id_queue_idx
    ON task (warehouse_id, entity_id, queue_name)
    WHERE entity_type IN ('table', 'view', 'generic-table');

ALTER TABLE idempotency_record ADD CONSTRAINT idempotency_operation_check CHECK (
    operation::text IN (
        'catalog-v1-create-namespace',
        'catalog-v1-update-namespace-properties',
        'catalog-v1-drop-namespace',
        'catalog-v1-create-table',
        'catalog-v1-update-table',
        'catalog-v1-drop-table',
        'catalog-v1-rename-table',
        'catalog-v1-register-table',
        'catalog-v1-create-view',
        'catalog-v1-replace-view',
        'catalog-v1-drop-view',
        'catalog-v1-rename-view',
        'catalog-v1-commit-transaction',
        'generic-table-v1-create-generic-table',
        'generic-table-v1-drop-generic-table',
        'generic-table-v1-rename-generic-table'
    )
);
