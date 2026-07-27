ALTER TABLE tabular DROP CONSTRAINT IF EXISTS tabular_metadata_location_check;
ALTER TABLE task DROP CONSTRAINT IF EXISTS task_warehouse_id_check;
ALTER TABLE task DROP CONSTRAINT IF EXISTS task_entity_check;
ALTER TABLE task_log DROP CONSTRAINT IF EXISTS task_log_warehouse_id_check;
ALTER TABLE task_log DROP CONSTRAINT IF EXISTS task_log_entity_check;

DROP VIEW IF EXISTS active_tables;
DROP VIEW IF EXISTS active_views;
DROP VIEW IF EXISTS active_tabulars;

DROP INDEX IF EXISTS task_warehouse_entity_id_queue_idx;

ALTER TYPE tabular_type RENAME TO tabular_type_old;
CREATE TYPE tabular_type AS ENUM ('table', 'view', 'generic-table', 'paimon-table');

ALTER TYPE entity_type RENAME TO entity_type_old;
CREATE TYPE entity_type AS ENUM (
    'table',
    'view',
    'project',
    'warehouse',
    'namespace',
    'role',
    'user',
    'server',
    'generic-table',
    'paimon-table'
);

ALTER TABLE tabular
    ALTER COLUMN typ TYPE tabular_type USING typ::text::tabular_type,
    ADD CONSTRAINT tabular_metadata_location_check CHECK (
        (typ = 'view' AND metadata_location IS NOT NULL)
        OR typ IN ('table', 'generic-table', 'paimon-table')
    );

ALTER TABLE task
    ALTER COLUMN entity_type TYPE entity_type USING entity_type::text::entity_type,
    ADD CONSTRAINT task_warehouse_id_check CHECK (
        (entity_type = 'project' AND warehouse_id IS NULL)
        OR (
            entity_type IN ('warehouse', 'table', 'view', 'generic-table', 'paimon-table')
            AND warehouse_id IS NOT NULL
        )
    ),
    ADD CONSTRAINT task_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_id IS NULL AND entity_name IS NULL)
        OR (
            entity_type IN ('table', 'view', 'generic-table', 'paimon-table')
            AND entity_id IS NOT NULL
            AND entity_name IS NOT NULL
        )
    );

ALTER TABLE task_log
    ALTER COLUMN entity_type TYPE entity_type USING entity_type::text::entity_type,
    ADD CONSTRAINT task_log_warehouse_id_check CHECK (
        (entity_type = 'project' AND warehouse_id IS NULL)
        OR (
            entity_type IN ('warehouse', 'table', 'view', 'generic-table', 'paimon-table')
            AND warehouse_id IS NOT NULL
        )
    ),
    ADD CONSTRAINT task_log_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_id IS NULL AND entity_name IS NULL)
        OR (
            entity_type IN ('table', 'view', 'generic-table', 'paimon-table')
            AND entity_id IS NOT NULL
            AND entity_name IS NOT NULL
        )
    );

DROP TYPE tabular_type_old;
DROP TYPE entity_type_old;

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
    WHERE entity_type IN ('table', 'view', 'generic-table', 'paimon-table');
