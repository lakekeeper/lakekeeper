-- Generic tables: catalog support for non-Iceberg formats (Lance, Delta, ...).
-- Recreates each affected enum since Lakekeeper runs all migrations in one tx
-- where ALTER TYPE ADD VALUE literals are unusable; see prior art in
-- 20251228101923_enable_tasks_on_project_level.sql.

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
ALTER TABLE tabular
    ALTER COLUMN typ TYPE tabular_type USING typ::text::tabular_type;
DROP TYPE tabular_type_old;

ALTER TYPE entity_type RENAME TO entity_type_old;
CREATE TYPE entity_type AS ENUM (
    'table', 'view', 'project', 'warehouse',
    'namespace', 'role', 'user', 'server', 'generic-table'
);
ALTER TABLE task
    ALTER COLUMN entity_type TYPE entity_type USING entity_type::text::entity_type;
ALTER TABLE task_log
    ALTER COLUMN entity_type TYPE entity_type USING entity_type::text::entity_type;
DROP TYPE entity_type_old;

ALTER TYPE api_endpoints RENAME TO api_endpoints_old;
CREATE TYPE api_endpoints AS ENUM (
    'sign-s3-request-global',
    'sign-s3-request-prefix',
    'catalog-v1-get-config',
    'catalog-v1-list-namespaces',
    'catalog-v1-create-namespace',
    'catalog-v1-load-namespace-metadata',
    'catalog-v1-namespace-exists',
    'catalog-v1-fetch-scan-tasks',
    'catalog-v1-drop-namespace',
    'catalog-v1-update-namespace-properties',
    'catalog-v1-list-tables',
    'catalog-v1-create-table',
    'catalog-v1-load-table',
    'catalog-v1-update-table',
    'catalog-v1-drop-table',
    'catalog-v1-table-exists',
    'catalog-v1-load-credentials',
    'catalog-v1-rename-table',
    'catalog-v1-register-table',
    'catalog-v1-report-metrics',
    'catalog-v1-commit-transaction',
    'catalog-v1-create-view',
    'catalog-v1-list-views',
    'catalog-v1-load-view',
    'catalog-v1-replace-view',
    'catalog-v1-drop-view',
    'catalog-v1-view-exists',
    'catalog-v1-rename-view',
    'management-v1-server-info',
    'management-v1-bootstrap',
    'management-v1-create-role',
    'management-v1-list-role',
    'management-v1-update-role',
    'management-v1-get-role',
    'management-v1-delete-role',
    'management-v1-search-role',
    'management-v1-whoami',
    'management-v1-search-user',
    'management-v1-update-user',
    'management-v1-get-user',
    'management-v1-delete-user',
    'management-v1-create-user',
    'management-v1-list-user',
    'management-v1-create-project',
    'management-v1-get-project',
    'management-v1-delete-project',
    'management-v1-rename-project',
    'management-v1-get-project-by-id-deprecated',
    'management-v1-load-endpoint-statistics',
    'management-v1-delete-project-by-id-deprecated',
    'management-v1-create-warehouse',
    'management-v1-list-warehouses',
    'management-v1-list-projects',
    'management-v1-get-warehouse',
    'management-v1-delete-warehouse',
    'management-v1-rename-warehouse',
    'management-v1-deactivate-warehouse',
    'management-v1-activate-warehouse',
    'management-v1-update-storage-profile',
    'management-v1-update-storage-credential',
    'management-v1-get-warehouse-statistics',
    'management-v1-list-deleted-tabulars',
    'management-v1-undrop-tabulars-deprecated',
    'management-v1-undrop-tabulars',
    'management-v1-update-warehouse-delete-profile',
    'permission-v1-get',
    'permission-v1-post',
    'permission-v1-head',
    'permission-v1-delete',
    'management-v1-set-warehouse-protection',
    'management-v1-set-namespace-protection',
    'management-v1-set-table-protection',
    'management-v1-set-view-protection',
    'catalog-v1-cancel-planning',
    'catalog-v1-fetch-planning-result',
    'catalog-v1-plan-table-scan',
    'management-v1-get-view-protection',
    'management-v1-get-table-protection',
    'management-v1-get-namespace-protection',
    'management-v1-rename-default-project-deprecated',
    'management-v1-get-default-project-deprecated',
    'management-v1-delete-default-project-deprecated',
    'permission-v1-put',
    'management-v1-rename-project-by-id-deprecated',
    'sign-s3-request-tabular',
    'management-v1-set-task-queue-config',
    'management-v1-get-task-queue-config',
    'management-v1-control-tasks',
    'management-v1-get-task-details',
    'management-v1-list-tasks',
    'management-v1-search-tabular',
    'management-v1-get-server-actions',
    'management-v1-get-user-actions',
    'management-v1-get-role-actions',
    'management-v1-get-warehouse-actions',
    'management-v1-get-project-actions',
    'management-v1-get-namespace-actions',
    'management-v1-get-table-actions',
    'management-v1-get-view-actions',
    'management-v1-batch-check-actions',
    'management-v1-get-role-metadata',
    'management-v1-update-role-source-system',
    'management-v1-set-project-task-queue-config',
    'management-v1-get-project-task-queue-config',
    'management-v1-control-project-tasks',
    'management-v1-get-project-task-details',
    'management-v1-list-project-tasks',
    'generic-table-v1-create-generic-table',
    'generic-table-v1-list-generic-tables',
    'generic-table-v1-load-generic-table',
    'generic-table-v1-drop-generic-table',
    'generic-table-v1-rename-generic-table',
    'generic-table-v1-load-generic-table-credentials',
    'management-v1-get-generic-table-actions'
);
ALTER TABLE endpoint_statistics
    ALTER COLUMN matched_path TYPE api_endpoints USING matched_path::text::api_endpoints;
ALTER TABLE idempotency_record
    ALTER COLUMN operation TYPE api_endpoints USING operation::text::api_endpoints;
DROP TYPE api_endpoints_old;

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

-- Views require metadata_location; tables and generic tables don't.
ALTER TABLE tabular ADD CONSTRAINT tabular_metadata_location_check
    CHECK (
        (typ = 'view' AND metadata_location IS NOT NULL)
        OR typ IN ('table', 'generic-table')
    );

ALTER TABLE task
    ADD CONSTRAINT task_warehouse_id_check CHECK (
        (entity_type = 'project' AND warehouse_id IS NULL)
        OR (entity_type IN ('warehouse', 'table', 'view', 'generic-table') AND warehouse_id IS NOT NULL)
    ),
    ADD CONSTRAINT task_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_id IS NULL AND entity_name IS NULL)
        OR (entity_type IN ('table', 'view', 'generic-table') AND entity_id IS NOT NULL AND entity_name IS NOT NULL)
    );

ALTER TABLE task_log
    ADD CONSTRAINT task_log_warehouse_id_check CHECK (
        (entity_type = 'project' AND warehouse_id IS NULL)
        OR (entity_type IN ('warehouse', 'table', 'view', 'generic-table') AND warehouse_id IS NOT NULL)
    ),
    ADD CONSTRAINT task_log_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_id IS NULL AND entity_name IS NULL)
        OR (entity_type IN ('table', 'view', 'generic-table') AND entity_id IS NOT NULL AND entity_name IS NOT NULL)
    );

ALTER TABLE idempotency_record ADD CONSTRAINT idempotency_operation_check CHECK (
    operation IN (
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
