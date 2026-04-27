ALTER TYPE tabular_type ADD VALUE IF NOT EXISTS 'generic-table';

CREATE TABLE generic_table (
    warehouse_id     UUID NOT NULL,
    generic_table_id UUID NOT NULL,
    format           TEXT NOT NULL,
    doc              TEXT,
    schema_info      JSONB,
    statistics       JSONB,
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
    PRIMARY KEY (warehouse_id, generic_table_id, key),
    FOREIGN KEY (warehouse_id, generic_table_id)
        REFERENCES generic_table(warehouse_id, generic_table_id) ON DELETE CASCADE
);

ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-create-generic-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-list-generic-tables';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-load-generic-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'generic-table-v1-drop-generic-table';
