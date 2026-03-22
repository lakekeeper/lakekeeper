CREATE TABLE generic_table (
    generic_table_id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    namespace_id     UUID NOT NULL REFERENCES namespace(namespace_id) ON DELETE CASCADE,
    warehouse_id     UUID NOT NULL REFERENCES warehouse(warehouse_id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    format           TEXT NOT NULL,
    base_location    TEXT NOT NULL,
    doc              TEXT,
    schema_info      JSONB,
    statistics       JSONB,
    properties       JSONB NOT NULL DEFAULT '{}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ,
    CONSTRAINT "unique_generic_table_name_per_namespace" UNIQUE (namespace_id, warehouse_id, name)
);
SELECT trigger_updated_at('generic_table');
CREATE INDEX "generic_table_namespace_id_idx" ON generic_table (warehouse_id, namespace_id);

ALTER TYPE api_endpoints ADD VALUE 'generic-table-v1-create-generic-table';
ALTER TYPE api_endpoints ADD VALUE 'generic-table-v1-list-generic-tables';
ALTER TYPE api_endpoints ADD VALUE 'generic-table-v1-load-generic-table';
ALTER TYPE api_endpoints ADD VALUE 'generic-table-v1-drop-generic-table';
