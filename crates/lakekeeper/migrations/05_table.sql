create table "table" (
    -- Table name should be part of PK for easier joins.
    table_id uuid primary key default uuid_generate_v1mc(),
    namespace_id uuid not null REFERENCES "namespace"(namespace_id),
    table_name Text collate "case_insensitive" not null,
    "metadata" jsonb not null,
    -- May be null if table is staged as part of a transaction.
    "metadata_location" text,
    -- Speed up S3 Signing requests. Otherwise not needed
    -- as the location is stored in the metadata.
    "table_location" text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz,
    CONSTRAINT "unique_table_name_per_namespace" UNIQUE (namespace_id, table_name)
);
-- And applying our `updated_at` trigger is as easy as this.
SELECT trigger_updated_at('"table"');
CREATE INDEX "table_namespace_id_idx" ON "table" (namespace_id);