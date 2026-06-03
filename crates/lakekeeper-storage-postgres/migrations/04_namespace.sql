create table "namespace" (
    -- Table name should be part of PK for easier joins.
    namespace_id uuid primary key default uuid_generate_v1mc(),
    warehouse_id uuid not null REFERENCES "warehouse"(warehouse_id),
    namespace_name Text [] collate "case_insensitive" not null,
    namespace_properties jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz,
    CONSTRAINT "unique_namespace_per_warehouse" UNIQUE (warehouse_id, namespace_name)
);
-- And applying our `updated_at` trigger is as easy as this.
SELECT trigger_updated_at('"namespace"');
CREATE INDEX "namespace_warehouse_id_idx" ON "namespace" (warehouse_id);