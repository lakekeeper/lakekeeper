create type warehouse_status as enum ('active', 'inactive');
create table "warehouse" (
    -- Table name should be part of PK for easier joins.
    warehouse_id uuid primary key default uuid_generate_v1mc(),
    project_id uuid not null,
    -- By applying our custom collation we can simply mark this column as `unique` and Postgres will enforce
    -- case-insensitive uniqueness for us, and lookups over `warehouse` will be case-insensitive by default.
    --
    -- Note that this collation doesn't support the `LIKE`/`ILIKE` operators so if you want to do searches
    -- over `warehouse` you will want a separate index with the default collation:
    --
    -- create index on "warehouse" (warehouse_name collate "ucs_basic");
    warehouse_name text collate "case_insensitive" not null,
    storage_profile jsonb not null default '{}'::jsonb,
    storage_secret_id uuid,
    created_at timestamptz not null default now(),
    updated_at timestamptz,
    "status" warehouse_status not null,
    CONSTRAINT unique_warehouse_name_in_project UNIQUE (project_id, warehouse_name)
);
-- And applying our `updated_at` trigger is as easy as this.
SELECT trigger_updated_at('"warehouse"');
-- Index for primary lookup based on project_id and warehouse_name.
create index "warehouse_project_id_name" on warehouse (
    project_id,
    warehouse_name collate "case_insensitive"
);
create index "warehouse_status_idx" on "warehouse" ("status");