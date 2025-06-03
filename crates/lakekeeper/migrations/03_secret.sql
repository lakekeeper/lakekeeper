create table "secret" (
    -- Table name should be part of PK for easier joins.
    secret_id uuid primary key default uuid_generate_v1mc(),
    "secret" bytea not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz
);
-- And applying our `updated_at` trigger is as easy as this.
SELECT trigger_updated_at('"secret"');