-- Example extension migration used by integration tests.
-- Demonstrates the ext_<feature>_* table prefix and the required CASCADE FK
-- from an extension table into an upstream-owned table.
CREATE TABLE ext_demo_state (
    id           UUID PRIMARY KEY,
    warehouse_id UUID NOT NULL REFERENCES warehouse(warehouse_id) ON DELETE CASCADE,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
