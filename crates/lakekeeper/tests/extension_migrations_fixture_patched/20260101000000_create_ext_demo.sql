-- Same DDL as `extension_migrations_fixture/20260101000000_create_ext_demo.sql`,
-- but with this extra comment so the file's SHA differs. Used by the
-- `test_extension_sha_patch_rewrites_checksum` test: an extension simulates
-- an in-place edit of a previously-shipped migration without bumping the
-- version, then verifies the sha_patches mechanism rewrites the tracker row.
CREATE TABLE ext_demo_state (
    id           UUID PRIMARY KEY,
    warehouse_id UUID NOT NULL REFERENCES warehouse(warehouse_id) ON DELETE CASCADE,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
