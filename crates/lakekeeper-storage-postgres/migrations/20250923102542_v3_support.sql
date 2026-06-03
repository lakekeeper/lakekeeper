ALTER TYPE table_format_version ADD VALUE IF NOT EXISTS '3';

-- Add table_encryption_keys table to support V3 tables
CREATE TABLE
    table_encryption_keys (
        warehouse_id uuid NOT NULL,
        table_id uuid NOT NULL,
        key_id TEXT NOT NULL,
        encrypted_key_metadata BYTEA NOT NULL,
        encrypted_by_id TEXT,
        properties JSONB,
        CONSTRAINT table_encryption_keys_pkey PRIMARY KEY (warehouse_id, table_id, key_id)
    );

CALL add_time_columns ('table_encryption_keys');

SELECT
    trigger_updated_at ('table_encryption_keys');

ALTER TABLE table_encryption_keys ADD CONSTRAINT table_encryption_keys_table_id_fkey FOREIGN KEY (warehouse_id, table_id) REFERENCES "table" (warehouse_id, table_id) ON DELETE CASCADE;

-- Add next_row_id column to table to support V3 tables.
-- Add not-null constraints for existing columns that should never be null after
-- LK Version 0.5.
ALTER TABLE "table"
ADD COLUMN IF NOT EXISTS next_row_id BIGINT CHECK (next_row_id >= 0),
ALTER COLUMN table_format_version
SET
    NOT NULL,
ALTER COLUMN last_column_id
SET
    NOT NULL,
ALTER COLUMN last_sequence_number
SET
    NOT NULL,
ALTER COLUMN last_updated_ms
SET
    NOT NULL,
ALTER COLUMN last_partition_id
SET
    NOT NULL;

UPDATE "table"
SET
    next_row_id = 0
WHERE
    next_row_id IS NULL;

ALTER TABLE "table"
ALTER COLUMN next_row_id
SET
    NOT NULL;

-- Add new Snapshot fields
ALTER TABLE table_snapshot
ADD COLUMN IF NOT EXISTS first_row_id BIGINT,
ADD COLUMN IF NOT EXISTS assigned_rows BIGINT,
ADD COLUMN IF NOT EXISTS key_id TEXT;