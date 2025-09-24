ALTER TYPE table_format_version ADD VALUE IF NOT EXISTS '3';

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
    NOT NULL,
UPDATE "table"
SET
    next_row_id = 0
WHERE
    next_row_id IS NULL;

ALTER TABLE "table"
ALTER COLUMN next_row_id
SET
    NOT NULL;