-- UP script to create the table_metrics table

CREATE TABLE table_metrics (
    table_id UUID NOT NULL REFERENCES "table"(table_id) ON DELETE CASCADE,
    total_records BIGINT NOT NULL,
    total_files_size_bytes BIGINT NOT NULL,
    total_data_files BIGINT NOT NULL,
    total_delete_files BIGINT NOT NULL,
    total_position_deletes BIGINT NOT NULL,
    total_equality_deletes BIGINT NOT NULL,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_id, reported_at) -- Composite key to store metrics over time
);

-- Index for efficient querying by table_id
CREATE INDEX idx_table_metrics_table_id ON table_metrics(table_id);

-- Apply the updated_at trigger (if it exists and is conventional)
-- SELECT trigger_updated_at('table_metrics');

-- Add time columns (if a helper function like add_time_columns exists and is conventional)
-- CALL add_time_columns('table_metrics');

-- DOWN script (for manual rollback if needed)
-- DROP TABLE table_metrics;
