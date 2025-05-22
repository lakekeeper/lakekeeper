ALTER TABLE table_snapshot
ADD CONSTRAINT unique_sequence_number_per_table_snapshot UNIQUE (table_id, sequence_number);

CREATE TABLE "table_metrics_scan_report" (
    metric_id uuid NOT NULL PRIMARY KEY default uuid_generate_v1mc(),
    table_id uuid NOT NULL,
    snapshot_id BIGINT NOT NULL,
    filter JSONB NOT NULL,
    schema_id INT NOT NULL,
    projected_field_ids INTEGER[] NOT NULL,
    projected_field_names TEXT[] NOT NULL,
    metrics JSONB NOT NULL,
    metadata JSONB,
    FOREIGN KEY (table_id, snapshot_id) REFERENCES "table_snapshot" (table_id, snapshot_id) ON DELETE CASCADE,
    FOREIGN KEY (table_id, schema_id) REFERENCES "table_schema" (table_id, schema_id) ON DELETE CASCADE
);

CREATE TABLE "table_metrics_commit_report" (
    metric_id uuid NOT NULL PRIMARY KEY default uuid_generate_v1mc(),
    table_id uuid NOT NULL,
    snapshot_id BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    operation VARCHAR(255) NOT NULL,
    metrics JSONB NOT NULL,
    metadata JSONB,
    FOREIGN KEY (table_id, snapshot_id) REFERENCES "table_snapshot" (table_id, snapshot_id) ON DELETE CASCADE,
    FOREIGN KEY (table_id, sequence_number) REFERENCES "table_snapshot" (table_id, sequence_number) ON DELETE CASCADE
);
