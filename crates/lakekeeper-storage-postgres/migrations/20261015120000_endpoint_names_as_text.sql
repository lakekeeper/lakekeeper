-- Endpoint names are stored as text.

-- Project deletes wait until the copy has its foreign key.
LOCK TABLE project IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE endpoint_statistics_text
(
    endpoint_statistics_id uuid        not null default uuid_generate_v1mc(),
    warehouse_id           uuid,
    project_id             text        not null,
    matched_path           text COLLATE "C" not null,
    status_code            int         not null,
    count                  bigint      not null default 0,
    timestamp              timestamptz not null default get_stats_date_default()
);
call add_time_columns('endpoint_statistics_text');

-- Copied without blocking writes of older servers; what they write after the copy is lost.
INSERT INTO endpoint_statistics_text
    (endpoint_statistics_id, warehouse_id, project_id, matched_path, status_code, count, timestamp, created_at, updated_at)
SELECT endpoint_statistics_id, warehouse_id, project_id, matched_path::text, status_code, count, timestamp, created_at, updated_at
FROM endpoint_statistics;

-- Built after the copy, which is faster.
ALTER TABLE endpoint_statistics_text
    ADD CONSTRAINT endpoint_statistics_text_pkey PRIMARY KEY (endpoint_statistics_id),
    ADD CONSTRAINT endpoint_statistics_text_key
        UNIQUE NULLS NOT DISTINCT (project_id, warehouse_id, matched_path, status_code, timestamp),
    ADD CONSTRAINT endpoint_statistics_project_id_fkey
        FOREIGN KEY (project_id) REFERENCES project (project_id) ON DELETE CASCADE;
CREATE INDEX idx_endpoint_stats_text_project_timestamp ON endpoint_statistics_text (project_id, timestamp DESC);
CREATE INDEX idx_endpoint_stats_text_project_warehouse_time ON endpoint_statistics_text (project_id, warehouse_id, timestamp DESC);
CREATE INDEX idx_endpoint_stats_text_project_status_time ON endpoint_statistics_text (project_id, status_code, timestamp DESC);

-- Locks project exclusively. Fails after 5 s instead of stalling older servers; the next start retries.
SET LOCAL lock_timeout = '5s';
DROP TABLE endpoint_statistics;
ALTER TABLE endpoint_statistics_text RENAME TO endpoint_statistics;
ALTER TABLE endpoint_statistics RENAME CONSTRAINT endpoint_statistics_text_pkey TO endpoint_statistics_pkey;
ALTER TABLE endpoint_statistics
    RENAME CONSTRAINT endpoint_statistics_text_key TO endpoint_statistics_project_id_warehouse_id_matched_path_st_key;
ALTER INDEX idx_endpoint_stats_text_project_timestamp RENAME TO idx_endpoint_stats_project_timestamp;
ALTER INDEX idx_endpoint_stats_text_project_warehouse_time RENAME TO idx_endpoint_stats_project_warehouse_time;
ALTER INDEX idx_endpoint_stats_text_project_status_time RENAME TO idx_endpoint_stats_project_status_time;

select trigger_updated_at('endpoint_statistics');

-- After the drop, in older servers' lock order; default collation for their cached plans.
ALTER TABLE idempotency_record
    DROP CONSTRAINT idempotency_operation_check,
    ALTER COLUMN operation TYPE text;
SET LOCAL lock_timeout TO DEFAULT;
