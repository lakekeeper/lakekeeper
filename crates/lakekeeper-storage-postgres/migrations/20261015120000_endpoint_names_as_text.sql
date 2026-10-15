-- Endpoint names are stored as text, so a new endpoint needs no migration.
--
-- endpoint_statistics is replaced by a new table: rewriting it in place would hold
-- its lock for the whole migration batch while servers of the previous release
-- block on their inserts. The previous table is kept read-only as
-- endpoint_statistics_legacy; it still uses the api_endpoints type, which those
-- servers also cast their inserts to.
ALTER TABLE endpoint_statistics RENAME TO endpoint_statistics_legacy;
ALTER TABLE endpoint_statistics_legacy
    RENAME CONSTRAINT endpoint_statistics_pkey TO endpoint_statistics_legacy_pkey;
ALTER TABLE endpoint_statistics_legacy
    RENAME CONSTRAINT endpoint_statistics_project_id_warehouse_id_matched_path_st_key TO endpoint_statistics_legacy_key;
ALTER TABLE endpoint_statistics_legacy
    RENAME CONSTRAINT endpoint_statistics_project_id_fkey TO endpoint_statistics_legacy_project_id_fkey;
ALTER INDEX idx_endpoint_stats_project_timestamp RENAME TO idx_endpoint_stats_legacy_project_timestamp;
ALTER INDEX idx_endpoint_stats_project_warehouse_time RENAME TO idx_endpoint_stats_legacy_project_warehouse_time;
ALTER INDEX idx_endpoint_stats_project_status_time RENAME TO idx_endpoint_stats_legacy_project_status_time;

CREATE TABLE endpoint_statistics
(
    endpoint_statistics_id uuid primary key default uuid_generate_v1mc(),
    warehouse_id           uuid,
    project_id             text        not null references project (project_id) on delete cascade,
    matched_path           text COLLATE "C" not null,
    status_code            int         not null,
    count                  bigint      not null default 0,
    timestamp              timestamptz not null default get_stats_date_default(),
    unique nulls not distinct (project_id, warehouse_id, matched_path, status_code, timestamp)
);

CREATE INDEX idx_endpoint_stats_project_timestamp ON endpoint_statistics (project_id, timestamp DESC);
CREATE INDEX idx_endpoint_stats_project_warehouse_time ON endpoint_statistics (project_id, warehouse_id, timestamp DESC);
CREATE INDEX idx_endpoint_stats_project_status_time ON endpoint_statistics (project_id, status_code, timestamp DESC);

select trigger_updated_at('endpoint_statistics');
call add_time_columns('endpoint_statistics');

-- idempotency_record is small (its rows expire) and is changed in place. The server
-- writes operation from a closed set, so the list of allowed names is not repeated here.
ALTER TABLE idempotency_record DROP CONSTRAINT idempotency_operation_check;
ALTER TABLE idempotency_record ALTER COLUMN operation TYPE text COLLATE "C";
