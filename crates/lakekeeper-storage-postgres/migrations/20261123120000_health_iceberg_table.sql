create type health_status as enum ('measured', 'not-measurable');

-- The latest health measurement of an Iceberg table: one row per table, written at the end of
-- each run. Soft-deleted tables keep their rows; dropping a table removes its row.
create table health_iceberg_table (
    warehouse_id        uuid not null,
    table_id            uuid not null,
    -- Version of the measurement code that computed the row. A row is only overwritten by the
    -- same or a newer version.
    measurement_version int not null,
    status              health_status not null,
    -- End of the run.
    measured_at         timestamptz not null,
    -- Earliest start of the next run.
    next_not_before     timestamptz not null,
    -- Highest severity of the findings: 0 none, 1 info, 2 warning, 3 critical.
    max_severity        smallint not null default 0 check (max_severity between 0 and 3),
    finding_count       int not null default 0 check (finding_count >= 0),
    -- Findings, the reason a table could not be measured, and scan details.
    result              jsonb not null check (octet_length(result::text) <= 1048576),
    -- Sizes in bytes. Null if the table could not be measured.
    live_data_bytes     bigint,
    live_delete_bytes   bigint,
    live_metadata_bytes bigint,
    primary key (warehouse_id, table_id),
    -- Locks "table" until the migration run commits, so table commits wait for the rest of the
    -- run. The table is new and empty: nothing to clean up or validate.
    constraint health_iceberg_table_table_id_fkey foreign key (warehouse_id, table_id)
        references "table" (warehouse_id, table_id) on delete cascade,
    -- A comparison with null gives null, which a check accepts, so null counts as false.
    constraint health_iceberg_table_sizes_check check (
        (status = 'measured'
            and coalesce(live_data_bytes >= 0 and live_delete_bytes >= 0
                         and live_metadata_bytes >= 0, false))
        or (status = 'not-measurable'
            and live_data_bytes is null and live_delete_bytes is null and live_metadata_bytes is null))
);

-- Ranking of a warehouse's tables. Queries use the index only if they spell the expression on
-- `live_data_bytes` exactly as here.
create index health_iceberg_table_rank_severity on health_iceberg_table (
    warehouse_id,
    max_severity desc,
    (coalesce(live_data_bytes, -1)) desc,
    table_id desc
);
