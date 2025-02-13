create table endpoint_stats
(
    warehouse_id uuid references warehouse (warehouse_id) on delete cascade,
    project_id   uuid references project (project_id) on delete cascade,
    matched_path text        not null,
    method       text        not null,
    status_code  int         not null,
    count        bigint      not null default 0,
    -- we keep stats in hourly intervals, every hour we create a new row,
    valid_until  timestamptz not null default get_stats_date_default(),
    primary key (project_id, warehouse_id, matched_path, method, status_code, valid_until)
);



select trigger_updated_at('endpoint_stats');
call add_time_columns('endpoint_stats');