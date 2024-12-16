create table partition_statistics
(
    snapshot_id        bigint not null,
    table_id           uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    statistics_path    text   not null,
    file_size_in_bytes bigint not null,
    PRIMARY KEY (table_id, snapshot_id)
);

call add_time_columns('partition_statistics');
select trigger_updated_at('partition_statistics');

create table table_statistics
(
    snapshot_id                 bigint not null,
    table_id                    uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    statistics_path             text   not null,
    file_size_in_bytes          bigint not null,
    file_footer_size_in_bytes   bigint not null,
    key_metadata                text,
    blob_metadata               jsonb not null,
    PRIMARY KEY (table_id, snapshot_id)
);

call add_time_columns('table_statistics');
select trigger_updated_at('table_statistics');
