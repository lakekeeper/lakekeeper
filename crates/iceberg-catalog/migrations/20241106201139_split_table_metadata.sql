create type table_format_version as enum ('1', '2');

alter table "table"
    add column table_format_version table_format_version,
    add column last_column_id       int,
    add column last_sequence_number bigint,
    add column last_updated_ms      bigint,
    add column last_partition_id    int,
    drop constraint "tabular_ident_fk",
    add constraint "tabular_ident_fk" foreign key (table_id) references tabular (tabular_id) on delete cascade,
    alter column metadata drop not null;

alter table tabular
    add column table_migrated boolean not null default false;

-- Create the function
CREATE OR REPLACE FUNCTION prohibit_updates_of_metadata_blob()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.metadata IS NOT NULL THEN
        RAISE EXCEPTION 'Insert or Update failed: metadata must be null';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER before_insert_check_metadata
    BEFORE INSERT OR UPDATE
    ON "table"
    FOR EACH ROW
EXECUTE FUNCTION prohibit_updates_of_metadata_blob();

alter table "view"
    drop constraint "tabular_ident_fk",
    add constraint "tabular_ident_fk" foreign key (view_id) references tabular (tabular_id) on delete cascade;

create table table_schema
(
    schema_id int   not null,
    table_id  uuid  not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    schema    jsonb not null,
    PRIMARY KEY (table_id, schema_id)
);

call add_time_columns('table_schema');
select trigger_updated_at('table_schema');

create table table_current_schema
(
    table_id  uuid primary key REFERENCES "table" (table_id) ON DELETE CASCADE,
    schema_id int not null,
    FOREIGN KEY (table_id, schema_id) REFERENCES table_schema (table_id, schema_id)
);

call add_time_columns('table_current_schema');
select trigger_updated_at('table_current_schema');

create table table_partition_spec
(
    partition_spec_id int   not null,
    table_id          uuid  not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    partition_spec    jsonb not null,
    PRIMARY KEY (table_id, partition_spec_id)
);

call add_time_columns('table_partition_spec');
select trigger_updated_at('table_partition_spec');

create table table_default_partition_spec
(
    table_id          uuid primary key REFERENCES "table" (table_id) ON DELETE CASCADE,
    partition_spec_id int not null,
    FOREIGN KEY (table_id, partition_spec_id) REFERENCES table_partition_spec (table_id, partition_spec_id)
);

call add_time_columns('table_default_partition_spec');
select trigger_updated_at('table_default_partition_spec');

create table table_properties
(
    table_id uuid not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    key      text not null,
    value    text not null,
    PRIMARY KEY (table_id, key)
);

call add_time_columns('table_properties');
select trigger_updated_at('table_properties');

create table table_snapshot
(
    snapshot_id        bigint not null,
    table_id           uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    parent_snapshot_id bigint,
    sequence_number    bigint not null,
    manifest_list      text   not null,
    summary            jsonb  not null,
    schema_id          int    not null,
    timestamp_ms       bigint not null,
    FOREIGN KEY (table_id, schema_id) REFERENCES table_schema (table_id, schema_id),
    -- FOREIGN KEY (table_id, parent_snapshot_id) REFERENCES table_snapshot (table_id, snapshot_id), ON DELETE SET NULL (parent_snapshot_id),
    PRIMARY KEY (table_id, snapshot_id)
);

call add_time_columns('table_snapshot');
select trigger_updated_at('table_snapshot');


create table table_snapshot_log
(
    sequence_number bigserial not null,
    table_id        uuid      not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    snapshot_id     bigint    not null,
    timestamp       bigint    not null,
    PRIMARY KEY (table_id, sequence_number)
);

call add_time_columns('table_snapshot_log');
select trigger_updated_at('table_snapshot_log');

create table table_metadata_log
(
    sequence_number bigserial not null,
    table_id        uuid      not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    timestamp       bigint    not null,
    metadata_file   text      not null,
    PRIMARY KEY (table_id, sequence_number)
);

call add_time_columns('table_metadata_log');
select trigger_updated_at('table_metadata_log');

create table table_sort_order
(
    sort_order_id bigint not null,
    table_id      uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    sort_order    jsonb  not null,
    PRIMARY KEY (table_id, sort_order_id)
);

call add_time_columns('table_sort_order');
select trigger_updated_at('table_sort_order');

create table table_default_sort_order
(
    table_id      uuid primary key REFERENCES "table" (table_id) ON DELETE CASCADE,
    sort_order_id bigint not null,
    FOREIGN KEY (table_id, sort_order_id) REFERENCES table_sort_order (table_id, sort_order_id)
);

call add_time_columns('table_default_sort_order');
select trigger_updated_at('table_default_sort_order');

create table table_refs
(
    table_id       uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    table_ref_name text   not null,
    snapshot_id    bigint not null,
    retention      jsonb  not null,
    PRIMARY KEY (table_id, table_ref_name),
    FOREIGN KEY (table_id, snapshot_id) REFERENCES table_snapshot (table_id, snapshot_id)
);

call add_time_columns('table_refs');
select trigger_updated_at('table_refs');
