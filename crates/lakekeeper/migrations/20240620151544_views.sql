create type view_format_version as enum ('v1');

create table view
(
    view_id             uuid primary key default uuid_generate_v1mc(),
    CONSTRAINT "tabular_ident_fk" FOREIGN KEY (view_id) REFERENCES tabular (tabular_id),
    view_format_version view_format_version not null
);

call add_time_columns('"view"');
select trigger_updated_at('"view"');


create table view_properties
(
    view_id uuid not null REFERENCES view (view_id) ON DELETE CASCADE,
    key     text not null,
    value   text not null,
    PRIMARY KEY (view_id, key)
);


call add_time_columns('view_properties');
select trigger_updated_at('"view_properties"');

create table view_schema
(
    schema_id int   not null,
    view_id   uuid  not null REFERENCES view (view_id) ON DELETE CASCADE,
    schema    jsonb not null,
    CONSTRAINT "unique_schema_per_view" unique (view_id, schema_id),
    PRIMARY KEY (view_id, schema_id)
);
call add_time_columns('view_schema');
select trigger_updated_at('view_schema');

create table view_version
(
    view_id              uuid        not null REFERENCES view (view_id) ON DELETE CASCADE,
    version_id           bigint      not null,
    schema_id            int         not null,
    timestamp            timestamptz not null,
    default_namespace_id uuid REFERENCES namespace (namespace_id),
    default_catalog      text,
    summary              jsonb       not null,
    FOREIGN KEY (view_id, schema_id) REFERENCES view_schema (view_id, schema_id) ON DELETE CASCADE,
    constraint "unique_version_per_metadata" unique (view_id, version_id),
    PRIMARY KEY (view_id, version_id)
);

call add_time_columns('view_version');
select trigger_updated_at('view_version');

create table current_view_metadata_version
(
    view_id    uuid primary key REFERENCES view (view_id) ON DELETE CASCADE,
    version_id int8 not null,
    FOREIGN KEY (view_id, version_id) REFERENCES view_version (view_id, version_id)
);

call add_time_columns('current_view_metadata_version');
select trigger_updated_at('"current_view_metadata_version"');

create table view_version_log
(
    view_id    uuid        not null,
    version_id bigint      not null,
    timestamp  timestamptz not null default now(),
    FOREIGN KEY (view_id, version_id) REFERENCES view_version (view_id, version_id) ON DELETE CASCADE,
    PRIMARY KEY (view_id, version_id, timestamp)
);

create index view_version_log_view_id_version_id_idx on view_version_log (view_id, version_id);

call add_time_columns('view_version_log');


create type view_representation_type as enum ('sql');
create table view_representation
(
    view_representation_id uuid primary key default uuid_generate_v1mc(),
    view_version_id        bigint                   not null,
    view_id                uuid                     not null REFERENCES view (view_id) ON DELETE CASCADE,
    typ                    view_representation_type not null,
    sql                    text                     not null,
    dialect                text                     not null,
    FOREIGN KEY (view_id, view_version_id) REFERENCES view_version (view_id, version_id) ON DELETE CASCADE
);

create index view_representation_view_id_version_id_idx on view_representation (view_id, view_version_id);

call add_time_columns('view_representation');
select trigger_updated_at('"view_representation"');
