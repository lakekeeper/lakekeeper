create type deletion_kind as enum ('default', 'purge');

alter table tabular
    add column deletion_kind deletion_kind;

alter table tabular
    drop constraint unique_name_per_namespace_id;


alter table tabular
    add constraint unique_name_per_namespace_id unique NULLS not distinct (namespace_id, name, deleted_at);