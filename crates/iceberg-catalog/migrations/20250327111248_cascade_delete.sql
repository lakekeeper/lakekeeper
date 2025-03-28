alter table tabular
    add column protected bool not null default false;

alter table tabular
    drop constraint tabular_namespace_id_fkey,
    add constraint tabular_namespace_id_fkey
        foreign key (namespace_id)
            references namespace (namespace_id)
            on delete cascade;

alter table namespace
    add column protected bool not null default false;

alter table warehouse
    add column protected bool not null default false;