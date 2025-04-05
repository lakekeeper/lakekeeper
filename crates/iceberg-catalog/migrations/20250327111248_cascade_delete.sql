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

alter type api_endpoints add value 'management-post-warehouse-protection';
alter type api_endpoints add value 'management-post-warehouse-namespace-protection';
alter type api_endpoints add value 'management-post-warehouse-table-protection';
alter type api_endpoints add value 'management-post-warehouse-view-protection';