alter table endpoint_statistics
    drop constraint endpoint_statistics_warehouse_id_fkey;
alter table endpoint_statistics
    add constraint endpoint_statistics_warehouse_id_fkey foreign key (warehouse_id)
        references warehouse (warehouse_id) on delete cascade;