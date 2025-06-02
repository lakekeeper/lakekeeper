create type tabular_type as enum ('table', 'view');

create table tabular
(
    -- view or table id
    tabular_id        uuid primary key,
    namespace_id      uuid                            not null references namespace (namespace_id),
    name              text collate "case_insensitive" not null,
    typ               tabular_type                    not null,
    metadata_location text,
    location          text                            not null,
    CONSTRAINT "unique_name_per_namespace_id" UNIQUE (namespace_id, name),
    CHECK ((typ = 'view' AND metadata_location IS NOT NULL) OR typ = 'table')
);

create index tabular_namespace_id_name_idx on tabular (namespace_id, name);
create index tabular_typ_tabular_id_idx on tabular (typ, tabular_id);
create index tabular_namespace_id_idx on tabular (namespace_id);
-- TODO: this index helps with this lookup crates/iceberg-catalog/src/catalog/s3_signer.rs:70, if we get rid of it (or
--       understand it's not called frequently) we could get rid of this index
create index tabular_location_idx on tabular (location TEXT_PATTERN_OPS);

create index namespace_namespace_name_idx on namespace (namespace_name);

call add_time_columns('tabular');
select trigger_updated_at('tabular');

-- Insert all existing names from table into tabular
insert into tabular (tabular_id, namespace_id, name, typ, metadata_location, location)
select table_id, namespace_id, table_name, 'table', metadata_location, table_location
from "table";


-- ..and drop the colums + index from table
alter table "table"
    add constraint "tabular_ident_fk" foreign key (table_id) references tabular (tabular_id) on update cascade,
    drop column namespace_id,
    drop column table_name,
    drop column metadata_location,
    drop column table_location;

create view active_tabulars as
select tabular_id,
       t.namespace_id,
       name,
       typ,
       metadata_location,
       location,
       w.warehouse_id,
       n.namespace_name
from tabular t
         join namespace n on t.namespace_id = n.namespace_id
         join warehouse w on n.warehouse_id = w.warehouse_id
    and w.status = 'active';

create view active_tables as
select tabular_id as table_id, t.namespace_id, name, metadata_location, location
from active_tabulars t
where typ = 'table';

create view active_views as
select tabular_id as view_id, t.namespace_id, name, metadata_location, location
from active_tabulars t
where typ = 'view';
