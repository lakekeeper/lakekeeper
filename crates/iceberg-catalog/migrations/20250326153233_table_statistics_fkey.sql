delete
from partition_statistics
where (snapshot_id, table_id) not in
      (select snapshot_id, table_id from table_snapshot where table_id = partition_statistics.table_id);

alter table partition_statistics
    add constraint partition_statistics_table_id_snapshot_id_fkey foreign key (table_id, snapshot_id)
        references table_snapshot (table_id, snapshot_id) on delete cascade;

delete
from table_statistics
where (snapshot_id, table_id) not in
      (select snapshot_id, table_id from table_snapshot where table_id = table_statistics.table_id);

alter table table_statistics
    add constraint table_statistics_table_id_snapshot_id_fkey foreign key (table_id, snapshot_id)
        references table_snapshot (table_id, snapshot_id) on delete cascade;