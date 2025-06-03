DELETE FROM partition_statistics ps
WHERE (ps.table_id, ps.snapshot_id) NOT IN (
    SELECT table_id, snapshot_id FROM table_snapshot
);
alter table partition_statistics
    add constraint partition_statistics_table_id_snapshot_id_fkey foreign key (table_id, snapshot_id)
        references table_snapshot (table_id, snapshot_id) on delete cascade;

DELETE FROM table_statistics ts
WHERE (ts.table_id, ts.snapshot_id) NOT IN (
    SELECT table_id, snapshot_id FROM table_snapshot
);
alter table table_statistics
    add constraint table_statistics_table_id_snapshot_id_fkey foreign key (table_id, snapshot_id)
        references table_snapshot (table_id, snapshot_id) on delete cascade;
