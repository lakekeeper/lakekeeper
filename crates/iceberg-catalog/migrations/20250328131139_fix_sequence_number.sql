DELETE
FROM table_snapshot t1
    USING table_snapshot t2
WHERE t1.created_at > t2.created_at
  AND t1.table_id = t2.table_id
  AND t1.sequence_number = t2.sequence_number;

alter table table_snapshot
    add constraint unique_table_snapshot_sequence_number unique (table_id, sequence_number);