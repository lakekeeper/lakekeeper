DELETE
FROM table_snapshot t1
    USING table_snapshot t2
        JOIN "table" ON t2.table_id = "table".table_id
WHERE t1.created_at < t2.created_at
  AND t1.table_id = t2.table_id
  AND t1.sequence_number = t2.sequence_number
  AND "table".table_format_version != '1';

-- table fmt version 1 has no sequence number, hence we only enforce the unique constraint via a partial unique index
CREATE OR REPLACE FUNCTION is_not_table_format_v1(table_id_param uuid)
    RETURNS BOOLEAN
    IMMUTABLE AS
$$
BEGIN
    RETURN EXISTS (SELECT 1
                   FROM "table" t
                   WHERE t.table_id = table_id_param
                     AND t.table_format_version != '1');
END;
$$ LANGUAGE plpgsql;

CREATE UNIQUE INDEX unique_table_snapshot_sequence_number
    ON table_snapshot (table_id, sequence_number)
    WHERE is_not_table_format_v1(table_id);