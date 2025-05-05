-- This repeats the function definition from the previous migration (20250328131139_fix_sequence_number.sql)
-- the other migration has been edited in-place which prevents new deployments in PG17 from failing during migrations.
-- This migration is here to move the index in old deployments to the new function too.
-- Table fmt version 1 has no sequence number, hence we only enforce the unique constraint via a partial unique index
DROP INDEX IF EXISTS unique_table_snapshot_sequence_number;

CREATE OR REPLACE FUNCTION is_not_table_format_v1(table_id_param uuid)
    RETURNS BOOLEAN
    SET search_path FROM CURRENT
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