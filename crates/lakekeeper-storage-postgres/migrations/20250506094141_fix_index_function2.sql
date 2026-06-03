-- This migration aligns existing deployments to the changes applied to `fix_index_function.sql` and
-- `fix-sequence-number.sql` while fixing #997
DROP INDEX IF EXISTS unique_table_snapshot_sequence_number;
DROP FUNCTION IF EXISTS is_not_table_format_v1;
