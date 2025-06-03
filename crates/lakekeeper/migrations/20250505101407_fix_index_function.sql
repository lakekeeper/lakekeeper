-- This file has been edited while attempting a fix for #997, the original content of this file contained a bug which
-- could case migrations to fail. We removed the offending lines.
DROP INDEX IF EXISTS unique_table_snapshot_sequence_number;
DROP FUNCTION IF EXISTS is_not_table_format_v1;
