-- This file has been edited while attempting a fix for #997, the original content of this file contained a bug which
-- could case migrations to fail. We removed the offending lines.
DELETE
FROM table_snapshot t1
    USING table_snapshot t2
        JOIN "table" ON t2.table_id = "table".table_id
WHERE t1.created_at < t2.created_at
  AND t1.table_id = t2.table_id
  AND t1.sequence_number = t2.sequence_number
  AND "table".table_format_version != '1';
