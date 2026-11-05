-- Drops cascade to every snapshot; those without a schema id used to outlive their table.
-- NOWAIT: writers may hold these tables while they wait for a lock this run holds. If busy, the migrator adds the key after the run commits.
-- After the run commits, the migrator also deletes the snapshots earlier drops left behind and validates the key.
DO $$
BEGIN
    LOCK TABLE table_snapshot, "table" IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    ALTER TABLE table_snapshot ADD CONSTRAINT table_snapshot_table_id_fkey
        FOREIGN KEY (warehouse_id, table_id) REFERENCES "table" (warehouse_id, table_id) ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN lock_not_available THEN
    RAISE NOTICE 'table_snapshot or "table" is busy: the foreign key between them is added after the migration commits';
END
$$;
