-- Cascades table drops to all snapshots. Only in an empty catalog: the run keeps these locks until it commits, so migrate adds the key to any other catalog after the commit, deletes leftovers and validates.
DO $$
BEGIN
    IF EXISTS (SELECT FROM "table") THEN
        RETURN;
    END IF;
    LOCK TABLE table_snapshot, "table" IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    ALTER TABLE table_snapshot ADD CONSTRAINT table_snapshot_table_id_fkey
        FOREIGN KEY (warehouse_id, table_id) REFERENCES "table" (warehouse_id, table_id) ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN lock_not_available THEN
    RAISE NOTICE 'table_snapshot or "table" is busy: the foreign key between them is added after the migration commits';
END
$$;
