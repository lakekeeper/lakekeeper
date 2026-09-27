-- Restores the index behind `list_tabulars` (added in 20260219081950).
--
-- `20260529000000_add_generic_table.sql` widened the "active tabular" predicate
-- of the list query to
--     deleted_at IS NULL AND (metadata_location IS NOT NULL OR typ = 'generic-table')
-- because generic tables are allowed to have a NULL `metadata_location`. The
-- index predicate was not widened with it, so it no longer covers every row the
-- query can return and Postgres stopped using it entirely -- listing a namespace
-- fell back to a sequential scan of `tabular`, the same plan as before
-- 20260219081950 was added.
--
-- Keep this predicate in sync with the `include_active` branch of `list_tabulars`
-- in src/tabular/mod.rs: if the query can return a row the index does not
-- contain, the planner silently stops using the index.
DROP INDEX IF EXISTS tabular_warehouse_namespace_created_at_idx;

CREATE INDEX tabular_warehouse_namespace_created_at_idx ON tabular (
    warehouse_id,
    namespace_id,
    created_at,
    tabular_id
)
WHERE
    deleted_at IS NULL
    AND (
        metadata_location IS NOT NULL
        OR typ = 'generic-table'
    );
