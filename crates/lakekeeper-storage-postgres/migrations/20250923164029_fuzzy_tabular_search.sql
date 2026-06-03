CREATE EXTENSION IF NOT EXISTS btree_gist;

-- The purpose is enabling fuzzy search on `<namespace_name>.<tabular_name>`.
--
-- # Example use case
--
-- * User searches for `finance region 42`
-- * Fuzzy search returns matches like
--   * `departments.finance.region_42`
--
-- Column `namespace.namespace_name` is duplicated in tabular to enable a trigram index on
-- `<namespace_name>.<tabular_name>`

-- Add the new column (initially nullable)
ALTER TABLE tabular
ADD COLUMN tabular_namespace_name text[] COLLATE case_insensitive;

-- Populate the new column for existing rows
UPDATE tabular t
SET tabular_namespace_name = n.namespace_name
FROM namespace n
WHERE t.warehouse_id = n.warehouse_id AND t.namespace_id = n.namespace_id;

-- Make the column non-nullable
ALTER TABLE tabular
ALTER COLUMN tabular_namespace_name SET NOT NULL;

ALTER TABLE namespace
ADD CONSTRAINT unique_warehouse_id_namespace_id_namespace_name UNIQUE (warehouse_id, namespace_id, namespace_name);

ALTER TABLE tabular
ADD CONSTRAINT tabular_warehouse_id_namespace_id_namespace_name_fkey
FOREIGN KEY (warehouse_id, namespace_id, tabular_namespace_name)
REFERENCES namespace(warehouse_id, namespace_id, namespace_name)
ON UPDATE CASCADE;

-- `array_to_string` is not `immutable` so it can't be used directly for the index.
-- Moreover, having the concat logic in a function allows re-using it queries constructed in Rust.
CREATE OR REPLACE FUNCTION concat_namespace_name_tabular_name(nsn text[], tn text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN nsn IS NULL OR cardinality(nsn) = 0 THEN tn
        ELSE array_to_string(nsn, '.') || '.' || tn
    END;
$$;

-- The trigram index bypasses collation. If case (in)sensitivity turns out to be a problem,
-- try to `LOWER` in `concat_namespace_name_tabular_name`.
DROP INDEX IF EXISTS tabular_name_namespace_name_gist_index;
CREATE INDEX tabular_name_namespace_name_gist_index
ON tabular
USING GIST (
    warehouse_id,
    concat_namespace_name_tabular_name(tabular_namespace_name, name)
    gist_trgm_ops(siglen=256)
)
WHERE deleted_at IS NULL AND metadata_location IS NOT NULL;

ALTER TYPE api_endpoints ADD VALUE 'management-v1-search-tabular';
