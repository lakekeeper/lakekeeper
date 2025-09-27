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
ADD COLUMN namespace_name text[] COLLATE public.case_insensitive;

-- Populate the new column for existing rows
UPDATE tabular t
SET namespace_name = n.namespace_name
FROM namespace n
WHERE t.warehouse_id = n.warehouse_id AND t.namespace_id = n.namespace_id;

-- Make the column non-nullable
ALTER TABLE public.tabular
ALTER COLUMN namespace_name SET NOT NULL;

-- Namespace id is unique, so this is already implied.
-- Needs to made explicit to enable foreign key and cascade below.
-- ALTER TABLE namespace
-- ADD CONSTRAINT unique_namespace_name_per_namespace_id UNIQUE NULLS NOT DISTINCT
--     (namespace_id, namespace_name);

-- There is a constraint on the namespace table making namespace_name unique per warehouse:
-- unique_namespace_per_warehouse
-- 
-- Using that for below foreign key.
ALTER TABLE tabular
ADD CONSTRAINT tabular_warehouse_id_namespace_name_fkey
FOREIGN KEY (warehouse_id, namespace_name)
REFERENCES namespace(warehouse_id, namespace_name)
ON UPDATE CASCADE;
-- add ON DELETE CASCADE here only if it's necessary

-- `array_to_string` is not `immutable` so it can't be used directly for the index.
-- ALTER TABLE tabular
-- ADD COLUMN namespace_name_flat text GENERATED ALWAYS
--     AS (array_to_string(namespace_name, '.' )) STORED;

CREATE OR REPLACE FUNCTION concat_namespace_name_tabular_name(nsn text[], tn text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT array_to_string(nsn, '.') || '.' || tn;
$$;

-- TODO the trigram index seems to bypass collation. Do we want to add sth like `LOWER(...)`?
DROP INDEX IF EXISTS tabular_name_namespace_name_index;
CREATE INDEX tabular_name_namespace_name_index
ON tabular
USING GIST (
    -- (namespace_name_flat || '.' || name)
    concat_namespace_name_tabular_name(namespace_name, name)
    gist_trgm_ops(siglen=256)
);

ALTER TYPE api_endpoints ADD VALUE 'management-v1-search-tabular';

-- -- Covered by the new fk added above.
-- ALTER TABLE tabular
-- DROP CONSTRAINT IF EXISTS tabular_namespace_id_fkey;


-- CREATE OR REPLACE FUNCTION public.sync_tabular_namespace_name()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     -- Check if the namespace_name was actually changed to avoid unnecessary updates
--     IF NEW.namespace_name IS DISTINCT FROM OLD.namespace_name THEN
--         UPDATE public.tabular
--         SET namespace_name = NEW.namespace_name
--         WHERE namespace_id = NEW.namespace_id;
--     END IF;

--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER trigger_update_namespace_name
-- AFTER UPDATE ON public.namespace
-- FOR EACH ROW
-- EXECUTE FUNCTION public.sync_tabular_namespace_name();

-- CREATE OR REPLACE FUNCTION public.set_tabular_namespace_name_on_insert()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     -- Fetch the namespace_name from the parent table and set it on the new row
--     SELECT n.namespace_name INTO NEW.namespace_name
--     FROM public.namespace n
--     WHERE n.namespace_id = NEW.namespace_id;

--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER trigger_set_namespace_name_on_insert
-- BEFORE INSERT ON public.tabular
-- FOR EACH ROW
-- EXECUTE FUNCTION public.set_tabular_namespace_name_on_insert();

-- TODO trigger `namespace_name` update when table is moved to different namespace
