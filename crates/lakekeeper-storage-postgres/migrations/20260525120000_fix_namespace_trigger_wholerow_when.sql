-- Replace the whole-row WHEN clause on namespace's trigger: pg_dump emits
-- `(old.* IS DISTINCT FROM new.*)` which pg_restore rejects on a table with
-- a generated column (namespace.depth, added in 20260219081950).

DROP TRIGGER IF EXISTS set_updated_at_and_increment_version ON namespace;

CREATE TRIGGER set_updated_at_and_increment_version
BEFORE UPDATE ON namespace
FOR EACH ROW
WHEN (
       OLD.namespace_name        IS DISTINCT FROM NEW.namespace_name
    OR OLD.namespace_properties  IS DISTINCT FROM NEW.namespace_properties
    OR OLD.protected             IS DISTINCT FROM NEW.protected
    OR OLD.warehouse_id          IS DISTINCT FROM NEW.warehouse_id
)
EXECUTE FUNCTION set_updated_at_and_increment_version();
