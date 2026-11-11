-- Namespace and tabular grants carry their object's namespace path, so one index range finds the grants below a namespace.

-- Every lock up front, strongest per table, in cascade order: grant reads and writes of the three tables wait until commit; namespace and tabular reads go on.
-- Gives up after 5 s, so a long-held lock such as a running pg_dump does not stall older servers' grant queries; the next run retries.
SET LOCAL lock_timeout = '5s';
LOCK TABLE namespace, tabular IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE grant_assignment IN ACCESS EXCLUSIVE MODE;
SET LOCAL lock_timeout TO DEFAULT;

-- A unique index, not a constraint: adding a constraint takes ACCESS EXCLUSIVE on tabular, which blocks table loads.
CREATE UNIQUE INDEX tabular_warehouse_id_tabular_id_namespace_name_key
    ON tabular (warehouse_id, tabular_id, tabular_namespace_name);
ALTER TABLE grant_assignment ADD COLUMN namespace_name text[] COLLATE case_insensitive;

-- Copies the path from the referenced row whatever the insert supplies, so older servers keep writing grants; FOR KEY SHARE holds the path until the insert's key check.
CREATE FUNCTION grant_copy_namespace_path() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- A row without its id columns is left to grant_resource_target, which reports it.
    IF NEW.resource_type = 'namespace' AND NEW.warehouse_id IS NOT NULL AND NEW.namespace_id IS NOT NULL THEN
        SELECT n.namespace_name INTO NEW.namespace_name FROM namespace n
        WHERE n.warehouse_id = NEW.warehouse_id AND n.namespace_id = NEW.namespace_id
        FOR KEY SHARE;
    ELSIF NEW.resource_type = 'tabular' AND NEW.warehouse_id IS NOT NULL AND NEW.tabular_id IS NOT NULL THEN
        SELECT t.tabular_namespace_name INTO NEW.namespace_name FROM tabular t
        WHERE t.warehouse_id = NEW.warehouse_id AND t.tabular_id = NEW.tabular_id
        FOR KEY SHARE;
    ELSE
        RETURN NEW;
    END IF;
    IF NOT FOUND THEN
        RAISE foreign_key_violation USING MESSAGE = 'the namespace or tabular of the grant does not exist';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER grant_copy_namespace_path BEFORE INSERT ON grant_assignment
    FOR EACH ROW EXECUTE FUNCTION grant_copy_namespace_path();

UPDATE grant_assignment g SET namespace_name = n.namespace_name
FROM namespace n
WHERE g.resource_type = 'namespace' AND n.warehouse_id = g.warehouse_id AND n.namespace_id = g.namespace_id;
UPDATE grant_assignment g SET namespace_name = t.tabular_namespace_name
FROM tabular t
WHERE g.resource_type = 'tabular' AND t.warehouse_id = g.warehouse_id AND t.tabular_id = g.tabular_id;

-- Validated: a wrong copy fails the run and changes nothing.
ALTER TABLE grant_assignment
    ADD CONSTRAINT grant_namespace_path_fkey FOREIGN KEY (warehouse_id, namespace_id, namespace_name)
        REFERENCES namespace (warehouse_id, namespace_id, namespace_name) ON UPDATE CASCADE ON DELETE CASCADE,
    ADD CONSTRAINT grant_tabular_path_fkey FOREIGN KEY (warehouse_id, tabular_id, namespace_name)
        REFERENCES tabular (warehouse_id, tabular_id, tabular_namespace_name) ON UPDATE CASCADE ON DELETE CASCADE,
    -- Named to sort after grant_resource_target, so a row without its id columns still reports that check.
    ADD CONSTRAINT grant_target_path
        CHECK ((resource_type IN ('namespace', 'tabular')) = (namespace_name IS NOT NULL)),
    -- The range lookup is exact only for one-dimensional paths with lower bound 1 and no NULL element.
    ADD CONSTRAINT grant_target_path_shape
        CHECK (array_ndims(namespace_name) = 1 AND array_lower(namespace_name, 1) = 1
               AND array_position(namespace_name, NULL) IS NULL);

CREATE INDEX grant_user_path_idx ON grant_assignment (user_id, warehouse_id, namespace_name)
    INCLUDE (privilege, resource_type, tabular_id) WHERE user_id IS NOT NULL AND namespace_name IS NOT NULL;
CREATE INDEX grant_role_path_idx ON grant_assignment (role_id, warehouse_id, namespace_name)
    INCLUDE (privilege, resource_type, tabular_id) WHERE role_id IS NOT NULL AND namespace_name IS NOT NULL;

-- grant_namespace_fkey and grant_tabular_fkey stay: dropping a key takes ACCESS EXCLUSIVE on namespace or tabular, which deadlocks with grant writes and blocks table loads.
