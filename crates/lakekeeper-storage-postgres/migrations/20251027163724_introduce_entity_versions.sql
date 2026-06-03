-- Add version column with default value 0
ALTER TABLE project
ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE warehouse
ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE namespace
ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

-- Combined function to increment version and set updated_at on each update
CREATE OR REPLACE FUNCTION set_updated_at_and_increment_version() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Helper function to create the combined trigger on a table
CREATE OR REPLACE FUNCTION trigger_updated_at_and_version_if_distinct(tablename regclass) RETURNS VOID AS $$
BEGIN
    EXECUTE FORMAT(
        'CREATE TRIGGER set_updated_at_and_increment_version
        BEFORE UPDATE
        ON %s
        FOR EACH ROW
        WHEN (OLD IS DISTINCT FROM NEW)
        EXECUTE FUNCTION set_updated_at_and_increment_version();',
        tablename
    );
END;
$$ LANGUAGE plpgsql;

-- Remove the old updated_at trigger
DROP TRIGGER IF EXISTS set_updated_at ON project;
DROP TRIGGER IF EXISTS set_updated_at ON warehouse;
DROP TRIGGER IF EXISTS set_updated_at ON namespace;

-- Add the new combined trigger
SELECT trigger_updated_at_and_version_if_distinct('"project"');
SELECT trigger_updated_at_and_version_if_distinct('"warehouse"');
SELECT trigger_updated_at_and_version_if_distinct('"namespace"');

