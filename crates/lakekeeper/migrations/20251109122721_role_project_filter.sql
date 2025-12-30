CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Drop redundant index. project_id is also part of `unique_role_name_in_project`
DROP INDEX IF EXISTS role_project_id_idx;

-- Drop the old single-column GiST index
DROP INDEX IF EXISTS role_name_gist_idx;

-- Create a composite GiST index
CREATE INDEX role_project_name_gist_idx ON role USING gist (project_id, name gist_trgm_ops (siglen = '256'));
