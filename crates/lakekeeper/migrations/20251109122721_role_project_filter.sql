CREATE EXTENSION IF NOT EXISTS btree_gist;

DROP INDEX IF EXISTS role_project_id_idx;

-- Drop the old single-column GiST index
DROP INDEX IF EXISTS role_name_gist_idx;

-- Create a composite GIN index for project_id (btree_gin) + name (trigram)
CREATE INDEX role_project_name_gist_idx ON public.role USING gist (project_id, name gist_trgm_ops (siglen = '256'));