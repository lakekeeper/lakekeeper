DROP INDEX IF EXISTS unique_role_source_id_per_project;

ALTER TABLE role ADD COLUMN provider_id TEXT NOT NULL DEFAULT 'lakekeeper';

UPDATE role SET source_id = id::text WHERE source_id IS NULL;

ALTER TABLE role ALTER COLUMN source_id SET NOT NULL;

ALTER TABLE role ADD CONSTRAINT unique_role_provider_source_in_project
    UNIQUE (project_id, provider_id, source_id);

CREATE INDEX role_project_created_id_idx ON role (project_id, created_at, id);
