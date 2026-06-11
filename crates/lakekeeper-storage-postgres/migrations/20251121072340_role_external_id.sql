ALTER TYPE api_endpoints ADD value 'management-v1-update-role-source-system';

ALTER TABLE role
ADD COLUMN source_id TEXT;

CREATE UNIQUE INDEX unique_role_source_id_per_project ON role (project_id, source_id);