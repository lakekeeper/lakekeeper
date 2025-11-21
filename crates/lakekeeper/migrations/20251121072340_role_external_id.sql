ALTER TABLE role
ADD COLUMN external_id TEXT;

CREATE UNIQUE INDEX unique_role_external_id_in_project ON role (project_id, external_id)
WHERE
    external_id IS NOT NULL;