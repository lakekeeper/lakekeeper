ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-role-members';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-add-role-members';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-remove-role-member';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-role-member-of';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-user-roles';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-role-transitive-members';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-user-transitive-roles';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-role-transitive-member-of';

-- A nameless role-provider stub stores name = NULL (placeholder rendered at read
-- time); reset existing sentinel-named stubs to NULL.
ALTER TABLE users ALTER COLUMN name DROP NOT NULL;
UPDATE users
SET name = NULL
WHERE last_updated_with::text = 'role-provider'
  AND name = 'Nameless User with id ' || id;

-- search_user ranks on COALESCE(name,'') || ' ' || COALESCE(email,'').
DROP INDEX IF EXISTS users_name_email_gist_idx;
CREATE INDEX users_name_email_coalesce_gist_idx ON users USING gist (
    (COALESCE(name, '') || ' ' || COALESCE(email, '')) gist_trgm_ops(siglen=256)
);

-- list_users keyset pagination + a bare-name trigram for its `name ILIKE` filter.
CREATE INDEX users_created_at_id_idx ON users (created_at, id) WHERE deleted_at IS NULL;
CREATE INDEX users_name_gist_idx ON users USING gist (name gist_trgm_ops(siglen=256));
