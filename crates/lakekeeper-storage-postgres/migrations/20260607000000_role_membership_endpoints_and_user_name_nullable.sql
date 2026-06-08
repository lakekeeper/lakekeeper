ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-role-members';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-add-role-members';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-remove-role-member';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-role-member-of';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-user-roles';

-- A role-provider stub (pre-created for an unknown user) has no name yet.
-- Represent that as NULL instead of a sentinel string; the placeholder is
-- rendered at read time.
ALTER TABLE users ALTER COLUMN name DROP NOT NULL;

-- Reset existing stubs to NULL.
UPDATE users
SET name = NULL
WHERE last_updated_with = 'role-provider'::user_last_updated_with
  AND name = 'Nameless User with id ' || id;
