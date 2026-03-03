ALTER TYPE user_last_updated_with ADD VALUE 'role-provider';

CREATE TABLE
    role_assignment (
        role_id UUID NOT NULL,
        user_id TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now (),
        CONSTRAINT role_assignment_pkey PRIMARY KEY (user_id, role_id),
        CONSTRAINT role_assignment_user_id_fkey FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
        CONSTRAINT role_assignment_role_id_fkey FOREIGN KEY (role_id) REFERENCES "role" (id) ON DELETE CASCADE
    );

CREATE INDEX role_assignment_role_id_idx ON role_assignment (role_id, user_id);

-- Tracks the last time a role provider successfully synced a user's role assignments
-- for a specific (user, project, provider) triple.
CREATE TABLE
    role_assignment_sync (
        user_id TEXT NOT NULL,
        project_id TEXT NOT NULL,
        provider_id TEXT NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT now (),
        CONSTRAINT role_assignment_sync_pkey PRIMARY KEY (user_id, project_id, provider_id),
        CONSTRAINT role_assignment_sync_user_fkey FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
        CONSTRAINT role_assignment_sync_project_fkey FOREIGN KEY (project_id) REFERENCES project (project_id) ON DELETE CASCADE
    );

-- Tracks the last time a role provider successfully synced a role's member list.
-- role_id already implies (project_id, provider_id) via the role table.
CREATE TABLE
    role_members_sync (
        role_id UUID NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT now (),
        CONSTRAINT role_members_sync_pkey PRIMARY KEY (role_id),
        CONSTRAINT role_members_sync_role_fkey FOREIGN KEY (role_id) REFERENCES "role" (id) ON DELETE CASCADE
    );