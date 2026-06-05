-- Stores role->role nesting (a member role granted into a parent role).
-- Mirrors Postgres pg_auth_members and the OpenFGA role#assignee subject.
-- Distinct from role_assignment (user->role), which is left untouched.
CREATE TABLE
    role_membership (
        parent_role_id UUID NOT NULL,
        member_role_id UUID NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now (),
        CONSTRAINT role_membership_pkey PRIMARY KEY (parent_role_id, member_role_id),
        CONSTRAINT role_membership_no_self CHECK (parent_role_id <> member_role_id),
        CONSTRAINT role_membership_parent_fkey FOREIGN KEY (parent_role_id) REFERENCES "role" (id) ON DELETE CASCADE,
        CONSTRAINT role_membership_member_fkey FOREIGN KEY (member_role_id) REFERENCES "role" (id) ON DELETE CASCADE
    );

-- Reverse-direction lookups (find a member's parents) for the recursive CTE.
CREATE INDEX role_membership_member_idx ON role_membership (member_role_id, parent_role_id);
