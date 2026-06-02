-- Drop the unique-email constraint on users. Email is metadata only:
-- nothing in lakekeeper resolves identity by email (all lookups go via
-- users.id), no FK references it, and Cedar already exposes it as optional.
-- Identity providers in the wild legitimately yield shared/default emails for
-- service principals, which today rejects valid role-provider batch syncs and
-- /v1/config auto-provisioning with a 409 EntityAlreadyExists.
DROP INDEX IF EXISTS unique_user_email;
