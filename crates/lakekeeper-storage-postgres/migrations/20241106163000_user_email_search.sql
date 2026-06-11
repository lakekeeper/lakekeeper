
DROP INDEX IF EXISTS users_name_gist_idx;
CREATE INDEX users_name_email_gist_idx ON users USING gist ( (name || ' ' || email) gist_trgm_ops(siglen=256));