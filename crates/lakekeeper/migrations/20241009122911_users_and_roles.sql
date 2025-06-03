CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

create type user_last_updated_with as enum ('create-endpoint', 'config-call-creation', 'update-endpoint');
create type user_type as enum ('application', 'human');

create table users
(
    id                text primary key,
    name              text                   not null,
    email             text,
    user_type         user_type              not null,
    last_updated_with user_last_updated_with not null,
    deleted_at        timestamptz
);

call add_time_columns('users');
select trigger_updated_at('users');

CREATE INDEX users_name_gist_idx ON users USING gist (name gist_trgm_ops(siglen=256));

create table role
(
    id          uuid primary key,
    project_id  uuid         not null references project (project_id),
    name        text  not null,
    description text
);

CREATE UNIQUE INDEX unique_role_name_in_project ON role (project_id, (lower(name)));
call add_time_columns('role');
select trigger_updated_at('role');

CREATE INDEX role_project_id_idx ON role (project_id);
CREATE INDEX role_name_gist_idx ON role USING gist (name gist_trgm_ops(siglen=256));
