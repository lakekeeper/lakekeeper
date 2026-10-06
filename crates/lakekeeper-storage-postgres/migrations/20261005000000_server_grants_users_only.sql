-- A server grant is held by a user, never by a role. Roles belong to a project, so a role
-- holding a server grant would hand server-wide authority to whoever manages that
-- project's role members.
delete from grant_assignment where resource_type = 'server' and principal_type = 'role';

alter table grant_assignment drop constraint if exists grant_server_principal_is_user;
alter table grant_assignment
    add constraint grant_server_principal_is_user check (
        resource_type <> 'server' or principal_type = 'user');
