create type tabular_delete_mode as enum ('soft', 'hard');

alter table warehouse
    add column tabular_expiration_seconds bigint;
alter table warehouse
    add column tabular_delete_mode tabular_delete_mode not null default 'soft';
alter table warehouse
    add check ( (tabular_expiration_seconds IS NOT NULL AND tabular_delete_mode = 'soft') OR
                (tabular_expiration_seconds IS NULL AND tabular_delete_mode = 'hard') );

alter table warehouse
    alter column tabular_delete_mode drop default;