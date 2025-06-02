create or replace procedure add_time_columns(tablename regclass)
    language plpgsql
as
$$
begin
    execute format(
            'alter table %s
            add column if not exists created_at timestamptz not null default now(),
            add column if not exists updated_at timestamptz;',
            tablename
            );
end;
$$;
