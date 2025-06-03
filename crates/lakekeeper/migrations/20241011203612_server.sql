create table server
(
    single_row         bool PRIMARY KEY DEFAULT true,
    server_id          uuid    not null,
    terms_accepted     boolean not null,
    open_for_bootstrap boolean not null,
    CONSTRAINT single_row CHECK (single_row)
);

call add_time_columns('server');
select trigger_updated_at('server');
