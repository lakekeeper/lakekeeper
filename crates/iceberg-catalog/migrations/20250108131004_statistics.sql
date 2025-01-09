create type statistic_type as enum ('endpoint', 'entity_count');
create type queue as enum ('stats', 'compact');


create table cron_schedule
(
    task_id  uuid primary key references task (task_id),
    schedule text  not null,-- cron schedule
    typ      queue not null,-- the queue to send the task to
    ran      int8  not null -- number of times cron was executed
);

create table stats_job
(
    task_id uuid primary key references task (task_id)
);

create table statistics
(
    statistics_id uuid primary key,
    warehouse_id  uuid not null REFERENCES warehouse (warehouse_id)
);

select trigger_updated_at('"statistics"');
call add_time_columns('statistics');

create table counters
(
    name         text    not null,
    statistic_id uuid REFERENCES statistics (statistics_id),
    value        decimal not null,
    PRIMARY KEY (name, statistic_id)
);

