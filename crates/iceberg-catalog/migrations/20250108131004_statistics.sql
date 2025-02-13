create type statistic_type as enum ('endpoint', 'entity_count');
create type task_source as enum ('system', 'user');
create type queue as enum ('stats', 'compact');
create type schedule_status as enum ('enabled', 'disabled');
alter type task_status rename value 'done' to 'success';

-- add delete cascade to all task queue foreign keys
alter table tabular_expirations
    drop constraint tabular_expirations_task_id_fkey;
alter table tabular_expirations
    add constraint tabular_expirations_task_id_fkey foreign key (task_id) references task (task_id) on delete cascade;
alter table tabular_purges
    drop constraint tabular_purges_task_id_fkey;
alter table tabular_purges
    add constraint tabular_purges_task_id_fkey foreign key (task_id) references task (task_id) on delete cascade;

alter table task
    rename column status to old_status;

alter table task
    add column schedule   text,
    add column status     schedule_status,
    add column next_tick  timestamptz,
    add column project_id uuid references project (project_id) ON DELETE CASCADE;

-- we're scheduling 'enabled' tasks and do so by checking for 'next_tick < now()'.
CREATE INDEX idx_task_next_tick_status ON task (status, next_tick)
    WHERE status = 'enabled' AND next_tick IS NOT NULL;
CREATE INDEX idx_task_project_id
    ON task (project_id);


create table task_instance
(
    task_instance_id   uuid primary key,
    task_id            uuid        not null references task (task_id) ON DELETE CASCADE,
    attempt            int         not null default 0,
    idempotency_key    uuid        not null,
    status             task_status not null,
    last_error_details text,
    picked_up_at       timestamptz,
    suspend_until      timestamptz,
    completed_at       timestamptz,
    CONSTRAINT task_instance_unique_idempotency_key UNIQUE (idempotency_key, task_id)
);

select trigger_updated_at('task_instance');
call add_time_columns('task_instance');

CREATE INDEX idx_task_instance_task_id
    ON task_instance (task_id);
CREATE INDEX idx_task_suspend_until_status_pending ON task_instance (status, suspend_until)
    WHERE status = 'pending';
CREATE INDEX idx_task_picked_up_at_status_running ON task_instance (status, picked_up_at)
    WHERE status = 'running';

create table task_instance_error_history
(
    task_instance_error_history_id uuid primary key,
    task_instance_id               uuid not null references task_instance (task_instance_id) ON DELETE CASCADE,
    error_details                  text not null
);

select trigger_updated_at('task_instance_error_history');
call add_time_columns('task_instance_error_history');


update task
set status     = 'disabled',
    project_id = w.project_id
from warehouse w
where task.warehouse_id = w.warehouse_id
  and (old_status = 'cancelled'
    or old_status = 'success'
    or old_status = 'failed');

insert into task_instance (task_instance_id, task_id, attempt, idempotency_key, status, suspend_until,
                           last_error_details)
select task_id,
       task_id,
       attempt,
       idempotency_key,
       old_status,
       suspend_until,
       last_error_details
from task;

update task_instance ti
set completed_at = task.updated_at
from task
where ti.task_id = task.task_id
  and ti.status = 'success';

alter table task
    drop column last_error_details,
    drop column old_status,
    drop column attempt,
    drop column picked_up_at,
    drop column suspend_until,
    drop column warehouse_id,
    alter column status set not null,
    alter column project_id set not null;


create table statistics_task
(
    task_id      uuid primary key REFERENCES task (task_id) ON DELETE CASCADE,
    warehouse_id uuid not null REFERENCES warehouse (warehouse_id) ON DELETE CASCADE
);

select trigger_updated_at('statistics_task');
call add_time_columns('statistics_task');


create table warehouse_statistics
(
    number_of_views  bigint      not null,
    number_of_tables bigint      not null,
    warehouse_id     uuid        not null REFERENCES warehouse (warehouse_id) ON DELETE CASCADE,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now(),
    PRIMARY KEY (warehouse_id, created_at)
);
-- TODO: partitioning?

select trigger_updated_at('warehouse_statistics');

CREATE INDEX idx_warehouse_stats_time
    ON warehouse_statistics (created_at DESC);
