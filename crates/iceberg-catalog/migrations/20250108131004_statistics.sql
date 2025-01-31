create type statistic_type as enum ('endpoint', 'entity_count');
create type task_source as enum ('system', 'user');
create type queue as enum ('stats', 'compact');
-- TODO: rename to task_status
create type task_status2 as enum ('active', 'inactive', 'cancelled', 'done');

alter table task
    rename column status to old_status;

alter table task
    add column schedule   text,
    add version           int not null default 0,
    add column status     task_status2,
    add column next_tick  timestamptz,
    add column project_id uuid references project (project_id) ON DELETE CASCADE;

update task
set status     = 'done',
    project_id = w.project_id
from warehouse w
where task.warehouse_id = w.warehouse_id
  and (old_status = 'cancelled'
    or old_status = 'done'
    or old_status = 'failed');

create table task_instance
(
    task_instance_id   uuid primary key,
    task_id            uuid        not null references task (task_id) ON DELETE CASCADE,
    attempt            int         not null default 0,
    idempotency_key    uuid        not null,
    status             task_status not null,
    last_error_details text,
    last_heartbeat_at  timestamptz,
    picked_up_at       timestamptz,
    suspend_until      timestamptz,
    completed_at       timestamptz,
    CONSTRAINT task_instance_unique_idempotency_key UNIQUE (idempotency_key, task_id)
);

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
  and ti.status = 'done';

alter table task
    drop column last_error_details,
    drop column old_status,
    drop column attempt,
    drop column picked_up_at,
    drop column suspend_until,
    drop column warehouse_id,
    alter column status set not null,
    alter column project_id set not null;

select trigger_updated_at('task_instance');
call add_time_columns('task_instance');


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
