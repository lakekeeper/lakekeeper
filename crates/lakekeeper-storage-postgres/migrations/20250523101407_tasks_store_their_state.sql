create type entity_type as enum ('tabular');
create type task_final_status as enum ('failed', 'cancelled', 'success');

alter table task
    rename column suspend_until to scheduled_for;

UPDATE task SET scheduled_for = now() WHERE scheduled_for IS NULL;

alter table task
    add column task_data         jsonb,
    add column entity_type       entity_type,
    add column entity_id         uuid,
    add column last_heartbeat_at timestamptz,
    -- we're moving old tasks into task_log and delete entries from task
    drop constraint task_parent_task_id_fkey,
    alter column scheduled_for set default now(),
    alter column scheduled_for set not null;

update task
set task_data   = jsonb_build_object(
        'tabular_id', te.tabular_id,
        'typ', te.typ,
        'deletion_kind', te.deletion_kind),
    entity_type = 'tabular',
    entity_id   = te.tabular_id
from tabular_expirations te
where te.task_id = task.task_id;

update task
set task_data   = jsonb_build_object(
        'tabular_id', tp.tabular_id,
        'typ', tp.typ,
        'tabular_location', tp.tabular_location),
    entity_type = 'tabular',
    entity_id   = tp.tabular_id
from tabular_purges tp
where tp.task_id = task.task_id;

drop table tabular_expirations;
drop table tabular_purges;

alter table task
    alter column task_data set not null,
    alter column entity_type set not null,
    alter column entity_id set not null;

create table task_config
(
    warehouse_id                  uuid references warehouse (warehouse_id) on delete cascade not null,
    queue_name                    text                                                       not null,
    config                        jsonb                                                      not null,
    max_time_since_last_heartbeat interval,
    primary key (warehouse_id, queue_name)
);

call add_time_columns('task_config');
select trigger_updated_at('task_config');

create table task_log
(
    task_id      uuid                                                       not null,
    attempt      integer                                                    not null,
    warehouse_id uuid references warehouse (warehouse_id) on delete cascade not null,
    queue_name   text                                                       not null,
    task_data    jsonb                                                      not null,
    status       task_final_status                                          not null,
    entity_id    uuid                                                       not null,
    entity_type  entity_type                                                not null,
    started_at   timestamptz,
    duration     interval,
    message      text,
    primary key (task_id, attempt)
);

call add_time_columns('task_log');

create index if not exists task_log_warehouse_id_idx
    on task_log (warehouse_id);
create index if not exists task_log_warehouse_id_entity_id_entity_type_idx
    on task_log (warehouse_id, entity_id, entity_type);


insert into task_log (task_id, warehouse_id, attempt, queue_name, task_data, status, message, entity_id,
                      entity_type, started_at, duration)
select task.task_id,
       task.warehouse_id,
       task.attempt,
       task.queue_name,
       task.task_data,
       CASE
           when task.status = 'cancelled' then 'cancelled'::task_final_status
           when task.status = 'failed' then 'failed'::task_final_status
           when task.status = 'done' then 'success'::task_final_status
           END,
       task.last_error_details,
       task.entity_id,
       task.entity_type,
       task.picked_up_at,
       CASE
           WHEN task.updated_at IS NOT NULL THEN age(task.picked_up_at, task.updated_at)
           ELSE '0' END
from task
where task.status != 'running'
  AND task.status != 'pending';

DELETE
FROM task
WHERE task.status != 'running'
  AND task.status != 'pending';

drop index if exists task_queue_name_status_idx;
drop index if exists task_warehouse_queue_name_idx;

alter table task
    alter column status type text,
    drop constraint unique_idempotency_key,
    drop column idempotency_key,
    drop column last_error_details;

create unique index if not exists task_warehouse_id_entity_type_entity_id_queue_name_idx
    on task (warehouse_id, entity_type, entity_id, queue_name);

UPDATE task
SET status = 'scheduled'
WHERE status = 'pending';

drop type task_status;
create type task_intermediate_status as enum ('running', 'scheduled', 'should-stop');
alter table task
    alter column status type task_intermediate_status using status::task_intermediate_status;

create index task_queue_name_status_idx on task (queue_name, status, scheduled_for);
create index task_warehouse_queue_name_idx on task (warehouse_id, queue_name);
create index if not exists task_entity_type_entity_id_idx
    on task (warehouse_id, entity_type, entity_id);

ALTER TYPE api_endpoints ADD VALUE 'management-v1-set-task-queue-config';
ALTER TYPE api_endpoints ADD VALUE 'management-v1-get-task-queue-config';