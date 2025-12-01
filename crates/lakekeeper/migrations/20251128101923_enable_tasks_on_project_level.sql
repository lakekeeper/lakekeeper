-- Adjust task tables to enable tasks on project level.
-- * task
-- * task_config
-- * task_log

-- 1. Add project_id to task tables

-- 1.1 Add column project_id nullable
alter table task
add column if not exists project_id text null references project (project_id) on
delete
	cascade;

alter table task_log
add column if not exists project_id text null references project (project_id) on
delete
	cascade;

alter table task_config
add column if not exists project_id text null references project (project_id) on
delete
	cascade;

-- 1.2 Fill project_id for existing rows based on warehouse_id
with project_info as (
select
	project_id,
	warehouse_id
from
	warehouse
)
update
	task
set
	project_id = p.project_id
from
	project_info as p
where
	task.project_id is null
	and task.warehouse_id = p.warehouse_id;

with project_info as (
select
	project_id,
	warehouse_id
from
	warehouse
)
update
	task_config
set
	project_id = p.project_id
from
	project_info as p
where
	task_config.project_id is null
	and task_config.warehouse_id = p.warehouse_id;

with project_info as (
select
	project_id,
	warehouse_id
from
	warehouse
)
update
	task_log
set
	project_id = p.project_id
from
	project_info as p
where
	task_log.project_id is null
	and task_log.warehouse_id = p.warehouse_id;

-- 1.3 Add NOT NULL constraint on project_id
alter table task
alter column project_id set
not null;

alter table task_config
alter column project_id set
not null;

alter table task_log
alter column project_id set
not null;

-- 2. Fix PRIMARY KEYs with warehouse_id

-- 2.1 Remove old PRIMARY KEY
alter table task_config
drop constraint if exists task_config_pkey;

-- 2.2 Create new PRIMARY KEY
alter table task_config
add column if not exists task_config_id uuid primary key default gen_random_uuid();

-- 3. Fix UNIQUE KEY constraints with warehouse_id via UNIQUE NULLS NOT DISTINCT

-- 3.1 Remove old unique keys with warehouse_id
alter table task
drop constraint if exists task_unique_warehouse_id_entity_type_entity_id_queue_name;

-- 3.2 Add new unique keys with NULL values enabled
alter table task
add unique nulls not distinct (project_id,
warehouse_id,
entity_type,
entity_id,
queue_name);

alter table task_config
add unique nulls not distinct (project_id,
warehouse_id,
queue_name);

-- 3.3 Make warehouse id optional on task tables
alter table task
alter column warehouse_id drop
not null;

alter table task_config
alter column warehouse_id drop
not null;

alter table task_log
alter column warehouse_id drop
not null;
