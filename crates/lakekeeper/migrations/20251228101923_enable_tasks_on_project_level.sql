-- Adjust task tables to enable tasks on project level:
-- task, task_config, task_log

-- Drop old indexes that will be replaced
DROP INDEX IF EXISTS task_warehouse_created_at_idx;
DROP INDEX IF EXISTS task_warehouse_id_entity_type_entity_id_idx;
DROP INDEX IF EXISTS task_warehouse_created_at_id_idx;

-- Recreate entity_type enum to add 'project' and 'warehouse' values.
-- We recreate the enum to use new values in the same transaction.
-- Additional entity types (namespace, role, user, server) are included for future use.
ALTER TYPE entity_type RENAME TO entity_type_old;
CREATE TYPE "entity_type" AS ENUM ('table', 'view', 'project', 'warehouse', 'namespace', 'role', 'user', 'server');

-- Add project_id columns to all tables (nullable initially) and convert to new entity_type enum
alter table task
add column if not exists project_id text null references project (project_id) on delete cascade,
alter column entity_type TYPE entity_type USING entity_type::text::entity_type;

alter table task_log
add column if not exists project_id text null references project (project_id) on delete cascade,
alter column entity_type TYPE entity_type USING entity_type::text::entity_type;

alter table task_config
add column if not exists project_id text null references project (project_id) on delete cascade;

DROP TYPE entity_type_old;

-- Fill project_id for all tables based on warehouse -> project mapping
with
	project_info as (
		select
			project_id,
			warehouse_id
		from
			warehouse
	),
	task_updates as (
		update task
		set
			project_id = p.project_id
		from
			project_info as p
		where
			task.project_id is null
			and task.warehouse_id = p.warehouse_id returning 1
	),
	task_config_updates as (
		update task_config
		set
			project_id = p.project_id
		from
			project_info as p
		where
			task_config.project_id is null
			and task_config.warehouse_id = p.warehouse_id returning 1
	)
update task_log
set
	project_id = p.project_id
from
	project_info as p
where
	task_log.project_id is null
	and task_log.warehouse_id = p.warehouse_id;

-- Set NOT NULL constraints on project_id
alter table task
alter column project_id
set
	not null;

alter table task_config
alter column project_id
set
	not null;

alter table task_log
alter column project_id
set
	not null;

-- Modify task_config: drop old PK and add new column + PK
alter table task_config
drop constraint if exists task_config_pkey,
alter column warehouse_id
drop not null,
add column if not exists task_config_id uuid default gen_random_uuid (),
add primary key (task_config_id),
add constraint task_config_project_id_warehouse_id_queue_name_key unique nulls not distinct (project_id, warehouse_id, queue_name);

-- Modify task constraints and make warehouse_id optional
alter table task
drop constraint if exists task_unique_warehouse_id_entity_type_entity_id_queue_name,
alter column warehouse_id
drop not null,
alter column entity_id
drop not null,
alter column entity_name
drop not null,
add constraint task_project_warehouse_id_entity_type_entity_id_queue_name_key unique nulls not distinct (
	project_id,
	warehouse_id,
	entity_type,
	entity_id,
	queue_name
),
-- Task table: full constraints for all entity types
drop constraint if exists task_warehouse_id_check,
drop constraint if exists task_entity_check,
-- warehouse_id required for warehouse/table/view, forbidden for project
add constraint task_warehouse_id_check check (
	(
		entity_type = 'project'
		and warehouse_id is null
	)
	or (
		entity_type in ('warehouse', 'table', 'view')
		and warehouse_id is not null
	)
),
-- entity_id/entity_name required for table/view, forbidden for project/warehouse
add constraint task_entity_check check (
	(
		entity_type in ('project', 'warehouse')
		and entity_id is null
		and entity_name is null
	)
	or (
		entity_type in ('table', 'view')
		and entity_id is not null
		and entity_name is not null
	)
);

-- Modify task_log constraints and make warehouse_id optional
alter table task_log
alter column warehouse_id
drop not null,
alter column entity_id
drop not null,
alter column entity_name
drop not null,
drop constraint if exists task_log_warehouse_id_check,
drop constraint if exists task_log_entity_check,
-- warehouse_id required for warehouse/table/view, forbidden for project
add constraint task_log_warehouse_id_check check (
	(
		entity_type = 'project'
		and warehouse_id is null
	)
	or (
		entity_type in ('warehouse', 'table', 'view')
		and warehouse_id is not null
	)
),
-- entity_id/entity_name required for table/view, forbidden for project/warehouse
add constraint task_log_entity_check check (
    (
        entity_type in ('project', 'warehouse')
        and entity_id is null
        and entity_name is null
    )
    or (
        entity_type in ('table', 'view')
        and entity_id is not null
        and entity_name is not null
    )
);

-- Add new values for api_endpoints enum
alter type api_endpoints add value if not exists 'management-v1-set-project-task-queue-config';
alter type api_endpoints add value if not exists 'management-v1-get-project-task-queue-config';
alter type api_endpoints add value if not exists 'management-v1-control-project-tasks';
alter type api_endpoints add value if not exists 'management-v1-get-project-task-details';
alter type api_endpoints add value if not exists 'management-v1-list-project-tasks';

-- Create new indexes with project_id
CREATE INDEX task_project_warehouse_created_at_id_idx ON public.task USING btree (project_id, warehouse_id, created_at DESC);
CREATE INDEX task_project_warehouse_id_entity_type_entity_id_idx ON public.task USING btree (
	project_id,
	warehouse_id,
	entity_type,
	entity_id,
	created_at DESC
);
CREATE INDEX task_project_warehouse_queue_created_at_idx ON public.task USING btree (
	project_id,
	warehouse_id,
	queue_name,
	created_at DESC
);
