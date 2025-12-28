-- Adjust task tables to enable tasks on project level.
-- * task
-- * task_config
-- * task_log
-- 1. Add project_id columns to all tables (nullable initially)
alter table task
add column if not exists project_id text null references project (project_id) on delete cascade;

alter table task_log
add column if not exists project_id text null references project (project_id) on delete cascade;

alter table task_config
add column if not exists project_id text null references project (project_id) on delete cascade;

-- 2. Fill project_id for all tables
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

-- 3. Set NOT NULL constraints on project_id
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

-- 4. Modify task_config: drop old PK and add new column + PK
alter table task_config
drop constraint if exists task_config_pkey,
alter column warehouse_id
drop not null,
add column if not exists task_config_id uuid default gen_random_uuid (),
add primary key (task_config_id),
add constraint task_config_project_id_warehouse_id_queue_name_key unique nulls not distinct (project_id, warehouse_id, queue_name);

-- 5. Add new value for entity_type enum to represent project-level tasks
alter type entity_type add value if not exists 'project';

alter type entity_type add value if not exists 'warehouse';

-- 6. Modify task constraints and make warehouse_id optional
-- Note: We can't use 'project'/'warehouse' enum values in CHECK constraints in this transaction.
-- Part 2 will add stricter constraints once enum values are committed.
alter table task
drop constraint if exists task_unique_warehouse_id_entity_type_entity_id_queue_name,
alter column warehouse_id
drop not null,
alter column entity_id
drop not null,
alter column entity_name
drop not null,
add constraint task_project_id_warehouse_id_entity_type_entity_id_queue_name_key unique nulls not distinct (
	project_id,
	warehouse_id,
	entity_type,
	entity_id,
	queue_name
),
-- Temporary constraints: only enforce rules for table/view (existing types)
-- warehouse_id required for table/view
add constraint task_warehouse_id_check check (
	entity_type not in ('table', 'view')
	or warehouse_id is not null
),
-- entity_id/entity_name required for table/view
add constraint task_entity_check check (
	entity_type not in ('table', 'view')
	or (
		entity_id is not null
		and entity_name is not null
	)
);

-- 7. Modify task_log constraints and make warehouse_id optional
-- Note: Stricter constraints added in part 2 once enum values are committed.
alter table task_log
alter column warehouse_id
drop not null,
alter column entity_id
drop not null,
-- Temporary: warehouse_id required for table/view
add constraint task_log_warehouse_id_check check (
	entity_type not in ('table', 'view')
	or warehouse_id is not null
),
-- Temporary: entity_id required for table/view
add constraint task_log_entity_check check (
	entity_type not in ('table', 'view')
	or entity_id is not null
);

-- 8. Add new values for api_endpoints enum
alter type api_endpoints add value if not exists 'management-v1-set-project-task-queue-config';

alter type api_endpoints add value if not exists 'management-v1-get-project-task-queue-config';

alter type api_endpoints add value if not exists 'management-v1-control-project-tasks';

alter type api_endpoints add value if not exists 'management-v1-get-project-task-details';

alter type api_endpoints add value if not exists 'management-v1-list-project-tasks';

-- 9 Update indexes
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

DROP INDEX IF EXISTS task_warehouse_created_at_idx;

DROP INDEX IF EXISTS task_warehouse_id_entity_type_entity_id_idx;

DROP INDEX IF EXISTS task_warehouse_queue_created_at_idx;