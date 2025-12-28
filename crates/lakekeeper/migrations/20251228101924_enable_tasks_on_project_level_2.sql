-- Replace temporary constraints with full constraints now that enum values are committed

-- Task table: full constraints for all entity types
alter table task 
	drop constraint if exists task_warehouse_id_check,
	drop constraint if exists task_entity_check,
	-- warehouse_id required for warehouse/table/view, forbidden for project
	add constraint task_warehouse_id_check check (
		(entity_type = 'project' and warehouse_id is null) or
		(entity_type in ('warehouse', 'table', 'view') and warehouse_id is not null)
	),
	-- entity_id/entity_name required for table/view, forbidden for project/warehouse
	add constraint task_entity_check check (
		(entity_type in ('project', 'warehouse') and entity_id is null and entity_name is null) or
		(entity_type in ('table', 'view') and entity_id is not null and entity_name is not null)
	);

-- Task_log table: full constraints for all entity types
alter table task_log
	drop constraint if exists task_log_warehouse_id_check,
	drop constraint if exists task_log_entity_check,
	-- warehouse_id required for warehouse/table/view, forbidden for project
	add constraint task_log_warehouse_id_check check (
		(entity_type = 'project' and warehouse_id is null) or
		(entity_type in ('warehouse', 'table', 'view') and warehouse_id is not null)
	),
	-- entity_id required for table/view, forbidden for project/warehouse
	add constraint task_log_entity_check check (
		(entity_type in ('project', 'warehouse') and entity_id is null) or
		(entity_type in ('table', 'view') and entity_id is not null)
	);
