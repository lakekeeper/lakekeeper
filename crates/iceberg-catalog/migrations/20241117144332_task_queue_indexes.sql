DROP INDEX IF EXISTS task_name_idx;
ALTER TABLE task RENAME COLUMN task_name TO queue_name;
ALTER INDEX task_name_status_idx RENAME TO task_queue_name_status_idx;

drop index task_warehouse_idx;
create index task_warehouse_queue_name_idx on task (warehouse_id, queue_name);