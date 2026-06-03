DROP INDEX IF EXISTS idx_task_warehouse_id;

DROP INDEX IF EXISTS task_entity_type_entity_id_idx;

DROP INDEX IF EXISTS task_warehouse_queue_name_idx;

CREATE INDEX IF NOT EXISTS task_warehouse_id_entity_type_entity_id_idx ON task USING btree (
    warehouse_id,
    entity_type,
    entity_id,
    created_at DESC
);

ALTER TABLE task
ADD COLUMN progress real DEFAULT 0.0 NOT NULL,
ADD COLUMN execution_details jsonb;

TRUNCATE TABLE task_log;

DROP INDEX IF EXISTS task_log_warehouse_id_idx;

DROP INDEX IF EXISTS task_log_warehouse_id_entity_id_entity_type_idx;

CREATE INDEX IF NOT EXISTS task_warehouse_created_at_id_idx ON task (warehouse_id, created_at DESC);

CREATE INDEX IF NOT EXISTS task_warehouse_queue_created_at_idx ON task (warehouse_id, queue_name, created_at DESC);

ALTER TABLE task_log
DROP COLUMN updated_at,
ADD COLUMN progress real DEFAULT 0.0 NOT NULL,
ADD COLUMN execution_details jsonb,
ADD COLUMN attempt_scheduled_for timestamptz NOT NULL,
ADD COLUMN last_heartbeat_at timestamptz,
ADD COLUMN parent_task_id uuid,
ADD COLUMN task_created_at timestamptz NOT NULL;

CREATE INDEX IF NOT EXISTS task_log_warehouse_id_entity_type_entity_id_idx ON task_log USING btree (
    warehouse_id,
    entity_type,
    entity_id,
    task_created_at DESC
);

CREATE INDEX IF NOT EXISTS task_log_warehouse_created_at_id_attempt_idx ON task_log (warehouse_id, task_created_at DESC);

CREATE INDEX IF NOT EXISTS task_log_warehouse_queue_created_at_idx ON task_log (warehouse_id, queue_name, task_created_at DESC);

ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-control-tasks';

ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-get-task-details';

ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-tasks';