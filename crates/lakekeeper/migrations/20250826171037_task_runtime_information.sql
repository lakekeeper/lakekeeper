DROP INDEX idx_task_warehouse_id;

DROP INDEX task_entity_type_entity_id_idx;

DROP INDEX task_warehouse_queue_name_idx;

TRUNCATE TABLE task_log;

ALTER TABLE task
ADD COLUMN progress real DEFAULT 0.0 NOT NULL,
ADD COLUMN execution_details jsonb;

DROP INDEX task_log_warehouse_id_idx;

DROP INDEX task_log_warehouse_id_entity_id_entity_type_idx;

CREATE INDEX task_warehouse_id_entity_type_entity_id_idx ON public.task_log USING btree (
    warehouse_id,
    entity_id,
    entity_type,
    created_at DESC
);

CREATE INDEX task_warehouse_created_at_id_idx ON public.task (warehouse_id, created_at DESC);

CREATE INDEX task_warehouse_queue_created_at_idx ON public.task (warehouse_id, queue_name, created_at DESC);

ALTER TABLE task_log
DROP COLUMN updated_at,
ADD COLUMN progress real DEFAULT 0.0 NOT NULL,
ADD COLUMN execution_details jsonb,
ADD COLUMN attempt_scheduled_for timestamptz NOT NULL,
ADD COLUMN last_heartbeat_at timestamptz,
ADD COLUMN parent_task_id uuid,
ADD COLUMN task_created_at timestamptz NOT NULL;

CREATE INDEX task_log_warehouse_id_entity_type_entity_id_idx ON public.task_log USING btree (
    warehouse_id,
    entity_id,
    entity_type,
    task_created_at DESC
);

CREATE INDEX task_log_warehouse_created_at_id_attempt_idx ON public.task_log (warehouse_id, task_created_at DESC);

CREATE INDEX task_log_warehouse_queue_created_at_idx ON public.task_log (warehouse_id, queue_name, task_created_at DESC);

ALTER TYPE api_endpoints ADD VALUE 'management-v1-control-tasks';

ALTER TYPE api_endpoints ADD VALUE 'management-v1-get-task-details';

ALTER TYPE api_endpoints ADD VALUE 'management-v1-list-tasks';