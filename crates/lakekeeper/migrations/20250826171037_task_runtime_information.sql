DROP INDEX idx_task_warehouse_id;

DROP INDEX task_entity_type_entity_id_idx;

ALTER TABLE task
ADD COLUMN progress real DEFAULT 0.0 NOT NULL,
ADD COLUMN execution_details jsonb;

DROP INDEX task_log_warehouse_id_idx;

DROP INDEX task_log_warehouse_id_entity_id_entity_type_idx;

ALTER TABLE task_log
DROP COLUMN updated_at,
ADD COLUMN progress real DEFAULT 0.0 NOT NULL,
ADD COLUMN execution_details jsonb;

CREATE INDEX task_log_warehouse_id_entity_type_entity_id_idx ON public.task_log USING btree (warehouse_id, entity_id, entity_type);

UPDATE task_log
SET
    progress = CASE
        WHEN status = 'success' THEN 1.0
        WHEN status IN ('cancelled', 'failed') THEN 0.0
        ELSE 0.0
    END;