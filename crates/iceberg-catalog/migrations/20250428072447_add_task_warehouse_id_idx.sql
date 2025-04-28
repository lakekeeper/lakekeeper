ALTER TABLE task
    ADD CONSTRAINT task_warehouse_id_fk FOREIGN KEY (warehouse_id)
        REFERENCES warehouse (warehouse_id);

ALTER TABLE tabular_purges
    DROP CONSTRAINT tabular_purges_task_id_fkey;
ALTER TABLE tabular_expirations
    DROP CONSTRAINT tabular_expirations_task_id_fkey;
ALTER TABLE tabular_purges
    ADD CONSTRAINT tabular_purges_task_id_fkey FOREIGN KEY (task_id)
        REFERENCES task (task_id) ON DELETE CASCADE;
ALTER TABLE tabular_expirations
    ADD CONSTRAINT tabular_expirations_task_id_fkey FOREIGN KEY (task_id)
        REFERENCES task (task_id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_task_warehouse_id ON task (warehouse_id);