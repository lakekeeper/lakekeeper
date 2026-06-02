-- This migration adds a foreign key constraint to the task table for the warehouse_id column.
-- This is required as some tasks, such as tabular_purges, require storage profiles to run.
-- First, drop the tasks from dependent tables
DELETE FROM tabular_purges
WHERE
    task_id IN (
        SELECT
            task_id
        FROM
            task
        WHERE
            warehouse_id NOT IN (
                SELECT
                    warehouse_id
                FROM
                    warehouse
            )
    );

DELETE FROM tabular_expirations
WHERE
    task_id IN (
        SELECT
            task_id
        FROM
            task
        WHERE
            warehouse_id NOT IN (
                SELECT
                    warehouse_id
                FROM
                    warehouse
            )
    );

-- Then delete the tasks that reference non-existent warehouses
DELETE FROM task
WHERE
    warehouse_id NOT IN (
        SELECT
            warehouse_id
        FROM
            warehouse
    );

-- Now add the foreign key constraint
ALTER TABLE task ADD CONSTRAINT task_warehouse_id_fk FOREIGN KEY (warehouse_id) REFERENCES warehouse (warehouse_id);

-- Drop cascade on task foreign key constraints
ALTER TABLE tabular_purges
DROP CONSTRAINT tabular_purges_task_id_fkey;

ALTER TABLE tabular_expirations
DROP CONSTRAINT tabular_expirations_task_id_fkey;

ALTER TABLE tabular_purges ADD CONSTRAINT tabular_purges_task_id_fkey FOREIGN KEY (task_id) REFERENCES task (task_id) ON DELETE CASCADE;

ALTER TABLE tabular_expirations ADD CONSTRAINT tabular_expirations_task_id_fkey FOREIGN KEY (task_id) REFERENCES task (task_id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_task_warehouse_id ON task (warehouse_id);