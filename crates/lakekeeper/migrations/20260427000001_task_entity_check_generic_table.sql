-- 'generic-table' was just added to the enum; PG won't let us use it as a literal here yet.

ALTER TABLE task
    DROP CONSTRAINT IF EXISTS task_warehouse_id_check,
    DROP CONSTRAINT IF EXISTS task_entity_check,
    ADD CONSTRAINT task_warehouse_id_check CHECK (
        CASE WHEN entity_type = 'project'
             THEN warehouse_id IS NULL
             ELSE warehouse_id IS NOT NULL
        END
    ),
    ADD CONSTRAINT task_entity_check CHECK (
        CASE WHEN entity_type IN ('project', 'warehouse')
             THEN entity_id IS NULL AND entity_name IS NULL
             ELSE entity_id IS NOT NULL AND entity_name IS NOT NULL
        END
    );

ALTER TABLE task_log
    DROP CONSTRAINT IF EXISTS task_log_warehouse_id_check,
    DROP CONSTRAINT IF EXISTS task_log_entity_check,
    ADD CONSTRAINT task_log_warehouse_id_check CHECK (
        CASE WHEN entity_type = 'project'
             THEN warehouse_id IS NULL
             ELSE warehouse_id IS NOT NULL
        END
    ),
    ADD CONSTRAINT task_log_entity_check CHECK (
        CASE WHEN entity_type IN ('project', 'warehouse')
             THEN entity_id IS NULL AND entity_name IS NULL
             ELSE entity_id IS NOT NULL AND entity_name IS NOT NULL
        END
    );
