ALTER TYPE entity_type RENAME TO entity_type_old;
CREATE TYPE entity_type AS ENUM ('table', 'view');

ALTER TABLE task ADD COLUMN entity_name Text [] collate "case_insensitive",
    DROP COLUMN entity_type,
    ADD COLUMN entity_type entity_type;
ALTER TABLE task_log ADD COLUMN entity_name Text [] collate "case_insensitive",
    DROP COLUMN entity_type,
    ADD COLUMN entity_type entity_type;

DROP TYPE entity_type_old;

UPDATE task SET 
    entity_name = CASE 
        WHEN n.namespace_name IS NOT NULL AND t.name IS NOT NULL THEN array_append(n.namespace_name, t.name)
        ELSE ARRAY['unknown']
    END,
    entity_type = CASE 
        WHEN task_alias.task_data ->> 'tabular_type' IN ('table', 'view') THEN (task_alias.task_data ->> 'tabular_type')::entity_type
        ELSE 'table'
    END
FROM task task_alias
LEFT JOIN tabular t ON t.tabular_id = task_alias.entity_id AND t.warehouse_id = task_alias.warehouse_id
LEFT JOIN namespace n ON n.namespace_id = t.namespace_id
WHERE task.task_id = task_alias.task_id;

ALTER TABLE task 
    ALTER COLUMN entity_name SET NOT NULL, 
    ALTER COLUMN entity_type SET NOT NULL;

UPDATE task_log SET 
    entity_name = CASE 
        WHEN n.namespace_name IS NOT NULL AND t.name IS NOT NULL THEN array_append(n.namespace_name, t.name)
        ELSE ARRAY['unknown']
    END,
    entity_type = CASE 
        WHEN task_alias.task_data ->> 'tabular_type' IN ('table', 'view') THEN (task_alias.task_data ->> 'tabular_type')::entity_type
        ELSE 'table'
    END
FROM task_log task_alias
LEFT JOIN tabular t ON t.tabular_id = task_alias.entity_id AND t.warehouse_id = task_alias.warehouse_id
LEFT JOIN namespace n ON n.namespace_id = t.namespace_id
WHERE task_log.task_id = task_alias.task_id;

ALTER TABLE task_log 
    ALTER COLUMN entity_name SET NOT NULL, 
    ALTER COLUMN entity_type SET NOT NULL;

CREATE INDEX IF NOT EXISTS task_warehouse_id_entity_type_entity_id_idx ON task USING btree (
    warehouse_id,
    entity_type,
    entity_id,
    created_at DESC
);

CREATE INDEX IF NOT EXISTS task_log_warehouse_id_entity_type_entity_id_idx ON task_log USING btree (
    warehouse_id,
    entity_type,
    entity_id,
    task_created_at DESC
);

ALTER TABLE task 
    DROP CONSTRAINT IF EXISTS task_unique_warehouse_id_entity_type_entity_id_queue_name, 
    ADD CONSTRAINT task_unique_warehouse_id_entity_type_entity_id_queue_name
    UNIQUE (warehouse_id, entity_type, entity_id, queue_name);
