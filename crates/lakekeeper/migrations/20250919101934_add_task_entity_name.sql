ALTER TABLE task ADD COLUMN entity_name Text [] collate "case_insensitive";
UPDATE task SET entity_name = CASE 
    WHEN n.namespace_name IS NOT NULL AND t.name IS NOT NULL THEN array_append(n.namespace_name, t.name)
    ELSE ARRAY['unknown']
END
FROM task task_alias
LEFT JOIN tabular t ON t.tabular_id = task_alias.entity_id AND t.warehouse_id = task_alias.warehouse_id
LEFT JOIN namespace n ON n.namespace_id = t.namespace_id
WHERE task.task_id = task_alias.task_id;
ALTER TABLE task ALTER COLUMN entity_name SET NOT NULL;

ALTER TABLE task_log ADD COLUMN entity_name Text [] collate "case_insensitive";
UPDATE task_log SET entity_name = CASE 
    WHEN n.namespace_name IS NOT NULL AND t.name IS NOT NULL THEN array_append(n.namespace_name, t.name)
    ELSE ARRAY['unknown']
END
FROM task_log task_alias
LEFT JOIN tabular t ON t.tabular_id = task_alias.entity_id AND t.warehouse_id = task_alias.warehouse_id
LEFT JOIN namespace n ON n.namespace_id = t.namespace_id
WHERE task_log.task_id = task_alias.task_id;
ALTER TABLE task_log ALTER COLUMN entity_name SET NOT NULL;
