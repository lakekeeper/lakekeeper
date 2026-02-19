-- Improve list tables/views performance by adding an index that covers the
-- common case: listing tabulars within a namespace, ordered for cursor
-- pagination.
CREATE INDEX tabular_warehouse_namespace_created_at_idx ON tabular (
    warehouse_id,
    namespace_id,
    created_at,
    tabular_id
)
WHERE
    deleted_at IS NULL
    AND metadata_location IS NOT NULL;

-- Allow per-row index lookups on task when joining against a page of tabulars.
CREATE INDEX task_warehouse_entity_id_queue_idx ON task (warehouse_id, entity_id, queue_name)
WHERE
    entity_type IN ('table', 'view');

-- Add a stored generated column for namespace depth. While array_length(namespace_name, 1)
-- can be used in a functional index, a stored column generally performs better and composes
-- more naturally in composite indexes, allowing the index below to filter by depth without a heap scan.
ALTER TABLE namespace
ADD COLUMN IF NOT EXISTS depth int GENERATED ALWAYS AS (array_length (namespace_name, 1)) STORED;

-- Support cursor pagination for list_namespaces filtered by depth, without an in-memory sort.
-- Covers: warehouse_id = $1 AND depth = $N ORDER BY created_at, namespace_id
CREATE INDEX namespace_warehouse_depth_created_at_idx ON namespace (warehouse_id, depth, created_at, namespace_id);