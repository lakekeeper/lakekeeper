-- Previous to version 0.9 a race condition could lead to tabular expiration tasks
-- being cancelled, but the "tabular.deleted_at" field not being reset for the tabular.
-- This migration handles this in two ways:
-- 1. Restore tabulars without conflicts by setting deleted_at to NULL
-- 2. Create new expiration tasks (90 days from now) for tabulars that would conflict
-- --------
-- 1. First, find soft-deleted tabulars without expiration tasks:
CREATE TEMPORARY TABLE tmp_soft_deleted_without_task AS
SELECT
    tab.tabular_id,
    tab.namespace_id,
    tab.name,
    ns.warehouse_id,
    tab.typ
FROM
    tabular tab
    LEFT JOIN task tsk ON tsk.entity_id = tab.tabular_id
    AND tsk.entity_type = 'tabular'
    AND tsk.queue_name = 'tabular_expiration'
    AND tsk.status = 'scheduled'
    INNER JOIN namespace ns ON ns.namespace_id = tab.namespace_id
WHERE
    tab.deleted_at IS NOT NULL
    AND tsk.entity_id IS NULL;

-- 2. Identify safe to restore vs. needs expiration
CREATE TEMPORARY TABLE tmp_safe_to_restore AS
SELECT
    sd.tabular_id
FROM
    tmp_soft_deleted_without_task sd
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            tabular existing
        WHERE
            existing.namespace_id = sd.namespace_id
            AND existing.name = sd.name
            AND existing.deleted_at IS NULL
    );

CREATE TEMPORARY TABLE tmp_need_expiration AS
SELECT
    tabular_id,
    warehouse_id,
    typ
FROM
    tmp_soft_deleted_without_task
WHERE
    tabular_id NOT IN (
        SELECT
            tabular_id
        FROM
            tmp_safe_to_restore
    );

-- 3. Restore tabulars that won't cause conflicts
UPDATE tabular tab
SET
    deleted_at = NULL
WHERE
    tab.tabular_id IN (
        SELECT
            tabular_id
        FROM
            tmp_safe_to_restore
    );

-- 4. Create new expiration tasks for tabulars that would conflict
INSERT INTO
    task (
        task_id,
        warehouse_id,
        queue_name,
        status,
        scheduled_for,
        attempt,
        created_at,
        task_data,
        entity_type,
        entity_id
    )
SELECT
    gen_random_uuid (),
    warehouse_id,
    'tabular_expiration',
    'scheduled',
    NOW () + INTERVAL '90 days',
    0,
    NOW (),
    json_build_object ('tabular_type', typ, 'deletion_kind', 'default'),
    'tabular',
    tabular_id
FROM
    tmp_need_expiration;

-- Clean up
DROP TABLE tmp_soft_deleted_without_task,
tmp_safe_to_restore,
tmp_need_expiration;