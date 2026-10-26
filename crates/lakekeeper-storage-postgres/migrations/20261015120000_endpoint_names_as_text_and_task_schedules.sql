-- Stores endpoint names as text and adds task schedules. One file, because the order of their
-- locks matters: older servers lock `task` before `project` and `idempotency_record`, so `task` is
-- locked after the copy of the endpoint statistics, which then holds no task lock, and before the
-- swap, which locks `project` and `idempotency_record` exclusively.

-- ---- Endpoint names as text: copy ----

-- Project deletes wait until the copy has its foreign key.
LOCK TABLE project IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE endpoint_statistics_text
(
    endpoint_statistics_id uuid        not null default uuid_generate_v1mc(),
    warehouse_id           uuid,
    project_id             text        not null,
    matched_path           text COLLATE "C" not null,
    status_code            int         not null,
    count                  bigint      not null default 0,
    timestamp              timestamptz not null default get_stats_date_default()
);
call add_time_columns('endpoint_statistics_text');

-- Copied without blocking writes of older servers; what they write after the copy is lost.
INSERT INTO endpoint_statistics_text
    (endpoint_statistics_id, warehouse_id, project_id, matched_path, status_code, count, timestamp, created_at, updated_at)
SELECT endpoint_statistics_id, warehouse_id, project_id, matched_path::text, status_code, count, timestamp, created_at, updated_at
FROM endpoint_statistics;

-- Built after the copy, which is faster.
ALTER TABLE endpoint_statistics_text
    ADD CONSTRAINT endpoint_statistics_text_pkey PRIMARY KEY (endpoint_statistics_id),
    ADD CONSTRAINT endpoint_statistics_text_key
        UNIQUE NULLS NOT DISTINCT (project_id, warehouse_id, matched_path, status_code, timestamp),
    ADD CONSTRAINT endpoint_statistics_project_id_fkey
        FOREIGN KEY (project_id) REFERENCES project (project_id) ON DELETE CASCADE;
CREATE INDEX idx_endpoint_stats_text_project_timestamp ON endpoint_statistics_text (project_id, timestamp DESC);
CREATE INDEX idx_endpoint_stats_text_project_warehouse_time ON endpoint_statistics_text (project_id, warehouse_id, timestamp DESC);
CREATE INDEX idx_endpoint_stats_text_project_status_time ON endpoint_statistics_text (project_id, status_code, timestamp DESC);

-- ---- Task schedules ----

-- One row per recurring job. Each due tick inserts a task whose `entity_id` is the schedule id,
-- so the unique key of `task` (unchanged, still matched by older servers) allows one active
-- instance per schedule.

-- Every lock the task schedules need, strongest per table. The foreign keys to `project` and
-- `warehouse` are locked first: creating a project writes `project` and then `task`, as do the
-- cascades of deleting one. A transaction that holds `task` and then writes `warehouse`, such as
-- deleting a warehouse, can still deadlock with it; Postgres then aborts one of the two.
-- Fails after 5 s instead of queueing older servers' task queries behind a long-held lock, such
-- as a running `pg_dump`; the next start retries.
SET LOCAL lock_timeout = '5s';
LOCK TABLE project, warehouse IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE task, task_log IN ACCESS EXCLUSIVE MODE;
SET LOCAL lock_timeout TO DEFAULT;

-- The id of the schedule with this scope and key. Derived, so a schedule deleted and created
-- again keeps its id and still sees the instance it left active. Never change the encoding:
-- existing rows would fail `task_schedule_id_check` on their next update. The body is bound at
-- creation, so the check also holds where the search path is empty, as in `pg_restore`.
CREATE OR REPLACE FUNCTION task_schedule_id(
    project_id text,
    warehouse_id uuid,
    queue_name text,
    schedule_key text
) RETURNS uuid
LANGUAGE sql IMMUTABLE PARALLEL SAFE
BEGIN ATOMIC
    SELECT uuid_generate_v5(
        '0b6f5d0e-3c1a-4f4e-9a57-4f2b8c9d1e60'::uuid,
        octet_length(project_id)::text || ':' || project_id
            || octet_length(COALESCE(warehouse_id::text, ''))::text || ':'
            || COALESCE(warehouse_id::text, '')
            || octet_length(queue_name)::text || ':' || queue_name
            || octet_length(schedule_key)::text || ':' || schedule_key
    );
END;

-- Target of `task_schedule_warehouse_fk`: a warehouse schedule belongs to its warehouse's project.
CREATE UNIQUE INDEX IF NOT EXISTS warehouse_warehouse_id_project_id_idx
    ON warehouse (warehouse_id, project_id);

-- `schedule_id` is `task_schedule_id(project_id, warehouse_id, queue_name, schedule_key)`, enforced
-- by `task_schedule_id_check`, so the primary key is the only unique key: one arbiter for
-- concurrent upserts.
CREATE TABLE IF NOT EXISTS task_schedule (
    schedule_id uuid PRIMARY KEY,
    project_id text NOT NULL REFERENCES project (project_id) ON DELETE CASCADE,
    warehouse_id uuid,
    queue_name text NOT NULL,
    -- Chosen and interpreted by the queue that owns the schedule.
    schedule_key text NOT NULL,
    interval_seconds bigint NOT NULL,
    -- Ticks fall on `anchor + k * interval` for k >= 0.
    anchor timestamptz NOT NULL,
    -- Becomes the `task_data` of every task the schedule produces.
    payload jsonb NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    next_run_at timestamptz NOT NULL,
    -- Ticks that got no run of their own: the previous instance was still active, or no server
    -- materialized them in time.
    missed_runs bigint NOT NULL DEFAULT 0,
    -- How ticks are computed. A server lists, updates and materializes only rows of versions it
    -- knows.
    spec_version smallint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    -- One minute to 100 years.
    CONSTRAINT task_schedule_interval_check CHECK (interval_seconds BETWEEN 60 AND 3153600000),
    CONSTRAINT task_schedule_key_check CHECK (length(schedule_key) BETWEEN 1 AND 512),
    CONSTRAINT task_schedule_id_check
        CHECK (schedule_id = task_schedule_id(project_id, warehouse_id, queue_name, schedule_key)),
    CONSTRAINT task_schedule_warehouse_fk FOREIGN KEY (warehouse_id, project_id)
        REFERENCES warehouse (warehouse_id, project_id) ON DELETE CASCADE
);

DROP TRIGGER IF EXISTS set_updated_at ON task_schedule;
SELECT trigger_updated_at('task_schedule');

CREATE INDEX IF NOT EXISTS task_schedule_due_idx ON task_schedule (next_run_at) WHERE enabled;
CREATE INDEX IF NOT EXISTS task_schedule_scope_idx
    ON task_schedule (project_id, warehouse_id, queue_name, schedule_key);
CREATE INDEX IF NOT EXISTS task_schedule_warehouse_idx ON task_schedule (warehouse_id)
    WHERE warehouse_id IS NOT NULL;

-- Listing a task's children. Partial: only tasks with a parent are indexed.
CREATE INDEX IF NOT EXISTS task_parent_task_id_idx
    ON task (parent_task_id) WHERE parent_task_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS task_log_parent_task_id_idx
    ON task_log (parent_task_id) WHERE parent_task_id IS NOT NULL;

-- Finding a task by its entity, including the active instance of a schedule.
CREATE INDEX IF NOT EXISTS task_entity_id_idx ON task (entity_id) WHERE entity_id IS NOT NULL;

-- Heartbeats update non-indexed columns only; free space on the page keeps them heap-only.
ALTER TABLE task SET (fillfactor = 70);

-- A project- or warehouse-scoped task may carry its schedule id. Every existing row satisfies
-- this check, so NOT VALID skips the scan.
ALTER TABLE task
    DROP CONSTRAINT IF EXISTS task_entity_check,
    ADD CONSTRAINT task_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_name IS NULL)
        OR (entity_type IN ('table', 'view', 'generic-table') AND entity_id IS NOT NULL AND entity_name IS NOT NULL)
    ) NOT VALID;

ALTER TABLE task_log
    DROP CONSTRAINT IF EXISTS task_log_entity_check,
    ADD CONSTRAINT task_log_entity_check CHECK (
        (entity_type IN ('project', 'warehouse') AND entity_name IS NULL)
        OR (entity_type IN ('table', 'view', 'generic-table') AND entity_id IS NOT NULL AND entity_name IS NOT NULL)
    ) NOT VALID;

-- How much worker capacity a task needs, in a unit its queue chooses; NULL counts as 0. A worker
-- can pick only tasks in a range of it. New columns are NULL, so NOT VALID skips the scan.
ALTER TABLE task
    ADD COLUMN IF NOT EXISTS resource_demand bigint,
    DROP CONSTRAINT IF EXISTS task_resource_demand_check,
    ADD CONSTRAINT task_resource_demand_check CHECK (resource_demand >= 0) NOT VALID;

ALTER TABLE task_log
    ADD COLUMN IF NOT EXISTS resource_demand bigint,
    DROP CONSTRAINT IF EXISTS task_log_resource_demand_check,
    ADD CONSTRAINT task_log_resource_demand_check CHECK (resource_demand >= 0) NOT VALID;

-- Picks and demand counts check the resource demand range on index entries, without reading the rows of other ranges; INCLUDE lets due counts read only the index.
DROP INDEX IF EXISTS task_queue_name_status_idx;
CREATE INDEX task_queue_name_status_idx
    ON task (queue_name, status, scheduled_for, task_id, (COALESCE(resource_demand, 0)))
    INCLUDE (resource_demand);

-- ---- Endpoint names as text: swap ----

-- Locks project exclusively. Fails after 5 s instead of stalling older servers; the next start retries.
SET LOCAL lock_timeout = '5s';
DROP TABLE endpoint_statistics;
ALTER TABLE endpoint_statistics_text RENAME TO endpoint_statistics;
ALTER TABLE endpoint_statistics RENAME CONSTRAINT endpoint_statistics_text_pkey TO endpoint_statistics_pkey;
ALTER TABLE endpoint_statistics
    RENAME CONSTRAINT endpoint_statistics_text_key TO endpoint_statistics_project_id_warehouse_id_matched_path_st_key;
ALTER INDEX idx_endpoint_stats_text_project_timestamp RENAME TO idx_endpoint_stats_project_timestamp;
ALTER INDEX idx_endpoint_stats_text_project_warehouse_time RENAME TO idx_endpoint_stats_project_warehouse_time;
ALTER INDEX idx_endpoint_stats_text_project_status_time RENAME TO idx_endpoint_stats_project_status_time;

select trigger_updated_at('endpoint_statistics');

-- After the drop, in older servers' lock order; default collation for their cached plans.
ALTER TABLE idempotency_record
    DROP CONSTRAINT idempotency_operation_check,
    ALTER COLUMN operation TYPE text;
SET LOCAL lock_timeout TO DEFAULT;
