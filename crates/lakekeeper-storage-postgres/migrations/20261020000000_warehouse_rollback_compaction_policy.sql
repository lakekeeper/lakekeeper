-- Locks warehouse and "table" exclusively. Fails after 5 s instead of stalling lookups and commits; the next start retries.
SET LOCAL lock_timeout = '5s';
alter table warehouse
    add column rollback_compaction_on_conflict boolean not null default false,
    add column rollback_compaction_max_age_seconds bigint not null default 86400
        check (rollback_compaction_max_age_seconds > 0),
    add column rollback_compaction_fence_seconds bigint not null default 60
        check (rollback_compaction_fence_seconds > 0);

-- When a commit last rolled back a compaction, and when one last removed snapshots it listed itself.
alter table "table"
    add column last_reap_at timestamptz,
    add column last_snapshot_removal_at timestamptz;
