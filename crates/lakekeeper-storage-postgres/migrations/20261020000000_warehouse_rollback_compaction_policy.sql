-- Locks warehouse exclusively. Fails after 5 s instead of stalling warehouse lookups; the next start retries.
SET LOCAL lock_timeout = '5s';
alter table warehouse
    add column rollback_compaction_on_conflict boolean not null default false,
    add column rollback_compaction_max_age_seconds bigint not null default 3600
        check (rollback_compaction_max_age_seconds > 0);
