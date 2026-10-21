-- Cache keys that a transaction changed. Every Lakekeeper instance reads the rows of newly
-- committed transactions and evicts those keys from its caches.
create table cache_change_log (
    id bigint generated always as identity,
    xid xid8 not null default pg_current_xact_id(),
    domain text not null,
    -- An empty array names the whole domain.
    keys text[] not null,
    -- Rows older than the retention are pruned.
    changed_at timestamptz not null default clock_timestamp(),
    -- Rows are read in (xid, id) order. The key also gives deletes a replica identity under
    -- a publication FOR ALL TABLES.
    constraint cache_change_log_pkey primary key (xid, id)
);

-- Pruning finds the old rows through this index. Every instance tries to prune every few
-- minutes, and most runs find nothing to delete.
create index cache_change_log_changed_at on cache_change_log (changed_at);

-- Rows are inserted and deleted in bulk; vacuum after a fixed number of rows, never by
-- a fraction of the table.
alter table cache_change_log set (
    vacuum_truncate = false,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 5000,
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold = 10000
);

-- Stable entry point for extensions: record that the current transaction changed `p_keys`
-- of `p_domain`. Each call adds one row, so pass all keys of a domain in one call. Like
-- every Lakekeeper table, the log is found through the session's search_path, so call it
-- from a Lakekeeper session.
create function lakekeeper_record_cache_change(
    p_domain text,
    p_keys text[]
) returns void
language sql
as $$
    insert into cache_change_log (domain, keys)
    values (p_domain, p_keys)
$$;
