-- Endpoint statistics record the matched route as this enum type, so a route that is
-- not registered here is rejected when its statistics are written.
alter type api_endpoints add value if not exists 'management-v1-list-namespace-subtree-grants';
alter type api_endpoints add value if not exists 'management-v1-revoke-namespace-subtree-grants';
alter type api_endpoints add value if not exists 'management-v1-list-warehouse-subtree-grants';
alter type api_endpoints add value if not exists 'management-v1-revoke-warehouse-subtree-grants';

-- A warehouse-rooted subtree reads each grant level by warehouse alone; no prior index
-- both narrows to the warehouse and carries the keyset tail, so without this one every
-- page reads the warehouse's whole grant set and top-N sorts it — linear in the
-- warehouse, per page, and quadratic over a full revoke loop.
create index if not exists grant_warehouse_level_idx on grant_assignment
    (warehouse_id, resource_type, created_at, grant_id)
    where warehouse_id is not null;

-- Subsumed by the index above: same leading column, same predicate. It existed to
-- serve the cascade when a warehouse row is deleted, which the wider index now does.
drop index if exists grant_warehouse_cascade_idx;
