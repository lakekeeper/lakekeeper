-- the old text_pattern_ops index was not useful for our only query which used something like:
-- select 1 from tabular where '123' like location || '%';
-- so we drop it and create a new btree index instead that we'll use for exact match queries
drop index if exists tabular_location_idx;
create index tabular_namespace_id_location_idx on tabular (namespace_id, location);
