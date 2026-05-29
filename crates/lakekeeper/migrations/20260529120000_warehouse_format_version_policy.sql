-- Per-warehouse Iceberg table format version policy.
-- `allowed_format_versions`: which format versions may be created in, or upgraded
--   to, within the warehouse. Defaults to all supported versions for existing rows.
-- `default_format_version`: format version applied when a create-table request
--   omits one. NULL resolves at runtime to v2 if allowed, else the highest allowed.
alter table warehouse
    add column allowed_format_versions smallint[] not null default '{1,2,3}',
    add column default_format_version smallint;

alter table warehouse
    add constraint warehouse_allowed_format_versions_nonempty
        check (cardinality(allowed_format_versions) > 0),
    add constraint warehouse_allowed_format_versions_valid
        check (allowed_format_versions <@ array[1, 2, 3]::smallint[]),
    add constraint warehouse_default_format_version_in_allowed
        check (default_format_version is null
               or default_format_version = any (allowed_format_versions));
