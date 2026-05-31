alter table warehouse
    add column allowed_format_versions smallint[] not null default '{1,2,3}',
    add column default_format_version smallint;

alter table warehouse
    add constraint warehouse_allowed_format_versions_nonempty
        check (cardinality(allowed_format_versions) > 0),
    add constraint warehouse_default_format_version_in_allowed
        check (default_format_version is null
               or default_format_version = any (allowed_format_versions));

alter type api_endpoints add value if not exists 'management-v1-update-warehouse-format-version-policy';
