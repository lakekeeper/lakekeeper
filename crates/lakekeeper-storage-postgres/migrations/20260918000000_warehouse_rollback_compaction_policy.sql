alter table warehouse
    add column rollback_compaction_on_conflict boolean not null default false;

alter type api_endpoints add value if not exists 'management-v1-update-warehouse-rollback-compaction-policy';
