create type managed_by as enum ('self-managed', 'instance-admin');

alter table warehouse
    add column managed_by managed_by not null default 'self-managed';

alter type api_endpoints add value if not exists 'management-v1-set-warehouse-managed-by';
