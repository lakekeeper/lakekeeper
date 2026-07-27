create type catalog_kind as enum ('iceberg', 'paimon');

alter table warehouse
    add column catalog_kind catalog_kind not null default 'iceberg';
