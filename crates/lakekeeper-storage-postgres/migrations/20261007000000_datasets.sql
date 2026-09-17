-- Datasets: a namespace-level resource holding a versioned collection of files
-- with branchable, immutable snapshots. Metadata only; nothing is written to
-- object storage.
--
-- A `tabular` subtype like generic-table, so identity, naming, protection and
-- soft-delete are inherited, as is grant coverage.
--
-- The enum rename-recreate below follows 20260529000000_add_generic_table:
-- `alter type ... add value` cannot be used, because the new value would not be
-- referenceable by the CHECK constraints recreated in the same transaction.

alter table tabular drop constraint if exists tabular_metadata_location_check;
alter table task drop constraint if exists task_warehouse_id_check;
alter table task drop constraint if exists task_entity_check;
alter table task_log drop constraint if exists task_log_warehouse_id_check;
alter table task_log drop constraint if exists task_log_entity_check;
alter table idempotency_record drop constraint if exists idempotency_operation_check;

drop view if exists active_tables;
drop view if exists active_views;
drop view if exists active_tabulars;

drop index if exists task_warehouse_entity_id_queue_idx;
-- Partial index whose predicate names an enum literal, so it blocks the type
-- swap below and has to be rebuilt after it (see 20260813000000).
drop index if exists tabular_warehouse_namespace_created_at_idx;

alter type tabular_type rename to tabular_type_old;
create type tabular_type as enum ('table', 'view', 'generic-table', 'dataset');

alter type entity_type rename to entity_type_old;
create type entity_type as enum (
    'table', 'view', 'project', 'warehouse',
    'namespace', 'role', 'user', 'server', 'generic-table', 'dataset'
);

-- Datasets carry no Iceberg metadata document, so metadata_location stays null,
-- as it may for tables and generic tables.
alter table tabular
    alter column typ type tabular_type using typ::text::tabular_type,
    add constraint tabular_metadata_location_check check (
        (typ = 'view' and metadata_location is not null)
        or typ in ('table', 'generic-table', 'dataset')
    );

alter table task
    alter column entity_type type entity_type using entity_type::text::entity_type,
    add constraint task_warehouse_id_check check (
        (entity_type = 'project' and warehouse_id is null)
        or (entity_type in ('warehouse', 'table', 'view', 'generic-table', 'dataset')
            and warehouse_id is not null)
    ),
    add constraint task_entity_check check (
        (entity_type in ('project', 'warehouse') and entity_id is null and entity_name is null)
        or (entity_type in ('table', 'view', 'generic-table', 'dataset')
            and entity_id is not null and entity_name is not null)
    );

alter table task_log
    alter column entity_type type entity_type using entity_type::text::entity_type,
    add constraint task_log_warehouse_id_check check (
        (entity_type = 'project' and warehouse_id is null)
        or (entity_type in ('warehouse', 'table', 'view', 'generic-table', 'dataset')
            and warehouse_id is not null)
    ),
    add constraint task_log_entity_check check (
        (entity_type in ('project', 'warehouse') and entity_id is null and entity_name is null)
        or (entity_type in ('table', 'view', 'generic-table', 'dataset')
            and entity_id is not null and entity_name is not null)
    );

drop type tabular_type_old;
drop type entity_type_old;

alter type api_endpoints add value if not exists 'dataset-v1-create-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-list-datasets';
alter type api_endpoints add value if not exists 'dataset-v1-load-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-drop-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-list-dataset-refs';
alter type api_endpoints add value if not exists 'dataset-v1-create-dataset-ref';
alter type api_endpoints add value if not exists 'dataset-v1-move-dataset-ref';
alter type api_endpoints add value if not exists 'dataset-v1-delete-dataset-ref';
alter type api_endpoints add value if not exists 'dataset-v1-list-dataset-files';
alter type api_endpoints add value if not exists 'dataset-v1-diff-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-commit-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-set-dataset-ref-protection';
alter type api_endpoints add value if not exists 'management-v1-get-dataset-actions';
alter type api_endpoints add value if not exists 'management-v1-get-dataset-protection';
alter type api_endpoints add value if not exists 'management-v1-set-dataset-protection';
alter type api_endpoints add value if not exists 'dataset-v1-rename-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-load-dataset-credentials';
alter type api_endpoints add value if not exists 'dataset-v1-import-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-create-dataset-access-grant';
alter type api_endpoints add value if not exists 'dataset-v1-revoke-dataset-access-grant';
alter type api_endpoints add value if not exists 'dataset-v1-sign-dataset-files';
alter type api_endpoints add value if not exists 'dataset-v1-restore-dataset-snapshot';
alter type api_endpoints add value if not exists 'dataset-v1-update-dataset-settings';
alter type api_endpoints add value if not exists 'dataset-v1-get-dataset-snapshot-materialization';
alter type api_endpoints add value if not exists 'dataset-v1-expire-dataset-snapshot';
alter type api_endpoints add value if not exists 'management-v1-set-dataset-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-dataset-tag';
alter type api_endpoints add value if not exists 'management-v1-list-dataset-tags';
alter type api_endpoints add value if not exists 'management-v1-list-dataset-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-dataset-grants';
alter type api_endpoints add value if not exists 'management-v1-get-dataset-grantable-privileges';

-- managed: Lakekeeper owns an exclusive prefix (purge and GC permitted).
-- imported: the prefix is borrowed; Lakekeeper never mutates or deletes it.
create type dataset_ownership as enum ('managed', 'imported');

create table dataset (
    warehouse_id uuid        not null,
    dataset_id   uuid        not null,
    ownership    dataset_ownership not null,
    -- allowed_content_types / max_file_size. Enforced at commit, never at PUT.
    constraints  jsonb,
    -- The dataset's own retention policy; null follows the warehouse's.
    retention    jsonb,
    -- Set once a commit records a file apart from its storage path; an import
    -- walks the whole manifest for such files only while it is.
    has_renamed_files boolean not null default false,
    version      bigint      not null default 0,
    created_at   timestamptz not null default now(),
    updated_at   timestamptz,
    primary key (warehouse_id, dataset_id),
    foreign key (warehouse_id, dataset_id)
        references tabular (warehouse_id, tabular_id) on delete cascade
);
select trigger_updated_at_and_version_if_distinct('"dataset"');

-- An immutable dataset version. `staging` rows are mid-insert and referenced by
-- no ref; the compare-and-set ref move is the visibility flip. `expired` rows are
-- retention's tombstones: complete, but hidden until restored or purged. Every
-- snapshot-id input must reject non-active snapshots, or a reader can observe a
-- torn manifest or one retention has let go.
create type dataset_snapshot_status as enum ('staging', 'active', 'expired');

create table dataset_snapshot (
    warehouse_id               uuid                    not null,
    dataset_id                 uuid                    not null,
    snapshot_id                uuid                    not null,
    parent_snapshot_id         uuid,
    -- The dataset's location when the snapshot was committed.
    location                   text                    not null,
    status                     dataset_snapshot_status not null default 'staging',
    -- Set when retention expires the snapshot; its rows go at `purge_after`
    -- unless it is restored first.
    expired_at                 timestamptz,
    purge_after                timestamptz,
    -- Full-manifest checkpoint: every row restated with change = 'added'.
    is_checkpoint              boolean                 not null default false,
    -- Correlation keys (pipeline_run_id, code_commit). The lineage hook.
    summary                    jsonb,
    -- The `Idempotency-Key` of the commit that published it, so a retry is
    -- answered with this snapshot, wherever the branch has moved since, and the
    -- files the commit's constraints left out, which the retry reports too.
    idempotency_key            uuid,
    skipped_files              jsonb,
    -- What the last materialization check found: files whose object a listing of
    -- the prefix did not show, and files whose object was written again since.
    -- Null until an import checks it.
    missing_files              bigint,
    changed_files              bigint,
    materialization_checked_at timestamptz,
    created_at                 timestamptz             not null default now(),
    primary key (warehouse_id, snapshot_id),
    foreign key (warehouse_id, dataset_id)
        references dataset (warehouse_id, dataset_id) on delete cascade,
    -- Restrict, not cascade: history must not disappear from under a descendant.
    foreign key (warehouse_id, parent_snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete restrict
);
-- The dataset cascade, and the staging sweep's age scan.
create index dataset_snapshot_dataset_created_at_idx
    on dataset_snapshot (warehouse_id, dataset_id, created_at, snapshot_id);
-- Proves "no child snapshot" for the `on delete restrict` above. Without it,
-- dropping a dataset scans the table once per cascaded snapshot.
create index dataset_snapshot_parent_idx
    on dataset_snapshot (warehouse_id, parent_snapshot_id)
    where parent_snapshot_id is not null;
-- Serves a commit's replay. Unique as the key is: one key, one request.
create unique index dataset_snapshot_idempotency_key_idx
    on dataset_snapshot (warehouse_id, idempotency_key)
    where idempotency_key is not null;

create type dataset_ref_type as enum ('branch', 'tag');

-- A named pointer to a snapshot: a branch moves, a tag is fixed at creation.
-- snapshot_id is the compare-and-set target of every commit and fast-forward.
create table dataset_ref (
    warehouse_id uuid             not null,
    dataset_id   uuid             not null,
    name         text             not null,
    typ          dataset_ref_type not null,
    snapshot_id  uuid,
    -- A structural rule, not authorization: refuses direct commits, resets and deletion.
    -- Changes reach a protected branch by fast-forward only.
    protected    boolean          not null default false,
    created_at   timestamptz      not null default now(),
    updated_at   timestamptz,
    primary key (warehouse_id, dataset_id, name),
    foreign key (warehouse_id, dataset_id)
        references dataset (warehouse_id, dataset_id) on delete cascade,
    foreign key (warehouse_id, snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete restrict,
    -- Only a branch on a dataset with no commits yet may point at nothing.
    constraint dataset_ref_tag_has_snapshot check (typ = 'branch' or snapshot_id is not null)
);
select trigger_updated_at('dataset_ref');
-- Proves "no ref points here" for the `on delete restrict` above and for the
-- staging sweep. Without it, each snapshot delete scans the warehouse's refs.
create index dataset_ref_snapshot_idx on dataset_ref (warehouse_id, snapshot_id);

-- One row per file per snapshot, stored as a delta against the parent snapshot.
-- Path-based, not content-addressed: two identical files are two rows.
create type dataset_manifest_change as enum ('added', 'removed', 'modified');

-- Hash-partitioned on snapshot_id. Every read and delete is scoped to one
-- snapshot, so each touches a single partition -- reconstruction probes one per
-- ancestor, pruned at run time -- and indexes and vacuum work per partition.
--
-- The owning dataset is recovered by joining dataset_snapshot; storing it here
-- too would let the two disagree.
create table dataset_manifest_entry (
    warehouse_id  uuid                    not null,
    snapshot_id   uuid                    not null,
    -- What users see, filter and address. Relative to the snapshot's location.
    -- Byte-order collation: keys sort as object stores list them, whatever the
    -- database's default, so an import can merge a listing against a manifest.
    logical_key   text collate "C"        not null,
    -- Where the bytes are: a path relative to the dataset's location, or a full
    -- URI inside it. Defaults to the logical key.
    physical_path text                    not null,
    change        dataset_manifest_change not null,
    etag          text,
    size          bigint,
    -- Advisory: declared, or inferred from the extension on import.
    content_type  text,
    -- Authoritative where present: an etag is not a content hash for multipart or
    -- SSE-KMS objects.
    checksum      text,
    -- On versioned buckets, pins the exact object version a tag resolves to.
    version_id    text,
    last_modified timestamptz,
    primary key (warehouse_id, snapshot_id, logical_key),
    -- Cascade: purging a snapshot must take its manifest with it.
    foreign key (warehouse_id, snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete cascade
) partition by hash (snapshot_id);

-- Commits insert in bulk and dropping a dataset deletes in bulk, so the default
-- 20% dead-tuple threshold is far too lax. Set per partition: a partitioned table
-- has no storage of its own.
do $$
begin
    for i in 0..15 loop
        execute format(
            'create table dataset_manifest_entry_p%s partition of dataset_manifest_entry
                 for values with (modulus 16, remainder %s)
                 with (autovacuum_vacuum_scale_factor = 0.01,
                       autovacuum_analyze_scale_factor = 0.01,
                       autovacuum_vacuum_cost_delay = 0)',
            i, i);
    end loop;
end $$;

-- A reader's standing to have one snapshot's files signed. Authorized once when
-- issued; each signing call only checks the row -- live, unexpired, same caller,
-- keys within scope. A row, not a token, so revoking it stops signing at once.
create table dataset_access_grant (
    warehouse_id    uuid        not null,
    grant_id        uuid        not null,
    dataset_id      uuid        not null,
    snapshot_id     uuid        not null,
    -- The ref the grant was issued through, for audit. The snapshot is what binds.
    ref_name        text        not null,
    -- The actor that obtained the grant; only it may use it.
    actor           text        not null,
    -- Narrows signing to files of this content type.
    content_type    text,
    created_at      timestamptz not null default now(),
    expires_at      timestamptz not null,
    revoked_at      timestamptz,
    -- The `Idempotency-Key` of the request that issued it; a retry is answered
    -- with this grant, where issuing another would leave one its caller never saw.
    idempotency_key uuid,
    primary key (warehouse_id, grant_id),
    foreign key (warehouse_id, dataset_id)
        references dataset (warehouse_id, dataset_id) on delete cascade,
    foreign key (warehouse_id, snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete cascade
);

-- Serves the sweep of a dataset's expired grants.
create index dataset_access_grant_expiry_idx
    on dataset_access_grant (warehouse_id, dataset_id, expires_at);
-- Serves the cascade from dataset_snapshot.
create index dataset_access_grant_snapshot_idx
    on dataset_access_grant (warehouse_id, snapshot_id);
-- Serves a grant request's replay.
create unique index dataset_access_grant_idempotency_key_idx
    on dataset_access_grant (warehouse_id, idempotency_key)
    where idempotency_key is not null;

-- What a running import listed, keyed by the staging snapshot it fills. Storage
-- lists in its own order -- ADLS depth-first -- so the listing is spooled here and
-- read back in key order, merged against the branch's manifest at a fixed memory
-- cost, then consulted by a materialization check. Scratch: cleared once the import
-- publishes, and gone with the staging snapshot if it does not.
-- Unlogged, as a crash loses the import that wrote it anyway and a listing-sized
-- write should cost no WAL.
create unlogged table dataset_import_listing (
    warehouse_id  uuid             not null,
    snapshot_id   uuid             not null,
    logical_key   text collate "C" not null,
    physical_path text             not null,
    size          bigint,
    last_modified timestamptz,
    etag          text,
    version_id    text,
    -- A file named apart from its storage path already points at this object, so
    -- the import does not register it again under its storage path.
    referenced    boolean          not null default false,
    primary key (warehouse_id, snapshot_id, logical_key),
    foreign key (warehouse_id, snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete cascade
);

create type dataset_degraded_file_problem as enum ('missing', 'changed');

-- The files the last materialization check of a snapshot found missing or
-- written again, the first of them only: a wiped prefix would otherwise restate
-- the manifest.
create table dataset_degraded_file (
    warehouse_id  uuid                          not null,
    snapshot_id   uuid                          not null,
    logical_key   text collate "C"              not null,
    physical_path text                          not null,
    problem       dataset_degraded_file_problem not null,
    primary key (warehouse_id, snapshot_id, logical_key),
    foreign key (warehouse_id, snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete cascade
);

create view active_tabulars as
select t.tabular_id,
       t.namespace_id,
       t.name,
       t.typ,
       t.metadata_location,
       t.fs_protocol,
       t.fs_location,
       t.warehouse_id,
       t.tabular_namespace_name as namespace_name
  from tabular t
  join warehouse w
    on t.warehouse_id = w.warehouse_id
   and w.status = 'active'::warehouse_status;

create view active_tables as
select tabular_id as table_id,
       namespace_id,
       warehouse_id,
       name,
       metadata_location,
       fs_protocol,
       fs_location
  from active_tabulars t
 where typ = 'table'::tabular_type;

create view active_views as
select tabular_id as view_id,
       namespace_id,
       warehouse_id,
       name,
       metadata_location,
       fs_protocol,
       fs_location
  from active_tabulars t
 where typ = 'view'::tabular_type;

create index task_warehouse_entity_id_queue_idx
    on task (warehouse_id, entity_id, queue_name)
    where entity_type in ('table', 'view', 'generic-table', 'dataset');

-- Rebuilt with 'dataset' in the predicate: a dataset has no metadata_location, so
-- without it listing a namespace holding datasets falls back to a seq scan --
-- the regression 20260813000000 was written to fix.
-- Keep in sync with the `include_active` branch of `list_tabulars` in
-- src/tabular/mod.rs.
create index tabular_warehouse_namespace_created_at_idx on tabular (
    warehouse_id,
    namespace_id,
    created_at,
    tabular_id
)
where
    deleted_at is null
    and (
        metadata_location is not null
        or typ in ('generic-table', 'dataset')
    );

alter table idempotency_record add constraint idempotency_operation_check check (
    operation::text in (
        'catalog-v1-create-namespace',
        'catalog-v1-update-namespace-properties',
        'catalog-v1-drop-namespace',
        'catalog-v1-create-table',
        'catalog-v1-update-table',
        'catalog-v1-drop-table',
        'catalog-v1-rename-table',
        'catalog-v1-register-table',
        'catalog-v1-create-view',
        'catalog-v1-replace-view',
        'catalog-v1-drop-view',
        'catalog-v1-rename-view',
        'catalog-v1-commit-transaction',
        'generic-table-v1-create-generic-table',
        'generic-table-v1-drop-generic-table',
        'generic-table-v1-rename-generic-table',
        'dataset-v1-create-dataset',
        'dataset-v1-drop-dataset',
        'dataset-v1-rename-dataset',
        'dataset-v1-commit-dataset',
        'dataset-v1-create-dataset-ref',
        'dataset-v1-move-dataset-ref',
        'dataset-v1-delete-dataset-ref',
        'dataset-v1-create-dataset-access-grant'
    )
);
