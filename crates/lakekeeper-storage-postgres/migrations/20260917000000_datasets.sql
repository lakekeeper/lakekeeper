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
alter type api_endpoints add value if not exists 'dataset-v1-commit-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-set-dataset-ref-protection';
alter type api_endpoints add value if not exists 'management-v1-get-dataset-actions';
alter type api_endpoints add value if not exists 'management-v1-get-dataset-protection';
alter type api_endpoints add value if not exists 'management-v1-set-dataset-protection';
alter type api_endpoints add value if not exists 'dataset-v1-rename-dataset';
alter type api_endpoints add value if not exists 'dataset-v1-load-dataset-credentials';
alter type api_endpoints add value if not exists 'dataset-v1-import-dataset';
alter type api_endpoints add value if not exists 'management-v1-set-dataset-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-dataset-tag';
alter type api_endpoints add value if not exists 'management-v1-list-dataset-tags';
alter type api_endpoints add value if not exists 'management-v1-list-dataset-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-dataset-grants';
alter type api_endpoints add value if not exists 'management-v1-get-dataset-grantable-privileges';

create table dataset (
    warehouse_id uuid        not null,
    dataset_id   uuid        not null,
    -- Snapshots copy this at commit time, so history does not depend on this row.
    location     text        not null,
    -- managed: Lakekeeper owns an exclusive prefix (purge and GC permitted).
    -- imported: the prefix is borrowed; Lakekeeper never mutates or deletes it.
    managed      boolean     not null,
    -- allowed_content_types / max_file_size. Enforced at commit, never at PUT.
    constraints  jsonb,
    version      bigint      not null default 0,
    created_at   timestamptz not null default now(),
    updated_at   timestamptz,
    primary key (warehouse_id, dataset_id),
    foreign key (warehouse_id, dataset_id)
        references tabular (warehouse_id, tabular_id) on delete cascade
);
select trigger_updated_at_and_version_if_distinct('"dataset"');

-- An immutable dataset version. `staging` rows are mid-insert and referenced by
-- no ref; the compare-and-set ref move is the visibility flip. Every snapshot-id
-- input must reject non-active snapshots, or a reader can observe a torn
-- manifest.
create type dataset_snapshot_status as enum ('staging', 'active');

create table dataset_snapshot (
    warehouse_id       uuid                    not null,
    dataset_id         uuid                    not null,
    snapshot_id        uuid                    not null,
    parent_snapshot_id uuid,
    -- Copied from dataset.location at commit time. Physical resolution is
    -- profile.base + snapshot.location + logical_key.
    location           text                    not null,
    status             dataset_snapshot_status not null default 'staging',
    -- Full-manifest checkpoint: every row restated with change = 'added'.
    is_checkpoint      boolean                 not null default false,
    -- Correlation keys (pipeline_run_id, code_commit). The lineage hook.
    summary            jsonb,
    created_at         timestamptz             not null default now(),
    primary key (warehouse_id, snapshot_id),
    foreign key (warehouse_id, dataset_id)
        references dataset (warehouse_id, dataset_id) on delete cascade,
    -- Restrict, not cascade: history must not disappear from under a descendant.
    foreign key (warehouse_id, parent_snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete restrict
);
-- Ref resolution and parent-chain walks (the fast-forward descendant check).
create index dataset_snapshot_dataset_created_at_idx
    on dataset_snapshot (warehouse_id, dataset_id, created_at, snapshot_id);
-- Proves "no child snapshot" for the `on delete restrict` above. Without it,
-- dropping a dataset scans the table once per cascaded snapshot.
create index dataset_snapshot_parent_idx
    on dataset_snapshot (warehouse_id, parent_snapshot_id)
    where parent_snapshot_id is not null;

-- Nearest-checkpoint lookup during manifest reconstruction.
create index dataset_snapshot_checkpoint_idx
    on dataset_snapshot (warehouse_id, dataset_id, created_at)
    where is_checkpoint;

create type dataset_ref_type as enum ('branch', 'tag');

-- A named pointer to a snapshot: a branch moves, a tag is fixed at creation.
-- snapshot_id is the compare-and-set target of every commit and fast-forward.
create table dataset_ref (
    warehouse_id uuid             not null,
    dataset_id   uuid             not null,
    name         text             not null,
    typ          dataset_ref_type not null,
    snapshot_id  uuid,
    -- A structural rule, not authorization: refuses direct commits and deletion.
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

-- One row per file per snapshot, stored as a delta against the parent snapshot.
-- Path-based, not content-addressed: two identical files are two rows.
create type dataset_manifest_change as enum ('added', 'removed', 'modified');

-- Deliberately not partitioned: lookups are snapshot-scoped, which the primary
-- key already serves, and expiry deletes by snapshot age rather than by dataset,
-- so partitions could never be dropped wholesale.
--
-- The owning dataset is recovered by joining dataset_snapshot; storing it here
-- too would let the two disagree.
create table dataset_manifest_entry (
    warehouse_id  uuid                    not null,
    snapshot_id   uuid                    not null,
    -- What users see, filter and address. Relative to the snapshot's location.
    logical_key   text                    not null,
    -- Where the bytes are. Identity mapping for imported data; for managed writes,
    -- data/<plan-uuid>/<logical_key>.
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
);
-- Commits insert in bulk and snapshot expiry deletes in bulk, so the default 20%
-- dead-tuple threshold is far too lax for this table.
alter table dataset_manifest_entry set (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 0
);

-- Serves the ?content_type= filter on top of the snapshot-scoped read that the
-- primary key already covers.
create index dataset_manifest_entry_content_type_idx
    on dataset_manifest_entry (warehouse_id, snapshot_id, content_type);

-- Per-file annotations, versioned like manifest rows, so a tag pins files and
-- their labels at the same instant. Table only; the API is a later milestone.
-- 'user' is caller-writable, 'external.*' and 'sys.*' are Lakekeeper's.
create table dataset_file_annotation (
    warehouse_id uuid        not null,
    snapshot_id  uuid        not null,
    logical_key  text        not null,
    namespace    text        not null,
    annotations  jsonb       not null,
    created_at   timestamptz not null default now(),
    primary key (warehouse_id, snapshot_id, logical_key, namespace),
    foreign key (warehouse_id, snapshot_id)
        references dataset_snapshot (warehouse_id, snapshot_id) on delete cascade,
    constraint dataset_file_annotation_namespace_check
        check (namespace = 'user' or namespace like 'external.%' or namespace like 'sys.%')
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
        'dataset-v1-commit-dataset'
    )
);
