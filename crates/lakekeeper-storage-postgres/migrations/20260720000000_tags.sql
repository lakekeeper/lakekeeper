-- Governance tags: project-scoped vocabulary (tag_definition), optional allowed
-- values, and per-target attachments (tag). Change history goes to the event/audit
-- system, not a table here.
-- Referential/structural integrity is enforced here; value validation (name
-- format, scope, allowed-values, same-project) is enforced in the catalog store.
-- Constraints are named so the store can map violations to typed errors.

-- Server-stamped provenance. Only 'manual' (public ApplyTag) for now; automated
-- producers (classification, external-catalog sync) add their value when added.
create type tag_source as enum ('manual');

-- Whether a definition's value is absent (marker), arbitrary (free_text), or
-- drawn from tag_allowed_value (enumerated). Explicit, never inferred.
create type tag_value_kind as enum ('marker', 'free_text', 'enumerated');

-- Surrogate id; children reference it, never the name (so rename is O(1) and
-- name comparisons stay off the FK path). Case-insensitive uniqueness via lower(name).
create table tag_definition (
    tag_definition_id uuid           primary key default uuid_generate_v1mc(),
    project_id        text           not null,
    name              text           not null,
    description       text,
    scope             text[]         not null,   -- target types; element values validated in Rust
    value_kind        tag_value_kind not null,   -- marker | free_text | enumerated (explicit)
    created_at        timestamptz    not null default now(),
    updated_at        timestamptz,
    constraint tag_definition_project_id_fkey foreign key (project_id)
        references project (project_id) on delete cascade,
    constraint tag_definition_scope_not_empty check (cardinality(scope) > 0)
);
create unique index tag_definition_name_idx on tag_definition (project_id, lower(name));
select trigger_updated_at('tag_definition');

-- Permitted values for an 'enumerated' definition (one row per value).
create table tag_allowed_value (
    tag_definition_id uuid        not null,
    value             text        not null,
    created_at        timestamptz not null default now(),
    primary key (tag_definition_id, value),
    constraint tag_allowed_value_definition_fkey foreign key (tag_definition_id)
        references tag_definition (tag_definition_id) on delete cascade
);

-- warehouse_id is always set (every target is under a warehouse) and leads the
-- composite FKs, making the MATCH SIMPLE FKs safe. Target type = which of
-- namespace_id / tabular_id / field_id is set (none => warehouse).
-- tabular_id covers table/view/generic-table and future tabular subtypes.
create table tag (
    tag_id            uuid        primary key default uuid_generate_v1mc(),
    tag_definition_id uuid        not null,
    warehouse_id      uuid        not null,
    namespace_id      uuid,
    tabular_id        uuid,
    field_id          integer,
    value             text,
    source            tag_source  not null,
    created_at        timestamptz not null default now(),
    updated_at        timestamptz,
    constraint tag_definition_id_fkey foreign key (tag_definition_id)
        references tag_definition (tag_definition_id) on delete restrict,
    constraint tag_warehouse_id_fkey foreign key (warehouse_id)
        references warehouse (warehouse_id) on delete cascade,
    constraint tag_namespace_fkey foreign key (warehouse_id, namespace_id)
        references namespace (warehouse_id, namespace_id) on delete cascade,
    constraint tag_tabular_fkey foreign key (warehouse_id, tabular_id)
        references tabular (warehouse_id, tabular_id) on delete cascade,
    -- Column integrity: field_id must name a live field of the tabular. MATCH SIMPLE skips this
    -- when field_id is null (warehouse/namespace/table tags), enforces it only for column tags.
    -- References the tabular_field spine, which survives schema evolution and is reaped only when
    -- the field leaves the last schema version -- so a column tag outlives ordinary commits and
    -- cascades away exactly when the column truly disappears.
    constraint tag_field_fkey foreign key (warehouse_id, tabular_id, field_id)
        references tabular_field (warehouse_id, tabular_id, field_id) on delete cascade,
    constraint tag_single_target
        check ((namespace_id is not null)::int + (tabular_id is not null)::int <= 1),
    constraint tag_field_requires_tabular
        check (field_id is null or tabular_id is not null),
    constraint tag_unique_target_definition_source unique nulls not distinct
        (warehouse_id, namespace_id, tabular_id, field_id, tag_definition_id, source)
);
select trigger_updated_at('tag');

-- tabular_id sits behind namespace_id in the unique index, so a dedicated index
-- is needed for tabular/column lookups and the tabular ON DELETE CASCADE. Warehouse
-- and namespace access paths are served by the unique index (it leads with
-- warehouse_id, namespace_id)
create index tag_tabular_idx on tag (warehouse_id, tabular_id) where tabular_id is not null;
-- serves the tag_definition_id_fkey ON DELETE RESTRICT check and reverse lookup by definition + value
create index tag_definition_idx on tag (tag_definition_id, value);

-- Register the tag management endpoints in the api_endpoints enum (endpoint
-- statistics). Added here, not used in this migration, so the in-txn ADD VALUE
-- is safe.
alter type api_endpoints add value if not exists 'management-v1-create-tag-definition';
alter type api_endpoints add value if not exists 'management-v1-list-tag-definitions';
alter type api_endpoints add value if not exists 'management-v1-get-tag-definition';
alter type api_endpoints add value if not exists 'management-v1-update-tag-definition';
alter type api_endpoints add value if not exists 'management-v1-delete-tag-definition';
alter type api_endpoints add value if not exists 'management-v1-set-warehouse-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-warehouse-tag';
alter type api_endpoints add value if not exists 'management-v1-list-warehouse-tags';
alter type api_endpoints add value if not exists 'management-v1-set-namespace-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-namespace-tag';
alter type api_endpoints add value if not exists 'management-v1-list-namespace-tags';
alter type api_endpoints add value if not exists 'management-v1-set-table-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-table-tag';
alter type api_endpoints add value if not exists 'management-v1-list-table-tags';
alter type api_endpoints add value if not exists 'management-v1-set-table-column-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-table-column-tag';
alter type api_endpoints add value if not exists 'management-v1-list-table-column-tags';
alter type api_endpoints add value if not exists 'management-v1-set-view-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-view-tag';
alter type api_endpoints add value if not exists 'management-v1-list-view-tags';
alter type api_endpoints add value if not exists 'management-v1-set-generic-table-tag';
alter type api_endpoints add value if not exists 'management-v1-delete-generic-table-tag';
alter type api_endpoints add value if not exists 'management-v1-list-generic-table-tags';
