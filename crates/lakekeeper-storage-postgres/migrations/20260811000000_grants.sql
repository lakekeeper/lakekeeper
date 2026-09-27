-- Grants: direct `principal -> privilege -> resource` permissions, one row per grant.
-- Authoritative only when the authorization backend does not manage grants itself.
-- Referential and structural integrity is enforced here; which privileges exist is
-- decided by the authorization backend, not by this schema.

-- Which kind of principal holds a grant.
create type grant_principal_type as enum ('user', 'role');

-- Which kind of resource a grant is held on. A `server` grant is not scoped to a
-- project; every other kind is.
--
-- Tables, views and generic tables share the single `tabular` value: the tabular table
-- already records which of the three a given id is, so storing it again here would let
-- a grant disagree with it. Readers recover the distinction by joining tabular.
create type grant_resource_type as enum
    ('server', 'project', 'warehouse', 'namespace', 'tabular', 'tag');

create table grant_assignment (
    -- Surrogate key, for ordering and pagination only. A grant's identity is its
    -- (principal, privilege, resource) triple; see grant_unique.
    grant_id          uuid        primary key default uuid_generate_v1mc(),

    -- Which of the principal columns below is set.
    principal_type    grant_principal_type not null,
    user_id           text,              -- principal_type='user'
    role_id           uuid,              -- principal_type='role'

    -- Privilege name from the authorization backend's vocabulary: stored and
    -- returned verbatim.
    privilege         text        not null,

    resource_type     grant_resource_type not null,
    -- Exactly one resource shape is populated, keyed by resource_type; a `server`
    -- grant populates none of them. grant_resource_target enforces that, and the
    -- composite foreign keys below depend on it: their MATCH SIMPLE semantics skip the
    -- check entirely if any of their columns is null, so a row with namespace_id or
    -- tabular_id set but warehouse_id null would reference nothing at all.
    project_id        text,              -- resource_type='project'
    warehouse_id      uuid,              -- warehouse, namespace and tabular grants
    namespace_id      uuid,              -- resource_type='namespace'
    tabular_id        uuid,              -- resource_type='tabular'
    tag_definition_id uuid,              -- resource_type='tag'

    -- No grantor column: attribution is an audit concern, not state.
    created_at        timestamptz not null default now(),

    -- Guard rail against an unbounded value, not validation: which privileges are
    -- legal is decided by the authorization backend and checked before the write.
    constraint grant_privilege_length check (char_length(privilege) between 1 and 256),

    -- Users are soft-deleted, so this cascade only fires on a hard delete. A deleted
    -- user keeps their id and can return, so their grants are removed explicitly when
    -- the user is deleted.
    constraint grant_user_fkey      foreign key (user_id)
        references users (id) on delete cascade,
    constraint grant_role_fkey      foreign key (role_id)
        references "role" (id) on delete cascade,
    constraint grant_project_fkey   foreign key (project_id)
        references project (project_id) on delete cascade,
    constraint grant_warehouse_fkey foreign key (warehouse_id)
        references warehouse (warehouse_id) on delete cascade,
    constraint grant_namespace_fkey foreign key (warehouse_id, namespace_id)
        references namespace (warehouse_id, namespace_id) on delete cascade,
    constraint grant_tabular_fkey   foreign key (warehouse_id, tabular_id)
        references tabular (warehouse_id, tabular_id) on delete cascade,
    constraint grant_tag_fkey       foreign key (tag_definition_id)
        references tag_definition (tag_definition_id) on delete cascade,

    -- Exactly one principal column is set, and the discriminator names it.
    constraint grant_principal_shape check (
        (principal_type = 'user' and user_id is not null and role_id is null) or
        (principal_type = 'role' and role_id is not null and user_id is null)),
    -- A tabular is the finest granularity a grant addresses.
    constraint grant_resource_target check (
        (resource_type = 'server'
            and num_nonnulls(project_id, warehouse_id, namespace_id, tabular_id, tag_definition_id) = 0) or
        (resource_type = 'project'
            and project_id is not null
            and num_nonnulls(warehouse_id, namespace_id, tabular_id, tag_definition_id) = 0) or
        (resource_type = 'warehouse'
            and warehouse_id is not null
            and num_nonnulls(project_id, namespace_id, tabular_id, tag_definition_id) = 0) or
        (resource_type = 'namespace'
            and warehouse_id is not null and namespace_id is not null
            and num_nonnulls(project_id, tabular_id, tag_definition_id) = 0) or
        (resource_type = 'tabular'
            and warehouse_id is not null and tabular_id is not null
            and num_nonnulls(project_id, namespace_id, tag_definition_id) = 0) or
        (resource_type = 'tag'
            and tag_definition_id is not null
            and num_nonnulls(project_id, warehouse_id, namespace_id, tabular_id) = 0)),

    -- Grants carry no mutable payload, so the whole row is the identity. That lets a
    -- write be insert-on-conflict-do-nothing and a revoke be a delete, each reporting
    -- exactly what changed. Adding a payload later means rebuilding this key.
    constraint grant_unique unique nulls not distinct
        (principal_type, user_id, role_id, resource_type, privilege,
         project_id, warehouse_id, namespace_id, tabular_id, tag_definition_id)
);

-- One index per access path, each serving that path's listing, its foreign key's
-- ON DELETE CASCADE check, and the keyset order. grant_unique leads with the principal,
-- so it cannot serve any of them.
--
-- Every index carries (created_at, grant_id) last. A single level routinely holds one
-- grant per principal, so without it each page sorts the whole level and paginating
-- costs the level's size squared. Necessary, not sufficient: the planner estimates a
-- keyset bound from the global created_at histogram, so a deep page into a level whose
-- grants are time-clustered can still flip to a sorted bitmap scan.
--
-- Each index that keys off a resource column also proves resource_type — as a column
-- before the keyset, or as its partial predicate — even though the column already
-- implies the type. Every listing filters on resource_type, and an index that cannot
-- prove that equality has it applied as an independent filter instead: the row
-- estimate falls below the page size, the planner stops discounting the ordered scan,
-- and each page bitmap-scans and sorts the whole level — the exact cost the trailing
-- keyset exists to avoid.
--
-- The warehouse-contained levels need three separate indexes: a combined
-- (warehouse_id, namespace_id, tabular_id, resource_type) cannot replace them, because
-- the tabular cascade that fires on every drop has no contiguous range in it, and
-- warehouse-level rows are not isolated by namespace_id IS NULL — tabular grants have a
-- null namespace_id too.
--
-- The warehouse level alone splits its two duties. Its cascade covers every
-- warehouse-contained grant, not just its own level, and an index wide enough for the
-- listing's keyset makes every entry unique — which forfeits btree deduplication and
-- pays for the width on every cascade probe. The bare column deduplicates; the
-- listing gets its own partial keyset index, whose predicate is what proves the
-- resource_type equality.
create index grant_warehouse_cascade_idx on grant_assignment (warehouse_id)
    where warehouse_id is not null;
create index grant_warehouse_idx on grant_assignment (warehouse_id, created_at, grant_id)
    where resource_type = 'warehouse';
create index grant_namespace_idx on grant_assignment
    (warehouse_id, namespace_id, resource_type, created_at, grant_id)
    where namespace_id is not null;
create index grant_tabular_idx on grant_assignment
    (warehouse_id, tabular_id, resource_type, created_at, grant_id)
    where tabular_id is not null;
-- Principal-scoped listing, plus the user and role cascade checks.
create index grant_user_idx on grant_assignment (user_id, created_at, grant_id)
    where user_id is not null;
create index grant_role_idx on grant_assignment (role_id, created_at, grant_id)
    where role_id is not null;
-- Project and tag-definition cascade checks, and their scoped listings. The cascade
-- probes carry no resource_type equality, so these keep the `is not null` predicate
-- and prove the type through the column instead.
create index grant_project_idx on grant_assignment
    (project_id, resource_type, created_at, grant_id)
    where project_id is not null;
create index grant_tag_idx on grant_assignment
    (tag_definition_id, resource_type, created_at, grant_id)
    where tag_definition_id is not null;
-- A server grant populates no resource column, so nothing else can locate one and
-- listing them would otherwise scan in proportion to every grant in the deployment.
create index grant_server_idx on grant_assignment (created_at, grant_id)
    where resource_type = 'server';
-- Deliberately no whole-table (created_at, grant_id) index: the only query shape that
-- would walk one — every grant in a project regardless of principal — has no endpoint.
-- If a project export surface lands, it brings that index with it.

-- Endpoint statistics record the matched route as this enum type, so a route that is
-- not registered here is rejected when its statistics are written.
alter type api_endpoints add value if not exists 'management-v1-list-grants';
alter type api_endpoints add value if not exists 'management-v1-get-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-list-warehouse-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-warehouse-grants';
alter type api_endpoints add value if not exists 'management-v1-list-server-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-server-grants';
alter type api_endpoints add value if not exists 'management-v1-list-project-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-project-grants';
alter type api_endpoints add value if not exists 'management-v1-list-namespace-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-namespace-grants';
alter type api_endpoints add value if not exists 'management-v1-list-tag-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-tag-grants';
alter type api_endpoints add value if not exists 'management-v1-list-table-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-table-grants';
alter type api_endpoints add value if not exists 'management-v1-list-view-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-view-grants';
alter type api_endpoints add value if not exists 'management-v1-list-generic-table-grants';
alter type api_endpoints add value if not exists 'management-v1-apply-generic-table-grants';

-- Tag definitions were the one grantable level with no action-introspection route, so
-- the grant-read gate on them was enforceable but invisible to a console.
alter type api_endpoints add value if not exists 'management-v1-get-tag-actions';

-- "Which privileges may I grant here", one per resource level.
alter type api_endpoints add value if not exists 'management-v1-get-server-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-get-project-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-get-warehouse-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-get-namespace-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-get-table-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-get-view-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-get-generic-table-grantable-privileges';
alter type api_endpoints add value if not exists 'management-v1-get-tag-grantable-privileges';
