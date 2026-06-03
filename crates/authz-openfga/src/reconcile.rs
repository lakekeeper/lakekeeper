//! Reconcile structural OpenFGA tuples against the catalog.
//!
//! Two public entry points, both generic over the [`CatalogStore`] backend:
//!
//! * [`rebuild_hierarchy_tuples_from_catalog`] — additive only. Reads the
//!   catalog, idempotently writes any hierarchy tuple the catalog implies.
//!   Never deletes. Safe under concurrent writes (no lock required).
//! * [`reconcile_hierarchy_tuples_from_catalog`] — additive **plus** drift
//!   deletion. The caller passes a lock guard to serialize concurrent
//!   reconciles; this module is agnostic to which lock primitive backs it.
//!   For Postgres deployments use [`lakekeeper_storage_postgres::PostgresAdvisoryLock`]
//!   with [`RECONCILE_LOCK_KEY`]; single-replica deployments may pass `()`.
//!
//! The shape of every emitted tuple comes from the `hierarchy_tuples_for_*`
//! helpers in [`crate::tuples`] — the same helpers the authorizer's
//! `create_*` methods use.
//!
//! # Deletion semantics
//!
//! A tuple `(A, R, B)` is a deletion candidate iff **all** of:
//!
//! 1. `R` is a managed structural relation (parent / child / project /
//!    server / warehouse / namespace).
//! 2. Both `user_type(A)` and `object_type(B)` are in the *managed* set:
//!    `server, project, warehouse, namespace, lakekeeper_table,
//!    lakekeeper_view, role`. `user`, retired pre-v4 `table`/`view`, and
//!    migration bookkeeping types are left alone.
//! 3. At least one of `A` or `B` exists in the catalog. Tuples whose
//!    endpoints are *both* unknown are left alone — no anchor to interpret.
//! 4. The catalog state contradicts the relationship.
//!
//! Ownership tuples (`actor -[ownership]-> *`), grants, role assignments,
//! and bootstrap admin tuples are **never** touched.
//!
//! # Operational notes (deletion mode)
//!
//! * The caller-provided lock guard serializes concurrent reconciles for
//!   the chosen backend. It does **not** block API writes — operators
//!   should run during low-traffic windows.
//! * Total runtime scales with OpenFGA store size at ~80k tuples/sec for
//!   the global Read scan, plus catalog read time.
//!
//! ## Concurrency model — eventual consistency
//!
//! This entry point does **not** stop API writes. The catalog snapshot is
//! built before the OpenFGA walk; concurrent renames or creates between
//! those two reads can cause **transient** inconsistencies:
//!
//! * The catalog snapshot may be missing edges that the API committed
//!   after the snapshot — reconcile won't add them this run.
//! * The catalog snapshot may have edges the API removed after the
//!   snapshot — reconcile may issue a wrong-direction delete.
//! * A rename racing with the walk can cause the diff to add the old
//!   edge back (from the snapshot) while the new edge is also present.
//!
//! All of these self-heal on the **next** reconcile run, which sees the
//! up-to-date catalog. If strict consistency during the run is required,
//! quiesce API writes externally for the duration.
//!
//! A future revision will add a shared advisory lock on the authorizer's
//! write paths so reconcile can drain in-flight writes and block new ones
//! for the duration. That makes reconcile strictly correct at the cost of
//! a brief API block (~30-60s on a large store).

use std::collections::{BTreeMap, HashMap, HashSet};

use lakekeeper::{
    ProjectId, WarehouseId,
    api::iceberg::v1::{ListNamespacesQuery, NamespaceIdent, PageToken, PaginationQuery},
    service::{
        ArcProjectId, CatalogListRolesByIdFilter, CatalogNamespaceOps, CatalogRoleOps,
        CatalogStore, CatalogTabularOps, CatalogWarehouseOps, GenericTableId, NamespaceId,
        ServerId, TableId, TabularId, TabularListFlags, Transaction, ViewId,
        authz::NamespaceParent, maintenance::MaintenanceLockGuard,
    },
};
use openfga_client::client::{
    BasicOpenFgaClient, ConsistencyPreference, TupleKey, TupleKeyWithoutCondition, WriteOptions,
};

use crate::{
    FgaType,
    entities::OpenFgaEntity,
    tuples::{
        hierarchy_tuples_for_generic_table, hierarchy_tuples_for_namespace,
        hierarchy_tuples_for_project, hierarchy_tuples_for_role, hierarchy_tuples_for_table,
        hierarchy_tuples_for_view, hierarchy_tuples_for_warehouse,
    },
};

/// OpenFGA enforces a limit of 100 tuples per Write RPC.
const WRITE_BATCH_SIZE: usize = 100;
/// OpenFGA hard-caps Read page size at 100 (proto-level).
const READ_PAGE_SIZE: i32 = 100;
/// Lock scope for OpenFGA reconcile-with-deletion. Pass this to the
/// backend's lock primitive (e.g.
/// `lakekeeper_storage_postgres::PostgresAdvisoryLock::try_acquire`)
/// when calling [`reconcile_hierarchy_tuples_from_catalog`] in
/// `AddMissingAndDeleteDrift` mode.
///
/// **Contract identifier only.** This key is the agreed-upon namespace
/// between callers of `reconcile_hierarchy_tuples_from_catalog` and the
/// lock backend; do not reuse it for unrelated locks. New maintenance
/// flows should pick their own distinct `i64`.
pub const RECONCILE_LOCK_KEY: i64 = 0x5f8e_2d63_a4b1_00ff;

// ============================================================================
// Public types
// ============================================================================

/// Selects reconcile semantics on
/// [`reconcile_hierarchy_tuples_from_catalog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileMode {
    /// Add missing hierarchy edges, never delete. Equivalent to
    /// [`rebuild_hierarchy_tuples_from_catalog`]. The advisory lock is
    /// still acquired (so a concurrent delete-mode run can't race), but
    /// nothing is deleted.
    AddMissingOnly,
    /// Add missing + delete drift. See module docs for full semantics.
    AddMissingAndDeleteDrift,
}

/// Report returned from reconcile entry points.
///
/// `tuples_submitted` is an **upper bound** on tuples actually persisted
/// because writes are idempotent and OpenFGA does not return a count of
/// duplicates. `tuples_deleted` is exact.
///
/// In `dry_run` mode the same fields describe what *would* have been
/// written or deleted; no OpenFGA mutation occurs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub dry_run: bool,
    pub tuples_submitted: u64,
    pub write_requests: u64,
    pub tuples_deleted: u64,
    pub delete_requests: u64,
    /// Tuples seen in OpenFGA whose `(user_type, relation, object_type)` is
    /// not a managed structural triple — bootstrap, ownership, grants, model-
    /// version bookkeeping, retired pre-v4 types, etc. Skipped per policy.
    pub tuples_ignored_unmanaged: u64,
    /// Tuples seen in OpenFGA where both endpoints are unknown to the
    /// catalog (no anchor for cleanup). Skipped per policy.
    pub tuples_ignored_orphan: u64,
    pub per_type: BTreeMap<&'static str, u64>,
}

impl ReconcileReport {
    fn record_write(&mut self, type_tag: &'static str, n: usize) {
        let n = n as u64;
        self.tuples_submitted += n;
        *self.per_type.entry(type_tag).or_insert(0) += n;
    }
}

/// Backwards-compatible alias.
pub type RebuildReport = ReconcileReport;

// ============================================================================
// Public entry: additive only (generic)
// ============================================================================

/// Add any structural hierarchy tuples the catalog implies but OpenFGA is
/// missing. Never deletes. Generic over the catalog backend.
///
/// When `dry_run` is true, the OpenFGA writes are skipped but the report
/// reflects what *would* have been written.
///
/// See module docs.
///
/// # Errors
/// * Catalog read or OpenFGA write fails.
pub async fn rebuild_hierarchy_tuples_from_catalog<C>(
    catalog_state: C::State,
    sink: &BasicOpenFgaClient,
    server_id: ServerId,
    dry_run: bool,
) -> anyhow::Result<ReconcileReport>
where
    C: CatalogStore,
{
    tracing::info!("rebuild (additive): starting for server {server_id} (dry_run={dry_run})");
    let mut report = ReconcileReport {
        dry_run,
        ..ReconcileReport::default()
    };
    let idx = CatalogIndex::build::<C>(&catalog_state, server_id).await?;
    log_index(&idx);
    write_missing_from_index(&idx, sink, &mut report, dry_run).await?;
    log_done(&report, "rebuild");
    Ok(report)
}

// ============================================================================
// Public entry: additive + delete drift
// ============================================================================

/// Reconcile structural tuples against the catalog, with optional drift
/// deletion. Generic over the catalog backend.
///
/// The caller passes a `lock_guard` that it owns for the duration of the
/// call; this module never inspects it and the guard drops at function
/// exit. Use it to serialize concurrent reconciles — e.g. acquire a
/// Postgres advisory lock with
/// `lakekeeper_storage_postgres::PostgresAdvisoryLock::try_acquire(
/// state, RECONCILE_LOCK_KEY)` and pass the guard in. Single-replica
/// deployments may pass [`lakekeeper::service::maintenance::NoMaintenanceLock`]
/// as an explicit opt-out.
///
/// When `dry_run` is true, no OpenFGA writes or deletes occur — the report
/// counts what *would* have been changed.
///
/// # Errors
/// * Catalog or OpenFGA call fails.
pub async fn reconcile_hierarchy_tuples_from_catalog<C>(
    catalog_state: C::State,
    lock_guard: impl MaintenanceLockGuard,
    sink: &BasicOpenFgaClient,
    server_id: ServerId,
    mode: ReconcileMode,
    dry_run: bool,
) -> anyhow::Result<ReconcileReport>
where
    C: CatalogStore,
{
    let _lock_guard = lock_guard;
    tracing::info!("reconcile: starting (mode={mode:?}, server_id={server_id}, dry_run={dry_run})");
    let mut report = ReconcileReport {
        dry_run,
        ..ReconcileReport::default()
    };

    let idx = CatalogIndex::build::<C>(&catalog_state, server_id).await?;
    log_index(&idx);

    if matches!(mode, ReconcileMode::AddMissingAndDeleteDrift) {
        diff_walk_and_delete(&idx, sink, &mut report, dry_run).await?;
    }

    // Always run the additive pass last so that anything missing (or freshly
    // unknown after a delete) gets added back.
    write_missing_from_index(&idx, sink, &mut report, dry_run).await?;

    log_done(&report, "reconcile");
    Ok(report)
}

// ============================================================================
// Catalog index
// ============================================================================

/// In-memory snapshot of the catalog hierarchy used to answer "should this
/// edge exist?" without further DB queries during the OpenFGA walk.
///
/// Snapshot consistency is best-effort — the build runs across multiple
/// short-lived read transactions. Per-delete revalidation in the deletion
/// path is the safety net.
#[derive(Debug)]
struct CatalogIndex {
    server_id: ServerId,
    projects: HashSet<ProjectId>,
    warehouses: HashMap<WarehouseId, ProjectId>,
    namespaces: HashMap<NamespaceId, NamespaceParent>,
    tables: HashMap<TableId, (WarehouseId, NamespaceId)>,
    views: HashMap<ViewId, (WarehouseId, NamespaceId)>,
    generic_tables: HashMap<GenericTableId, (WarehouseId, NamespaceId)>,
    roles: HashMap<lakekeeper::service::RoleId, ProjectId>,
}

impl CatalogIndex {
    async fn build<C>(state: &C::State, server_id: ServerId) -> anyhow::Result<Self>
    where
        C: CatalogStore,
    {
        let mut idx = CatalogIndex {
            server_id,
            projects: HashSet::new(),
            warehouses: HashMap::new(),
            namespaces: HashMap::new(),
            tables: HashMap::new(),
            views: HashMap::new(),
            generic_tables: HashMap::new(),
            roles: HashMap::new(),
        };

        // Projects
        let mut tx = C::Transaction::begin_read(state.clone())
            .await
            .map_err(|e| anyhow::anyhow!("reconcile: begin_read for projects: {e}"))?;
        let projects = C::list_projects(None, tx.transaction())
            .await
            .map_err(|e| anyhow::anyhow!("reconcile: list_projects: {e}"))?;
        tx.commit()
            .await
            .map_err(|e| anyhow::anyhow!("reconcile: commit projects tx: {e}"))?;

        for project in projects {
            let pid: ProjectId = (*project.project_id).clone();
            idx.projects.insert(pid.clone());

            let warehouses = C::list_warehouses(&pid, None, state.clone())
                .await
                .map_err(|e| anyhow::anyhow!("reconcile: list_warehouses: {e}"))?;
            for w in warehouses {
                idx.warehouses.insert(w.warehouse_id, pid.clone());
                Self::index_warehouse_contents::<C>(state, &mut idx, w.warehouse_id).await?;
            }

            Self::index_project_roles::<C>(state, &mut idx, &pid).await?;
        }

        Ok(idx)
    }

    async fn index_warehouse_contents<C>(
        state: &C::State,
        idx: &mut CatalogIndex,
        warehouse_id: WarehouseId,
    ) -> anyhow::Result<()>
    where
        C: CatalogStore,
    {
        let mut next_parents: Vec<NamespaceIdent> = Vec::new();
        Self::index_namespaces_at_level::<C>(state, idx, warehouse_id, None, &mut next_parents)
            .await?;
        while !next_parents.is_empty() {
            let parents = std::mem::take(&mut next_parents);
            for parent in parents {
                Self::index_namespaces_at_level::<C>(
                    state,
                    idx,
                    warehouse_id,
                    Some(parent),
                    &mut next_parents,
                )
                .await?;
            }
        }

        let mut page_token = PageToken::Empty;
        loop {
            let mut tx = C::Transaction::begin_read(state.clone())
                .await
                .map_err(|e| anyhow::anyhow!("reconcile: begin_read for tabulars: {e}"))?;
            let pagination = PaginationQuery::new(page_token.clone(), None);
            let page = C::list_tabulars(
                warehouse_id,
                None,
                TabularListFlags::all(),
                tx.transaction(),
                None,
                pagination,
            )
            .await
            .map_err(|e| anyhow::anyhow!("reconcile: list_tabulars: {e}"))?;
            tx.commit()
                .await
                .map_err(|e| anyhow::anyhow!("reconcile: commit tabulars tx: {e}"))?;

            let mut last_token: Option<String> = None;
            for (tabular_id, info, token) in page.into_iter_with_page_tokens() {
                last_token = Some(token);
                let ns_id = info.namespace_id();
                match tabular_id {
                    TabularId::Table(t) => {
                        idx.tables.insert(t, (warehouse_id, ns_id));
                    }
                    TabularId::View(v) => {
                        idx.views.insert(v, (warehouse_id, ns_id));
                    }
                    TabularId::GenericTable(g) => {
                        idx.generic_tables.insert(g, (warehouse_id, ns_id));
                    }
                }
            }
            match last_token {
                Some(t) if !t.is_empty() => page_token = PageToken::Present(t),
                _ => break,
            }
        }

        Ok(())
    }

    async fn index_namespaces_at_level<C>(
        state: &C::State,
        idx: &mut CatalogIndex,
        warehouse_id: WarehouseId,
        parent: Option<NamespaceIdent>,
        next_parents: &mut Vec<NamespaceIdent>,
    ) -> anyhow::Result<()>
    where
        C: CatalogStore,
    {
        let mut page_token = PageToken::Empty;
        loop {
            let mut tx = C::Transaction::begin_read(state.clone())
                .await
                .map_err(|e| anyhow::anyhow!("reconcile: begin_read for namespaces: {e}"))?;
            let query = ListNamespacesQuery {
                page_token: page_token.clone(),
                page_size: None,
                parent: parent.clone(),
                return_uuids: true,
                return_protection_status: false,
            };
            let response = C::list_namespaces(warehouse_id, &query, tx.transaction())
                .await
                .map_err(|e| anyhow::anyhow!("reconcile: list_namespaces: {e}"))?;
            tx.commit()
                .await
                .map_err(|e| anyhow::anyhow!("reconcile: commit namespace tx: {e}"))?;

            let mut last_token: Option<String> = None;
            for (ns_id, ns_with_parent, token) in response.namespaces.into_iter_with_page_tokens() {
                last_token = Some(token);
                let parent_ref = match ns_with_parent.parent {
                    Some((parent_id, _)) => NamespaceParent::Namespace(parent_id),
                    None => NamespaceParent::Warehouse(warehouse_id),
                };
                idx.namespaces.insert(ns_id, parent_ref);
                next_parents.push(ns_with_parent.namespace.namespace_ident.clone());
            }
            match last_token {
                Some(t) if !t.is_empty() => page_token = PageToken::Present(t),
                _ => break,
            }
        }
        Ok(())
    }

    async fn index_project_roles<C>(
        state: &C::State,
        idx: &mut CatalogIndex,
        project_id: &ProjectId,
    ) -> anyhow::Result<()>
    where
        C: CatalogStore,
    {
        let mut page_token = PageToken::Empty;
        let project_arc: ArcProjectId = std::sync::Arc::new(project_id.clone());
        loop {
            let pagination = PaginationQuery::new(page_token.clone(), None);
            let response = C::list_roles(
                project_arc.clone(),
                CatalogListRolesByIdFilter::builder().build(),
                pagination,
                state.clone(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("reconcile: list_roles: {e}"))?;
            for role in &response.roles {
                idx.roles.insert(role.id, project_id.clone());
            }
            match response.next_page_token {
                Some(t) if !t.is_empty() => page_token = PageToken::Present(t),
                _ => break,
            }
        }
        Ok(())
    }

    /// Whether the catalog knows the entity at `fga_id`. Returns `None` for
    /// entity types that are not managed by the catalog (e.g. `user:`,
    /// `auth_model_id:`) — caller treats `None` as "no anchor here".
    fn knows(&self, fga_id: &str) -> Option<bool> {
        let (ty, id) = split_fga(fga_id)?;
        match ty {
            FgaType::Server => Some(id == self.server_id.to_string()),
            FgaType::Project => {
                use std::str::FromStr;
                ProjectId::from_str(id)
                    .ok()
                    .map(|p| self.projects.contains(&p))
            }
            FgaType::Warehouse => parse_uuid(id)
                .map(WarehouseId::new)
                .map(|w| self.warehouses.contains_key(&w)),
            FgaType::Namespace => parse_uuid(id)
                .map(NamespaceId::new)
                .map(|n| self.namespaces.contains_key(&n)),
            FgaType::Role => {
                let id = id.split('#').next().unwrap_or(id);
                parse_uuid(id)
                    .map(lakekeeper::service::RoleId::new)
                    .map(|r| self.roles.contains_key(&r))
            }
            FgaType::Table => {
                let (_, t) = id.split_once('/')?;
                parse_uuid(t)
                    .map(TableId::new)
                    .map(|t| self.tables.contains_key(&t))
            }
            FgaType::View => {
                let (_, v) = id.split_once('/')?;
                parse_uuid(v)
                    .map(ViewId::new)
                    .map(|v| self.views.contains_key(&v))
            }
            FgaType::GenericTable => {
                let (_, g) = id.split_once('/')?;
                parse_uuid(g)
                    .map(GenericTableId::new)
                    .map(|g| self.generic_tables.contains_key(&g))
            }
            FgaType::User | FgaType::ModelVersion | FgaType::AuthModelId => None,
        }
    }
}

fn split_fga(s: &str) -> Option<(FgaType, &str)> {
    use std::str::FromStr;
    let (ty, id) = s.split_once(':')?;
    Some((FgaType::from_str(ty).ok()?, id))
}

fn parse_uuid(s: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(s).ok()
}

// ============================================================================
// Structural-edge classification
// ============================================================================

/// Whether the tuple `(user, relation, object)` is one of the structural
/// hierarchy edges this module manages. Returns `false` for ownership,
/// grants, server-admin, model-version bookkeeping, and retired pre-v4
/// types — those are left alone unconditionally.
fn is_managed_structural(tuple: &TupleKey) -> bool {
    let Some((u_ty, _)) = split_fga(&tuple.user) else {
        return false;
    };
    let Some((o_ty, _)) = split_fga(&tuple.object) else {
        return false;
    };
    let r = tuple.relation.as_str();
    matches!(
        (u_ty, r, o_ty),
        (FgaType::Server, "server", FgaType::Project)
            | (
                FgaType::Project,
                "project",
                FgaType::Server | FgaType::Warehouse | FgaType::Role
            )
            | (FgaType::Warehouse, "warehouse", FgaType::Project)
            | (
                FgaType::Warehouse | FgaType::Namespace,
                "parent",
                FgaType::Namespace
            )
            | (FgaType::Namespace, "namespace", FgaType::Warehouse)
            | (
                FgaType::Namespace | FgaType::Table | FgaType::View | FgaType::GenericTable,
                "child",
                FgaType::Namespace
            )
            | (
                FgaType::Namespace,
                "parent",
                FgaType::Table | FgaType::View | FgaType::GenericTable
            )
    )
}

// ============================================================================
// Additive write pass (shared by both modes)
// ============================================================================

async fn write_missing_from_index(
    idx: &CatalogIndex,
    sink: &BasicOpenFgaClient,
    report: &mut ReconcileReport,
    dry_run: bool,
) -> anyhow::Result<()> {
    let server = idx.server_id.to_openfga();
    let mut writer = BatchWriter::new(sink, report, dry_run);

    for project in &idx.projects {
        writer
            .push("project", hierarchy_tuples_for_project(&server, project))
            .await?;
    }
    for (warehouse, project) in &idx.warehouses {
        writer
            .push(
                "warehouse",
                hierarchy_tuples_for_warehouse(project, *warehouse),
            )
            .await?;
    }
    for (ns_id, parent) in &idx.namespaces {
        writer
            .push("namespace", hierarchy_tuples_for_namespace(parent, *ns_id))
            .await?;
    }
    for (tab_id, (wh, ns)) in &idx.tables {
        writer
            .push("table", hierarchy_tuples_for_table(*wh, *tab_id, *ns))
            .await?;
    }
    for (view_id, (wh, ns)) in &idx.views {
        writer
            .push("view", hierarchy_tuples_for_view(*wh, *view_id, *ns))
            .await?;
    }
    for (gt_id, (wh, ns)) in &idx.generic_tables {
        writer
            .push(
                "generic_table",
                hierarchy_tuples_for_generic_table(*wh, *gt_id, *ns),
            )
            .await?;
    }
    for (role_id, project) in &idx.roles {
        writer
            .push("role", hierarchy_tuples_for_role(project, *role_id))
            .await?;
    }
    writer.flush().await
}

// ============================================================================
// Drift-deletion pass
// ============================================================================

async fn diff_walk_and_delete(
    idx: &CatalogIndex,
    sink: &BasicOpenFgaClient,
    report: &mut ReconcileReport,
    dry_run: bool,
) -> anyhow::Result<()> {
    let consistent_sink = sink
        .clone()
        .set_consistency(ConsistencyPreference::HigherConsistency);

    let expected = build_expected_set(idx);

    let mut delete_buffer: Vec<TupleKeyWithoutCondition> = Vec::with_capacity(WRITE_BATCH_SIZE);
    let mut continuation: Option<String> = None;

    loop {
        let resp = consistent_sink
            .read(READ_PAGE_SIZE, None, continuation.clone())
            .await
            .map_err(|e| anyhow::anyhow!("reconcile: openfga Read failed: {e}"))?;
        let resp = resp.into_inner();

        for tuple in resp.tuples.into_iter().filter_map(|t| t.key) {
            if !is_managed_structural(&tuple) {
                report.tuples_ignored_unmanaged += 1;
                continue;
            }
            let key = (
                tuple.user.clone(),
                tuple.relation.clone(),
                tuple.object.clone(),
            );
            if expected.contains(&key) {
                // Catalog says this edge should exist — leave alone. The
                // additive pass will be a no-op for it (idempotent write).
                continue;
            }

            // Tuple is a managed-structural-tuple that catalog state does
            // not endorse. Decide whether to delete based on anchors.
            let user_known = idx.knows(&tuple.user).unwrap_or(false);
            let object_known = idx.knows(&tuple.object).unwrap_or(false);
            if !user_known && !object_known {
                report.tuples_ignored_orphan += 1;
                continue;
            }

            delete_buffer.push(TupleKeyWithoutCondition {
                user: tuple.user,
                relation: tuple.relation,
                object: tuple.object,
            });

            if delete_buffer.len() >= WRITE_BATCH_SIZE {
                let chunk = std::mem::take(&mut delete_buffer);
                flush_deletes(&consistent_sink, chunk, report, dry_run).await?;
            }
        }

        if resp.continuation_token.is_empty() {
            break;
        }
        continuation = Some(resp.continuation_token);
    }
    if !delete_buffer.is_empty() {
        flush_deletes(&consistent_sink, delete_buffer, report, dry_run).await?;
    }
    Ok(())
}

fn build_expected_set(idx: &CatalogIndex) -> HashSet<(String, String, String)> {
    let mut expected: HashSet<(String, String, String)> = HashSet::new();
    let server = idx.server_id.to_openfga();
    let push = |t: TupleKey, e: &mut HashSet<(String, String, String)>| {
        e.insert((t.user, t.relation, t.object));
    };
    for project in &idx.projects {
        for t in hierarchy_tuples_for_project(&server, project) {
            push(t, &mut expected);
        }
    }
    for (warehouse, project) in &idx.warehouses {
        for t in hierarchy_tuples_for_warehouse(project, *warehouse) {
            push(t, &mut expected);
        }
    }
    for (ns_id, parent) in &idx.namespaces {
        for t in hierarchy_tuples_for_namespace(parent, *ns_id) {
            push(t, &mut expected);
        }
    }
    for (tab_id, (wh, ns)) in &idx.tables {
        for t in hierarchy_tuples_for_table(*wh, *tab_id, *ns) {
            push(t, &mut expected);
        }
    }
    for (view_id, (wh, ns)) in &idx.views {
        for t in hierarchy_tuples_for_view(*wh, *view_id, *ns) {
            push(t, &mut expected);
        }
    }
    for (gt_id, (wh, ns)) in &idx.generic_tables {
        for t in hierarchy_tuples_for_generic_table(*wh, *gt_id, *ns) {
            push(t, &mut expected);
        }
    }
    for (role_id, project) in &idx.roles {
        for t in hierarchy_tuples_for_role(project, *role_id) {
            push(t, &mut expected);
        }
    }
    expected
}

async fn flush_deletes(
    sink: &BasicOpenFgaClient,
    chunk: Vec<TupleKeyWithoutCondition>,
    report: &mut ReconcileReport,
    dry_run: bool,
) -> anyhow::Result<()> {
    let n = chunk.len();
    if !dry_run {
        sink.write_with_options(None, Some(chunk), WriteOptions::new_idempotent())
            .await
            .map_err(|e| anyhow::anyhow!("reconcile: openfga delete failed: {e}"))?;
    }
    report.tuples_deleted += n as u64;
    report.delete_requests += 1;
    Ok(())
}

// ============================================================================
// Internal write helper
// ============================================================================

struct BatchWriter<'a> {
    sink: &'a BasicOpenFgaClient,
    buffer: Vec<TupleKey>,
    options: WriteOptions,
    report: &'a mut ReconcileReport,
    dry_run: bool,
}

impl<'a> BatchWriter<'a> {
    fn new(sink: &'a BasicOpenFgaClient, report: &'a mut ReconcileReport, dry_run: bool) -> Self {
        Self {
            sink,
            buffer: Vec::with_capacity(WRITE_BATCH_SIZE),
            options: WriteOptions::new_idempotent(),
            report,
            dry_run,
        }
    }

    async fn push(&mut self, type_tag: &'static str, tuples: Vec<TupleKey>) -> anyhow::Result<()> {
        self.report.record_write(type_tag, tuples.len());
        self.buffer.extend(tuples);
        while self.buffer.len() >= WRITE_BATCH_SIZE {
            let chunk: Vec<TupleKey> = self.buffer.drain(..WRITE_BATCH_SIZE).collect();
            self.write_chunk(chunk).await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.buffer);
        self.write_chunk(chunk).await
    }

    async fn write_chunk(&mut self, chunk: Vec<TupleKey>) -> anyhow::Result<()> {
        if !self.dry_run {
            self.sink
                .write_with_options(Some(chunk), None, self.options)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("reconcile: openfga write_with_options failed: {e}")
                })?;
        }
        self.report.write_requests += 1;
        Ok(())
    }
}

// ============================================================================
// Logging helpers
// ============================================================================

fn log_index(idx: &CatalogIndex) {
    tracing::info!(
        "reconcile: catalog index built — {} projects, {} warehouses, {} namespaces, {} tables, {} views, {} generic_tables, {} roles",
        idx.projects.len(),
        idx.warehouses.len(),
        idx.namespaces.len(),
        idx.tables.len(),
        idx.views.len(),
        idx.generic_tables.len(),
        idx.roles.len()
    );
}

fn log_done(report: &ReconcileReport, fn_label: &str) {
    let label = if report.dry_run { "dry-run" } else { "applied" };
    tracing::info!(
        "{fn_label} ({label}): submitted={}, deleted={}, ignored_unmanaged={}, ignored_orphan={}",
        report.tuples_submitted,
        report.tuples_deleted,
        report.tuples_ignored_unmanaged,
        report.tuples_ignored_orphan,
    );
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod openfga_integration_tests {
    use std::collections::HashSet;

    use lakekeeper::{
        api::{
            CreateNamespaceRequest, RequestMetadata,
            iceberg::v1::{Prefix, namespace::NamespaceService},
            management::v1::{
                ApiServer,
                role::{CreateRoleRequest, Service as RoleService},
            },
        },
        server::{CatalogServer, NAMESPACE_ID_PROPERTY},
        service::{
            NamespaceIdent, RoleId,
            authn::UserId,
            authz::{Authorizer as _, NamespaceParent},
        },
    };
    use lakekeeper_integration_tests::SetupTestCatalog;
    use lakekeeper_storage_postgres::{CatalogState, PostgresAdvisoryLock, PostgresBackend};
    use openfga_client::client::{
        BasicOpenFgaClient, ConsistencyPreference, ReadRequestTupleKey, TupleKey,
        TupleKeyWithoutCondition,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        OpenFGAAuthorizer, migration::tests::authorizer_for_empty_store, relations::TableRelation,
    };

    type TupleIdent = (String, String, String);

    fn ident(t: &TupleKey) -> TupleIdent {
        (t.user.clone(), t.relation.clone(), t.object.clone())
    }

    async fn read_all_tuples(client: &BasicOpenFgaClient) -> HashSet<TupleIdent> {
        client
            .read_all_pages(None::<ReadRequestTupleKey>, 100, 1000)
            .await
            .expect("read_all_pages")
            .into_iter()
            .filter_map(|t| t.key)
            .filter(|k| k.relation != "exists" && k.relation != "openfga_id")
            .map(|k| ident(&k))
            .collect()
    }

    async fn empty_sink_store() -> BasicOpenFgaClient {
        let (_client, authorizer) = authorizer_for_empty_store().await;
        authorizer
            .client
            .clone()
            .set_consistency(ConsistencyPreference::HigherConsistency)
    }

    /// Populate one project + one warehouse + 2 namespaces (root + child) + 1 role.
    /// Tabulars are not created here; their tuple shapes are pinned by the
    /// drift-detector unit tests in `crate::tuples::tests`.
    async fn populate(
        authorizer: &OpenFGAAuthorizer,
        pool: &sqlx::PgPool,
        operator_id: &UserId,
    ) -> (
        lakekeeper_integration_tests::TestWarehouseResponse,
        NamespaceId,
        NamespaceId,
        RoleId,
    ) {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .authorizer(authorizer.clone())
            .user_id(Some(operator_id.clone()))
            .build()
            .setup()
            .await;

        let root_ns = CatalogServer::create_namespace(
            Some(Prefix::from(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::from_vec(vec!["ns_root".to_string()]).unwrap(),
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::test_user(operator_id.clone()),
        )
        .await
        .unwrap();
        let root_ns_id = NamespaceId::from_str_or_internal(
            root_ns
                .properties
                .as_ref()
                .unwrap()
                .get(NAMESPACE_ID_PROPERTY)
                .unwrap(),
        )
        .unwrap();

        let child_ns = CatalogServer::create_namespace(
            Some(Prefix::from(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::from_vec(vec![
                    "ns_root".to_string(),
                    "ns_child".to_string(),
                ])
                .unwrap(),
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::test_user(operator_id.clone()),
        )
        .await
        .unwrap();
        let child_ns_id = NamespaceId::from_str_or_internal(
            child_ns
                .properties
                .as_ref()
                .unwrap()
                .get(NAMESPACE_ID_PROPERTY)
                .unwrap(),
        )
        .unwrap();

        let role = ApiServer::create_role(
            CreateRoleRequest::builder()
                .name(format!("role_{}", Uuid::now_v7()))
                .build(),
            ctx.clone(),
            RequestMetadata::test_user(operator_id.clone()),
        )
        .await
        .unwrap();

        let _ = authorizer;
        (warehouse, root_ns_id, child_ns_id, role.id)
    }

    fn pg_state(pool: &sqlx::PgPool) -> CatalogState {
        CatalogState::from_pools(pool.clone(), pool.clone())
    }

    // ---- Additive (rebuild) regressions ----------------------------------

    #[sqlx::test]
    async fn test_rebuild_is_idempotent(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();
        let _ = populate(&authorizer, &pool, &operator_id).await;

        let report_1 = rebuild_hierarchy_tuples_from_catalog::<PostgresBackend>(
            pg_state(&pool),
            &authorizer.client,
            server_id,
            false,
        )
        .await
        .unwrap();
        let state_after_first = read_all_tuples(&authorizer.client).await;

        let report_2 = rebuild_hierarchy_tuples_from_catalog::<PostgresBackend>(
            pg_state(&pool),
            &authorizer.client,
            server_id,
            false,
        )
        .await
        .unwrap();
        let state_after_second = read_all_tuples(&authorizer.client).await;

        assert!(
            report_1.tuples_submitted > 0,
            "first rebuild should have submitted edges"
        );
        assert_eq!(
            report_1.tuples_submitted, report_2.tuples_submitted,
            "idempotent rebuilds submit the same number of tuples per run"
        );
        assert_eq!(state_after_first, state_after_second);
    }

    #[sqlx::test]
    async fn test_rebuild_preserves_existing_grants_and_ownership(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();
        let _ = populate(&authorizer, &pool, &operator_id).await;

        let bogus_wh = uuid::Uuid::now_v7();
        let bogus_table = uuid::Uuid::now_v7();
        let grant_tuple = TupleKey {
            user: format!("user:oidc~{}", Uuid::now_v7()),
            relation: TableRelation::Select.to_string(),
            object: format!("lakekeeper_table:{bogus_wh}/{bogus_table}"),
            condition: None,
        };
        authorizer
            .client
            .write(Some(vec![grant_tuple.clone()]), None)
            .await
            .unwrap();

        let state_before = read_all_tuples(&authorizer.client).await;
        assert!(state_before.contains(&ident(&grant_tuple)));

        rebuild_hierarchy_tuples_from_catalog::<PostgresBackend>(
            pg_state(&pool),
            &authorizer.client,
            server_id,
            false,
        )
        .await
        .unwrap();

        let state_after = read_all_tuples(&authorizer.client).await;
        for t in &state_before {
            assert!(
                state_after.contains(t),
                "rebuild removed a tuple that it must not touch: {t:?}"
            );
        }
        assert!(state_after.contains(&ident(&grant_tuple)));
    }

    #[sqlx::test]
    async fn test_rebuild_repairs_missing_root_namespace_edge(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();
        let (warehouse, root_ns_id, _child, _role) =
            populate(&authorizer, &pool, &operator_id).await;

        let parent_edge = hierarchy_tuples_for_namespace(
            &NamespaceParent::Warehouse(warehouse.warehouse_id),
            root_ns_id,
        )
        .into_iter()
        .next()
        .unwrap();
        authorizer
            .client
            .write(
                None,
                Some(vec![TupleKeyWithoutCondition {
                    user: parent_edge.user.clone(),
                    relation: parent_edge.relation.clone(),
                    object: parent_edge.object.clone(),
                }]),
            )
            .await
            .unwrap();

        let state_after_delete = read_all_tuples(&authorizer.client).await;
        assert!(!state_after_delete.contains(&ident(&parent_edge)));

        rebuild_hierarchy_tuples_from_catalog::<PostgresBackend>(
            pg_state(&pool),
            &authorizer.client,
            server_id,
            false,
        )
        .await
        .unwrap();

        let state_after_rebuild = read_all_tuples(&authorizer.client).await;
        assert!(state_after_rebuild.contains(&ident(&parent_edge)));
    }

    #[sqlx::test]
    async fn test_rebuild_into_fresh_store_emits_only_hierarchy(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_client, source_authorizer) = authorizer_for_empty_store().await;
        let server_id = source_authorizer.server_id();
        let (warehouse, root_ns_id, child_ns_id, role_id) =
            populate(&source_authorizer, &pool, &operator_id).await;

        let sink = empty_sink_store().await;
        let report = rebuild_hierarchy_tuples_from_catalog::<PostgresBackend>(
            pg_state(&pool),
            &sink,
            server_id,
            false,
        )
        .await
        .unwrap();

        let expect_per_type: BTreeMap<&'static str, u64> = [
            ("project", 2),
            ("warehouse", 2),
            ("namespace", 4),
            ("role", 1),
        ]
        .into_iter()
        .collect();
        assert_eq!(report.per_type, expect_per_type);

        let sink_tuples = read_all_tuples(&sink).await;
        let mut expected: HashSet<TupleIdent> = HashSet::new();
        let server_str = server_id.to_openfga();
        for t in hierarchy_tuples_for_project(&server_str, &warehouse.project_id) {
            expected.insert(ident(&t));
        }
        for t in hierarchy_tuples_for_warehouse(&warehouse.project_id, warehouse.warehouse_id) {
            expected.insert(ident(&t));
        }
        for t in hierarchy_tuples_for_namespace(
            &NamespaceParent::Warehouse(warehouse.warehouse_id),
            root_ns_id,
        ) {
            expected.insert(ident(&t));
        }
        for t in
            hierarchy_tuples_for_namespace(&NamespaceParent::Namespace(root_ns_id), child_ns_id)
        {
            expected.insert(ident(&t));
        }
        for t in hierarchy_tuples_for_role(&warehouse.project_id, role_id) {
            expected.insert(ident(&t));
        }
        for t in &expected {
            assert!(sink_tuples.contains(t), "missing expected tuple {t:?}");
        }
        for (user, relation, object) in &sink_tuples {
            assert!(
                relation != "ownership" && relation != "project_admin",
                "sink contains non-hierarchy tuple {user:?} -[{relation}]-> {object:?}"
            );
        }
    }

    // ---- Reconcile-with-deletion: drift cleanup --------------------------

    #[sqlx::test]
    async fn test_reconcile_deletes_drifted_namespace_parent_edge(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();
        let (warehouse, root_ns_id, _child, _role) =
            populate(&authorizer, &pool, &operator_id).await;

        // Inject a stale namespace→table parent edge as if a table that is
        // tracked elsewhere was renamed to a different namespace. Both endpoints
        // are managed types; root_ns_id is in catalog; the table is not.
        let bogus_table_uuid = Uuid::now_v7();
        let stale_forward = TupleKey {
            user: format!("namespace:{root_ns_id}"),
            relation: "parent".to_string(),
            object: format!(
                "lakekeeper_table:{}/{bogus_table_uuid}",
                warehouse.warehouse_id
            ),
            condition: None,
        };
        let stale_inverse = TupleKey {
            user: stale_forward.object.clone(),
            relation: "child".to_string(),
            object: stale_forward.user.clone(),
            condition: None,
        };
        authorizer
            .client
            .write(
                Some(vec![stale_forward.clone(), stale_inverse.clone()]),
                None,
            )
            .await
            .unwrap();

        let state_before = read_all_tuples(&authorizer.client).await;
        assert!(state_before.contains(&ident(&stale_forward)));
        assert!(state_before.contains(&ident(&stale_inverse)));

        let state = pg_state(&pool);
        let lock = PostgresAdvisoryLock::try_acquire(&state, RECONCILE_LOCK_KEY)
            .await
            .expect("acquire lock")
            .expect("lock free");
        let report = reconcile_hierarchy_tuples_from_catalog::<PostgresBackend>(
            state,
            lock,
            &authorizer.client,
            server_id,
            ReconcileMode::AddMissingAndDeleteDrift,
            false,
        )
        .await
        .unwrap();

        assert!(
            report.tuples_deleted >= 2,
            "expected both forward and inverse drift edges to be deleted; report={report:?}"
        );

        let state_after = read_all_tuples(&authorizer.client).await;
        assert!(
            !state_after.contains(&ident(&stale_forward)),
            "stale forward edge must be deleted"
        );
        assert!(
            !state_after.contains(&ident(&stale_inverse)),
            "stale inverse edge must be deleted"
        );
    }

    #[sqlx::test]
    async fn test_reconcile_preserves_unmanaged_and_orphan_unknown(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();
        let _ = populate(&authorizer, &pool, &operator_id).await;

        // Unmanaged: ownership tuple — never touched.
        let bogus_wh = Uuid::now_v7();
        let bogus_table = Uuid::now_v7();
        let ownership_tuple = TupleKey {
            user: format!("user:oidc~{}", Uuid::now_v7()),
            relation: "ownership".to_string(),
            object: format!("lakekeeper_table:{bogus_wh}/{bogus_table}"),
            condition: None,
        };
        // Both-orphan: structural relation, both endpoints unknown to catalog.
        // Use bogus UUIDs so neither is in the catalog.
        let bogus_wh2 = Uuid::now_v7();
        let bogus_t2 = Uuid::now_v7();
        let bogus_ns = Uuid::now_v7();
        let both_orphan = TupleKey {
            user: format!("namespace:{bogus_ns}"),
            relation: "parent".to_string(),
            object: format!("lakekeeper_table:{bogus_wh2}/{bogus_t2}"),
            condition: None,
        };
        authorizer
            .client
            .write(
                Some(vec![ownership_tuple.clone(), both_orphan.clone()]),
                None,
            )
            .await
            .unwrap();

        let state = pg_state(&pool);
        let lock = PostgresAdvisoryLock::try_acquire(&state, RECONCILE_LOCK_KEY)
            .await
            .expect("acquire lock")
            .expect("lock free");
        reconcile_hierarchy_tuples_from_catalog::<PostgresBackend>(
            state,
            lock,
            &authorizer.client,
            server_id,
            ReconcileMode::AddMissingAndDeleteDrift,
            false,
        )
        .await
        .unwrap();

        let state_after = read_all_tuples(&authorizer.client).await;
        assert!(
            state_after.contains(&ident(&ownership_tuple)),
            "ownership tuple (unmanaged relation) must be preserved"
        );
        assert!(
            state_after.contains(&ident(&both_orphan)),
            "both-endpoints-unknown tuple must be preserved (no anchor)"
        );
    }

    #[sqlx::test]
    async fn test_reconcile_dry_run_reports_without_mutating(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();
        let (warehouse, root_ns_id, _child, _role) =
            populate(&authorizer, &pool, &operator_id).await;

        // Plant drift the same way the deletion test does — a stale parent
        // edge that catalog state contradicts.
        let bogus_table = Uuid::now_v7();
        let stale_forward = TupleKey {
            user: format!("namespace:{root_ns_id}"),
            relation: "parent".to_string(),
            object: format!("lakekeeper_table:{}/{bogus_table}", warehouse.warehouse_id),
            condition: None,
        };
        authorizer
            .client
            .write(Some(vec![stale_forward.clone()]), None)
            .await
            .unwrap();

        let state_before = read_all_tuples(&authorizer.client).await;

        let state = pg_state(&pool);
        let lock = PostgresAdvisoryLock::try_acquire(&state, RECONCILE_LOCK_KEY)
            .await
            .expect("acquire lock")
            .expect("lock free");
        let report = reconcile_hierarchy_tuples_from_catalog::<PostgresBackend>(
            state,
            lock,
            &authorizer.client,
            server_id,
            ReconcileMode::AddMissingAndDeleteDrift,
            true, // dry_run
        )
        .await
        .unwrap();

        assert!(report.dry_run, "report must mark itself as a dry run");
        assert!(
            report.tuples_deleted >= 1,
            "dry run should still account for what it would delete; report={report:?}"
        );

        let state_after = read_all_tuples(&authorizer.client).await;
        assert_eq!(
            state_before, state_after,
            "dry run must not mutate the OpenFGA store"
        );
    }

    // ---- Generic-table regressions --------------------------------------

    #[sqlx::test]
    async fn test_rebuild_restores_missing_generic_table_parent_edge(pool: sqlx::PgPool) {
        use std::collections::HashMap;

        use lakekeeper::{
            api::{
                data::v1::generic_tables::{
                    CreateGenericTableRequest, GenericTableService as _, ListGenericTablesQuery,
                },
                iceberg::v1::namespace::NamespaceParameters,
            },
            service::{GenericTableFormat, GenericTableId},
        };

        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();

        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool.clone())
            .authorizer(authorizer.clone())
            .user_id(Some(operator_id.clone()))
            .build()
            .setup()
            .await;

        let ns_name = "ns_gt".to_string();
        let create_ns = CatalogServer::create_namespace(
            Some(Prefix::from(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::from_vec(vec![ns_name.clone()]).unwrap(),
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::test_user(operator_id.clone()),
        )
        .await
        .unwrap();
        let ns_id = NamespaceId::from_str_or_internal(
            create_ns
                .properties
                .as_ref()
                .unwrap()
                .get(NAMESPACE_ID_PROPERTY)
                .unwrap(),
        )
        .unwrap();

        // Create generic table via the full server path so authz tuples are
        // written through `authorizer.create_generic_table`.
        CatalogServer::create_generic_table(
            NamespaceParameters {
                prefix: Some(Prefix::from(warehouse.warehouse_id.to_string())),
                namespace: NamespaceIdent::from_vec(vec![ns_name.clone()]).unwrap(),
            },
            CreateGenericTableRequest {
                name: "my_gt".to_string(),
                format: GenericTableFormat::Unknown("lance".to_string()),
                base_location: None,
                doc: None,
                properties: HashMap::default(),
                schema: None,
                statistics: None,
            },
            ctx.clone(),
            RequestMetadata::test_user(operator_id.clone()),
        )
        .await
        .unwrap();

        // The id isn't returned from create; list to discover it.
        let listed = CatalogServer::list_generic_tables(
            NamespaceParameters {
                prefix: Some(Prefix::from(warehouse.warehouse_id.to_string())),
                namespace: NamespaceIdent::from_vec(vec![ns_name.clone()]).unwrap(),
            },
            ListGenericTablesQuery::default(),
            ctx.clone(),
            RequestMetadata::test_user(operator_id.clone()),
        )
        .await
        .unwrap();
        let gt_id: GenericTableId = listed
            .identifiers
            .iter()
            .find(|i| i.name == "my_gt")
            .and_then(|i| i.id)
            .expect("generic table id present in list response");

        // Delete one hierarchy edge — rebuild must restore it.
        let parent_edge =
            crate::tuples::hierarchy_tuples_for_generic_table(warehouse.warehouse_id, gt_id, ns_id)
                .into_iter()
                .next()
                .unwrap();
        authorizer
            .client
            .write(
                None,
                Some(vec![TupleKeyWithoutCondition {
                    user: parent_edge.user.clone(),
                    relation: parent_edge.relation.clone(),
                    object: parent_edge.object.clone(),
                }]),
            )
            .await
            .unwrap();
        let state_after_delete = read_all_tuples(&authorizer.client).await;
        assert!(!state_after_delete.contains(&ident(&parent_edge)));

        rebuild_hierarchy_tuples_from_catalog::<PostgresBackend>(
            pg_state(&pool),
            &authorizer.client,
            server_id,
            false,
        )
        .await
        .unwrap();

        let state_after_rebuild = read_all_tuples(&authorizer.client).await;
        assert!(
            state_after_rebuild.contains(&ident(&parent_edge)),
            "rebuild must restore the generic-table parent edge"
        );
    }

    #[sqlx::test]
    async fn test_reconcile_deletes_drifted_generic_table_parent_edge(pool: sqlx::PgPool) {
        let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
        let (_svc_client, authorizer) = authorizer_for_empty_store().await;
        let server_id = authorizer.server_id();
        let (warehouse, root_ns_id, _child, _role) =
            populate(&authorizer, &pool, &operator_id).await;

        // Inject a stale namespace→generic_table parent edge with a generic_table
        // id that does not exist in the catalog.
        let bogus_gt_uuid = Uuid::now_v7();
        let stale_forward = TupleKey {
            user: format!("namespace:{root_ns_id}"),
            relation: "parent".to_string(),
            object: format!(
                "lakekeeper_generic_table:{}/{bogus_gt_uuid}",
                warehouse.warehouse_id
            ),
            condition: None,
        };
        let stale_inverse = TupleKey {
            user: stale_forward.object.clone(),
            relation: "child".to_string(),
            object: stale_forward.user.clone(),
            condition: None,
        };
        authorizer
            .client
            .write(
                Some(vec![stale_forward.clone(), stale_inverse.clone()]),
                None,
            )
            .await
            .unwrap();

        let state_before = read_all_tuples(&authorizer.client).await;
        assert!(state_before.contains(&ident(&stale_forward)));
        assert!(state_before.contains(&ident(&stale_inverse)));

        let state = pg_state(&pool);
        let lock = PostgresAdvisoryLock::try_acquire(&state, RECONCILE_LOCK_KEY)
            .await
            .expect("acquire lock")
            .expect("lock free");
        let report = reconcile_hierarchy_tuples_from_catalog::<PostgresBackend>(
            state,
            lock,
            &authorizer.client,
            server_id,
            ReconcileMode::AddMissingAndDeleteDrift,
            false,
        )
        .await
        .unwrap();

        assert!(
            report.tuples_deleted >= 2,
            "expected both forward and inverse stale generic-table edges to be deleted; report={report:?}"
        );

        let state_after = read_all_tuples(&authorizer.client).await;
        assert!(
            !state_after.contains(&ident(&stale_forward)),
            "stale forward generic-table edge must be deleted"
        );
        assert!(
            !state_after.contains(&ident(&stale_inverse)),
            "stale inverse generic-table edge must be deleted"
        );
    }
}
