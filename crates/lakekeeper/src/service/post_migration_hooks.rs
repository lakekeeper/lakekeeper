use anyhow::Context;

use crate::service::{
    CatalogNamespaceOps, CatalogRoleOps, CatalogStore, CatalogTabularOps, SystemRoleSeederCap,
    SystemRoleSpec, Transaction, install_system_role_registry, registered_system_roles,
    tasks::task_log_cleanup_queue,
};

/// Which conditional post-migration hooks to run.
///
/// Hooks that must happen once per upgrade rather than on every startup cannot decide that for
/// themselves — only the caller that ran the migrations knows what this run applied. It passes the
/// answer here. Backend-specific knowledge (which migration version gates what) stays in the
/// binary, so this stays generic over the catalog store.
///
/// Every gate on this struct must guard a hook that is **idempotent and safe to retry**, because
/// `lakekeeper migrate --force-idempotent-post-migration-hooks` turns them all on at once to recover
/// from an earlier failure. A hook that cannot be re-run does not belong behind one of these flags.
#[derive(Debug, Default, Clone, Copy)]
pub struct PostMigrationHookOptions {
    /// Repair stored namespace paths that carry the caller's casing instead of the referenced
    /// row's: namespace path prefixes and `tabular`'s denormalised copy of its namespace path.
    /// Set when the migration the repair is pinned to was applied by this run — see
    /// `lakekeeper-storage-postgres`'s `NAMESPACE_PATH_CASING_REPAIR_AFTER`.
    pub repair_namespace_path_casing: bool,
    /// Treat a failure of any gated hook above, or of creating missing task log cleanup
    /// schedules, as fatal instead of logging it.
    ///
    /// Off for a normal migration: a transient failure should not block an upgrade, since these
    /// hooks repair or backfill rather than gatekeep. On when an operator asked for the hooks
    /// explicitly (`--force-idempotent-post-migration-hooks`), because someone who requested a
    /// repair needs to be told it did not happen rather than having to find it in the logs.
    pub fail_on_idempotent_hook_error: bool,
}

/// Runs post-migration housekeeping. `system_roles` is the spec set the
/// binary wants installed in the registry for this process — pass an
/// empty `Vec` for OSS (no system roles seeded); downstream binaries
/// pass their full list. Installation is logged
/// and is a no-op-with-error if the registry was already set in this
/// process; the failure is non-fatal (logged, startup continues).
pub async fn run_post_migration_hooks<C: CatalogStore>(
    state: C::State,
    system_roles: Vec<SystemRoleSpec>,
    options: PostMigrationHookOptions,
) -> anyhow::Result<()> {
    if let Err(rejected) = install_system_role_registry(system_roles) {
        // Already installed in this process. Surfaced by the installer's
        // own ERROR log; don't escalate here.
        let _ = rejected;
    }
    if let Err(e) = task_log_cleanup_queue::sync_project_schedules::<C>(state.clone()).await {
        // Not fatal by default: a pending cleanup task from an older server also creates its
        // project's schedule.
        let e = anyhow::anyhow!(e).context(
            "Task log cleanup schedules were not synced. This hook is idempotent and safe to \
             retry: re-run `migrate --force-idempotent-post-migration-hooks`.",
        );
        if options.fail_on_idempotent_hook_error {
            return Err(e);
        }
        tracing::error!("{e:?}");
    }
    if options.repair_namespace_path_casing
        && let Err(e) = repair_namespace_path_casing::<C>(state.clone()).await
    {
        // Not fatal by default: the catalog serves correct results either way, only cache hit
        // rate suffers, and a blip should not block an upgrade. But this hook is gated on the
        // migration that introduced it, so it will not run again by itself once that migration
        // is recorded — say how to retry it, or the drift is silently permanent.
        let e = e.context(
            "Namespace path prefix casing was not repaired. This hook is idempotent and safe to \
             retry: re-run `migrate --force-idempotent-post-migration-hooks`.",
        );
        if options.fail_on_idempotent_hook_error {
            return Err(e);
        }
        tracing::error!("{e:?}");
    }
    backfill_registered_system_roles::<C>(state)
        .await
        .with_context(
            || "Failed to backfill registered catalog-managed system roles in post-migration hook",
        )?;
    Ok(())
}

/// Bring stored namespace paths in line with the row they reference.
///
/// Two places hold a namespace path that is not the namespace's own name: a namespace's path prefix,
/// which must carry its parent's stored spelling, and `tabular`'s denormalised copy of its
/// containing namespace's path. `create_namespace` and `rename_tabular` used to store the caller's
/// spelling instead. A wrong prefix leaves the namespace — and its whole subtree — permanently
/// unservable from the namespace cache; a wrong tabular copy makes the tabular report a path that
/// disagrees with the namespace it sits in. The write paths no longer allow either; this repairs
/// rows that predate the fixes.
///
/// Namespaces are repaired first, so that the tabular copies adopt already corrected paths.
///
/// Gated by the caller on the migration it is pinned to having just been applied, so it runs once per
/// upgrade rather than on every startup. Still written to be idempotent and to derive what needs
/// repairing from the data, because that is what makes re-pinning it to a later migration enough to
/// re-run it if another write path is ever found to store a caller-cased path.
async fn repair_namespace_path_casing<C: CatalogStore>(state: C::State) -> anyhow::Result<()> {
    let mut t = C::Transaction::begin_write(state)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to begin write transaction"))?;
    let repaired = C::repair_namespace_path_casing(t.transaction())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to repair namespace path prefix casing"))?;
    let repaired_tabulars = C::repair_tabular_namespace_path_casing(t.transaction())
        .await
        .map_err(|e| {
            anyhow::anyhow!(e).context("Failed to repair tabular namespace path casing")
        })?;
    t.commit()
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to commit namespace path casing repair"))?;
    if repaired > 0 {
        tracing::info!(
            "Post-migration hook: repaired the stored path of {repaired} namespace(s) whose prefix \
             casing disagreed with their parent"
        );
    }
    if repaired_tabulars > 0 {
        tracing::info!(
            "Post-migration hook: repaired the stored namespace path of {repaired_tabulars} \
             tabular(s) whose casing disagreed with their namespace"
        );
    }
    Ok(())
}

/// Upsert every existing project with the catalog-managed system roles
/// in the process-wide registry (see
/// [`crate::service::install_system_role_registry`]). New projects pick the
/// rows up via the `create_project` code path; this hook covers existing
/// projects and also refreshes `name` / `description` of previously-seeded
/// rows when the registry's specs change between releases.
///
/// No-op if no extension has registered any specs (OSS default).
async fn backfill_registered_system_roles<C: CatalogStore>(state: C::State) -> anyhow::Result<()> {
    upsert_system_roles_in_all_projects::<C>(state, registered_system_roles()).await
}

/// Inner loop of [`backfill_registered_system_roles`], parameterized on
/// `roles` so tests can drive it with an explicit fixture instead of the
/// process-wide registry (whose `OnceLock` would pollute other tests in
/// the same binary).
///
/// `pub(crate)` for production use by [`backfill_registered_system_roles`].
/// Downstream test crates reach this via the `pub` wrapper exported from
/// [`lakekeeper_storage_postgres::tests::upsert_system_roles_in_all_projects`], gated on the
/// `test-utils` feature.
#[allow(unreachable_pub)] // re-exported via `pub use` in service/mod.rs for downstream test crates
pub async fn upsert_system_roles_in_all_projects<C: CatalogStore>(
    state: C::State,
    roles: &[SystemRoleSpec],
) -> anyhow::Result<()> {
    if roles.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Post-migration hook: backfilling {} registered system role(s) per project",
        roles.len()
    );

    let mut t = C::Transaction::begin_write(state)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to begin write transaction"))?;

    let projects = C::list_projects(None, t.transaction())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to list projects"))?;

    let cap = SystemRoleSeederCap::for_storage_backend_seeding();
    let mut total_upserted = 0usize;

    for project in &projects {
        let upserted = C::upsert_system_roles(&project.project_id, roles, cap, t.transaction())
            .await
            .map_err(|e| {
                anyhow::anyhow!(e).context(format!(
                    "Failed to seed registered system roles for project {}",
                    project.project_id,
                ))
            })?;
        total_upserted += upserted.len();
    }

    t.commit().await.map_err(|e| {
        anyhow::anyhow!(e).context("Failed to commit system role backfill transaction")
    })?;

    tracing::info!(
        "System role backfill complete: {total_upserted} row(s) inserted or refreshed \
         across {} project(s) ({} role(s) unchanged)",
        projects.len(),
        projects.len() * roles.len() - total_upserted,
    );
    Ok(())
}
