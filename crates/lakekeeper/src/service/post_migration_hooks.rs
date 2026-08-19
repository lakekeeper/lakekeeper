use std::collections::HashSet;

use anyhow::Context;

use crate::{
    CONFIG,
    api::management::v1::tasks::{ListTasksRequest, TaskStatus},
    service::{
        CatalogNamespaceOps, CatalogRoleOps, CatalogStore, CatalogTaskOps, SystemRoleSeederCap,
        SystemRoleSpec, Transaction, install_system_role_registry, registered_system_roles,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, TaskFilter,
            task_log_cleanup_queue::{self, TaskLogCleanupPayload, TaskLogCleanupTask},
        },
    },
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
    /// Repair namespace path prefixes stored with the caller's casing instead of the parent's.
    /// Set when the migration the repair is pinned to was applied by this run — see
    /// `lakekeeper-storage-postgres`'s `NAMESPACE_PATH_CASING_REPAIR_AFTER`.
    pub repair_namespace_path_casing: bool,
    /// Treat a failure of any gated hook above as fatal instead of logging it.
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
    if let Err(e) = initialize_cron_tasks::<C>(state.clone()).await {
        // This is a non-critical hook, so we log the error but do not fail the migration.
        tracing::error!("Failed to initialize cron tasks in post-migration hook: {e:?}");
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

/// Bring namespace path prefixes in line with their parent rows' spelling.
///
/// A namespace's path prefix references its parent, so it must carry the parent's stored spelling.
/// `create_namespace` used to store the caller's spelling instead, which left the row — and its whole
/// subtree — permanently unservable from the namespace cache. The write paths no longer allow it;
/// this repairs rows that predate the fix.
///
/// Gated by the caller on the migration it is pinned to having just been applied, so it runs once per
/// upgrade rather than on every startup. Still written to be idempotent and to derive what needs
/// repairing from the data, because that is what makes re-pinning it to a later migration enough to
/// re-run it if another write path is ever found to store a caller-cased prefix.
async fn repair_namespace_path_casing<C: CatalogStore>(state: C::State) -> anyhow::Result<()> {
    let mut t = C::Transaction::begin_write(state)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to begin write transaction"))?;
    let repaired = C::repair_namespace_path_casing(t.transaction())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to repair namespace path prefix casing"))?;
    t.commit()
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to commit namespace path casing repair"))?;
    if repaired > 0 {
        tracing::info!(
            "Post-migration hook: repaired the stored path of {repaired} namespace(s) whose prefix \
             casing disagreed with their parent"
        );
    }
    Ok(())
}

async fn initialize_cron_tasks<C: CatalogStore>(state: C::State) -> anyhow::Result<()> {
    // Schedule Task Log Cleanup for all projects that don't have it yet.
    tracing::info!(
        "Post-migration hook: initializing task log cleanup cron tasks for all projects"
    );
    let mut t = C::Transaction::begin_write(state)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to begin write transaction"))?;
    let projects = C::list_projects(None, t.transaction())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to list projects"))?;
    // ToDo: Paginate
    let scheduled_project_ids =
        get_scheduled_project_ids::<C>(&task_log_cleanup_queue::QUEUE_NAME, &mut t).await?;
    let projects_to_schedule = projects
        .iter()
        .filter(|project| !scheduled_project_ids.contains(&project.project_id))
        .collect::<Vec<_>>();
    if projects_to_schedule.is_empty() {
        tracing::info!("All projects already have task log cleanup tasks scheduled.");
        return Ok(());
    }

    let n_to_schedule = projects_to_schedule.len();
    tracing::info!("Scheduling task log cleanup tasks for {n_to_schedule} projects",);
    for project in projects_to_schedule {
        let project_id = project.project_id.clone();
        TaskLogCleanupTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id,
                parent_task_id: None,
                scheduled_for: None,
                entity: TaskEntity::Project,
            },
            TaskLogCleanupPayload::new(),
            t.transaction(),
        )
        .await
        .map_err(|e| {
            e.append_detail(format!(
                "Failed to queue next `{}` task.",
                task_log_cleanup_queue::QUEUE_NAME.as_str(),
            ))
        })?;
    }
    t.commit().await.map_err(|e| {
        anyhow::anyhow!(e).context("Failed to commit transaction scheduling task log cleanup tasks")
    })?;
    tracing::info!("Successfully scheduled task log cleanup tasks for {n_to_schedule} projects",);

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

async fn get_scheduled_project_ids<C: CatalogStore>(
    queue_name: &crate::service::tasks::TaskQueueName,
    transaction: &mut <C as CatalogStore>::Transaction,
) -> anyhow::Result<HashSet<crate::service::ArcProjectId>> {
    const MAX_ITERATIONS: usize = 100;

    let mut project_ids = HashSet::new();
    let mut page_token = None;
    let mut iterations = 0;

    loop {
        if iterations >= MAX_ITERATIONS {
            tracing::warn!(
                "Reached maximum pagination iterations ({MAX_ITERATIONS}) while listing scheduled tasks"
            );
            break;
        }
        iterations += 1;

        let response = C::list_tasks(
            &TaskFilter::All,
            &ListTasksRequest::builder()
                .status(Some(vec![TaskStatus::Scheduled, TaskStatus::Running]))
                .queue_name(Some(vec![queue_name.clone()]))
                .page_size(Some(CONFIG.pagination_size_max.into()))
                .page_token(page_token)
                .build(),
            transaction.transaction(),
        )
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Failed to list existing scheduled tasks"))?;

        project_ids.extend(
            response
                .tasks
                .iter()
                .map(|task| task.task_metadata.project_id().clone()),
        );

        if !has_more_task_pages(
            response.tasks.is_empty(),
            response.next_page_token.as_deref(),
        ) {
            break;
        }
        page_token = response.next_page_token;
    }

    Ok(project_ids)
}

/// Whether [`get_scheduled_project_ids`] should request another page given the
/// page just returned by `list_tasks`.
///
/// `list_tasks` echoes the supplied page token back even when a page comes back
/// empty (so a caller can resume the same cursor later), so the end of the
/// result set is signalled by an empty page — not by a missing token.
fn has_more_task_pages(page_was_empty: bool, next_page_token: Option<&str>) -> bool {
    !page_was_empty && next_page_token.is_some()
}

#[cfg(test)]
mod tests {
    use super::has_more_task_pages;

    #[test]
    fn stops_paginating_on_empty_terminal_page() {
        // A non-empty page with a continuation token: keep going.
        assert!(has_more_task_pages(false, Some("token")));

        // `list_tasks` echoes the supplied token back on an empty terminal page,
        // so an empty page must stop pagination even though a token is present.
        assert!(!has_more_task_pages(true, Some("token")));

        // No token: stop regardless of whether the page had rows.
        assert!(!has_more_task_pages(false, None));
        assert!(!has_more_task_pages(true, None));
    }
}
