use std::collections::HashSet;

use anyhow::Context;

use crate::{
    CONFIG,
    api::management::v1::tasks::{ListTasksRequest, TaskStatus},
    service::{
        CatalogRoleOps, CatalogStore, CatalogTaskOps, SystemRoleSeederCap, SystemRoleSpec,
        Transaction, install_system_role_registry, registered_system_roles,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, TaskFilter,
            task_log_cleanup_queue::{self, TaskLogCleanupPayload, TaskLogCleanupTask},
        },
    },
};

/// Runs post-migration housekeeping. `system_roles` is the spec set the
/// binary wants installed in the registry for this process — pass an
/// empty `Vec` for OSS (no system roles seeded); downstream binaries
/// pass their full list. Installation is logged
/// and is a no-op-with-error if the registry was already set in this
/// process; the failure is non-fatal (logged, startup continues).
pub async fn run_post_migration_hooks<C: CatalogStore>(
    state: C::State,
    system_roles: Vec<SystemRoleSpec>,
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
    backfill_registered_system_roles::<C>(state)
        .await
        .with_context(
            || "Failed to backfill registered catalog-managed system roles in post-migration hook",
        )?;
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

        if response.next_page_token.is_none() {
            break;
        }
        page_token = response.next_page_token;
    }

    Ok(project_ids)
}
