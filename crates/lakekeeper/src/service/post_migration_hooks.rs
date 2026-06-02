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
/// [`crate::tests::upsert_system_roles_in_all_projects`], gated on the
/// `test-utils` feature.
pub(crate) async fn upsert_system_roles_in_all_projects<C: CatalogStore>(
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

    let cap = SystemRoleSeederCap::new();
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

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    use super::*;
    use crate::{
        ProjectId,
        implementations::postgres::{CatalogState, PostgresBackend, PostgresTransaction},
        service::{RoleSourceId, SYSTEM_ROLE_PROVIDER_ID},
    };

    fn spec(source_id: &str, name: &'static str, description: &'static str) -> SystemRoleSpec {
        SystemRoleSpec {
            source_id: source_id.parse::<RoleSourceId>().unwrap(),
            name,
            description,
        }
    }

    /// Read every system role for `project_id`. Returns
    /// `(source_id, name, description, version)` tuples ordered by
    /// `source_id` — same shape the original raw query returned, so
    /// existing assertions don't need to change.
    async fn list_system_roles(
        pool: &PgPool,
        project_id: &ProjectId,
    ) -> Vec<(String, String, Option<String>, i64)> {
        use crate::{
            api::iceberg::v1::PageToken, implementations::postgres::role::list_roles,
            service::CatalogListRolesByIdFilter,
        };
        let provider = &*SYSTEM_ROLE_PROVIDER_ID;
        let providers = [provider];
        let filter = CatalogListRolesByIdFilter::builder()
            .provider_ids(Some(&providers))
            .build();
        let mut roles = list_roles(
            Some(project_id),
            filter,
            crate::api::iceberg::v1::PaginationQuery {
                page_size: Some(100),
                page_token: PageToken::Empty,
            },
            pool,
        )
        .await
        .unwrap()
        .roles;
        roles.sort_by(|a, b| {
            a.ident
                .source_id()
                .as_str()
                .cmp(b.ident.source_id().as_str())
        });
        roles
            .into_iter()
            .map(|r| {
                (
                    r.ident.source_id().as_str().to_string(),
                    r.name.clone(),
                    r.description.clone(),
                    *r.version,
                )
            })
            .collect()
    }

    #[sqlx::test]
    async fn test_upsert_system_roles_in_all_projects_inserts_then_refreshes(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        // Three projects, none with system roles yet.
        let p1 = ProjectId::new_random();
        let p2 = ProjectId::new_random();
        let p3 = ProjectId::new_random();
        for pid in &[&p1, &p2, &p3] {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresBackend::create_project(pid, format!("Project {pid}"), t.transaction())
                .await
                .unwrap();
            t.commit().await.unwrap();
        }
        for pid in &[&p1, &p2, &p3] {
            assert!(list_system_roles(&pool, pid).await.is_empty());
        }

        // First backfill: all three projects get both specs.
        let specs = vec![
            spec("admin_role", "Admin", "Admin description"),
            spec("user_role", "User", "User description"),
        ];
        upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &specs)
            .await
            .unwrap();

        for pid in &[&p1, &p2, &p3] {
            let rows = list_system_roles(&pool, pid).await;
            assert_eq!(rows.len(), 2, "project {pid} should have 2 system roles");
            assert_eq!(rows[0].0, "admin_role");
            assert_eq!(rows[0].1, "Admin");
            assert_eq!(rows[0].2.as_deref(), Some("Admin description"));
            assert_eq!(rows[0].3, 0);
            assert_eq!(rows[1].0, "user_role");
        }

        // Second backfill with the SAME specs is a no-op via IS DISTINCT
        // FROM — no row's version bumps.
        upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &specs)
            .await
            .unwrap();
        for pid in &[&p1, &p2, &p3] {
            let rows = list_system_roles(&pool, pid).await;
            for row in &rows {
                assert_eq!(row.3, 0, "version must not bump on no-op upsert");
            }
        }

        // Third backfill with an updated description for one spec refreshes
        // every project's matching row; the other spec stays unchanged.
        let refreshed_specs = vec![
            spec("admin_role", "Admin", "Updated admin description"),
            spec("user_role", "User", "User description"),
        ];
        upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &refreshed_specs)
            .await
            .unwrap();
        for pid in &[&p1, &p2, &p3] {
            let rows = list_system_roles(&pool, pid).await;
            assert_eq!(rows.len(), 2);
            // admin_role got new description, version bumps
            assert_eq!(rows[0].0, "admin_role");
            assert_eq!(rows[0].2.as_deref(), Some("Updated admin description"));
            assert_eq!(rows[0].3, 1, "admin version bumps after description change");
            // user_role unchanged, version stays at 0
            assert_eq!(rows[1].0, "user_role");
            assert_eq!(rows[1].2.as_deref(), Some("User description"));
            assert_eq!(rows[1].3, 0, "user version unchanged");
        }
    }

    #[sqlx::test]
    async fn test_upsert_system_roles_in_all_projects_with_empty_specs_is_noop(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let p1 = ProjectId::new_random();
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_project(&p1, "Empty-Backfill".to_string(), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Empty specs — no rows inserted, no error.
        upsert_system_roles_in_all_projects::<PostgresBackend>(state.clone(), &[])
            .await
            .unwrap();
        assert!(list_system_roles(&pool, &p1).await.is_empty());
    }

    /// Locks in the OSS no-impact contract: `run_post_migration_hooks`
    /// with an empty `Vec` returns `Ok` AND seeds no system-role rows in
    /// any existing project. We create a project first so the assertion
    /// catches a hypothetical regression where the hook silently mutates
    /// data on the OSS path.
    ///
    /// Note: the `OnceLock` registry may have been populated by a prior
    /// test in this process; the empty install is rejected and ignored.
    /// If the prior install had specs, backfill against this fresh DB's
    /// project would seed those — so the assertion would fail under
    /// registry pollution. In the current test binary that's not the
    /// case (the only tests installing specs use the cap-gated helper
    /// directly, not `run_post_migration_hooks`).
    #[sqlx::test]
    async fn test_run_post_migration_hooks_oss_no_registry_is_ok(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let p1 = ProjectId::new_random();
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_project(&p1, "OSS-NoOp".to_string(), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        run_post_migration_hooks::<PostgresBackend>(state, Vec::new())
            .await
            .unwrap();

        assert!(
            list_system_roles(&pool, &p1).await.is_empty(),
            "OSS path must not create any system-role rows"
        );
    }

    /// Backfill is now fatal: a failure inside
    /// [`upsert_system_roles_in_all_projects`] propagates via `?`.  We
    /// inject the failure by closing the pool before the helper starts,
    /// which causes the first `begin_write` to return `PoolClosed`.
    ///
    /// This indirectly proves the new `?` in `run_post_migration_hooks`
    /// fires: that wrapper differs from the old one only in dropping the
    /// `if let Err(e) = ...` swallow in favour of `?`, so propagation here
    /// implies propagation there.
    ///
    /// **Why not test `run_post_migration_hooks` directly?** That would
    /// require installing the `OnceLock` registry with a non-empty spec
    /// list, which leaks into every other test in the same binary (the
    /// installer is process-wide and rejects the second install). Either
    /// every sibling test must accept the polluted registry, or this
    /// would need its own integration-test binary. The indirect test
    /// here was chosen as the lighter trade-off — re-introducing the
    /// `Err` swallow in `run_post_migration_hooks` is a code-review
    /// concern, guarded by the documented `?` at line 37 and by the
    /// commit-message convention.
    #[sqlx::test]
    async fn test_upsert_system_roles_in_all_projects_propagates_errors(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        pool.close().await;

        let specs = vec![spec("svc_admin", "Service Admin", "X")];
        let result = upsert_system_roles_in_all_projects::<PostgresBackend>(state, &specs).await;
        assert!(
            result.is_err(),
            "closed pool must propagate as Err, got: {result:?}"
        );
    }
}
