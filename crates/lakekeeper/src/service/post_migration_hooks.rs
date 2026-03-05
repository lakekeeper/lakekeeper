use std::collections::HashSet;

use crate::{
    CONFIG,
    api::management::v1::tasks::{ListTasksRequest, TaskStatus},
    service::{
        CatalogStore, CatalogTaskOps, Transaction,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, TaskFilter,
            task_log_cleanup_queue::{self, TaskLogCleanupPayload, TaskLogCleanupTask},
        },
    },
};

pub async fn run_post_migration_hooks<C: CatalogStore>(state: C::State) -> anyhow::Result<()> {
    if let Err(e) = initialize_cron_tasks::<C>(state).await {
        // This is a non-critical hook, so we log the error but do not fail the migration.
        tracing::error!("Failed to initialize cron tasks in post-migration hook: {e:?}");
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
