use crate::api::management::v1::task::{ListTasksQuery, ListTasksRequest, TaskService};
use crate::api::management::v1::warehouse::{CreateWarehouseResponse, TabularDeleteProfile};
use crate::api::ApiContext;
use crate::implementations::postgres::{PostgresCatalog, SecretsState};
use crate::service::authz::AllowAllAuthorizer;
use crate::service::task_queue::TaskQueueConfig;
use crate::service::{State, UserId};
use crate::tests::random_request_metadata;
use crate::DEFAULT_PROJECT_ID;
use sqlx::PgPool;
use std::str::FromStr;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

mod test {
    use crate::api::management::v1::task::TaskService;
    use crate::api::management::v1::warehouse::Service;
    use crate::api::management::v1::ApiServer;
    use crate::tests::{random_request_metadata, spawn_drop_queues};
    use sqlx::PgPool;
    use std::str::FromStr;
    use std::time::Duration;
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_warehouse_creation_creates_stats_task(pool: PgPool) {
        let setup = super::setup_stats_test(
            pool,
            1,
            1,
            chrono::Duration::seconds(60),
            Duration::from_secs(1),
            None,
        )
        .await;
        let whi = setup.warehouse.warehouse_id;
        let tasks = setup
            .ctx
            .list_tasks(
                random_request_metadata(),
                crate::api::management::v1::task::ListTasksRequest {
                    project_ident: None,
                },
                crate::api::management::v1::task::ListTasksQuery {
                    page_token: None,
                    page_size: 100,
                },
            )
            .await
            .unwrap();

        let t = tasks
            .tasks
            .into_iter()
            .find(|t| t.queue_name == "stats")
            .unwrap();
        assert!(t.schedule.is_some());
        assert_eq!(
            t.details
                .unwrap()
                .get("statistics_task")
                .unwrap()
                .get("task_data")
                .unwrap()
                .get("warehouse_id")
                .unwrap(),
            &serde_json::Value::String(whi.to_string())
        );
        let tasks_instances = setup
            .ctx
            .list_task_instances(
                random_request_metadata(),
                crate::api::management::v1::task::ListTaskInstancesQuery {
                    task_id: Some(t.task_id),
                    page_token: None,
                    page_size: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(tasks_instances.tasks.len(), 1);
        assert_eq!(tasks_instances.tasks[0].task_id, t.task_id);
    }

    #[sqlx::test]
    async fn test_stats_task_produces_correct_values(pool: PgPool) {
        let setup = super::setup_stats_test(
            pool,
            1,
            1,
            chrono::Duration::seconds(60),
            Duration::from_millis(500),
            // Update every second
            Some(cron::Schedule::from_str("0/1 * * * * *").unwrap()),
        )
        .await;
        spawn_drop_queues(&setup.ctx);
        let whi = setup.warehouse.warehouse_id;
        tokio::time::sleep(Duration::from_millis(1250)).await;
        let stats =
            ApiServer::get_warehouse_stats(whi, setup.ctx.clone(), random_request_metadata())
                .await
                .unwrap();
        // ideally we'd have a single stat here, but it turns out to be quite flaky so we're going for
        // more than one.. longer intervals could help but we don't want to block tests so long
        assert!(!stats.stats.is_empty());
        assert_eq!(stats.warehouse_ident, *whi);
        let stats = stats.stats.into_iter().next().unwrap();
        assert_eq!(stats.number_of_tables, 1);
        assert_eq!(stats.number_of_views, 1);

        let _ = crate::tests::create_table(
            setup.ctx.clone(),
            &setup.warehouse.warehouse_id.to_string(),
            setup.namespace_name.as_str(),
            &Uuid::now_v7().to_string(),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(1100)).await;
        let stats =
            ApiServer::get_warehouse_stats(whi, setup.ctx.clone(), random_request_metadata())
                .await
                .unwrap();
        assert!(stats.stats.len() > 1);
        assert_eq!(stats.warehouse_ident, *whi);
        assert!(stats.stats[0].taken_at > stats.stats[1].taken_at);
        assert_eq!(stats.stats.first().unwrap().number_of_tables, 2);
        assert_eq!(stats.stats.first().unwrap().number_of_views, 1);
        assert_eq!(stats.stats.last().unwrap().number_of_tables, 1);
        assert_eq!(stats.stats.last().unwrap().number_of_views, 1);
    }
}

// TODO: test with multiple warehouses and projects

struct StatsSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    warehouse: CreateWarehouseResponse,
    namespace_name: String,
}

async fn setup_stats_test(
    pool: PgPool,
    n_tabs: usize,
    n_views: usize,
    expiration_seconds: chrono::Duration,
    poll_interval: std::time::Duration,
    schedule: Option<cron::Schedule>,
) -> StatsSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .try_init()
        .ok();
    let prof = crate::tests::test_io_profile();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer,
        TabularDeleteProfile::Soft { expiration_seconds },
        Some(UserId::OIDC("test-user-id".to_string())),
        Some(TaskQueueConfig {
            max_retries: 1,
            max_age: chrono::Duration::seconds(60),
            poll_interval,
        }),
        // every 10 seconds
        schedule.or(Some(cron::Schedule::from_str("0/10 * * * * *").unwrap())),
    )
    .await;

    let tasks = ctx
        .list_tasks(
            random_request_metadata(),
            ListTasksRequest {
                project_ident: None,
            },
            ListTasksQuery {
                page_token: None,
                page_size: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        tasks
            .tasks
            .into_iter()
            .filter(|t| t.queue_name == "stats" && t.project_id == DEFAULT_PROJECT_ID.unwrap())
            .count(),
        1
    );

    let ns_name = "ns1";

    let _ = crate::tests::create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        ns_name.to_string(),
    )
    .await;
    for i in 0..n_tabs {
        let tab_name = format!("tab{i}");

        let _ = crate::tests::create_table(
            ctx.clone(),
            &warehouse.warehouse_id.to_string(),
            ns_name,
            &tab_name,
        )
        .await
        .unwrap();
    }

    for i in 0..n_views {
        let view_name = format!("view{i}");
        crate::tests::create_view(
            ctx.clone(),
            &warehouse.warehouse_id.to_string(),
            ns_name,
            &view_name,
            None,
        )
        .await
        .unwrap();
    }

    StatsSetup {
        ctx,
        warehouse,
        namespace_name: ns_name.to_string(),
    }
}
