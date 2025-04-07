use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{management::v1::warehouse::TabularDeleteProfile, ApiContext},
    implementations::postgres::{PostgresCatalog, SecretsState},
    service::{authz::AllowAllAuthorizer, task_queue::TaskQueueConfig, State, UserId},
    tests::TestWarehouseResponse,
};

mod test {
    use std::time::Duration;

    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        api::{
            iceberg::types::PageToken,
            management::v1::{warehouse::Service, ApiServer, GetWarehouseStatisticsQuery},
        },
        tests::{random_request_metadata, spawn_drop_queues},
    };

    #[sqlx::test]
    async fn test_stats_task_produces_correct_values(pool: PgPool) {
        let setup = super::setup_stats_test(pool, 1, 1).await;
        spawn_drop_queues(&setup.ctx);
        let whi = setup.warehouse.warehouse_id;
        let stats = ApiServer::get_warehouse_statistics(
            whi,
            GetWarehouseStatisticsQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            setup.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert!(!stats.stats.is_empty());
        assert_eq!(stats.warehouse_ident, *whi);
        let stats = stats.stats.into_iter().next().unwrap();
        assert_eq!(stats.number_of_tables, 1);
        assert_eq!(stats.number_of_views, 1);
        let tn = Uuid::now_v7().to_string();
        // sleep one second so that we get a new stats entry
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let _ = crate::tests::create_table(
            setup.ctx.clone(),
            &setup.warehouse.warehouse_id.to_string(),
            setup.namespace_name.as_str(),
            &tn,
        )
        .await
        .unwrap();

        tracing::info!("created table {}", tn);

        let stats = ApiServer::get_warehouse_statistics(
            whi,
            GetWarehouseStatisticsQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            setup.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(stats.stats.len(), 2);
        assert_eq!(stats.warehouse_ident, *whi);
        assert!(
            stats.stats[0].timestamp > stats.stats[1].timestamp,
            "{stats:?}",
        );
        assert_eq!(stats.stats.first().unwrap().number_of_tables, 2);
        assert_eq!(stats.stats.first().unwrap().number_of_views, 1);
        assert_eq!(stats.stats.last().unwrap().number_of_tables, 1);
        assert_eq!(stats.stats.last().unwrap().number_of_views, 1);
        tracing::info!("dropping table {}", tn);
        super::super::drop_table(
            setup.ctx.clone(),
            setup.warehouse.warehouse_id.to_string().as_str(),
            setup.namespace_name.as_str(),
            tn.as_str(),
            None,
        )
        .await
        .unwrap();
        tracing::info!("dropped table {}", tn);

        let new_stats = ApiServer::get_warehouse_statistics(
            whi,
            GetWarehouseStatisticsQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
            },
            setup.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(new_stats.stats.len(), 2);
        assert_eq!(new_stats.warehouse_ident, *whi);
        assert!(
            new_stats.stats[0].updated_at > stats.stats[0].updated_at,
            "new stats: {new_stats:?}, old stats: {stats:?}"
        );
        assert_eq!(new_stats.stats.first().unwrap().number_of_tables, 1);
        assert_eq!(new_stats.stats.first().unwrap().number_of_views, 1);
        assert_eq!(new_stats.stats.last().unwrap().number_of_tables, 1);
        assert_eq!(new_stats.stats.last().unwrap().number_of_views, 1);
    }
}

// TODO: test with multiple warehouses and projects

struct StatsSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    warehouse: TestWarehouseResponse,
    namespace_name: String,
}

async fn configure_trigger(pool: &PgPool) {
    sqlx::query!(
        r#"CREATE OR REPLACE FUNCTION get_stats_interval_unit() RETURNS text AS
        $$
        BEGIN
            RETURN 'second';
        END;
        $$ LANGUAGE plpgsql;"#
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn setup_stats_test(pool: PgPool, n_tabs: usize, n_views: usize) -> StatsSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .try_init()
        .ok();
    configure_trigger(&pool).await;
    let prof = crate::tests::test_io_profile();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer,
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        Some(TaskQueueConfig {
            max_retries: 1,
            max_age: chrono::Duration::seconds(60),
            poll_interval: std::time::Duration::from_secs(10),
        }),
        1,
    )
    .await;

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
