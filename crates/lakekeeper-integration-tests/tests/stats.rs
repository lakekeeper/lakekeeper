use lakekeeper::{
    api::{ApiContext, management::v1::warehouse::TabularDeleteProfile},
    service::{State, UserId, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::TestWarehouseResponse;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

mod test {
    use std::time::Duration;

    use lakekeeper::api::{
        iceberg::types::PageToken,
        management::v1::{ApiServer, GetWarehouseStatisticsQuery, warehouse::Service},
    };
    use lakekeeper_integration_tests::{random_request_metadata, spawn_build_in_queues};
    use sqlx::PgPool;
    use uuid::Uuid;

    // The stats trigger truncates `now()` to the configured interval unit
    // ('second' under test, see `configure_trigger`) and pushes the previous
    // counts to `warehouse_statistics_history` only when a write crosses that
    // boundary. Setup operations (create_table / create_view) may straddle a
    // second on slow CI runners, so the number of history rows present after
    // setup is not deterministic. This test therefore asserts on relative
    // deltas (length grew by exactly 1 across a boundary-crossing action,
    // 0 across an in-bucket action) and on the latest-by-timestamp row's
    // counts, rather than absolute counts.
    #[sqlx::test]
    async fn test_stats_task_produces_correct_values(pool: PgPool) {
        let setup = super::setup_stats_test(pool, 1, 1).await;

        let cancellation_token = lakekeeper::CancellationToken::new();
        let queues_handle =
            spawn_build_in_queues(&setup.ctx, None, cancellation_token.clone()).await;
        let whi = setup.warehouse.warehouse_id;

        let get_stats = async || {
            ApiServer::get_warehouse_statistics(
                whi,
                GetWarehouseStatisticsQuery {
                    page_token: PageToken::NotSpecified,
                    page_size: None,
                },
                setup.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap()
        };

        // Baseline after setup: there is at least one row (the current bucket).
        // Setup may have produced extra history rows depending on wall-clock
        // straddling; capture whatever len() is, do not assume 1.
        let baseline = get_stats().await;
        assert!(!baseline.stats.is_empty());
        assert_eq!(baseline.warehouse_ident, *whi);
        let baseline_current = baseline.stats.first().unwrap();
        assert_eq!(baseline_current.number_of_tables, 1);
        assert_eq!(baseline_current.number_of_views, 1);
        let baseline_len = baseline.stats.len();
        let baseline_current_ts = baseline_current.timestamp;
        let baseline_current_updated_at = baseline_current.updated_at;

        // Sleep past the next-second boundary so the next write is guaranteed
        // to land in a fresh bucket and push a history row.
        let tn = Uuid::now_v7().to_string();
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let _ = lakekeeper_integration_tests::create_table(
            setup.ctx.clone(),
            &setup.warehouse.warehouse_id.to_string(),
            setup.namespace_name.as_str(),
            &tn,
            false,
        )
        .await
        .unwrap();
        tracing::info!("created table {}", tn);

        let after_add = get_stats().await;
        assert_eq!(
            after_add.stats.len(),
            baseline_len + 1,
            "expected exactly one new history row from the boundary-crossing write; baseline={baseline:?}, after_add={after_add:?}",
        );
        assert_eq!(after_add.warehouse_ident, *whi);
        let after_add_current = after_add.stats.first().unwrap();
        assert!(
            after_add_current.timestamp > baseline_current_ts,
            "newest stats timestamp should advance; after_add={after_add:?}",
        );
        assert_eq!(after_add_current.number_of_tables, 2);
        assert_eq!(after_add_current.number_of_views, 1);
        // The row that was the current bucket at baseline is now in history
        // at the same timestamp, frozen at the pre-add counts.
        let frozen = after_add
            .stats
            .iter()
            .find(|s| s.timestamp == baseline_current_ts)
            .expect("baseline's current row should now appear in history");
        assert_eq!(frozen.number_of_tables, 1);
        assert_eq!(frozen.number_of_views, 1);

        // Drop the table. We do not sleep; depending on how fast the test
        // runs the drop may land in the same second-bucket as the add (in
        // which case the current row is updated in place) or in the next
        // bucket (in which case the trigger pushes a history row). Both are
        // valid outcomes — assert only that the latest row reflects the
        // post-drop counts and that the post-add row is preserved with its
        // counts at the moment of the drop.
        lakekeeper_integration_tests::drop_table(
            setup.ctx.clone(),
            setup.warehouse.warehouse_id.to_string().as_str(),
            setup.namespace_name.as_str(),
            tn.as_str(),
            None,
            false,
        )
        .await
        .unwrap();

        let after_drop = get_stats().await;
        assert_eq!(after_drop.warehouse_ident, *whi);
        assert!(
            after_drop.stats.len() >= after_add.stats.len(),
            "drop must not lose history rows; after_add={after_add:?}, after_drop={after_drop:?}",
        );
        let after_drop_current = after_drop.stats.first().unwrap();
        assert!(
            after_drop_current.updated_at > baseline_current_updated_at,
            "current row should have been touched; after_drop={after_drop:?}",
        );
        assert_eq!(after_drop_current.number_of_tables, 1);
        assert_eq!(after_drop_current.number_of_views, 1);

        cancellation_token.cancel();
        queues_handle.await.unwrap();
    }
}

// TODO: test with multiple warehouses and projects

struct StatsSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
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
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init()
        .ok();
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let (ctx, warehouse) = lakekeeper_integration_tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
        None,
    )
    .await;
    // Must run AFTER `setup` — the latter applies migrations which
    // `CREATE OR REPLACE` the function back to `'hour'`. The original
    // (inline-crate) test was protected by sqlx::test auto-migrating
    // from `crates/lakekeeper/migrations/` before the test body ran;
    // the integration-tests crate has no `./migrations` dir so we have
    // to flip the order.
    configure_trigger(&pool).await;

    let ns_name = "ns1";

    let _ = lakekeeper_integration_tests::create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        ns_name.to_string(),
    )
    .await;
    for i in 0..n_tabs {
        let tab_name = format!("tab{i}");

        let _ = lakekeeper_integration_tests::create_table(
            ctx.clone(),
            &warehouse.warehouse_id.to_string(),
            ns_name,
            &tab_name,
            false,
        )
        .await
        .unwrap();
    }

    for i in 0..n_views {
        let view_name = format!("view{i}");
        lakekeeper_integration_tests::create_view(
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
